"""Offline USD estimates from local rate cards, never provider credentials or HTTP.

The optional LiteLLM package supplies a populated catalog. Reading its bundled
JSON does not import the SDK or trigger its remote catalog loader. Unsupported
billing dimensions remain unknown instead of silently using text-token rates.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import importlib.util
import json
from pathlib import Path
import re
from types import MappingProxyType
from typing import Protocol, TYPE_CHECKING, runtime_checkable

from autoagent.core.money import calculated_units

if TYPE_CHECKING:
    from .recording import LLMCallRecord


@dataclass(frozen=True, slots=True)
class TokenPrice:
    """Application overrides in USD per million tokens, including explicit zero."""

    input_per_million_usd: str
    output_per_million_usd: str
    cached_input_per_million_usd: str | None = None
    cache_creation_per_million_usd: str | None = None
    reasoning_per_million_usd: str | None = None

    def __post_init__(self):
        for value in (
            self.input_per_million_usd, self.output_per_million_usd,
            self.cached_input_per_million_usd, self.cache_creation_per_million_usd, self.reasoning_per_million_usd,
        ):
            if value is not None:
                if not isinstance(value, str):
                    raise TypeError("Prices must be decimal strings.")
                try:
                    amount = Decimal(value)
                except InvalidOperation as error:
                    raise ValueError("Prices must be decimal strings.") from error
                if not amount.is_finite() or amount < 0:
                    raise ValueError("Prices must be finite and nonnegative.")

    def rates(self):
        values = dict(input_cost_per_token=self.input_per_million_usd,
                      output_cost_per_token=self.output_per_million_usd,
                      cache_read_input_token_cost=self.cached_input_per_million_usd,
                      cache_creation_input_token_cost=self.cache_creation_per_million_usd,
                      output_cost_per_reasoning_token=self.reasoning_per_million_usd)
        return {key: Decimal(value) / 1_000_000 for key, value in values.items() if value is not None}


@dataclass(frozen=True, slots=True)
class CostEstimate:
    units: int | None
    metadata: dict


@runtime_checkable
class CostCalculator(Protocol):
    def calculate(self, record: LLMCallRecord) -> CostEstimate: ...


def _load_catalog(path: str):
    raw = Path(path).read_bytes()
    data = json.loads(raw, parse_float=Decimal)
    if not isinstance(data, dict):
        raise ValueError("A price catalog must be a model-to-rate dictionary.")
    prices = {name: {key: value for key, value in entry.items() if "cost" in key or key in ("litellm_provider", "mode")}
              for name, entry in data.items() if isinstance(entry, dict)}
    return prices, hashlib.sha256(raw).hexdigest()


class LocalTokenPricing:
    """Pinned local catalog plus per-model application overrides.

    Custom calculators can implement CostCalculator for additional modalities or
    billable tools. Provider/model aliases may use an explicit pricing_provider.
    """

    def __init__(self, *, overrides=None, catalog_path: str | Path | None = None,
                 use_litellm_catalog: bool = True):
        overrides = dict(overrides or {})
        if any(not isinstance(key, str) or not key.strip() or not isinstance(value, TokenPrice)
               for key, value in overrides.items()):
            raise TypeError("Price overrides map model names to TokenPrice.")
        self.overrides = MappingProxyType(overrides)
        self.catalog_path = None if catalog_path is None else str(Path(catalog_path).resolve())
        self.use_litellm_catalog = use_litellm_catalog
        self._catalog = None
        self._version = None

    def _load(self):
        if self._catalog is not None:
            return
        path = self.catalog_path
        if path is None and self.use_litellm_catalog:
            spec = importlib.util.find_spec("litellm")
            if spec is not None and spec.submodule_search_locations:
                path = str(Path(next(iter(spec.submodule_search_locations))) / "model_prices_and_context_window_backup.json")
        if path is None:
            self._catalog, self._version = {}, "unavailable"
        else:
            self._catalog, self._version = _load_catalog(path)

    @property
    def catalog_info(self):
        self._load()
        return dict(source="local", version=self._version, model_count=len(self._catalog))

    def _lookup(self, record):
        provider = record.billing_details.get("pricing_provider") or record.provider_id
        models = tuple(dict.fromkeys(value for value in (record.response_model, record.requested_model) if value))
        candidates = tuple(dict.fromkeys(key for model in models for key in (f"{provider}/{model}", model)))
        for key in candidates:
            if key in self.overrides:
                rates = self.overrides[key].rates()
                revision = hashlib.sha256(json.dumps({key: str(value) for key, value in rates.items()}, sort_keys=True).encode()).hexdigest()
                return key, rates, "configured", revision
        self._load()
        for key in candidates:
            entry = self._catalog.get(key)
            if entry is None:
                continue
            catalog_provider = entry.get("litellm_provider")
            if key.startswith(f"{provider}/") or catalog_provider == provider or provider == "litellm":
                return key, entry, "litellm_local_catalog" if self.catalog_path is None else "local_catalog", self._version
        return None

    def calculate(self, record):
        found = self._lookup(record)
        if found is None:
            return CostEstimate(None, dict(currency="USD", reason="price_not_found"))
        key, entry, source, version = found
        meta = dict(currency="USD", source=source, pricing_key=key, price_version=version,
                    rounding="ceil_1e-12_usd")
        def unknown(reason):
            return CostEstimate(None, {**meta, "reason": reason})
        # Explicit zero overrides make local/free models zero even without usage.
        if source == "configured" and all(value == 0 for value in entry.values()):
            return CostEstimate(0, {**meta, "rates": {key: str(value) for key, value in entry.items()}})
        if record.input_tokens is None or record.kind == "generation" and record.output_tokens is None:
            return unknown("usage_missing")
        if record.billing_details.get("unsupported_modalities"):
            return unknown("modality_pricing_unsupported")
        if record.billing_details.get("billable_tools"):
            return unknown("tool_pricing_unsupported")
        if record.billing_details.get("unsupported_billing"):
            return unknown("billing_dimensions_unsupported")
        usage = record.billing_details.get("usage", {})
        tier = record.billing_details.get("service_tier")
        if tier not in (None, "default", "auto", "standard", "priority", "flex", "batch"):
            return unknown("service_tier_unsupported")
        suffix = {"priority": "_priority", "flex": "_flex", "batch": "_batches"}.get(tier, "")
        thresholds = [int(value) * 1000 for value in re.findall(r"input_cost_per_token_above_(\d+)k_tokens", " ".join(entry))]
        cache_read = record.cache_read_tokens if record.cache_read_tokens is not None else usage.get("cache_read_input_tokens")
        cache_write = usage.get("cache_creation_input_tokens")
        if cache_write is None:
            details = usage.get("prompt_tokens_details") or usage.get("input_tokens_details") or {}
            cache_write = details.get("cache_creation_tokens", details.get("cache_write_tokens"))
        if cache_write is None and isinstance(usage.get("cache_creation"), dict):
            cache_write = sum(value for value in usage["cache_creation"].values() if type(value) is int)
        for value in (cache_read, cache_write):
            if value is not None and (type(value) is not int or value < 0):
                return unknown("cache_usage_invalid")
        exclusive = record.billing_details.get("input_excludes_cache", False)
        input_tokens = record.input_tokens + ((cache_read or 0) + (cache_write or 0) if exclusive else 0)
        threshold = max((value for value in thresholds if input_tokens > value), default=None)
        def rate(name, fallback=None):
            base = name + suffix
            tiered = name + f"_above_{threshold // 1000}k_tokens" + suffix if threshold is not None else base
            if suffix and tiered not in entry and base not in entry and name in entry:
                return None
            value = entry.get(tiered, entry.get(base, fallback))
            if value is None:
                return None
            value = Decimal(str(value))
            return value if value.is_finite() and value >= 0 else None
        input_rate, output_rate = rate("input_cost_per_token"), rate("output_cost_per_token", Decimal(0) if record.kind == "embedding" else None)
        if input_rate is None or output_rate is None:
            return unknown("rate_missing")
        read_rate = rate("cache_read_input_token_cost", input_rate)
        write_rate = rate("cache_creation_input_token_cost", input_rate)
        reasoning_rate = rate("output_cost_per_reasoning_token", output_rate)
        if any(value is None for value in (read_rate, write_rate, reasoning_rate)):
            return unknown("rate_invalid")
        if cache_read is None and read_rate != input_rate:
            return unknown("cache_read_usage_missing")
        if cache_write is None and write_rate != input_rate:
            return unknown("cache_creation_usage_missing")
        if (cache_write or 0) and any("cache_creation_input_token_cost_above_" in name and "hr" in name for name in entry):
            return unknown("cache_duration_pricing_unsupported")
        if record.reasoning_tokens is None and reasoning_rate != output_rate:
            return unknown("reasoning_usage_missing")
        plain_input = input_tokens - (cache_read or 0) - (cache_write or 0)
        plain_output = (record.output_tokens or 0) - (record.reasoning_tokens or 0)
        if plain_input < 0 or plain_output < 0:
            return unknown("usage_categories_inconsistent")
        parts = dict(input=(plain_input, input_rate), cached_input=(cache_read or 0, read_rate),
                     cache_creation=(cache_write or 0, write_rate), output=(plain_output, output_rate),
                     reasoning=(record.reasoning_tokens or 0, reasoning_rate))
        with localcontext() as context:
            context.prec = 60
            amount = sum((Decimal(count) * price for count, price in parts.values()), Decimal(0))
            units = calculated_units(amount)
        meta["components"] = {name: dict(tokens=count, usd_per_token=str(price)) for name, (count, price) in parts.items()}
        return CostEstimate(units, meta)


__all__ = ["TokenPrice", "CostEstimate", "CostCalculator", "LocalTokenPricing"]
