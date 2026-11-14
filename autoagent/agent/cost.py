"""Decimal text-token estimates using input, output, and cached-input rates only."""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import importlib.util
import json
from pathlib import Path
from types import MappingProxyType
from typing import Protocol, TYPE_CHECKING, runtime_checkable

from autoagent.core.money import calculated_units

if TYPE_CHECKING:
    from .recording import LLMCallRecord

PRICE_FIELDS = ("input_cost_per_token", "output_cost_per_token", "cache_read_input_token_cost")
_call_prices = ContextVar("autoagent_call_prices", default=None)


@contextmanager
def token_pricing_context(pricing):
    """Freeze selected connection/model prices for one Provider call."""
    token = _call_prices.set(pricing)
    try:
        yield
    finally:
        _call_prices.reset(token)


def current_token_pricing():
    return _call_prices.get()


@dataclass(frozen=True, slots=True)
class TokenPrice:
    """Explicit USD prices per million tokens; omitted cached-input prices are zero."""
    input_per_million_usd: str
    output_per_million_usd: str
    cached_input_per_million_usd: str | None = None

    def __post_init__(self):
        for value in (self.input_per_million_usd, self.output_per_million_usd, self.cached_input_per_million_usd):
            if value is None:
                continue
            if not isinstance(value, str):
                raise TypeError("Prices must be decimal strings.")
            try:
                amount = Decimal(value)
            except InvalidOperation as error:
                raise ValueError("Prices must be decimal strings.") from error
            if not amount.is_finite() or amount < 0:
                raise ValueError("Prices must be finite and nonnegative.")

    def rates(self):
        return dict(zip(PRICE_FIELDS, (Decimal(self.input_per_million_usd) / 1_000_000,
            Decimal(self.output_per_million_usd) / 1_000_000,
            Decimal(self.cached_input_per_million_usd or "0") / 1_000_000)))


@dataclass(frozen=True, slots=True)
class CostEstimate:
    units: int | None
    metadata: dict


@runtime_checkable
class CostCalculator(Protocol):
    def calculate(self, record: LLMCallRecord) -> CostEstimate: ...


def load_litellm_catalog():
    """Read the installed package's catalog without importing it or making HTTP calls."""
    spec = importlib.util.find_spec("litellm")
    if spec is None or not spec.submodule_search_locations:
        return {}, "unavailable"
    return _load_catalog(Path(next(iter(spec.submodule_search_locations))) / "model_prices_and_context_window_backup.json")


def _load_catalog(path):
    raw = Path(path).read_bytes()
    data = json.loads(raw, parse_float=Decimal)
    if not isinstance(data, dict):
        raise ValueError("A price catalog must be a model-to-rate dictionary.")
    return data, hashlib.sha256(raw).hexdigest()


class LocalTokenPricing:
    """Three-rate pricing, with zero for missing rates and no implicit SDK catalog lookup."""
    def __init__(self, *, overrides=None, catalog_path=None, use_litellm_catalog=False):
        overrides = dict(overrides or {})
        if any(not isinstance(key, str) or not key.strip() or not isinstance(value, TokenPrice)
               for key, value in overrides.items()):
            raise TypeError("Price overrides map model names to TokenPrice.")
        self.overrides = MappingProxyType(overrides)
        self.catalog_path = None if catalog_path is None else str(Path(catalog_path).resolve())
        self.use_litellm_catalog = use_litellm_catalog
        self._catalog = self._version = None

    def _load(self):
        if self._catalog is None:
            self._catalog, self._version = (_load_catalog(self.catalog_path) if self.catalog_path else
                load_litellm_catalog() if self.use_litellm_catalog else ({}, "unavailable"))

    @property
    def catalog_info(self):
        self._load()
        return dict(source="local", version=self._version, model_count=len(self._catalog))

    def calculate(self, record):
        snapshot = record.billing_details.get("token_pricing")
        if snapshot is not None:
            entry, source, version = snapshot["rates"], snapshot["source"], snapshot["version"]
            key = record.requested_model
        else:
            provider = record.billing_details.get("pricing_provider") or record.provider_id
            candidates = tuple(dict.fromkeys(key for model in (record.requested_model, record.response_model) if model
                for key in (f"{provider}/{model}", model)))
            key = next((key for key in candidates if key in self.overrides), None)
            if key is not None:
                entry, source = self.overrides[key].rates(), "configured"
                version = hashlib.sha256(json.dumps(entry, sort_keys=True, default=str).encode()).hexdigest()
            else:
                self._load()
                key = next((key for key in candidates if key in self._catalog), None)
                entry = self._catalog.get(key, {})
                source, version = "local_catalog" if key else "zero_default", self._version
        rates = {name: Decimal(str(entry.get(name, 0))) for name in PRICE_FIELDS}
        if any(not value.is_finite() or value < 0 for value in rates.values()):
            raise ValueError("Prices must be finite and nonnegative.")
        cached = record.cache_read_tokens or 0
        input_count = record.input_tokens or 0
        usage = record.billing_details.get("usage", {})
        if cached == 0:
            cached = usage.get("cache_read_input_tokens", 0) or 0
        # Anthropic-style usage excludes cache reads/writes from input_tokens.
        if record.billing_details.get("input_excludes_cache"):
            input_count += cached + (usage.get("cache_creation_input_tokens", 0) or 0)
        plain = max(0, input_count - cached)
        parts = dict(input=(plain, rates[PRICE_FIELDS[0]]), cached_input=(cached, rates[PRICE_FIELDS[2]]),
            output=(record.output_tokens or 0, rates[PRICE_FIELDS[1]]))
        with localcontext() as context:
            context.prec = 60
            units = calculated_units(sum((Decimal(count) * rate for count, rate in parts.values()), Decimal(0)))
        return CostEstimate(units, dict(currency="USD", source=source, pricing_key=key, price_version=version,
            rounding="ceil_1e-12_usd", usage_reported=record.usage_reported,
            components={name: dict(tokens=count, usd_per_token=str(rate)) for name, (count, rate) in parts.items()}))


__all__ = ["TokenPrice", "CostEstimate", "CostCalculator", "LocalTokenPricing"]
