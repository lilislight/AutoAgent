"""Project Provider/model catalog, loaded atomically from JSON on the Runtime loop."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, contextmanager
from collections.abc import AsyncIterator
from decimal import Decimal
from dataclasses import dataclass
import hashlib
import json
import logging
from pathlib import Path
import re
from typing import Literal
from urllib.parse import parse_qsl, urlsplit, quote

from pydantic import Field, SecretStr, ValidationError, field_validator, model_validator

from autoagent.agent.models._base import Model, NonEmpty
from autoagent.agent.context import AgentModelSelection
from autoagent.agent.models import LLMRequest, LLMStreamChunk
from autoagent.agent.cost import PRICE_FIELDS, load_litellm_catalog, token_pricing_context

logger = logging.getLogger(__name__)


def connection_url(value):
    if value is None:
        return value
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("Connection URL requires HTTP or HTTPS.")
    if parsed.username or parsed.password or any(key.lower() in {"token", "api_key", "key", "access_token", "secret"}
            for key, _ in parse_qsl(parsed.query)):
        raise ValueError("Use api_key rather than URL credentials.")
    return value


class ModelInfo(Model):
    max_input_tokens: int | None = Field(default=None, gt=0)
    max_output_tokens: int | None = Field(default=None, gt=0)
    input_cost_per_token: Decimal | None = Field(default=None, ge=0, allow_inf_nan=False)
    output_cost_per_token: Decimal | None = Field(default=None, ge=0, allow_inf_nan=False)
    cache_read_input_token_cost: Decimal | None = Field(default=None, ge=0, allow_inf_nan=False)

    @field_validator(*PRICE_FIELDS, mode="before")
    @classmethod
    def _decimal(cls, value):
        if value is None or isinstance(value, Decimal):
            return value
        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            raise ValueError("Prices must be decimal numbers.")
        try:
            return Decimal(str(value))
        except Exception:
            raise ValueError("Prices must be decimal numbers.") from None


class ReasoningEffort(Model):
    values: tuple[NonEmpty, ...] = Field(min_length=1)
    default: NonEmpty

    @field_validator("values", mode="before")
    @classmethod
    def _values(cls, value):
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def _default(self):
        if len(set(self.values)) != len(self.values) or self.default not in self.values:
            raise ValueError("Reasoning values must be unique and include the default.")
        return self


class ProviderModel(Model):
    id: NonEmpty
    name: NonEmpty | None = None
    enabled: bool = True
    url: str | None = None
    reasoning_effort: ReasoningEffort | None = None
    model_info: ModelInfo = Field(default_factory=ModelInfo)
    _url = field_validator("url")(connection_url)


class ProviderConfig(Model):
    enabled: bool = True
    backend: Literal["openai", "litellm"] = "openai"
    protocol: Literal["openai.chat_completions", "openai.responses"] = "openai.chat_completions"
    base_url: str | None = None
    api_key: SecretStr | None = Field(default=None, repr=False)
    timeout_seconds: float = Field(default=120, gt=0, allow_inf_nan=False)
    models: tuple[ProviderModel, ...] = ()
    _url = field_validator("base_url")(connection_url)

    @field_validator("models", mode="before")
    @classmethod
    def _models(cls, value):
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def _unique(self):
        if len({model.id for model in self.models}) != len(self.models):
            raise ValueError("Model IDs must be unique within each Provider.")
        return self


class ProvidersFile(Model):
    default_model: AgentModelSelection | None = None
    compaction_model: AgentModelSelection | None = None
    providers: dict[NonEmpty, ProviderConfig] = Field(default_factory=dict)


class CatalogProvider:
    """Borrow lazily constructed SDK transports and pin prices for each actual call."""
    def __init__(self, provider_id, config, models, version):
        self.provider_id, self.config = provider_id, config
        self.protocol = config.protocol
        self.models, self.version = models, version
        self.clients = {}
        self._secrets = (config.api_key.get_secret_value(),) if config.api_key else ()
        self._active = 0
        self._idle = asyncio.Event()
        self._idle.set()
        self._retired = False

    def _get(self, model):
        if model not in self.models or not self.models[model][0].enabled:
            raise ValueError(f"Model is unavailable in Provider {self.provider_id!r}: {model!r}.")
        declared, info = self.models[model]
        url = declared.url or self.config.base_url
        if url not in self.clients:
            from autoagent.agent import (OpenAIProviderConfig, OpenAIChatCompletionsProvider, OpenAIResponsesProvider,
                LiteLLMProvider, LiteLLMProviderConfig, DeepSeekProvider, DeepSeekProviderConfig)
            options = dict(provider_id=self.provider_id, timeout_seconds=self.config.timeout_seconds,
                api_key=self.config.api_key or "autoagent-no-auth")
            if url is not None:
                options["base_url"] = url
            if self.config.backend == "litellm":
                client = self._make_transport(LiteLLMProvider, LiteLLMProviderConfig(**options), protocol=self.protocol)
            elif self.provider_id == "deepseek":
                client = self._make_transport(DeepSeekProvider, DeepSeekProviderConfig(**options), protocol=self.protocol)
            else:
                kind = OpenAIResponsesProvider if self.protocol == "openai.responses" else OpenAIChatCompletionsProvider
                client = self._make_transport(kind, OpenAIProviderConfig(**options))
            self.clients[url] = client
        pricing = dict(rates={key: str(info.get(key, 0)) for key in PRICE_FIELDS},
            source="json" if self.config.backend == "openai" else "litellm_local_with_json_overrides", version=self.version)
        return self.clients[url], pricing

    def _make_transport(self, kind, configuration, **options):
        owner = self
        class Transport(kind):
            def _error(self, error):
                return owner._safe_error(super()._error(error))
        return Transport(configuration, **options)

    def supports_tool_media(self, model: str) -> bool:
        """Expose the selected transport's media contract before composing history."""
        entry = self.models.get(model)
        if entry is None or not entry[0].enabled:
            return False
        transport, _ = self._get(model)
        return getattr(getattr(transport, "adapter", None), "tool_media", False)

    def _safe_error(self, error):
        from autoagent.agent import LLMProviderError, LLMStructuredOutputError, LLMUnsupportedRequest
        from autoagent.core.usage import UsageAccountingError
        if isinstance(error, (LLMStructuredOutputError, LLMUnsupportedRequest, UsageAccountingError, ImportError)):
            return error
        message = str(error)
        for secret in sorted(self._secrets, key=len, reverse=True):
            for value in (secret, json.dumps(secret)[1:-1], quote(secret, safe="")):
                message = message.replace(value, "[REDACTED]")
        return LLMProviderError(message or "Provider request failed.", provider_id=self.provider_id,
            protocol=self.protocol, status_code=getattr(error, "status_code", None), retryable=getattr(error, "retryable", None))

    @contextmanager
    def _borrow(self, request):
        if self._retired:
            raise ValueError(f"Provider configuration has changed: {self.provider_id!r}.")
        client, prices = self._get(request.model)
        self._active += 1
        self._idle.clear()
        try:
            with token_pricing_context(prices):
                yield client
        finally:
            self._active -= 1
            if not self._active:
                self._idle.set()

    async def ainvoke(self, request):
        with self._borrow(request) as client:
            request = self._request(request)
            try:
                return await client.ainvoke(request)
            except Exception as error:
                raise self._safe_error(error) from None

    async def astream(self, request):
        with self._borrow(request) as client:
            source = client.astream(self._request(request))
            try:
                async for chunk in source:
                    yield chunk
            except Exception as error:
                raise self._safe_error(error) from None
            finally:
                await source.aclose()

    def _request(self, request):
        effort = self.models[request.model][0].reasoning_effort
        if request.reasoning_effort is None and effort is not None and not request.routing.get("reasoning_effort_bound"):
            return request.model_copy(update={"reasoning_effort": effort.default})
        return request

    async def aclose(self):
        self._retired = True
        await self._idle.wait()
        clients, self.clients = self.clients, {}
        await asyncio.gather(*(client.aclose() for client in clients.values()))


@dataclass(frozen=True, slots=True)
class ProviderRegistration:
    name: str
    value: object
    version: str


class HostProviders:
    def __init__(self, host):
        self.host = host
        configured = host.environment.get("AUTOAGENT_PROVIDERS_FILE", "").strip()
        path = Path(configured).expanduser() if configured else Path("providers.json")
        self.path = (path if path.is_absolute() else host.root / path).resolve()
        self.explicit_path = bool(configured)
        self._registrations = {}
        self._provider_versions = {}
        self.records = {}
        self.instances = []
        self._draining = set()
        self.managed_ids = set()
        self.version = ""
        self.default_model = None
        self.compaction_model = None
        self._lock = asyncio.Lock()

    def _read(self):
        if not self.path.exists() and not self.explicit_path:
            return ProvidersFile(), {}, "empty"
        try:
            raw = self.path.read_bytes()
            data = ProvidersFile.model_validate_json(raw)
        except ValidationError as error:
            failures = [".".join(map(str, item["loc"])) + ": " + item["msg"]
                for item in error.errors(include_input=False, include_context=False, include_url=False)[:3]]
            raise ValueError(f"Invalid Provider configuration in {self.path}: " + "; ".join(failures)) from None
        except (OSError, ValueError):
            raise ValueError(f"Cannot load Provider configuration from {self.path}; check the file and JSON schema.") from None
        resolved = {}
        for name, config in data.providers.items():
            key = config.api_key.get_secret_value() if config.api_key else None
            if key is not None and (match := re.fullmatch(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", key)):
                key = self.host.environment.get(match[1])
                if config.enabled and not key:
                    raise ValueError(f"Provider {name!r} requires environment variable {match[1]!r}.")
                config = config.model_copy(update={"api_key": SecretStr(key) if key else None})
            resolved[name] = config
        # Include resolved credentials in the fingerprint, never in API responses.
        identity = hashlib.sha256(raw)
        for config in resolved.values():
            identity.update((config.api_key.get_secret_value() if config.api_key else "").encode())
        for selected in (data.default_model, data.compaction_model):
            if selected is None:
                continue
            provider = resolved.get(selected.provider)
            model = next((item for item in provider.models if item.id == selected.model and item.enabled), None) if provider and provider.enabled else None
            if model is None:
                raise ValueError("Default model must refer to an enabled configured Provider and model.")
            if selected.reasoning_effort is not None and (model.reasoning_effort is None or selected.reasoning_effort not in model.reasoning_effort.values):
                raise ValueError("Default reasoning effort must be supported by the selected model.")
        return data, resolved, identity.hexdigest()

    async def _reload(self):
        async with self._lock:
            data, configs, version = await asyncio.to_thread(self._read)
            if any(config.enabled and config.backend == "litellm" and any(model.enabled for model in config.models)
                   for config in configs.values()):
                from autoagent.agent.providers._litellm_sdk import load_litellm_sdk
                load_litellm_sdk()
            catalog, catalog_version = await asyncio.to_thread(load_litellm_catalog) if any(config.backend == "litellm" for config in configs.values()) else ({}, "none")
            version = hashlib.sha256((version + catalog_version).encode()).hexdigest()
            if version == self.version:
                return self._describe()
            staged = {}
            for name, config in configs.items():
                if name not in self.managed_ids and name in self._registrations:
                    raise ValueError(f"Provider {name!r} is already registered by project code.")
                models = {}
                for model in config.models:
                    base = catalog.get(model.id, {}) if config.backend == "litellm" else {}
                    info = {key: base[key] for key in (*PRICE_FIELDS, "max_input_tokens", "max_output_tokens")
                        if base.get(key) is not None}
                    info.update(model.model_info.model_dump(mode="python", exclude_none=True))
                    for key in PRICE_FIELDS:
                        info.setdefault(key, Decimal(0))
                    models[model.id] = (model, info)
                identity = hashlib.sha256(json.dumps(dict(configuration=config.model_dump(mode="json"),
                    models={key: info for key, (model, info) in models.items()}), sort_keys=True, default=str).encode())
                identity.update((config.api_key.get_secret_value() if config.api_key else "").encode())
                provider_version = identity.hexdigest()
                previous = self.records.get(name)
                staged[name] = previous if previous is not None and previous.version == provider_version else CatalogProvider(name, config, models, provider_version)
            for name in self.records.keys() - staged.keys():
                self._registrations.pop(name, None)
            for name, previous in self.records.items():
                if staged.get(name) is not previous:
                    self._provider_versions.pop((name, previous.version), None)
                    task = asyncio.create_task(self._retire(previous))
                    self._draining.add(task)
                    task.add_done_callback(self._draining.discard)
            for name, value in staged.items():
                if self.records.get(name) is value:
                    continue
                entry = ProviderRegistration(name, value, value.version)
                self._registrations[name] = entry
                self._provider_versions[name, entry.version] = entry
            self.instances = list(staged.values())
            self.managed_ids.update(staged)
            self.records, self.version = staged, version
            self.default_model, self.compaction_model = data.default_model, data.compaction_model
            return self._describe()

    @staticmethod
    async def _retire(provider):
        try:
            await provider.aclose()
        except Exception as error:
            logger.warning("Could not close retired Provider %s (%s).", provider.provider_id, type(error).__name__)

    def _describe(self):
        items = []
        for name, provider in self.records.items():
            config = provider.config
            items.append(dict(id=name, enabled=config.enabled, backend=config.backend, protocol=config.protocol,
                base_url=config.base_url, timeout_seconds=config.timeout_seconds, api_key_configured=bool(config.api_key),
                models=[dict(id=model.id, name=model.name or model.id, enabled=model.enabled,
                    url=model.url or config.base_url, reasoning_effort=model.reasoning_effort.model_dump(mode="json") if model.reasoning_effort else None,
                    model_info={key:str(value) if isinstance(value, Decimal) else value for key, value in info.items()})
                    for model, info in provider.models.values()]))
        return dict(items=items, path=str(self.path), version=self.version,
            default_model=self.default_model.model_dump(mode="json") if self.default_model else None,
            compaction_model=self.compaction_model.model_dump(mode="json") if self.compaction_model else None)

    def register(self, name, provider, *, version="1", replace=False):
        """Register a code-created LLM Provider without an Agent resource registry."""
        if not isinstance(name, str) or not name.strip() or not isinstance(version, str) or not version.strip():
            raise ValueError("Provider names and versions must be nonempty strings.")
        if any(not callable(getattr(provider, method, None)) for method in ("ainvoke", "astream")):
            raise TypeError("Provider must implement ainvoke and astream.")
        if name in self.managed_ids:
            raise ValueError("A JSON-managed Provider cannot be replaced by code registration.")
        if name in self._registrations and not replace:
            raise ValueError("Provider is already registered.")
        if (name, version) in self._provider_versions:
            raise ValueError("A Provider version cannot be rebound; use a new version.")
        entry = ProviderRegistration(name, provider, version)
        self._registrations[name] = entry
        self._provider_versions[name, version] = entry
        return entry

    def has(self, name):
        return name in self._registrations and (name not in self.managed_ids or
            name in self.records and self.records[name].config.enabled)

    def registration(self, name, *, version=None):
        if not self.has(name):
            raise KeyError(name)
        entry = self._registrations.get(name) if version is None else self._provider_versions.get((name, version))
        if entry is None:
            raise KeyError((name, version))
        return entry

    def get(self, name):
        if name not in self.managed_ids and name in self._registrations:
            return self._registrations[name].value
        value = self.records.get(name)
        if value is None or not value.config.enabled:
            raise ValueError(f"Provider is unavailable: {name!r}.")
        return value

    def model(self, provider, model):
        value = self.get(provider)
        if model not in value.models or not value.models[model][0].enabled:
            raise ValueError(f"Model is unavailable: {model!r}.")
        return value.models[model]

    async def describe(self):
        if self.host._read_only:
            return await self.host._perform(self._describe_async)
        return await self.host._perform(lambda: self.host.runtime_services.run(self._describe_async))

    async def _describe_async(self):
        return self._describe()

    async def reload(self):
        return await self.host._perform(lambda: self.host.runtime_services.run(self._reload), control=True)

    def install_operators(self, setup):
        from autoagent.agent import LLM_CALL, LLMSelection, llm_call_operator
        if not any(item.capability_id == LLM_CALL.id and item.enabled for item in setup.operators):
            def resolve(request):
                provider = request.routing.get("provider")
                if provider is None:
                    matches = [name for name, value in self.records.items() if value.config.enabled
                        and request.model in value.models and value.models[request.model][0].enabled]
                    if len(matches) != 1:
                        raise ValueError("Set routing.provider to select an unambiguous configured Provider.")
                    provider = matches[0]
                return LLMSelection(self.get(provider), request.model)
            adapter = llm_call_operator(resolver=resolve, id="host.llm.call")
            from autoagent.core import Operator
            async def invoke(request: LLMRequest) -> AsyncIterator[LLMStreamChunk]:
                selected = self.default_model
                if request.model is None and selected is not None and request.routing.get("provider") in (None, selected.provider):
                    request = request.model_copy(update={"model": selected.model,
                        "reasoning_effort": request.reasoning_effort or selected.reasoning_effort,
                        "routing": {**request.routing, "provider": selected.provider}})
                source = adapter.handler(request)
                try:
                    async for chunk in source:
                        yield chunk
                finally:
                    await source.aclose()
            setup.register_operator(Operator(invoke, id="host.llm.call"), capability_id=LLM_CALL.id, default=True)

    @asynccontextmanager
    async def lifespan(self, setup):
        await self._reload()
        try:
            yield
        finally:
            await asyncio.gather(*(value.aclose() for value in self.instances), *tuple(self._draining))
            self.instances.clear()
