"""Application Provider connections; SDK and Host capability defaults stay unchanged."""

from contextlib import asynccontextmanager
import asyncio
import json
from typing import Literal
from uuid import uuid4
from urllib.parse import urlsplit, parse_qsl

from pydantic import Field, field_validator

from autoagent.agent.models._base import Model, NonEmpty
from .credentials import CredentialReference, CredentialUnavailable


def connection_url(value):
    if value is None:
        return value
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("Connection URL requires HTTP or HTTPS.")
    if parsed.username or parsed.password or any(key.lower() in {"token", "api_key", "key", "access_token", "secret"} for key, _ in parse_qsl(parsed.query)):
        raise ValueError("Use credential references rather than URL credentials.")
    return value


class ApplicationProviderConfig(Model):
    name: NonEmpty
    backend: Literal["openai", "litellm"] = "openai"
    provider_id: NonEmpty = "openai"
    protocol: Literal["openai.chat_completions", "openai.responses"] = "openai.chat_completions"
    base_url: str | None = None
    model_prefix: NonEmpty | None = None
    api_key: CredentialReference | None = None
    headers: dict[str, CredentialReference] = Field(default_factory=dict)
    timeout_seconds: float = Field(default=120, gt=0, allow_inf_nan=False)
    models: tuple[NonEmpty, ...] = ()
    enabled: bool = True

    _url = field_validator("base_url")(connection_url)

    @field_validator("models", mode="before")
    @classmethod
    def _models(cls, value):
        return tuple(value) if isinstance(value, list) else value


class _Provider:
    """Create transport clients on the Runtime loop and redact transport failures."""
    def __init__(self, config, credentials):
        self.config, self.credentials = config, credentials
        self.provider_id = config.provider_id
        self.protocol = config.protocol
        self.client = None
        self._secrets = ()

    def _get(self):
        if self.client is None:
            from autoagent.agent import (OpenAIProviderConfig, OpenAIChatCompletionsProvider, OpenAIResponsesProvider,
                LiteLLMProvider, LiteLLMProviderConfig, DeepSeekProvider, DeepSeekProviderConfig)
            key = self.credentials.resolve(self.config.api_key) if self.config.api_key else None
            options = dict(provider_id=self.config.provider_id, timeout_seconds=self.config.timeout_seconds,
                # A nonempty placeholder prevents SDKs from silently borrowing
                # unrelated process-global Provider keys for a no-auth endpoint.
                api_key=key if key is not None else 'autoagent-no-auth',
                headers={key:self.credentials.resolve(ref) for key, ref in self.config.headers.items()})
            self._secrets = tuple(value for value in (key, *options['headers'].values()) if value)
            if self.config.base_url is not None:
                options['base_url'] = self.config.base_url
            if self.config.backend == 'litellm':
                self.client = self._make_transport(LiteLLMProvider, LiteLLMProviderConfig(**options, model_prefix=self.config.model_prefix), protocol=self.protocol)
            elif self.config.provider_id == 'deepseek':
                self.client = self._make_transport(DeepSeekProvider, DeepSeekProviderConfig(**options), protocol=self.protocol)
            else:
                kind = OpenAIResponsesProvider if self.protocol == 'openai.responses' else OpenAIChatCompletionsProvider
                self.client = self._make_transport(kind, OpenAIProviderConfig(**options))
        return self.client

    def _make_transport(self, kind, configuration, **options):
        owner = self
        class Transport(kind):
            def _error(self, error):
                # Redact before the SDK Provider writes its usage/error record.
                return owner._safe_error(super()._error(error))
        return Transport(configuration, **options)

    def _safe_error(self, error):
        from autoagent.agent import LLMProviderError, LLMStructuredOutputError, LLMUnsupportedRequest
        from autoagent.core.usage import UsageAccountingError
        if isinstance(error, (CredentialUnavailable, LLMStructuredOutputError, LLMUnsupportedRequest, UsageAccountingError, ImportError)):
            return error
        message = str(error)
        from urllib.parse import quote
        for secret in sorted(self._secrets, key=len, reverse=True):
            for value in (secret, json.dumps(secret)[1:-1], quote(secret, safe='')):
                message = message.replace(value, '[REDACTED]')
        return LLMProviderError(message or 'Provider request failed; check the connection configuration.',
            provider_id=self.provider_id, protocol=self.protocol,
            status_code=getattr(error, 'status_code', None), retryable=getattr(error, 'retryable', None))

    async def ainvoke(self, request):
        try:
            return await self._get().ainvoke(request)
        except Exception as error:
            raise self._safe_error(error) from None

    async def astream(self, request):
        source = None
        try:
            source = self._get().astream(request)
            async for chunk in source:
                yield chunk
        except Exception as error:
            raise self._safe_error(error) from None
        finally:
            if source is not None:
                await source.aclose()

    async def aclose(self):
        if self.client is not None:
            await self.client.aclose()


class ApplicationProviders:
    def __init__(self, app):
        self.app = app
        self.records = {}
        self.instances = []
        self.lock = asyncio.Lock()
        self.active = False

    @asynccontextmanager
    async def lifespan(self):
        for row in await self.app.storage.context('provider_records'):
            self._publish(row['id'], row)
        self.active = True
        try:
            yield
        finally:
            self.active = False
            await asyncio.gather(*(value.aclose() for value in self.instances))
            self.instances.clear()

    def _publish(self, provider_id, row):
        registry = self.app.context.resources
        alias = 'app:' + provider_id
        if row is None:
            self.records.pop(provider_id, None)
            if any(entry.name == alias for entry in registry.entries('provider', include_disabled=True)):
                registry.set_enabled('provider', alias, False)
            return
        config = ApplicationProviderConfig.model_validate_json(json.dumps(row['configuration']))
        self.records[provider_id] = (row, config)
        provider = _Provider(config, self.app.credentials)
        self.instances.append(provider)
        registry.register_provider(alias, provider, version=str(row['version']) + ':' + uuid4().hex,
            replace=any(entry.name == alias for entry in registry.entries('provider', include_disabled=True)))
        if not config.enabled:
            registry.set_enabled('provider', alias, False)

    async def describe(self):
        async def describe():
            items = [dict(id=key, alias='app:'+key, version=row['version'], configuration=config.model_dump(mode='json'), editable=True)
                for key, (row, config) in self.records.items()]
            items.extend(dict(id=entry.name, alias=entry.name, version=entry.version, editable=False,
                configuration=dict(name=entry.name, enabled=True, models=[])) for entry in self.app.context.resources.entries('provider')
                if not entry.name.startswith('app:'))
            return dict(items=items)
        return await self.app.host._perform(lambda: self.app.context.approvals._run(describe))

    async def save(self, config, *, provider_id=None, expected_version=0):
        if not self.active:
            raise ValueError('Agent application Provider management is not active.')
        if config is not None and not isinstance(config, ApplicationProviderConfig):
            config = ApplicationProviderConfig.model_validate_json(json.dumps(config))
        provider_id = provider_id or uuid4().hex
        if not provider_id or not all(char.isalnum() or char in '_-' for char in provider_id):
            raise ValueError('Provider ID must contain letters, digits, underscores or hyphens.')
        async def update():
            async with self.lock:
                row = await self.app.storage.context('save_provider', provider_id,
                    config.model_dump(mode='json') if config else None, expected_version)
                self._publish(provider_id, row)
                return dict(id=provider_id, alias='app:'+provider_id, version=row['version'], configuration=row['configuration']) if row else None
        return await self.app.host._perform(lambda: self.app.context.approvals._run(update), control=True)


__all__ = ['ApplicationProviderConfig']
