"""Strict internal OAuth messages; decoding never infers client policy."""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import httpx

from ..token_provider import TokenProviderError, TokenSet

_SCOPE = re.compile(r"[\x21\x23-\x5b\x5d-\x7e]+(?: [\x21\x23-\x5b\x5d-\x7e]+)*")
_MAX_MILLIS = 9007199254740991


def endpoint(value: str | None) -> httpx.URL:
    if value is None:
        raise TokenProviderError()
    url = httpx.URL(value)
    if not url.is_absolute_url or url.userinfo or url.fragment:
        raise TokenProviderError()
    if url.scheme != "https" and not (url.scheme == "http" and url.host in {"localhost", "127.0.0.1", "::1"}):
        raise TokenProviderError()
    return url


def _document(body: str) -> dict[str, Any]:
    value = json.loads(body, parse_float=Decimal)
    if not isinstance(value, dict):
        raise TokenProviderError()
    return value


def _string(data: dict[str, Any], name: str, *, required: bool = False, allow_empty: bool = False) -> str | None:
    if name not in data:
        if required:
            raise TokenProviderError()
        return None
    value = data[name]
    if not isinstance(value, str) or (not allow_empty and not value):
        raise TokenProviderError()
    return value


@dataclass(frozen=True, slots=True, repr=False)
class DiscoveryMetadata:
    issuer: str
    token_url: str | None
    authorization_url: str | None
    methods: tuple[str, ...] | None

    @classmethod
    def parse(cls, body: str) -> DiscoveryMetadata:
        data = _document(body)
        issuer = _string(data, "issuer", required=True)
        assert issuer is not None
        token = _string(data, "token_endpoint")
        authorization = _string(data, "authorization_endpoint")
        if token is not None:
            endpoint(token)
        if authorization is not None:
            endpoint(authorization)
        for name in ("jwks_uri", "registration_endpoint", "revocation_endpoint", "introspection_endpoint"):
            value = _string(data, name)
            if value is not None:
                endpoint(value)
        methods = None
        if "token_endpoint_auth_methods_supported" in data:
            raw = data["token_endpoint_auth_methods_supported"]
            if not isinstance(raw, list) or any(not isinstance(item, str) for item in raw):
                raise TokenProviderError()
            methods = tuple(raw)
        return cls(issuer, token, authorization, methods)


@dataclass(frozen=True, slots=True, repr=False)
class TokenSuccessResponse:
    access_token: str
    token_type: str
    expires_in: int | None
    refresh_token: str | None
    scope: str | None

    @classmethod
    def parse(cls, body: str) -> TokenSuccessResponse:
        data = _document(body)
        access = _string(data, "access_token", required=True)
        token_type = _string(data, "token_type", required=True)
        assert access is not None and token_type is not None
        lifetime = None
        if "expires_in" in data:
            lifetime = data["expires_in"]
            if type(lifetime) not in {int, Decimal} or not math.isfinite(lifetime) or lifetime != math.floor(lifetime):
                raise TokenProviderError()
            lifetime = int(lifetime)
        scope = _string(data, "scope")
        if scope is not None and not _SCOPE.fullmatch(scope):
            raise TokenProviderError()
        return cls(access, token_type, lifetime, _string(data, "refresh_token"), scope)

    def tokens(self, scopes: frozenset[str] | set[str] | tuple[str, ...], now: float) -> TokenSet:
        if self.token_type.lower() != "bearer" or (
            self.scope is not None and not set(scopes).issubset(self.scope.split(" "))
        ):
            raise TokenProviderError()
        expires_at = None
        if self.expires_in is not None:
            millis = math.floor(now * 1000) + self.expires_in * 1000
            if self.expires_in <= 0 or not math.isfinite(millis) or abs(millis) > _MAX_MILLIS:
                raise TokenProviderError()
            expires_at = millis / 1000
        return TokenSet(self.access_token, expires_at, self.refresh_token)


@dataclass(frozen=True, slots=True, repr=False)
class TokenErrorResponse:
    code: str

    @classmethod
    def parse(cls, body: str) -> TokenErrorResponse:
        data = _document(body)
        code = _string(data, "error", required=True)
        assert code is not None
        _string(data, "error_description", allow_empty=True)
        _string(data, "error_uri")
        return cls(code)
