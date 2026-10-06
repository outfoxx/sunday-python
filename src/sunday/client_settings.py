# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Resolved settings for application-supplied client transport factories."""

from __future__ import annotations

import re
from base64 import b64encode
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from urllib.parse import urljoin, urlsplit
from uuid import uuid4

from .credentials import (
    ApiKeyCredentials,
    BasicCredentials,
    BearerCredentials,
    Credentials,
    OAuthCredentials,
    ProviderCredentials,
    validate_credentials,
)
from .security import SecurityBinding, SecurityEndpoints
from .token_manager import TokenManager
from .token_manager_factory import TokenManagerFactory
from .token_provider import TokenConfiguration, TokenProvider, TokenRequest, TokenSet


@dataclass(frozen=True, slots=True, init=False)
class ClientSettings:
    """Immutable endpoint and security inputs; constructing settings does not perform network I/O."""

    base_url: str
    bindings: Mapping[str, tuple[SecurityBinding, ...]]
    _credentials: Mapping[str, Credentials] = field(repr=False)
    token_manager: TokenManager | None = field(repr=False)

    def __init__(
        self,
        base_url: str,
        bindings: Mapping[str, Sequence[SecurityBinding]] | None = None,
        credentials: Mapping[str, Credentials] | None = None,
        *,
        token_manager_factory: TokenManagerFactory | None = None,
    ) -> None:
        endpoint = urlsplit(base_url)
        if (
            endpoint.scheme not in {"http", "https"}
            or not endpoint.hostname
            or endpoint.username is not None
            or endpoint.password is not None
            or "?" in base_url
            or "#" in base_url
        ):
            raise ValueError("Client endpoint must be an absolute HTTP or HTTPS URL")
        values = {
            operation: tuple(
                replace(
                    binding,
                    scopes=tuple(binding.scopes),
                    discovery_url=_endpoint(base_url, binding.discovery_url),
                    authorization_url=_endpoint(base_url, binding.authorization_url),
                    token_url=_endpoint(base_url, binding.token_url),
                    refresh_url=_endpoint(base_url, binding.refresh_url),
                )
                for binding in alternatives
            )
            for operation, alternatives in (bindings or {}).items()
        }
        supplied = dict(credentials or {})
        for alternatives in values.values():
            for binding in alternatives:
                if binding.scheme not in supplied:
                    raise ValueError(f"Missing credentials for scheme '{binding.scheme}'")
                validate_credentials(supplied[binding.scheme], binding)
        object.__setattr__(self, "base_url", base_url)
        object.__setattr__(self, "bindings", MappingProxyType(values))
        object.__setattr__(self, "_credentials", MappingProxyType(supplied))
        object.__setattr__(self, "token_manager", self._prepare_token_manager(token_manager_factory))

    @staticmethod
    def server_url(template: str, variables: Mapping[str, str], document_base_url: str | None = None) -> str:
        """Expand variables once and resolve relative servers against their document location."""
        expanded = re.sub(r"\{([^}]+)\}", lambda match: variables[match[1]], template)
        result = urljoin(document_base_url or "", expanded)
        url = urlsplit(result)
        if (
            url.scheme not in {"http", "https"}
            or not url.hostname
            or url.username
            or url.password
            or url.query
            or url.fragment
        ):
            raise ValueError("Invalid HTTP server endpoint; relative servers require an HTTP document base URL")
        return result

    @classmethod
    def resolve(
        cls,
        base_url: str,
        alternatives: Mapping[str, Sequence[Sequence[SecurityBinding]]],
        credentials: Mapping[str, Credentials],
        selection: Mapping[str, Sequence[str]] | None = None,
        alternative_selection: Mapping[str, int] | None = None,
        *,
        token_manager_factory: TokenManagerFactory | None = None,
    ) -> ClientSettings:
        """Choose complete alternatives; alternative_selection selects a zero-based candidate including scopes."""
        if (set(selection or {}) | set(alternative_selection or {})) - alternatives.keys():
            raise ValueError("Unknown operation selection")
        bindings: dict[str, tuple[SecurityBinding, ...]] = {}
        for operation, candidates in alternatives.items():
            selected = (selection or {}).get(operation)
            usable: list[tuple[SecurityBinding, ...]] = []
            selected_index = (alternative_selection or {}).get(operation)
            for index, candidate in enumerate(candidates):
                if selected_index is not None and selected_index != index:
                    continue
                if selected is not None and set(selected) != {binding.scheme for binding in candidate}:
                    continue
                try:
                    for binding in candidate:
                        validate_credentials(credentials[binding.scheme], binding)
                except (KeyError, ValueError):
                    continue
                usable.append(tuple(candidate))
            if len(usable) != 1:
                raise ValueError(f"Operation '{operation}' requires one complete security alternative")
            bindings[operation] = usable[0]
        return cls(base_url, bindings, credentials, token_manager_factory=token_manager_factory)

    def _prepare_token_manager(self, factory: TokenManagerFactory | None) -> TokenManager | None:
        """Prepare providers through the selected transport module without acquiring tokens."""
        owners: dict[str, str] = {}
        providers: dict[str, TokenProvider] = {}
        for alternatives in self.bindings.values():
            for binding in alternatives:
                if binding.provider in owners:
                    if owners[binding.provider] != binding.scheme:
                        raise ValueError("Distinct credential schemes require distinct provider bindings")
                    continue
                owners[binding.provider] = binding.scheme
                credential = self._credentials[binding.scheme]
                if isinstance(credential, ProviderCredentials):
                    providers[binding.provider] = credential.provider
                elif isinstance(credential, OAuthCredentials):
                    endpoints = credential.endpoints
                    resolved = (
                        replace(
                            credential,
                            endpoints=SecurityEndpoints(
                                discovery_url=_endpoint(self.base_url, endpoints.discovery_url),
                                authorization_url=_endpoint(self.base_url, endpoints.authorization_url),
                                token_url=_endpoint(self.base_url, endpoints.token_url),
                                refresh_url=_endpoint(self.base_url, endpoints.refresh_url),
                            ),
                        )
                        if endpoints is not None
                        else credential
                    )
                    providers[binding.provider] = credential.provider_factory(resolved)
                else:
                    providers[binding.provider] = _StaticProvider(credential)
        if not providers:
            return None
        return factory(MappingProxyType(providers)) if factory is not None else TokenManager(providers)


class _StaticProvider:
    def __init__(self, credentials: BearerCredentials | ApiKeyCredentials | BasicCredentials) -> None:
        self.identity = str(uuid4())
        if isinstance(credentials, BearerCredentials):
            self._token = credentials.token
        elif isinstance(credentials, ApiKeyCredentials):
            self._token = credentials.key
        else:
            self._token = b64encode(f"{credentials.username}:{credentials.password}".encode()).decode("ascii")

    def configure(self, binding: SecurityBinding) -> TokenConfiguration:
        return TokenConfiguration(self.identity)

    async def acquire(self, request: TokenRequest) -> TokenSet:
        return TokenSet(self._token)


def _endpoint(base_url: str, endpoint: str | None) -> str | None:
    if endpoint is None:
        return None
    if "{" in endpoint or "}" in endpoint:
        raise ValueError("Security endpoint URLs do not support server variables")
    return urljoin(base_url, endpoint)
