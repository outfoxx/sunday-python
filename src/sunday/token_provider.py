# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Application credential acquisition and storage contracts."""

from dataclasses import dataclass, field
from typing import Protocol

from .security import SecurityBinding, SecurityEndpoints


@dataclass(frozen=True, slots=True)
class TokenSet:
    """Credentials with an optional expiry in Unix seconds; secrets are omitted from repr."""

    access_token: str = field(repr=False)
    expires_at: float | None = None
    refresh_token: str | None = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class TokenConfiguration:
    """Non-secret identities isolate clients and sessions; grant_identity changes with grant inputs."""

    client_identity: str
    grant_identity: str | None = None
    endpoints: SecurityEndpoints | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class TokenRequest(SecurityBinding):
    """Resolved acquisition parameters without client secrets or authorization codes."""

    client_identity: str
    grant_identity: str | None = None


class TokenProvider(Protocol):
    """Application provider; cancellation propagates to its asynchronous acquisition calls."""

    @property
    def identity(self) -> str:
        """Non-secret provider identity used to isolate cached credentials."""
        ...

    def configure(self, binding: SecurityBinding) -> TokenConfiguration:
        """Resolve the application client/session and optional deployment endpoint overrides."""
        ...

    async def acquire(self, request: TokenRequest) -> TokenSet:
        """Acquire credentials for a fresh grant or an external/static binding."""
        ...


class RefreshingTokenProvider(TokenProvider, Protocol):
    """Optional provider capability for rotating an existing refresh token."""

    async def refresh(self, request: TokenRequest, refresh_token: str) -> TokenSet:
        """Exchange a refresh token; invalid interactive sessions raise AuthorizationRequiredError."""
        ...


class TokenStore(Protocol):
    """Application-owned credential storage; implementations must keep token values private."""

    async def load(self, key: str) -> TokenSet | None:
        """Return stored credentials for this non-secret cache key."""
        ...

    async def save(self, key: str, tokens: TokenSet) -> None:
        """Atomically replace credentials, including a newly rotated refresh token."""
        ...

    async def remove(self, key: str) -> None:
        """Remove credentials for a terminated application session."""
        ...


class AuthorizationRequiredError(Exception):
    """The application must obtain fresh authorization and supply a new grant identity."""

    def __init__(self) -> None:
        super().__init__("Fresh application authorization is required")


class TokenProviderError(Exception):
    """Safe provider failure that does not expose credential-bearing provider messages."""

    def __init__(self) -> None:
        super().__init__("The credential provider could not supply usable credentials")
