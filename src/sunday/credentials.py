# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Transport-independent credentials narrowed by generated client factories."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Literal

from .security import SecurityBinding, SecurityEndpoints
from .token_provider import TokenProvider, TokenRequest


@dataclass(frozen=True, slots=True)
class AuthorizationGrant:
    """Fresh application-authorized PKCE result, consumed once by the OAuth provider."""

    code: str = field(repr=False)
    redirect_uri: str
    code_verifier: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class BearerCredentials:
    """Bearer token without its wire prefix."""

    token: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class ApiKeyCredentials:
    """API key whose wire placement comes from the contract."""

    key: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class BasicCredentials:
    """Basic authentication inputs encoded by the runtime."""

    username: str = field(repr=False)
    password: str = field(repr=False)


@dataclass(frozen=True, slots=True, kw_only=True)
class OAuthCredentials:
    """OAuth client configuration; interactive authorization remains application-owned."""

    identity: str
    client_id: str
    flow: Literal["clientCredentials", "authorizationCode"]
    provider_factory: Callable[["OAuthCredentials"], TokenProvider] = field(repr=False)
    client_secret: str | None = field(default=None, repr=False)
    authentication: Literal["none", "client_secret_basic", "client_secret_post"] = "none"
    grant_identity: str | None = None
    authorization: Callable[[TokenRequest], Awaitable[AuthorizationGrant]] | None = field(default=None, repr=False)
    endpoints: SecurityEndpoints | None = None
    issuer: str | None = None


@dataclass(frozen=True, slots=True)
class ProviderCredentials:
    """Application acquisition for mechanisms outside the built-in OAuth flows."""

    provider: TokenProvider = field(repr=False)
    flow: Literal["clientCredentials", "authorizationCode", "external", "static"] | None = None


type Credentials = BearerCredentials | ApiKeyCredentials | BasicCredentials | OAuthCredentials | ProviderCredentials


def validate_credentials(credentials: Credentials, binding: SecurityBinding) -> None:
    """Reject incompatible credentials before constructing transports or acquiring tokens."""
    if isinstance(credentials, ProviderCredentials):
        if credentials.flow is not None and credentials.flow != binding.flow:
            raise ValueError("Provider flow does not match")
        return
    prefix = binding.transport.prefix.lower() if binding.transport.prefix else None
    if isinstance(credentials, OAuthCredentials):
        if prefix != "bearer" or credentials.flow != binding.flow:
            raise ValueError("OAuth credentials do not match the selected security binding")
        if not credentials.identity.strip() or not credentials.client_id.strip():
            raise ValueError("OAuth identities must not be blank")
        if credentials.authentication not in {"none", "client_secret_basic", "client_secret_post"}:
            raise ValueError("Unsupported OAuth client authentication")
        if (credentials.authentication == "none") != (credentials.client_secret is None):
            raise ValueError("Client secrets require an explicit OAuth client authentication method")
        if credentials.client_secret == "":
            raise ValueError("Client secret must not be empty")
        if credentials.flow == "clientCredentials" and credentials.authentication == "none":
            raise ValueError("Client credentials require client authentication")
        if credentials.flow == "authorizationCode" and (
            not credentials.grant_identity
            or not credentials.grant_identity.strip()
            or credentials.authorization is None
        ):
            raise ValueError("Authorization code credentials require a fresh application authorization session")
        return
    if binding.flow not in {"static", "external"}:
        raise ValueError("Static credentials cannot satisfy an OAuth acquisition binding")
    valid = (
        (isinstance(credentials, BearerCredentials) and prefix == "bearer" and bool(credentials.token))
        or (isinstance(credentials, ApiKeyCredentials) and prefix is None and bool(credentials.key))
        or (isinstance(credentials, BasicCredentials) and prefix == "basic" and ":" not in credentials.username)
    )
    if not valid:
        raise ValueError("Credentials do not match the selected security binding")
