# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""OAuth token exchange; interactive authorization and trust configuration remain application-owned."""

from __future__ import annotations

import hashlib
import math
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from time import time
from typing import Literal
from urllib.parse import quote_plus

import httpx

from ..security import SecurityBinding, SecurityEndpoints
from ..token_provider import AuthorizationRequiredError, TokenConfiguration, TokenProviderError, TokenRequest, TokenSet


@dataclass(frozen=True, slots=True)
class AuthorizationGrant:
    """Fresh authorization result with an S256 PKCE verifier from the application's browser flow.

    The application checks state, issuer, redirect URI, and the authorization response before handing
    off this grant. A grant may be exchanged once, including when the exchange fails or is canceled.
    """

    code: str = field(repr=False)
    redirect_uri: str
    code_verifier: str = field(repr=False)


class HttpxOAuthTokenProvider:
    """OAuth client-credentials, application-authorized PKCE, and refresh exchange using a borrowed client.

    ``identity`` changes when application credential configuration changes. ``grant_identity`` identifies
    a fresh interactive session. Discovery requires a separately configured ``issuer`` and cannot alter
    that trust value. Client defaults, cookies, redirects, and automatic auth never accompany exchanges.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        identity: str,
        client_id: str,
        client_secret: str | None = None,
        authentication: Literal["none", "client_secret_basic", "client_secret_post"] = "none",
        grant_identity: str | None = None,
        authorization: Callable[[TokenRequest], Awaitable[AuthorizationGrant]] | None = None,
        endpoints: SecurityEndpoints | None = None,
        issuer: str | None = None,
        now: Callable[[], float] = time,
    ) -> None:
        if not identity.strip() or not client_id.strip():
            raise ValueError("OAuth provider and client identities must not be blank")
        if authentication not in {"none", "client_secret_basic", "client_secret_post"}:
            raise ValueError("Unsupported OAuth client authentication; use an application TokenProvider")
        if (authentication == "none") != (client_secret is None):
            raise ValueError("Client secrets require an explicit OAuth client authentication method")
        if client_secret is not None and not client_secret:
            raise ValueError("Client secret must not be empty")
        self.identity = identity
        self._client = client
        self._client_id = client_id
        self._secret = client_secret
        self._authentication = authentication
        self._grant_identity = grant_identity
        self._authorization = authorization
        self._endpoints = endpoints
        self._issuer = issuer
        self._now = now
        self._consumed_codes: set[bytes] = set()

    def configure(self, _binding: SecurityBinding) -> TokenConfiguration:
        """Select the application client and session without changing the generated profile."""
        return TokenConfiguration(self._client_id, self._grant_identity, self._endpoints)

    async def acquire(self, request: TokenRequest) -> TokenSet:
        """Acquire a client token or consume one application-authorized PKCE grant."""
        try:
            request = await self._resolve_endpoints(request)
            if request.flow == "clientCredentials":
                if self._authentication == "none":
                    raise TokenProviderError()
                form = {"grant_type": "client_credentials"}
            elif request.flow == "authorizationCode":
                if self._authorization is None or len(self._consumed_codes) >= 1024:
                    raise AuthorizationRequiredError()
                grant = await self._authorization(request)
                if (
                    not grant.code
                    or not grant.redirect_uri
                    or not re.fullmatch(r"[A-Za-z0-9._~-]{43,128}", grant.code_verifier)
                ):
                    raise AuthorizationRequiredError()
                code_key = hashlib.sha256(grant.code.encode()).digest()
                if len(self._consumed_codes) >= 1024 or code_key in self._consumed_codes:
                    raise AuthorizationRequiredError()
                self._consumed_codes.add(code_key)
                form = {
                    "grant_type": "authorization_code",
                    "code": grant.code,
                    "redirect_uri": grant.redirect_uri,
                    "code_verifier": grant.code_verifier,
                }
            else:
                raise TokenProviderError()
            return await self._exchange(request, request.token_url, form)
        except (AuthorizationRequiredError, TokenProviderError):
            raise
        except httpx.TransportError:
            raise TokenProviderError("temporary") from None
        except Exception:
            raise TokenProviderError() from None

    async def refresh(self, request: TokenRequest, refresh_token: str) -> TokenSet:
        """Rotate refresh credentials without using the authorization code again."""
        try:
            request = await self._resolve_endpoints(request)
            return await self._exchange(
                request,
                request.refresh_url or request.token_url,
                {"grant_type": "refresh_token", "refresh_token": refresh_token},
            )
        except (AuthorizationRequiredError, TokenProviderError):
            raise
        except httpx.TransportError:
            raise TokenProviderError("temporary") from None
        except Exception:
            raise TokenProviderError() from None

    async def _resolve_endpoints(self, request: TokenRequest) -> TokenRequest:
        if request.discovery_url is None:
            return request
        if not self._issuer:
            raise TokenProviderError()
        response = await self._client.send(
            httpx.Request("GET", _endpoint(request.discovery_url), headers={"Accept": "application/json"}),
            auth=None,
            follow_redirects=False,
        )
        _check_availability(response.status_code)
        document = response.json()
        if response.status_code != 200 or not isinstance(document, dict) or document.get("issuer") != self._issuer:
            raise TokenProviderError()
        methods = document.get("token_endpoint_auth_methods_supported", ["client_secret_basic"])
        if not isinstance(methods, list) or self._authentication not in methods:
            raise TokenProviderError()
        token_url = request.token_url or document.get("token_endpoint")
        authorization_url = request.authorization_url or document.get("authorization_endpoint")
        if not isinstance(token_url, str) or (authorization_url is not None and not isinstance(authorization_url, str)):
            raise TokenProviderError()
        return replace(request, token_url=token_url, authorization_url=authorization_url)

    async def _exchange(self, request: TokenRequest, endpoint: str | None, form: dict[str, str]) -> TokenSet:
        if request.scopes:
            form["scope"] = " ".join(request.scopes)
        if request.audience is not None:
            form["audience"] = request.audience
        if request.resource is not None:
            form["resource"] = request.resource
        auth = None
        if self._authentication == "client_secret_basic":
            auth = httpx.BasicAuth(quote_plus(self._client_id), quote_plus(self._secret or ""))
        else:
            form["client_id"] = self._client_id
            if self._authentication == "client_secret_post":
                form["client_secret"] = self._secret or ""
        response = await self._client.send(
            httpx.Request("POST", _endpoint(endpoint), data=form, headers={"Accept": "application/json"}),
            auth=auth,
            follow_redirects=False,
        )
        _check_availability(response.status_code)
        data = response.json()
        if not isinstance(data, dict):
            raise TokenProviderError()
        if response.status_code != 200:
            if data.get("error") == "invalid_grant":
                if request.flow == "authorizationCode":
                    raise AuthorizationRequiredError()
                raise TokenProviderError("invalid_grant")
            if data.get("error") in ("temporarily_unavailable", "server_error"):
                raise TokenProviderError("temporary")
            raise TokenProviderError()
        access_token, token_type = data.get("access_token"), data.get("token_type")
        if (
            not isinstance(access_token, str)
            or not access_token
            or not isinstance(token_type, str)
            or token_type.lower() != "bearer"
        ):
            raise TokenProviderError()
        lifetime = data.get("expires_in")
        expires_at = None
        if lifetime is not None:
            if type(lifetime) not in {int, float} or not math.isfinite(lifetime) or lifetime <= 0:
                raise TokenProviderError()
            expires_at = self._now() + lifetime
            if not math.isfinite(expires_at):
                raise TokenProviderError()
        refresh_token = data.get("refresh_token")
        if refresh_token is not None and (not isinstance(refresh_token, str) or not refresh_token):
            raise TokenProviderError()
        scope = data.get("scope")
        if scope is not None and (not isinstance(scope, str) or not set(request.scopes).issubset(scope.split())):
            raise TokenProviderError()
        return TokenSet(access_token, expires_at, refresh_token)


def _check_availability(status: int) -> None:
    if status in (408, 429) or 500 <= status <= 599:
        raise TokenProviderError("temporary")


def _endpoint(value: str | None) -> httpx.URL:
    if value is None:
        raise TokenProviderError()
    url = httpx.URL(value)
    if not url.is_absolute_url or url.userinfo or url.fragment:
        raise TokenProviderError()
    if url.scheme != "https" and not (url.scheme == "http" and url.host in {"localhost", "127.0.0.1", "::1"}):
        raise TokenProviderError()
    return url
