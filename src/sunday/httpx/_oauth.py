# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""OAuth token exchange; interactive authorization and trust configuration remain application-owned."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from time import time
from typing import Literal
from urllib.parse import quote_plus

import httpx
from authlib.oauth2.auth import ClientAuth  # type: ignore[import-untyped]
from authlib.oauth2.rfc6749.parameters import prepare_token_request  # type: ignore[import-untyped]

from ..security import SecurityBinding, SecurityEndpoints
from ..token_provider import AuthorizationRequiredError, TokenConfiguration, TokenProviderError, TokenRequest, TokenSet
from ._oauth_wire import DiscoveryMetadata, TokenErrorResponse, TokenSuccessResponse
from ._oauth_wire import endpoint as _endpoint


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
        if response.status_code != 200:
            raise TokenProviderError()
        document = DiscoveryMetadata.parse(response.text)
        if document.issuer != self._issuer:
            raise TokenProviderError()
        # Public PKCE clients do not authenticate; Keycloak need not advertise "none".
        public_code = self._authentication == "none" and request.flow == "authorizationCode"
        methods = document.methods if document.methods is not None else ("client_secret_basic",)
        if not public_code and self._authentication not in methods:
            raise TokenProviderError()
        token_url = request.token_url if request.token_url is not None else document.token_url
        authorization_url = (
            request.authorization_url if request.authorization_url is not None else document.authorization_url
        )
        _endpoint(token_url)
        if authorization_url is not None:
            _endpoint(authorization_url)
        elif request.flow == "authorizationCode":
            raise TokenProviderError()
        return replace(request, token_url=token_url, authorization_url=authorization_url)

    async def _exchange(self, request: TokenRequest, endpoint: str | None, form: dict[str, str]) -> TokenSet:
        if request.scopes:
            form["scope"] = " ".join(request.scopes)
        if request.audience is not None:
            form["audience"] = request.audience
        if request.resource is not None:
            form["resource"] = request.resource
        grant_type = form.pop("grant_type")
        body = prepare_token_request(grant_type, **form)
        client_id, secret = self._client_id, self._secret
        if self._authentication == "client_secret_basic":
            # Authlib applies HTTP Basic directly; OAuth requires form-encoding each credential first.
            client_id, secret = quote_plus(client_id), quote_plus(secret or "")
        url, headers, body = ClientAuth(client_id, secret, self._authentication).prepare(
            "POST",
            str(_endpoint(endpoint)),
            {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"},
            body,
        )
        response = await self._client.send(
            httpx.Request("POST", url, content=body, headers=headers),
            auth=None,
            follow_redirects=False,
        )
        _check_availability(response.status_code)
        if response.status_code != 200:
            code = TokenErrorResponse.parse(response.text).code
            if code == "invalid_grant":
                if request.flow == "authorizationCode":
                    raise AuthorizationRequiredError()
                raise TokenProviderError("invalid_grant")
            if code in ("temporarily_unavailable", "server_error"):
                raise TokenProviderError("temporary")
            raise TokenProviderError()
        return TokenSuccessResponse.parse(response.text).tokens(request.scopes, self._now())


def _check_availability(status: int) -> None:
    if status in (408, 429) or 500 <= status <= 599:
        raise TokenProviderError("temporary")
