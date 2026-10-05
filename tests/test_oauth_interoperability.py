"""The native HTTPX transport exercises the same acquisition/rotation contract in both modes."""

import asyncio
import base64
import hashlib
import re
import secrets
from typing import Literal
from urllib.parse import parse_qs, urlencode, urlsplit

import httpx
import pytest
from playwright.async_api import Request, Route, async_playwright

from oauth_support.provider import Provider
from sunday import AuthorizationRequiredError, SecurityTransport, TokenRequest
from sunday.httpx import AuthorizationGrant, HttpxOAuthTokenProvider


async def authorize(provider: Provider, client_id: str) -> AuthorizationGrant:
    verifier = "v" * 64
    if provider.mode == "replay":
        return AuthorizationGrant("synthetic-code", provider.callback, verifier)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")
    state = secrets.token_urlsafe(24)
    url = (
        provider.issuer
        + "/protocol/openid-connect/auth?"
        + urlencode(
            {
                "client_id": client_id,
                "redirect_uri": provider.callback,
                "response_type": "code",
                "scope": "openid",
                "state": state,
                "code_challenge": challenge,
                "code_challenge_method": "S256",
            }
        )
    )
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch()
        try:
            page = await browser.new_page()
            result: asyncio.Future[str] = asyncio.get_running_loop().create_future()

            def observe(request: Request) -> None:
                parsed = urlsplit(request.url)
                if parsed.scheme + "://" + parsed.netloc + parsed.path != provider.callback or result.done():
                    return
                params = parse_qs(parsed.query)
                if params.get("state") != [state] or "code" not in params:
                    result.set_exception(ValueError("Invalid authorization response"))
                else:
                    result.set_result(params["code"][0])

            async def callback(route: Route) -> None:
                await route.fulfill(body="Authorized")

            page.on("request", observe)
            await page.route(re.compile("^" + re.escape(provider.callback)), callback)
            await page.goto(url)
            await page.locator("#username").fill("synthetic-user")
            await page.locator("#password").fill("synthetic-password")
            await page.locator("#kc-login").click(no_wait_after=True)
            code = await asyncio.wait_for(result, timeout=15)
            return AuthorizationGrant(code, provider.callback, verifier)
        finally:
            await browser.close()


def replay(provider: Provider, client_id: str, authentication: str) -> None:
    endpoint = f"/realms/{provider.realm}/protocol/openid-connect/token"
    with httpx.Client(base_url=provider.base, trust_env=False) as admin:
        admin.delete("/__admin/mappings").raise_for_status()
        admin.post("/__admin/scenarios/reset").raise_for_status()
        mappings = [
            {
                "request": {"method": "GET", "urlPath": urlsplit(provider.discovery).path},
                "response": {
                    "status": 200,
                    "jsonBody": {
                        "issuer": provider.issuer,
                        "token_endpoint": provider.base + endpoint,
                        "authorization_endpoint": provider.issuer + "/protocol/openid-connect/auth",
                        "token_endpoint_auth_methods_supported": ["client_secret_basic", "client_secret_post"],
                    },
                },
            }
        ]
        for index, grant in enumerate(["authorization_code", "refresh_token"]):
            form = {"grant_type": {"equalTo": grant}}
            if index == 0:
                form.update(
                    {
                        "code": {"equalTo": "synthetic-code"},
                        "code_verifier": {"equalTo": "v" * 64},
                        "redirect_uri": {"equalTo": provider.callback},
                    }
                )
            else:
                form["refresh_token"] = {"equalTo": "synthetic-refresh-1"}
            headers = {}
            if authentication == "client_secret_basic":
                headers["Authorization"] = {
                    "equalTo": "Basic " + base64.b64encode(f"{client_id}:synthetic-secret".encode()).decode()
                }
            else:
                form["client_id"] = {"equalTo": client_id}
                if authentication == "client_secret_post":
                    form["client_secret"] = {"equalTo": "synthetic-secret"}
            mappings.append(
                {
                    "scenarioName": "rotation",
                    "requiredScenarioState": "Started" if index == 0 else "acquired",
                    "newScenarioState": "acquired" if index == 0 else "rotated",
                    "request": {"method": "POST", "urlPath": endpoint, "formParameters": form, "headers": headers},
                    "response": {
                        "status": 200,
                        "jsonBody": {
                            "access_token": f"synthetic-access-{index}",
                            "token_type": "Bearer",
                            "expires_in": 60,
                            "refresh_token": f"synthetic-refresh-{index + 1}",
                        },
                    },
                }
            )
        mappings.append(
            {
                "priority": 10,
                "request": {"method": "POST", "urlPath": endpoint},
                "response": {"status": 400, "jsonBody": {"error": "invalid_grant"}},
            }
        )
        for mapping in mappings:
            admin.post("/__admin/mappings", json=mapping).raise_for_status()


@pytest.mark.anyio
@pytest.mark.parametrize("authentication", ["none", "client_secret_basic", "client_secret_post"])
async def test_acquisition_and_refresh(
    oauth_provider: Provider,
    authentication: Literal["none", "client_secret_basic", "client_secret_post"],
) -> None:
    client_id = {"none": "public", "client_secret_basic": "basic", "client_secret_post": "post"}[authentication]
    if oauth_provider.mode == "replay":
        replay(oauth_provider, client_id, authentication)

    async def authorization(_: TokenRequest) -> AuthorizationGrant:
        return await authorize(oauth_provider, client_id)

    async with httpx.AsyncClient(trust_env=False) as client:
        provider = HttpxOAuthTokenProvider(
            client,
            identity="interop",
            client_id=client_id,
            client_secret=None if authentication == "none" else "synthetic-secret",
            authentication=authentication,
            issuer=oauth_provider.issuer,
            authorization=authorization,
        )
        request = TokenRequest(
            scheme="identity",
            provider="identity",
            profile="external",
            flow="authorizationCode",
            transport=SecurityTransport(location="header", name="Authorization", prefix="Bearer"),
            client_identity=client_id,
            discovery_url=oauth_provider.discovery,
        )
        tokens = await provider.acquire(request)
        assert tokens.access_token and tokens.refresh_token
        rotated = await provider.refresh(request, tokens.refresh_token)
        assert rotated.access_token and rotated.refresh_token and rotated.refresh_token != tokens.refresh_token
        with pytest.raises(AuthorizationRequiredError):
            await provider.refresh(request, tokens.refresh_token)
