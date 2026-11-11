"""Standard settings composition and custom adapters share generated operation contracts."""

import anyio
import httpx
import pytest

from sunday import (
    ClientSettings,
    EventStreamOptions,
    ProviderCredentials,
    RequestCancellationError,
    RequestSpec,
    SecurityBinding,
    SecurityTransport,
    TokenConfiguration,
    TokenManager,
    TokenProviderError,
    TokenRequest,
    TokenSet,
)
from sunday.httpx import HttpxTransport, StandardAuthenticationAdapter, transport_from_settings

pytestmark = pytest.mark.anyio
BINDING = SecurityBinding(
    scheme="identity",
    provider="identity",
    flow="clientCredentials",
    transport=SecurityTransport(location="header", name="Authorization", prefix="Bearer"),
)
SPEC: RequestSpec[None] = RequestSpec("GET", "/value", security=(BINDING,))


async def test_generated_settings_standard_expiry_and_rejection() -> None:
    now = 1000.0
    acquired = refreshed = sent = 0
    rejecting = False

    class Provider:
        identity = "fixture"

        def configure(self, binding: SecurityBinding) -> TokenConfiguration:
            return TokenConfiguration("client")

        async def acquire(self, request: TokenRequest) -> TokenSet:
            nonlocal acquired
            acquired += 1
            return TokenSet("initial", expires_at=now + 100, refresh_token="r1")

        async def refresh(self, request: TokenRequest, refresh_token: str) -> TokenSet:
            nonlocal refreshed
            refreshed += 1
            return TokenSet(f"renewed-{refreshed}", expires_at=now + 100, refresh_token="r2")

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal sent
        sent += 1
        assert request.headers["Authorization"].startswith("Bearer ")
        return httpx.Response(401 if rejecting else 204, headers={"WWW-Authenticate": "Bearer error=invalid_token"})

    manager = TokenManager(Provider(), now=lambda: now, expiry_skew=0)
    settings = ClientSettings.resolve(
        "https://example.test", {"read": [[BINDING]]}, {"identity": ProviderCredentials(manager)}
    )
    assert settings.token_providers["identity"] is manager
    async with (
        httpx.AsyncClient(base_url=settings.base_url, transport=httpx.MockTransport(respond)) as client,
        transport_from_settings(settings, client) as transport,
    ):
        assert acquired == 0
        await transport.execute_request(SPEC)
        await transport.execute_request(SPEC)
        now += 101
        await transport.execute_request(SPEC)
        assert (acquired, refreshed) == (1, 1)
        rejecting = True
        with pytest.raises(RequestCancellationError):
            await transport.execute_request(SPEC)
        assert sent == 4
        rejecting = False
        await transport.execute_request(SPEC)
        assert refreshed == 2


async def test_same_generated_operation_custom_plain_and_explicit_missing_policy() -> None:
    sent: list[str] = []
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        sent.append(request.headers.get("Authorization", ""))
        return httpx.Response(204)

    async def custom(request: httpx.Request) -> httpx.Request:
        nonlocal calls
        calls += 1
        request.headers["Authorization"] = "Custom credential"
        return request

    async with httpx.AsyncClient(base_url="https://example.test", transport=httpx.MockTransport(respond)) as client:
        async with HttpxTransport(client, request_adapter=custom) as direct:
            await direct.execute_request(SPEC)
        async with HttpxTransport(client) as plain:
            await plain.execute_request(SPEC)
        authentication = StandardAuthenticationAdapter({})
        async with HttpxTransport(client, request_adapter=authentication, response_adapter=authentication) as missing:
            with pytest.raises(RequestCancellationError):
                await missing.execute_request(SPEC)
            await missing.execute_request(RequestSpec("GET", "/value"))
    assert calls == 1
    assert sent == ["Custom credential", "", ""]


@pytest.mark.parametrize("cancel", [False, True])
async def test_feedback_failure_still_stops_event_stream(cancel: bool) -> None:
    failure = RequestCancellationError("provider stop") if cancel else TokenProviderError("temporary")
    rejections = sent = 0
    fail_acquisition = False

    class Provider:
        identity = "feedback"

        def configure(self, binding: SecurityBinding) -> TokenConfiguration:
            return TokenConfiguration("client")

        async def acquire(self, request: TokenRequest) -> TokenSet:
            if fail_acquisition:
                raise TokenProviderError("temporary")
            return TokenSet("access")

        async def reject(self, request: TokenRequest, rejected_token: str) -> bool:
            nonlocal rejections
            rejections += 1
            raise failure

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal sent
        sent += 1
        return httpx.Response(401, headers={"WWW-Authenticate": "Bearer error=invalid_token"})

    authentication = StandardAuthenticationAdapter({"identity": Provider()})
    async with httpx.AsyncClient(base_url="https://example.test", transport=httpx.MockTransport(respond)) as client:
        transport = HttpxTransport(client, request_adapter=authentication, response_adapter=authentication)
        with anyio.fail_after(2), pytest.raises(RequestCancellationError) as caught:
            async for _ in transport.event_stream(SPEC, lambda event: event, options=EventStreamOptions(retry=0)):
                pytest.fail("Rejected stream must not emit")
        if cancel:
            assert caught.value is failure
        assert sent == rejections == 1
        fail_acquisition = True
        with pytest.raises(httpx.TransportError) as temporary:
            await transport.execute_request(SPEC)
        assert not isinstance(temporary.value, RequestCancellationError)
        assert sent == 1
