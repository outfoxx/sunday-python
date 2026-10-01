# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""HTTPX credential attachment and conservative authentication recovery."""

from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import quote

import httpx

from ..security import SecurityBinding
from ..token_manager import TokenLease, TokenManager
from ..token_provider import TokenProviderError


@dataclass(frozen=True, slots=True)
class RequestSecurity:
    """Selected alternative and credential receipts for one native request."""

    bindings: tuple[SecurityBinding, ...]
    leases: tuple[TokenLease, ...] = ()

    async def authorize(self, request: httpx.Request, manager: TokenManager | None) -> RequestSecurity:
        """Attach the entire credential set after every provider succeeds."""
        if not self.bindings:
            return self
        if manager is None:
            raise TokenProviderError()
        url, headers = request.url, httpx.Headers(request.headers)
        if "authorization" in headers and not any(
            binding.transport.location == "header" and binding.transport.name.lower() == "authorization"
            for binding in self.bindings
        ):
            raise TokenProviderError()
        names: set[tuple[str, str]] = set()
        for binding in self.bindings:
            transport = binding.transport
            name = transport.name.lower() if transport.location == "header" else transport.name
            key = (transport.location, name)
            if key in names:
                raise TokenProviderError()
            names.add(key)
            if not self.leases and (
                (transport.location == "header" and name in headers)
                or (transport.location == "query" and name in url.params)
                or (transport.location == "cookie" and name in dict(_cookies(headers)))
            ):
                raise TokenProviderError()
        # Acquiring sequentially avoids leaving sibling tasks running when one provider fails.
        leases = tuple([await manager.credentials(binding) for binding in self.bindings])
        for binding, lease in zip(self.bindings, leases, strict=True):
            transport = binding.transport
            credential = lease.tokens.access_token
            if transport.prefix:
                credential = f"{transport.prefix} {credential}"
            if transport.location == "header":
                headers[transport.name] = credential
            elif transport.location == "query":
                url = url.copy_set_param(transport.name, credential)
            else:
                cookies = [(name, value) for name, value in _cookies(headers) if name != transport.name]
                cookies.append((transport.name, quote(credential, safe="")))
                headers["cookie"] = "; ".join(f"{name}={value}" for name, value in cookies)
        request.url = url
        request.headers = headers
        return RequestSecurity(self.bindings, leases)

    def rejected_leases(self, request: httpx.Request, response: httpx.Response) -> tuple[TokenLease, ...]:
        """Recover only safe bodyless requests with an explicit invalid bearer token challenge."""
        if response.status_code != 401 or request.method not in {"GET", "HEAD", "OPTIONS"}:
            return ()
        try:
            if request.content:
                return ()
        except httpx.RequestNotRead:
            return ()
        if not _invalid_bearer_challenge(response.headers.get("www-authenticate", "")):
            return ()
        return tuple(
            lease
            for binding, lease in zip(self.bindings, self.leases, strict=True)
            if binding.transport.location == "header"
            and binding.transport.name.lower() == "authorization"
            and (binding.transport.prefix or "").lower() == "bearer"
        )

    def diagnostic_request(self, request: httpx.Request) -> httpx.Request:
        """Remove managed credentials from native error/response diagnostics."""
        url, headers = request.url, httpx.Headers(request.headers)
        for binding in self.bindings:
            transport = binding.transport
            if transport.location == "query":
                url = url.copy_set_param(transport.name, "[redacted]")
            elif transport.location == "header":
                headers[transport.name] = "[redacted]"
            else:
                headers["cookie"] = "[redacted]"
        return httpx.Request(request.method, url, headers=headers)


def _cookies(headers: httpx.Headers) -> list[tuple[str, str]]:
    result: list[tuple[str, str]] = []
    for entry in headers.get("cookie", "").split(";"):
        name, separator, value = entry.partition("=")
        if separator:
            result.append((name.strip(), value.strip()))
    return result


def _invalid_bearer_challenge(header: str) -> bool:
    parts: list[str] = []
    start, quoted, escaped = 0, False, False
    for index, character in enumerate(header):
        if escaped:
            escaped = False
        elif quoted and character == "\\":
            escaped = True
        elif character == '"':
            quoted = not quoted
        elif not quoted and character == ",":
            parts.append(header[start:index].strip())
            start = index + 1
    if quoted or escaped:
        return False
    parts.append(header[start:].strip())
    bearer = False
    for part in parts:
        challenge = re.fullmatch(r"([a-z][a-z0-9_-]*)\s+(?!\s*=)(.*)", part, re.IGNORECASE)
        if challenge:
            bearer, part = challenge[1].lower() == "bearer", challenge[2]
        elif re.fullmatch(r"[a-z][a-z0-9_-]*", part, re.IGNORECASE):
            bearer = False
        if bearer and re.fullmatch(r'(?i:error)\s*=\s*(?:"invalid_token"|invalid_token)', part):
            return True
    return False


class AuthenticationRecoveryBudget:
    """One authentication recovery across an invocation, including event reconnects."""

    def __init__(self) -> None:
        self._used = False

    def consume(self) -> bool:
        if self._used:
            return False
        self._used = True
        return True
