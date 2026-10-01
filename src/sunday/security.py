# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Selected security metadata; credentials remain application bindings."""

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True, slots=True, kw_only=True)
class SecurityEndpoints:
    """Acquisition endpoints whose deployment overrides do not change profile or server trust."""

    discovery_url: str | None = None
    authorization_url: str | None = None
    token_url: str | None = None
    refresh_url: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class SecurityTransport:
    """Wire location of one credential within a complete security alternative."""

    location: Literal["header", "query", "cookie"]
    name: str
    prefix: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class SecurityBinding(SecurityEndpoints):
    """One logical scheme and its explicitly selected credential provider."""

    scheme: str
    provider: str
    profile: str | None = None
    flow: Literal["clientCredentials", "authorizationCode", "external", "static"]
    scopes: tuple[str, ...] = ()
    audience: str | None = None
    resource: str | None = None
    transport: SecurityTransport
