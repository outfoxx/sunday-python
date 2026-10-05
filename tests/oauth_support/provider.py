"""Test-owned OAuth provider lifecycle, independent of CI job orchestration."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import socket
import subprocess
import tarfile
import tempfile
import time
import uuid
from contextlib import suppress
from pathlib import Path
from typing import BinaryIO

import httpx

KEYCLOAK_URL = "https://github.com/keycloak/keycloak/releases/download/26.2.5/keycloak-26.2.5.tar.gz"
KEYCLOAK_SHA = "e99e5f8783ea8f1cc04140b7033ea7291ff9898a088f37399b88651d81238f88"
KEYCLOAK_IMAGE = "quay.io/keycloak/keycloak@sha256:4883630ef9db14031cde3e60700c9a9a8eaf1b5c24db1589d6a2d43de38ba2a9"
WIREMOCK_URL = (
    "https://repo.maven.apache.org/maven2/org/wiremock/wiremock-standalone/3.13.1/wiremock-standalone-3.13.1.jar"
)
WIREMOCK_SHA = "bdf4c705e7fd61c778e59a19f75396eac4520efeabfac97643a53979bd4d5716"


def backend(mode: str, system: str, ci: str | None) -> str:
    if mode not in {"replay", "live"}:
        raise ValueError("SUNDAY_OAUTH_TEST_MODE must be replay or live")
    if mode == "replay":
        return "wiremock-java"
    enabled = ci is not None and ci.lower() not in {"", "0", "false"}
    return "keycloak-java" if system == "Darwin" and enabled else "keycloak-container"


def artifact(cache: Path, url: str, checksum: str) -> Path:
    cache.mkdir(parents=True, exist_ok=True)
    target = cache / url.rsplit("/", 1)[1]
    if target.exists():
        if hashlib.sha256(target.read_bytes()).hexdigest() != checksum:
            raise RuntimeError("OAuth artifact cache integrity failure")
        return target
    temporary = cache / f".{uuid.uuid4()}.download"
    try:
        digest = hashlib.sha256()
        with httpx.stream("GET", url, follow_redirects=True, timeout=120, trust_env=False) as response:
            response.raise_for_status()
            with temporary.open("wb") as output:
                for block in response.iter_bytes():
                    digest.update(block)
                    output.write(block)
        if digest.hexdigest() != checksum:
            raise RuntimeError("OAuth artifact download integrity failure")
        temporary.replace(target)
        return target
    finally:
        temporary.unlink(missing_ok=True)


class Provider:
    """One disposable provider; callers own it with a session-scoped fixture."""

    def __init__(self, mode: str, cache: Path, *, startup_timeout: float = 120) -> None:
        self.mode = mode
        self.backend = backend(mode, platform.system(), os.environ.get("CI"))
        self.cache = cache
        self.startup_timeout = startup_timeout
        self.realm = "sunday-" + uuid.uuid4().hex
        self.directory = Path(tempfile.mkdtemp(prefix="sunday-oauth-"))
        self.process: subprocess.Popen[bytes] | None = None
        self.log: BinaryIO | None = None
        self.container: str | None = None
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            self.port = reservation.getsockname()[1]
        self.base = f"http://127.0.0.1:{self.port}"
        self.issuer = f"{self.base}/realms/{self.realm}"
        self.discovery = self.issuer + "/.well-known/openid-configuration"
        self.callback = "http://127.0.0.1:49173/callback"

    def start(self) -> Provider:
        try:
            command = self._command()
            self.log = (self.directory / "provider.log").open("wb")
            self.process = subprocess.Popen(command, stdout=self.log, stderr=subprocess.STDOUT)
            self._ready()
            return self
        except Exception:
            self.close()
            raise RuntimeError(f"OAuth infrastructure startup failed ({self.backend})") from None

    def _command(self) -> list[str]:
        if self.backend == "wiremock-java":
            jar = artifact(self.cache, WIREMOCK_URL, WIREMOCK_SHA)
            return ["java", "-jar", str(jar), "--bind-address", "127.0.0.1", "--port", str(self.port)]
        realm = self._realm()
        imports = self.directory / "import"
        imports.mkdir()
        (imports / f"{self.realm}-realm.json").write_text(json.dumps(realm))
        options = ["start-dev", "--import-realm", "--http-port", str(self.port), "--hostname", self.base]
        if self.backend == "keycloak-java":
            archive = artifact(self.cache, KEYCLOAK_URL, KEYCLOAK_SHA)
            with tarfile.open(archive) as bundle:
                bundle.extractall(self.directory, filter="data")
            distribution = self.directory / "keycloak-26.2.5"
            shutil.copytree(imports, distribution / "data/import")
            return [str(distribution / "bin/kc.sh"), *options, "--http-host", "127.0.0.1"]
        # This branch is unreachable on macOS CI; even Docker discovery is forbidden there.
        self.container = "sunday-oauth-" + uuid.uuid4().hex
        return [
            "docker",
            "run",
            "--rm",
            "--name",
            self.container,
            "-p",
            f"127.0.0.1:{self.port}:{self.port}",
            "-v",
            f"{imports}:/opt/keycloak/data/import:ro",
            KEYCLOAK_IMAGE,
            *options,
        ]

    def _realm(self) -> dict[str, object]:
        clients = []
        for name, public, authenticator in [
            ("public", True, "client-secret"),
            ("basic", False, "client-secret"),
            ("post", False, "client-secret"),
        ]:
            clients.append(
                {
                    "clientId": name,
                    "enabled": True,
                    "publicClient": public,
                    "secret": "synthetic-secret",
                    "clientAuthenticatorType": authenticator,
                    "standardFlowEnabled": True,
                    "serviceAccountsEnabled": not public,
                    "redirectUris": [self.callback],
                    "protocol": "openid-connect",
                    "attributes": {"pkce.code.challenge.method": "S256"},
                }
            )
        return {
            "realm": self.realm,
            "enabled": True,
            "sslRequired": "none",
            "revokeRefreshToken": True,
            "refreshTokenMaxReuse": 0,
            "clients": clients,
            "users": [
                {
                    "username": "synthetic-user",
                    "email": "synthetic@example.invalid",
                    "emailVerified": True,
                    "firstName": "Synthetic",
                    "lastName": "User",
                    "enabled": True,
                    "credentials": [{"type": "password", "value": "synthetic-password", "temporary": False}],
                }
            ],
        }

    def _ready(self) -> None:
        target = self.base + "/__admin/mappings" if self.mode == "replay" else self.discovery
        deadline = time.monotonic() + self.startup_timeout
        with httpx.Client(timeout=1, trust_env=False) as client:
            while time.monotonic() < deadline:
                if self.process is None or self.process.poll() is not None:
                    raise RuntimeError("Provider exited before readiness")
                try:
                    if client.get(target).status_code == 200:
                        return
                except httpx.TransportError:
                    pass
                time.sleep(0.1)
        raise RuntimeError("Provider readiness timeout")

    def close(self) -> None:
        try:
            if self.container is not None:
                # Docker failure must not prevent cleanup of the owned CLI process and directory.
                with suppress(OSError, subprocess.SubprocessError):
                    subprocess.run(
                        ["docker", "rm", "-f", self.container],
                        timeout=20,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        check=False,
                    )
            if self.process is not None and self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=10)
        finally:
            if self.log is not None:
                self.log.close()
            shutil.rmtree(self.directory, ignore_errors=True)
