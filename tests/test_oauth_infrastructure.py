"""Provider selection and artifact-integrity failures never fall back to replay."""

import hashlib
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from oauth_support.provider import Provider, artifact, backend


@pytest.mark.parametrize("ci", [None, "", "0", "false", "FALSE"])
def test_disabled_ci_uses_container(ci: str | None) -> None:
    assert backend("live", "Darwin", ci) == "keycloak-container"


@pytest.mark.parametrize("ci", ["1", "true", "yes", "github"])
def test_mac_ci_never_discovers_docker(ci: str) -> None:
    with patch("shutil.which", side_effect=AssertionError("No executable discovery")):
        assert backend("live", "Darwin", ci) == "keycloak-java"
        assert backend("live", "Linux", ci) == "keycloak-container"
        assert backend("replay", "Darwin", ci) == "wiremock-java"


def test_invalid_mode_fails() -> None:
    with pytest.raises(ValueError, match="replay or live"):
        backend("automatic", "Darwin", "true")


def test_cached_artifact_is_verified(tmp_path: Path) -> None:
    path = tmp_path / "provider.jar"
    path.write_bytes(b"verified fixture")
    checksum = hashlib.sha256(path.read_bytes()).hexdigest()
    assert artifact(tmp_path, "https://example.invalid/provider.jar", checksum) == path
    path.write_bytes(b"tampered")
    with pytest.raises(RuntimeError, match="integrity"):
        artifact(tmp_path, "https://example.invalid/provider.jar", checksum)


def test_failed_start_cleans_directory(tmp_path: Path) -> None:
    provider = Provider("replay", tmp_path)
    with (
        patch.object(provider, "_command", side_effect=RuntimeError("synthetic-secret")),
        pytest.raises(RuntimeError, match=r"failed \(wiremock-java\)") as failure,
    ):
        provider.start()
    assert "synthetic-secret" not in str(failure.value)
    assert not provider.directory.exists()


def test_readiness_timeout_cleans_owned_process(tmp_path: Path) -> None:
    provider = Provider("replay", tmp_path, startup_timeout=0)
    process = Mock()
    process.poll.return_value = None
    with (
        patch.object(provider, "_command", return_value=["java", "synthetic"]),
        patch("subprocess.Popen", return_value=process),
        pytest.raises(RuntimeError, match=r"failed \(wiremock-java\)"),
    ):
        provider.start()
    process.terminate.assert_called_once()
    process.wait.assert_called_once_with(timeout=10)
    assert not provider.directory.exists()


def test_container_cleanup_failure_still_terminates_process(tmp_path: Path) -> None:
    provider = Provider("live", tmp_path)
    provider.container = "sunday-owned-fixture"
    process = Mock()
    process.poll.return_value = None
    provider.process = process
    with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("docker", 20)):
        provider.close()
    process.terminate.assert_called_once()
    assert not provider.directory.exists()


def test_mac_ci_failure_makes_zero_docker_calls(tmp_path: Path) -> None:
    with patch("platform.system", return_value="Darwin"), patch.dict("os.environ", {"CI": "true"}):
        provider = Provider("live", tmp_path)
    with (
        patch("oauth_support.provider.artifact", side_effect=RuntimeError("cache integrity")),
        patch("subprocess.run") as run,
        patch("subprocess.Popen") as spawn,
        patch("shutil.which") as discover,
        pytest.raises(RuntimeError, match=r"failed \(keycloak-java\)"),
    ):
        provider.start()
    run.assert_not_called()
    spawn.assert_not_called()
    discover.assert_not_called()
    assert not provider.directory.exists()


@pytest.mark.parametrize(
    "command", [[sys.executable, "-c", "import time; time.sleep(60)"], [sys.executable, "-c", "raise SystemExit(1)"]]
)
def test_real_process_failure_is_reaped(tmp_path: Path, command: list[str]) -> None:
    provider = Provider("replay", tmp_path, startup_timeout=0.2)
    with patch.object(provider, "_command", return_value=command), pytest.raises(RuntimeError, match="wiremock-java"):
        provider.start()
    assert provider.process is not None
    assert provider.process.poll() is not None
    assert not provider.directory.exists()
    provider.close()


def test_bad_download_removes_partial_artifact(tmp_path: Path) -> None:
    response = Mock()
    response.iter_bytes.return_value = [b"tampered"]
    stream = Mock()
    stream.__enter__ = Mock(return_value=response)
    stream.__exit__ = Mock(return_value=False)
    with patch("httpx.stream", return_value=stream), pytest.raises(RuntimeError, match="integrity"):
        artifact(tmp_path, "https://example.invalid/provider.jar", "invalid")
    assert list(tmp_path.iterdir()) == []
