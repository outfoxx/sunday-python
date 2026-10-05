"""Provider selection and artifact-integrity failures never fall back to replay."""

import hashlib
import subprocess
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
