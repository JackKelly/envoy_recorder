import pytest
from pathlib import Path
from pydantic import ValidationError
from envoy_recorder.config_loader import (
    EnvoyRecorderConfig,
    PathsConfig,
    IntervalsConfig,
)


def test_default_paths() -> None:
    paths = PathsConfig()
    assert paths.cache_dir == Path("./solar_cache")
    assert paths.live_file == Path("./solar_cache/live.jsonl")


def test_default_intervals() -> None:
    intervals = IntervalsConfig()
    assert intervals.rotate_minutes == 15


def test_load_non_existent_file_fails_if_missing_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Clear environment variables to ensure we don't pick up anything from the host
    monkeypatch.delenv("ENVOY_RECORDER_ENVOY__IP_ADDRESS", raising=False)
    monkeypatch.delenv("ENVOY_RECORDER_ENVOY__TOKEN", raising=False)
    monkeypatch.delenv("ENVOY_RECORDER_HUGGING_FACE__REPO_ID", raising=False)
    monkeypatch.delenv("ENVOY_RECORDER_HUGGING_FACE__TOKEN", raising=False)

    with pytest.raises(ValidationError):
        EnvoyRecorderConfig.load("non_existent_config.toml")


def test_load_valid_toml(tmp_path: Path) -> None:
    config_file = tmp_path / "config.toml"
    content = """
[envoy]
ip_address = "192.168.1.100"
token = "envoy_secret"

[hugging_face]
repo_id = "user/repo"
token = "hf_secret"

[paths]
cache_dir = "/tmp/cache"
live_file = "/tmp/cache/live.jsonl"

[intervals]
rotate_minutes = 30
"""
    config_file.write_text(content)

    config = EnvoyRecorderConfig.load(str(config_file))

    assert str(config.envoy.ip_address) == "192.168.1.100"
    assert config.envoy.token == "envoy_secret"
    assert config.hugging_face.repo_id == "user/repo"
    assert config.hugging_face.token == "hf_secret"
    assert config.paths.cache_dir == Path("/tmp/cache")
    assert config.paths.live_file == Path("/tmp/cache/live.jsonl")
    assert config.intervals.rotate_minutes == 30


def test_load_with_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENVOY_RECORDER_ENVOY__IP_ADDRESS", "1.2.3.4")
    monkeypatch.setenv("ENVOY_RECORDER_ENVOY__TOKEN", "envoy_env_token")
    monkeypatch.setenv("ENVOY_RECORDER_HUGGING_FACE__REPO_ID", "env/repo")
    monkeypatch.setenv("ENVOY_RECORDER_HUGGING_FACE__TOKEN", "hf_env_token")

    # We still call load, which will try to load config.toml (might not exist)
    # but BaseSettings should pick up environment variables.
    config = EnvoyRecorderConfig.load("non_existent.toml")

    assert str(config.envoy.ip_address) == "1.2.3.4"
    assert config.envoy.token == "envoy_env_token"
    assert config.hugging_face.repo_id == "env/repo"
    assert config.hugging_face.token == "hf_env_token"


def test_load_partial_toml(tmp_path: Path) -> None:
    config_file = tmp_path / "config.toml"
    # Only provide required fields, let others use defaults
    content = """
[envoy]
ip_address = "192.168.1.100"
token = "envoy_secret"

[hugging_face]
repo_id = "user/repo"
token = "hf_secret"
"""
    config_file.write_text(content)

    config = EnvoyRecorderConfig.load(str(config_file))

    assert str(config.envoy.ip_address) == "192.168.1.100"
    assert config.paths.cache_dir == Path("./solar_cache")  # default
    assert config.intervals.rotate_minutes == 15  # default


def test_load_empty_toml_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ENVOY_RECORDER_ENVOY__IP_ADDRESS", raising=False)
    monkeypatch.delenv("ENVOY_RECORDER_ENVOY__TOKEN", raising=False)
    monkeypatch.delenv("ENVOY_RECORDER_HUGGING_FACE__REPO_ID", raising=False)
    monkeypatch.delenv("ENVOY_RECORDER_HUGGING_FACE__TOKEN", raising=False)

    config_file = tmp_path / "config.toml"
    config_file.write_text("")

    with pytest.raises(ValidationError):
        EnvoyRecorderConfig.load(str(config_file))


def test_load_invalid_toml(tmp_path: Path) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text("invalid = [toml")

    with pytest.raises(Exception):  # tomllib.TOMLDecodeError or similar
        EnvoyRecorderConfig.load(str(config_file))


def test_invalid_ip(tmp_path: Path) -> None:
    config_file = tmp_path / "config.toml"
    content = """
[envoy]
ip_address = "not-an-ip"
token = "envoy_secret"

[hugging_face]
repo_id = "user/repo"
token = "hf_secret"
"""
    config_file.write_text(content)

    with pytest.raises(ValidationError) as excinfo:
        EnvoyRecorderConfig.load(str(config_file))
    assert "ip_address" in str(excinfo.value)
