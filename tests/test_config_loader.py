from pathlib import Path

import pytest
from pydantic import ValidationError

from envoy_recorder.config_loader import (
    EnvoyRecorderConfig,
    IntervalsConfig,
    PathsConfig,
)


def test_default_paths() -> None:
    paths = PathsConfig(storage_bucket="r2:bucket/directory")
    assert paths.live_buffer == Path("./data/live_buffer")
    assert paths.live_buffer_incoming == Path("./data/live_buffer/incoming")
    assert paths.parquet_archive == Path("./data/parquet_archive")
    assert paths.storage_bucket == "r2:bucket/directory"


def test_default_intervals() -> None:
    intervals = IntervalsConfig()
    assert intervals.flush_buffer_every_n_minutes == 15


def test_load_non_existent_file_fails_if_missing_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Clear environment variables to ensure we don't pick up anything from the host
    monkeypatch.delenv("ENVOY_RECORDER_ENVOY__IP_ADDRESS", raising=False)
    monkeypatch.delenv("ENVOY_RECORDER_ENVOY__TOKEN", raising=False)
    monkeypatch.delenv("ENVOY_RECORDER_PATHS__STORAGE_BUCKET", raising=False)

    with pytest.raises(ValidationError):
        EnvoyRecorderConfig.load("non_existent_config.toml")


def test_load_valid_toml(tmp_path: Path) -> None:
    config_file = tmp_path / "config.toml"
    content = """
[envoy]
ip_address = "192.168.1.100"
token = "envoy_secret"

[paths]
live_buffer = "/tmp/live_buffer"
parquet_archive = "/tmp/parquet_archive"
storage_bucket = "remote:bucket/directory"

[intervals]
flush_buffer_every_n_minutes = 30
"""
    config_file.write_text(content)

    config = EnvoyRecorderConfig.load(str(config_file))

    assert str(config.envoy.ip_address) == "192.168.1.100"
    assert config.envoy.token == "envoy_secret"
    assert config.paths.live_buffer == Path("/tmp/live_buffer")
    assert config.paths.parquet_archive == Path("/tmp/parquet_archive")
    assert config.paths.storage_bucket == "remote:bucket/directory"
    assert config.intervals.flush_buffer_every_n_minutes == 30


def test_load_with_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENVOY_RECORDER_ENVOY__IP_ADDRESS", "1.2.3.4")
    monkeypatch.setenv("ENVOY_RECORDER_ENVOY__TOKEN", "envoy_env_token")
    monkeypatch.setenv("ENVOY_RECORDER_PATHS__STORAGE_BUCKET", "remote:bucket/path")

    # We still call load, which will try to load config.toml (might not exist)
    # but BaseSettings should pick up environment variables.
    config = EnvoyRecorderConfig.load("non_existent.toml")

    assert str(config.envoy.ip_address) == "1.2.3.4"
    assert config.envoy.token == "envoy_env_token"
    assert config.paths.storage_bucket == "remote:bucket/path"


def test_load_partial_toml(tmp_path: Path) -> None:
    config_file = tmp_path / "config.toml"
    # Only provide required fields, let others use defaults
    content = """
[envoy]
ip_address = "192.168.1.100"
token = "envoy_secret"

[paths]
storage_bucket = "foo/bar"
"""
    config_file.write_text(content)

    config = EnvoyRecorderConfig.load(str(config_file))

    assert str(config.envoy.ip_address) == "192.168.1.100"
    assert config.paths.live_buffer == Path("./data/live_buffer")  # default
    assert config.intervals.flush_buffer_every_n_minutes == 15  # default


def test_load_empty_toml_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ENVOY_RECORDER_ENVOY__IP_ADDRESS", raising=False)
    monkeypatch.delenv("ENVOY_RECORDER_ENVOY__TOKEN", raising=False)

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

[paths]
storage_bucket = "foo/bar"
"""
    config_file.write_text(content)

    with pytest.raises(ValidationError) as excinfo:
        EnvoyRecorderConfig.load(str(config_file))
    assert "ip_address" in str(excinfo.value)
