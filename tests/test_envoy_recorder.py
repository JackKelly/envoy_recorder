from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from envoy_recorder.envoy_recorder import EnvoyRecorder
from envoy_recorder.config_loader import (
    EnvoyRecorderConfig,
    PathsConfig,
    IntervalsConfig,
    EnvoyConfig,
    LoggingConfig,
)
from envoy_recorder.schemas import ProcessedEnvoyDataFrame


@pytest.fixture
def temp_envoy_recorder(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> EnvoyRecorder:
    # Set up environment variables to point to temp directories
    live_buffer = tmp_path / "live_buffer"
    parquet_archive = tmp_path / "parquet_archive"

    # Construct a custom config object
    config = EnvoyRecorderConfig(
        paths=PathsConfig(
            live_buffer=live_buffer,
            parquet_archive=parquet_archive,
            storage_bucket="r2:bucket/directory",
        ),
        intervals=IntervalsConfig(),
        envoy=EnvoyConfig(
            ip_address="127.0.0.1",
            token="test_token",
        ),
        logging=LoggingConfig(level="DEBUG"),
    )

    # Mock EnvoyRecorderConfig.load to return our custom config
    monkeypatch.setattr(EnvoyRecorderConfig, "load", lambda *args, **kwargs: config)

    # Load config and create directories
    recorder = EnvoyRecorder()
    return recorder


def test_load_relevant_parquet_archive(temp_envoy_recorder: EnvoyRecorder) -> None:
    archive_path = temp_envoy_recorder._config.paths.parquet_archive

    # Create dummy data for May and June 2026
    may_data = pl.DataFrame(
        {
            "serial_number": ["SN123", "SN123"],
            "period_end_time": [
                datetime(2026, 5, 15, 12, 0, 0),
                datetime(2026, 5, 16, 12, 0, 0),
            ],
            "period_duration": [timedelta(minutes=15), timedelta(minutes=15)],
            "joules_produced": [100, 200],
            "ac_voltage_mV": [240000, 240000],
            "ac_current_mA": [1000, 1000],
            "dc_voltage_mV": [30000, 30000],
            "dc_current_mA": [1000, 1000],
            "ac_frequency_mHz": [50000, 50000],
            "inverter_temperature_Celsius": [25, 25],
            "power_conversion_error_seconds": [0, 0],
            "power_conversion_max_error_cycles": [0, 0],
            "flags": [0, 0],
            "watt_hours_today": [10, 20],
            "year": [2026, 2026],
            "month": [5, 5],
        },
        schema=ProcessedEnvoyDataFrame.dtypes,
    )

    june_data = pl.DataFrame(
        {
            "serial_number": ["SN123", "SN123"],
            "period_end_time": [
                datetime(2026, 6, 1, 12, 0, 0),
                datetime(2026, 6, 2, 12, 0, 0),
            ],
            "period_duration": [timedelta(minutes=15), timedelta(minutes=15)],
            "joules_produced": [300, 400],
            "ac_voltage_mV": [240000, 240000],
            "ac_current_mA": [1000, 1000],
            "dc_voltage_mV": [30000, 30000],
            "dc_current_mA": [1000, 1000],
            "ac_frequency_mHz": [50000, 50000],
            "inverter_temperature_Celsius": [25, 25],
            "power_conversion_error_seconds": [0, 0],
            "power_conversion_max_error_cycles": [0, 0],
            "flags": [0, 0],
            "watt_hours_today": [30, 40],
            "year": [2026, 2026],
            "month": [6, 6],
        },
        schema=ProcessedEnvoyDataFrame.dtypes,
    )

    # Write to partitioned parquet archive
    may_data.write_parquet(archive_path, partition_by=["year", "month"])
    june_data.write_parquet(archive_path, partition_by=["year", "month"])

    # Case 1: new_df contains only June data
    new_df_june = pl.DataFrame(
        {
            "year": [2026],
            "month": [6],
        }
    )
    loaded_df = temp_envoy_recorder._load_relevant_parquet_archive(new_df_june)
    assert loaded_df.height == 2
    assert (loaded_df["month"] == 6).all()

    # Case 2: new_df contains both May and June data (e.g. late-arriving May data)
    new_df_both = pl.DataFrame(
        {
            "year": [2026, 2026],
            "month": [5, 6],
        }
    )
    loaded_df = temp_envoy_recorder._load_relevant_parquet_archive(new_df_both)
    assert loaded_df.height == 4
    assert set(loaded_df["month"].unique().to_list()) == {5, 6}

    # Case 3: new_df is empty
    new_df_empty = pl.DataFrame(schema={"year": pl.UInt16, "month": pl.UInt8})
    loaded_df = temp_envoy_recorder._load_relevant_parquet_archive(new_df_empty)
    assert loaded_df.height == 0
