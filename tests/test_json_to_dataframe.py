import json
from pathlib import Path

import polars as pl

from envoy_recorder.json_to_dataframe import envoy_json_files_to_dataframe


def test_envoy_json_to_dataframe_real_data():
    json_path = Path(__file__).parent.parent / "example_envoy_json_data"
    df = envoy_json_files_to_dataframe(json_path)

    assert len(df) > 0
    assert "serial_number" in df.columns
    assert "period_end_time" in df.columns
    assert df["serial_number"].dtype == pl.Categorical
    assert df["joules_produced"].dtype == pl.UInt32


def test_filtering_non_pcu_devices(tmp_path: Path):
    # Create data with one PCU and one non-PCU
    envoy_json = {
        "deviceCount": 2,
        "deviceDataLimit": 100,
        "123": {
            "devName": "pcu",
            "sn": "SN123",
            "active": True,
            "modGone": False,
            "channels": [
                {
                    "chanEid": 1,
                    "created": 1000,
                    "wattHours": {"today": 10},
                    "lastReading": {
                        "eid": 1,
                        "interval_type": 0,
                        "endDate": 1000,
                        "duration": 900,
                        "flags": 0,
                        "flags_hex": "0x0",
                        "joulesProduced": 100,
                        "acVoltageINmV": 240000,
                        "acFrequencyINmHz": 50000,
                        "dcVoltageINmV": 30000,
                        "dcCurrentINmA": 1000,
                        "channelTemp": 25,
                        "pwrConvErrSecs": 0,
                        "pwrConvMaxErrCycles": 0,
                        "joulesUsed": 0,
                        "leadingVArs": 0,
                        "laggingVArs": 0,
                        "acCurrentInmA": 0,
                        "l1NAcVoltageInmV": 0,
                        "l2NAcVoltageInmV": 0,
                        "l3NAcVoltageInmV": 0,
                        "rssi": 0,
                        "issi": 0,
                    },
                }
            ],
        },
        "456": {
            "devName": "battery",
            "sn": "SN456",
            "active": True,
            "modGone": False,
            "channels": [],
        },
    }

    json_path = tmp_path / "filter.json"
    with open(json_path, "w") as f:
        json.dump(envoy_json, f)

    df = envoy_json_files_to_dataframe(json_path)

    # Only SN123 should be present
    assert len(df) == 1
    assert df["serial_number"][0] == "SN123"


def test_deduplication(tmp_path: Path):
    # Same data retrieved twice
    device_data = {
        "deviceCount": 1,
        "deviceDataLimit": 100,
        "123": {
            "devName": "pcu",
            "sn": "SN123",
            "active": True,
            "modGone": False,
            "channels": [
                {
                    "chanEid": 1,
                    "created": 1000,
                    "wattHours": {"today": 10},
                    "lastReading": {
                        "eid": 1,
                        "interval_type": 0,
                        "endDate": 1000,
                        "duration": 900,
                        "flags": 0,
                        "flags_hex": "0x0",
                        "joulesProduced": 100,
                        "acVoltageINmV": 240000,
                        "acFrequencyINmHz": 50000,
                        "dcVoltageINmV": 30000,
                        "dcCurrentINmA": 1000,
                        "channelTemp": 25,
                        "pwrConvErrSecs": 0,
                        "pwrConvMaxErrCycles": 0,
                        "joulesUsed": 0,
                        "leadingVArs": 0,
                        "laggingVArs": 0,
                        "acCurrentInmA": 0,
                        "l1NAcVoltageInmV": 0,
                        "l2NAcVoltageInmV": 0,
                        "l3NAcVoltageInmV": 0,
                        "rssi": 0,
                        "issi": 0,
                    },
                }
            ],
        },
    }

    for i in range(2):
        with open(tmp_path / f"dedupe_{i}.json", mode="w") as f:
            json.dump(device_data, f)

    df = envoy_json_files_to_dataframe(tmp_path)

    # Should only have 1 row after deduplication
    assert len(df) == 1
