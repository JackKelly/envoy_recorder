import json
from pathlib import Path

import polars as pl

from envoy_recorder.jsonl_to_dataframe import envoy_jsonl_to_dataframe


def test_envoy_jsonl_to_dataframe_real_data():
    jsonl_path = Path(__file__).parent / "test_data.jsonl"
    df = envoy_jsonl_to_dataframe(jsonl_path)

    # We know there are 4 lines with 10 PCUs each, but they might be duplicates.
    # From the read output of live.jsonl, they looked very similar.
    assert len(df) > 0
    assert "serial_number" in df.columns
    assert "period_end_time" in df.columns
    assert df["serial_number"].dtype == pl.Categorical
    assert df["joules_produced"].dtype == pl.UInt32


def test_filtering_non_pcu_devices(tmp_path: Path):
    # Create data with one PCU and one non-PCU
    data = {
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

    jsonl_path = tmp_path / "filter.jsonl"
    retrieval_time = 2000
    envoy_json = json.dumps(data)
    line = f'{{"retrieval_time": {retrieval_time}, "envoy_json": {envoy_json}}}'
    with open(jsonl_path, "w") as f:
        f.write(line + "\n")

    df = envoy_jsonl_to_dataframe(jsonl_path)

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

    jsonl_path = tmp_path / "dedup.jsonl"
    envoy_json = json.dumps(device_data)
    line1 = f'{{"retrieval_time": 2000, "envoy_json": {envoy_json}}}'
    line2 = f'{{"retrieval_time": 2100, "envoy_json": {envoy_json}}}'
    with open(jsonl_path, "w") as f:
        f.write(line1 + "\n")
        f.write(line2 + "\n")

    df = envoy_jsonl_to_dataframe(jsonl_path)

    # Should only have 1 row after deduplication
    assert len(df) == 1
