import marimo

__generated_with = "0.18.4"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import requests
    import os
    import urllib3
    import time
    import polars as pl
    from pathlib import Path
    return Path, os, pl, requests, time, urllib3


@app.cell
def _(urllib3):
    # Disable SSL Warnings (Envoy uses self-signed certs)
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    return


@app.cell
def _(os, requests, time):
    token = os.environ["ENPHASE_TOKEN"]
    headers = {"Authorization": f"Bearer {token}"}
    url = "http://envoy.local/ivp/pdm/device_data"

    data_retrieval_time = round(time.time())

    # TODO: Handle exceptions thrown by `get`
    response = requests.get(url, headers=headers, verify=False, timeout=10)
    response.raise_for_status()

    data = response.json()
    data
    return data, data_retrieval_time


@app.cell
def _():
    def flatten_dict(in_dict: dict, prefixes: tuple[str, ...] = ()) -> dict:
        flat_dict = {}
        for k, v in in_dict.items():
            if isinstance(v, dict):
                new_prefixes = prefixes + (k,)
                flat_dict.update(flatten_dict(v, new_prefixes))
            else:
                new_key = "_".join(prefixes + (k,))
                assert new_key not in flat_dict, (
                    f"{new_key} already in flat_dict! Old value = {flat_dict[new_key]}, new value = {v}."
                )
                flat_dict[new_key] = v
        return flat_dict


    # Test!
    assert flatten_dict({"a": 1, "b": {"c": 3, "d": {"e": 10}}}) == {"a": 1, "b_c": 3, "b_d_e": 10}
    return (flatten_dict,)


@app.cell
def _():
    # Created with:
    #     columns = list(records[0].keys())
    #     columns.sort()
    # And then manually tweaked!
    columns = [
        "device_id",
        "sn",
        "created",
        "data_retrieval_time",
        "active",
        "chanEid",
        "modGone",
        "watts_max",
        "watts_now",
        "watts_nowUsed",
        "wattHours_today",
        "wattHours_week",
        "wattHours_yesterday",
        "lastReading_acCurrentInmA",
        "lastReading_acFrequencyINmHz",
        "lastReading_acVoltageINmV",
        "lastReading_channelTemp",
        "lastReading_dcCurrentINmA",
        "lastReading_dcVoltageINmV",
        "lastReading_duration",
        "lastReading_eid",
        "lastReading_endDate",
        "lastReading_flags",
        "lastReading_flags_hex",
        "lastReading_interval_type",
        "lastReading_issi",
        "lastReading_joulesProduced",
        "lastReading_joulesUsed",
        "lastReading_l1NAcVoltageInmV",
        "lastReading_l2NAcVoltageInmV",
        "lastReading_l3NAcVoltageInmV",
        "lastReading_laggingVArs",
        "lastReading_leadingVArs",
        "lastReading_pwrConvErrSecs",
        "lastReading_pwrConvMaxErrCycles",
        "lastReading_rssi",
        "lifetime_createdTime",
        "lifetime_duration",
        "lifetime_joulesProduced",
    ]
    return (columns,)


@app.cell
def _(columns, data, data_retrieval_time, flatten_dict):
    records = []

    # The JSON keys are "Device IDs" (e.g., "553648384"), not Serial Numbers.
    for key, value in data.items():
        if key in ("deviceCount", "deviceDataLimit"):
            continue

        # Sanity checks:
        assert isinstance(value, dict)
        assert value["devName"] == "pcu"  # PCU = Power Conditioning Unit (Inverter)
        channels = value["channels"]
        assert len(channels) == 1, f"Expected exactly 1 channel for inverter ID {key}, found {len(channels)}."

        # Extract the data we want, and flatten
        channel_data = channels[0]
        flat_channel_data = flatten_dict(channel_data)
        for k in ("sn", "active", "modGone"):
            flat_channel_data[k] = value[k]
        flat_channel_data["device_id"] = key
        flat_channel_data["data_retrieval_time"] = data_retrieval_time

        assert set(flat_channel_data.keys()) == set(columns)
        records.append(flat_channel_data)
    return (records,)


@app.cell
def _(columns, pl, records):
    df = pl.DataFrame(records, schema=columns)
    df
    return (df,)


@app.cell
def _(Path, df):
    csv_filename = Path("pv_data.csv")

    include_header = not csv_filename.exists()
    csv_string = df.write_csv(include_header=include_header)
    with csv_filename.open(mode="a") as _f:
        _f.write(csv_string)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
