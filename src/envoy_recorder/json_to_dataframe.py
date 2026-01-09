from pathlib import Path

import patito as pt
import polars as pl

from envoy_recorder.logging import get_logger
from envoy_recorder.schemas import ProcessedEnvoyDataFrame

log = get_logger(__name__)

# After all the jiggling around we've done above, the rows will be in arbitrary order, so we
# have to sort them as a final step. Sorting by serial number *first* should make the data
# easier to compress because the values should change fairly smoothly across rows, which Parquet
# likes because Parquet it is a columnar format.
PRIMARY_KEYS = ("serial_number", "period_end_time")

PARTITION_KEYS = ("year", "month")


def directory_of_json_files_to_dataframe(directory: Path) -> pt.DataFrame[ProcessedEnvoyDataFrame]:
    assert directory.exists(), f"{directory} does not exist!"
    assert directory.is_dir(), f"{directory} is not a directory!"
    files = list(directory.glob("*.json"))
    log.info("Loading %d json files into Polars DataFrame...", len(files))
    assert len(files) > 0, f"No *.json files found in directory {directory}!"

    df = pl.concat([pl.read_json(f) for f in files])

    # After `scan_ndjson` there's a column per device, and columns "deviceCount" and
    # "deviceDataLimit". Each device column contains a struct that looks like this:

    #     {"devName": "pcu", "sn": "482202080196", "active": true, "modGone": true, "channels": [
    #         {"chanEid": 1627390225, "created": 1767802330, etc...

    # Drop columns we don't care about
    df = df.drop(["deviceCount", "deviceDataLimit"], strict=False)

    # Turn the wide format (where each device ID is its own column) to a tall and long format where
    # there's a single device ID column, and a `stats` column which holds a struct containing data
    # for just that device ID.
    df = df.unpivot(variable_name="device_id", value_name="stats")

    # `unnest("stats")` moves the struct's fields into separate DataFrame columns:
    df = df.unnest("stats")

    # `channels` is a list of length 1 (because each IQ7+ micro-inverter only has a single PV panel
    # connected to it.) So, here, `explode("channels")` effectively just changes `channels` from a
    # list containing one struct, into that struct (removing the list).
    df = df.explode("channels").unnest("channels")

    # The code that saves Envoy data to JSONL is deliberately very simple. It doesn't check for
    # duplicates. It just grabs data from the Envoy and saves it to disk. So let's remove duplicates
    # now. 'sn' = 'serial number'; 'created' = the unix timestamp (in seconds) when the recording
    # was created.
    df = df.unique(subset=["sn", "created"])

    # We're only after data for micro-inverters (not batteries or any other kit).
    # PCU = Power Conditioning Unit (an inverter that "conditions" DC power to AC power).
    df = df.filter(pl.col("devName") == "pcu")

    # Now let's select just the columns we want:
    df = df.select(
        pl.col("sn").alias("serial_number"),
        pl.col("wattHours").struct.field("today").alias("watt_hours_today"),
        pl.col("lastReading").struct.unnest(),
    )
    df = df.drop(
        [
            "eid",
            "interval_type",
            "flags",
            "joulesUsed",
            "leadingVArs",
            "laggingVArs",
            "l1NAcVoltageInmV",
            "l2NAcVoltageInmV",
            "l3NAcVoltageInmV",
            "rssi",
            "issi",
        ],
        strict=False,
    )
    df = df.rename(
        {
            "endDate": "period_end_time",
            "duration": "period_duration",
            "joulesProduced": "joules_produced",
            "acVoltageINmV": "ac_voltage_mV",
            "acCurrentInmA": "ac_current_mA",
            "dcVoltageINmV": "dc_voltage_mV",
            "dcCurrentINmA": "dc_current_mA",
            "acFrequencyINmHz": "ac_frequency_mHz",
            "channelTemp": "inverter_temperature_Celsius",
            "pwrConvErrSecs": "power_conversion_error_seconds",
            "pwrConvMaxErrCycles": "power_conversion_max_error_cycles",
        }
    )

    # Convert the `flags_hex` string to a UInt64. `flags` is almost certainly a 64-bit bit mask,
    # because `flags_hex` is a 16-character hex string. 16 chars x 4 bits per char = 64 bits. We use
    # the `flags_hex` instead of the `flags` int because it's possible that the JSON parser will
    # have used a float during processing. So it's safest to parse the hex string.
    #
    # Gemini thinks it knows what some of the bits mean in this bit mask, e.g.:
    #
    # - bit 0: Normal production;
    # - bit 1: Envoy lost touch with this PCU;
    # - bit 2: AC freq / voltage out of range;
    # - bit 3: Possible panel/cabling insulation fault;
    # - bit 4: Inverter throttled due to heat;
    # - bit 5: Internal PCU component failure;
    #
    # I haven't checked Gemini's interpretations of this bit mask!
    df = df.with_columns(
        pl.col("flags_hex")
        .str.slice(2)  # Remove the "0x"
        .str.to_integer(base=16, dtype=pl.UInt64)  # Convert hex to integer
        .alias("flags")
    )

    # Convert times and periods
    df = df.with_columns(
        pl.from_epoch("period_end_time", time_unit="s").dt.replace_time_zone("UTC"),
        pl.duration(seconds="period_duration").alias("period_duration"),
    )

    # Append 'year' and 'month' columns for the Hive partitioning
    df = df.with_columns(
        pl.col("period_end_time").dt.year().alias("year").cast(pl.UInt16),
        pl.col("period_end_time").dt.month().alias("month").cast(pl.UInt8),
    )

    # Cast types
    df = df.cast(
        {
            "serial_number": pl.Categorical,
            #
            # --- Energy ---
            # 15 mins @ 300 W = 270,000 Joules.
            # 12 hours @ 300 W = 12,960,000 Joules.
            # All these values comfortably fit into UInt32 (max: 4 billion).
            "joules_produced": pl.UInt32,
            #
            # 24 hours @ 300 W = 7,200 Wh.
            "watt_hours_today": pl.UInt16,
            #
            # --- Raw Electrical Stats (milli-units) ---
            # 240V = 240,000 mV. Fits in UInt32.
            # Using Unsigned (UInt) because voltage and current can't be negative here.
            "ac_voltage_mV": pl.UInt32,
            "ac_current_mA": pl.UInt32,
            "dc_voltage_mV": pl.UInt32,
            "dc_current_mA": pl.UInt32,
            "ac_frequency_mHz": pl.UInt32,
            #
            # --- Temperature ---
            # Use Int8 (signed) because temperature can be negative (-20°C).
            # Int8 range is -128 to +127 °C.
            "inverter_temperature_Celsius": pl.Int8,
            #
            # --- Error Counters ---
            # Likely small numbers, but I haven't found any documentation
            # on these, so let's be safe and use Int32.
            "power_conversion_error_seconds": pl.Int32,
            "power_conversion_max_error_cycles": pl.Int32,
        }
    )

    df = df.sort(PRIMARY_KEYS)

    # Force the column ordering. Polars.concat requires the column ordering to be consistent.
    df = df.select(
        [
            "serial_number",
            "period_end_time",
            "period_duration",
            "joules_produced",
            "ac_voltage_mV",
            "ac_current_mA",
            "dc_voltage_mV",
            "dc_current_mA",
            "ac_frequency_mHz",
            "inverter_temperature_Celsius",
            "power_conversion_error_seconds",
            "power_conversion_max_error_cycles",
            "flags",
            "watt_hours_today",
            "year",
            "month",
        ]
    )

    log.info("Successfully read %d rows of data into a Polars DataFrame.", df.height)

    return ProcessedEnvoyDataFrame.validate(df)
