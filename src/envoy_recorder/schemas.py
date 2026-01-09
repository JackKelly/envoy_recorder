from datetime import datetime, timedelta

import patito as pt
import polars as pl


class ProcessedEnvoyDataFrame(pt.Model):
    serial_number: str = pt.Field(dtype=pl.Categorical)
    period_end_time: datetime = pt.Field(dtype=pl.Datetime(time_unit="us", time_zone="UTC"))
    period_duration: timedelta = pt.Field(dtype=pl.Duration(time_unit="us"))
    joules_produced: int = pt.Field(dtype=pl.UInt32)
    ac_voltage_mV: int = pt.Field(dtype=pl.UInt32)
    ac_current_mA: int = pt.Field(dtype=pl.UInt32)
    dc_voltage_mV: int = pt.Field(dtype=pl.UInt32)
    dc_current_mA: int = pt.Field(dtype=pl.UInt32)
    ac_frequency_mHz: int = pt.Field(dtype=pl.UInt32)
    inverter_temperature_Celsius: int = pt.Field(dtype=pl.Int8)
    power_conversion_error_seconds: int = pt.Field(dtype=pl.Int32)
    power_conversion_max_error_cycles: int = pt.Field(dtype=pl.Int32)
    flags: int = pt.Field(dtype=pl.UInt64)
    watt_hours_today: int = pt.Field(dtype=pl.UInt16)
