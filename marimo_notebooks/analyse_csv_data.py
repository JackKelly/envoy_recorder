import marimo

__generated_with = "0.18.4"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import altair as alt
    return alt, pl


@app.cell
def _(pl):
    inverters = pl.read_csv("individual_inverter_production.csv")

    # Convert the datetime
    inverters = inverters.with_columns(
        pl.from_epoch("created", time_unit="s"),
        pl.from_epoch("data_retrieval_time", time_unit="s"),
        pl.from_epoch("lastReading_endDate", time_unit="s"),
        pl.from_epoch("lifetime_createdTime", time_unit="s"),
        pl.from_epoch("chanEid", time_unit="s"),
        pl.from_epoch("lastReading_eid", time_unit="s"),
        pl.duration(seconds="lastReading_duration").alias("lastReading_duration"),
        pl.duration(seconds="lifetime_duration").alias("lifetime_duration"),
    ).sort(["data_retrieval_time", "created", "device_id"])

    inverters
    return (inverters,)


@app.cell
def _(inverters, pl):
    print("Durations in seconds between each 'created' timestamp, per device ID:\n")
    for device_id, data in inverters.group_by("device_id"):
        diffs = data.select(pl.col("created").diff().alias("diff")).filter(pl.col("diff") > 0)
        print(f"ID {device_id[0]} = {diffs.to_series()}")
    return


@app.cell
def _(inverters, pl):
    all_same = inverters.select((pl.col("created") == pl.col("lastReading_endDate")).all()).to_numpy()[0, 0]
    print("Is the 'created' timestamp always the same as 'lastReading_endDate'?", all_same)
    return


@app.cell
def _(alt, inverters):
    # Plot bars of energy for each inverter, using the actual duration and endDate.
    _data_to_plot = (
        inverters.select("device_id", "created", "lastReading_endDate", "lastReading_joulesProduced", "watts_now")
        .unique(subset=["device_id", "created"])
        .sort(["created", "device_id"])
        # .filter(pl.col("device_id") == 553650688)
    )
    inverter_plot = (
        alt.Chart(_data_to_plot)
        .mark_bar(width=2)
        .encode(
            x=alt.X(
                "lastReading_endDate",
                type="temporal",
                axis=alt.Axis(
                    grid=True,
                    gridWidth=0.2,
                    # tickCount=alt.TimeIntervalStep(interval="minute", step=15),
                    format="%H:%M",
                ),
            ),
            y=alt.Y("watts_now", axis=alt.Axis(grid=False)),
            color=alt.Color("device_id", type="ordinal"),
        )
    )
    inverter_plot
    return (inverter_plot,)


@app.cell
def _(pl):
    whole_system = (
        pl.read_csv("whole_system_production.csv")
        .filter(pl.col("readingTime") > 0)
        .with_columns(
            pl.from_epoch("readingTime", time_unit="s"),
            pl.from_epoch("data_retrieval_time", time_unit="s"),
        )
        .sort(["data_retrieval_time", "readingTime"])
    )
    whole_system
    return (whole_system,)


@app.cell
def _(whole_system):
    whole_system.unique("readingTime").sort("readingTime").select("readingTime").to_series().diff()
    return


@app.cell
def _(alt, inverter_plot, pl, whole_system):
    _data_to_plot = whole_system.unique("readingTime").sort("readingTime")
    _data_to_plot = _data_to_plot.with_columns((pl.col("wNow") / 10).alias("wNow_scaled"))
    whole_system_base = alt.Chart(_data_to_plot).encode(
        x=alt.X(
            "readingTime",
            type="temporal",
            axis=alt.Axis(
                grid=True,
                gridWidth=0.2,
                format="%H:%M",
            ),
        ),
        y="wNow_scaled",
    )

    whole_system_line = whole_system_base.mark_line()
    whole_system_points = whole_system_base.mark_point()

    (whole_system_line + whole_system_points + inverter_plot).properties(height=400).interactive()
    return


@app.cell
def _(inverters, pl):
    inverters_unique = inverters.unique(subset=["device_id", "created"]).with_columns(
        (pl.col("lastReading_joulesProduced") / pl.col("lastReading_duration").dt.total_seconds())
        .round()
        .alias("joules_div_duration")
    )

    inverters_unique.select(pl.col("joules_div_duration") == pl.col("watts_now")).to_series().all()
    return (inverters_unique,)


@app.cell
def _(inverters_unique):
    inverters_unique
    return


@app.cell
def _(inverters_unique):
    for _id, data_for_id in inverters_unique.group_by("device_id"):
        data_for_id = data_for_id.sort("created")
        Wh_since_last_reading = data_for_id.select("wattHours_today").to_series().diff()
        break
    return (data_for_id,)


@app.cell
def _(data_for_id, pl):
    data_for_id.with_columns(
        pl.col("wattHours_today").diff().alias("wH_today_diff"), (pl.col("watts_now") / 4).alias("watts_now_div_4")
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
