import marimo

__generated_with = "0.19.1"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    from envoy_recorder.schemas import ProcessedEnvoyDataFrame
    return mo, pl


@app.cell
def _(pl):
    df = pl.scan_parquet("data/parquet_archive/").collect()
    df
    return (df,)


@app.cell
def _(df, mo):
    unique_inverters = df["serial_number"].unique().sort().to_list()
    selected_inverters = mo.ui.multiselect(options=unique_inverters, value=unique_inverters)
    return (selected_inverters,)


@app.cell
def _(df, mo, pl, selected_inverters):
    import altair as alt
    import datetime

    # Calculate watts by dividing joules_produced by period_duration in seconds
    # period_duration is in microseconds, so we convert it to seconds for the calculation
    df_with_watts = df.with_columns(
        (pl.col("joules_produced") / pl.col("period_duration").dt.total_seconds()).alias("watts")
    )

    filtered_df = df_with_watts.drop(["period_duration"]).filter(
        [
            pl.col("period_end_time") > datetime.datetime(2026, 1, 10, 8, tzinfo=datetime.timezone.utc),
            pl.col("serial_number").is_in(selected_inverters.value),
        ]
    )

    # Create an Altair line plot
    chart = (
        alt.Chart(filtered_df)
        .mark_line(point=True, strokeWidth=1)
        .encode(
            # X-axis: period_end_time as a temporal scale
            x=alt.X("period_end_time:T", title="Time", axis=alt.Axis(format="%H:%M")),
            # Y-axis: calculated watts as a quantitative scale
            y=alt.Y("watts:Q", title="Power (Watts)"),
            # Color lines by serial_number to distinguish each micro-inverter
            color=alt.Color("serial_number:N", title="Micro-inverter Serial Number"),
            # Add tooltips for interactive data exploration
            tooltip=[
                alt.Tooltip("period_end_time:T", title="Time", format="%Y-%m-%d %H:%M:%S"),
                alt.Tooltip("serial_number:N", title="Serial Number"),
                alt.Tooltip("watts:Q", title="Power (Watts)", format=".2f"),
                alt.Tooltip("joules_produced:Q", title="Joules Produced"),
                # Display period_duration in microseconds, as it is in the schema
                # alt.Tooltip("period_duration", title="Period Duration (µs)")
            ],
        )
        .properties(title="Power Output (Watts) of Micro-inverters over Time", height=600)
        .interactive()
    )

    mo.vstack([selected_inverters, chart])
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
