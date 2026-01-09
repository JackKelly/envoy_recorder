import marimo

__generated_with = "0.19.0"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import datetime
    import os
    return datetime, mo, os, pl


@app.cell
def _(datetime, mo, os, pl):
    def create_dummy_dataframe(start_date: datetime.date, end_date: datetime.date) -> pl.DataFrame:
        # Create a Series of dates using pl.date_range
        date_series: pl.Series = pl.date_range(
            start=start_date,
            end=end_date,
            interval="1d",  # '1d' for daily, '1mo' for monthly, '1h' for hourly, etc.
            closed="both",  # Include both start and end dates
            eager=True,  # Return a Series immediately
        ).dt.date()  # Convert to date objects if you want pure dates, otherwise it's datetime

        # Create the Polars DataFrame
        df = pl.DataFrame({"date": date_series})

        # Add year and month columns for Hive partitioning
        return df.with_columns(
            pl.col("date").dt.ordinal_day().alias("value"),
            pl.col("date").dt.year().alias("year"),
            pl.col("date").dt.month().alias("month"),
        )


    # Define the output directory
    output_dir: str = "dummy_data_parquet"

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Save to Hive-partitioned Parquet
    # Polars `write_parquet` supports partitioning directly
    df_partitioned = create_dummy_dataframe(datetime.date(2023, 1, 1), datetime.date(2023, 2, 10))
    df_partitioned.write_parquet(
        file=output_dir,
        partition_by=["year", "month"],
    )

    mo.md(f"Dummy data saved to `{output_dir}` partitioned by year and month.")
    return create_dummy_dataframe, output_dir


@app.cell
def _(output_dir: str, pl):
    df_loaded = pl.scan_parquet(output_dir)
    df_loaded
    return (df_loaded,)


@app.cell
def _(df_loaded, pl):
    max_month = df_loaded.select(pl.col("month").max()).collect().item()

    df_last_month_polars: pl.DataFrame = (
        df_loaded.filter(
            pl.col("month") == max_month
        ).collect()  # This triggers the execution and loads the filtered data into RAM
    )


    df_last_month_polars
    return (df_last_month_polars,)


@app.cell
def _(create_dummy_dataframe, datetime, df_last_month_polars: "pl.DataFrame"):
    df_to_append = create_dummy_dataframe(datetime.date(2023, 2, 11), datetime.date(2023, 3, 4))

    merged = df_last_month_polars.vstack(df_to_append)
    merged
    return (merged,)


@app.cell
def _(merged, output_dir: str):
    merged.write_parquet(
        file=output_dir,
        partition_by=["year", "month"],
    )
    return


@app.cell
def _(output_dir: str, pl):
    pl.scan_parquet(output_dir).collect()
    return


@app.cell
def _(pl):
    from envoy_recorder.schemas import ProcessedEnvoyDataFrame
    pl.scan_parquet("*.parquet", schema=ProcessedEnvoyDataFrame.dtypes)
    return


@app.cell
def _(df_loaded, pl):
    start, end = df_loaded.select(min=pl.col("date").min(), max=pl.col("date").max()).collect()
    return (start,)


@app.cell
def _(start):
    start.item()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
