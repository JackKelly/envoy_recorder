import shutil
import time
from datetime import date
from pathlib import Path

import patito as pt
import polars as pl
import requests
import urllib3
from requests import Response

from envoy_recorder.config_loader import EnvoyRecorderConfig
from envoy_recorder.json_to_dataframe import PRIMARY_KEYS, directory_of_json_files_to_dataframe
from envoy_recorder.logging import get_logger
from envoy_recorder.schemas import ProcessedEnvoyDataFrame

log = get_logger(__name__)


class EnvoyRecorder:
    def __init__(self) -> None:
        self._config = EnvoyRecorderConfig.load()
        self._config.paths.create_directories()

    def run(self) -> None:
        envoy_data = self._fetch_data_from_envoy()
        self._save_to_live_buffer(envoy_data)
        if self._live_buffer_is_old_enough_to_flush():
            log.info("Flushing incoming live buffer to parquet archive...")
            new_live_buffer_path = self._move_live_buffer()
            self._append_to_parquet(new_live_buffer_path)

    def _fetch_data_from_envoy(self) -> str:
        # Disable SSL Warnings because the Envoy uses self-signed certs.
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        token = self._config.envoy.token
        ip_address = self._config.envoy.ip_address
        headers = {"Authorization": f"Bearer {token}"}
        url = f"http://{ip_address}/ivp/pdm/device_data"

        log.debug("Fetching data from %s...", url)
        response: Response = requests.get(url, headers=headers, verify=False, timeout=10)
        response.raise_for_status()
        envoy_json: str = response.text
        log.debug("Successfully retrieved data from %s.", url)
        N_CHARS = 100
        log.debug(
            "First %d characters of response text (after removing whitespace for logging): '%s'",
            N_CHARS,
            envoy_json[:N_CHARS].replace("\n", "").replace(" ", ""),
        )

        return envoy_json

    def _save_to_live_buffer(self, envoy_json: str):
        t = round(time.time())
        filename = self._config.paths.live_buffer_incoming / f"{t}.json"
        log.debug("Writing Envoy JSON data to %s", filename)
        with open(filename, "w") as f:
            f.write(envoy_json)

    def _live_buffer_is_old_enough_to_flush(self) -> bool:
        oldest_buffer_file_ts = self._timestamp_of_oldest_file_in_live_buffer()
        if oldest_buffer_file_ts is None:
            return False
        age_in_seconds = round(time.time()) - oldest_buffer_file_ts
        log.debug(
            "It has been %d seconds since the current live buffer was started.", age_in_seconds
        )
        assert age_in_seconds >= 0
        age_of_live_file_in_minutes = round(age_in_seconds / 60)
        return age_of_live_file_in_minutes > self._config.intervals.flush_buffer_every_n_minutes

    def _timestamp_of_oldest_file_in_live_buffer(self) -> int | None:
        live_buffer_filenames = sorted(self._config.paths.live_buffer_incoming.glob("*.json"))
        if len(live_buffer_filenames) == 0:
            return None
        else:
            return int(live_buffer_filenames[0].stem)

    def _move_live_buffer(self) -> Path:
        """Moving is an atomic filesystem operation."""
        old_path = self._config.paths.live_buffer_incoming
        t = round(time.time())
        new_path = self._config.paths.live_buffer / f"processing_{t}"
        log.info("Moving %s to %s", old_path, new_path)
        return old_path.rename(new_path)

    def _append_to_parquet(self, buffer_processing_path: Path) -> None:
        new_df = directory_of_json_files_to_dataframe(buffer_processing_path)
        old_df = self._load_last_month_of_parquet_archive()
        merged_df = old_df.vstack(new_df)
        merged_df = merged_df.unique(subset=PRIMARY_KEYS)
        merged_df = merged_df.sort(PRIMARY_KEYS)
        start, end = merged_df.select(
            start=pl.col("period_end_time").min(), end=pl.col("period_end_time").max()
        )
        log.info(
            "The merged dataframe has %d rows of data, from %s to %s.",
            merged_df.height,
            start.item(),
            end.item(),
        )
        if merged_df.equals(old_df):
            # Don't bother writing a new Parquet to disk if there's no new data. For example, this
            # will happen at night, when the inverters stop reporting data but the envoy repeats the
            # last reading from earlier in the day.
            log.info("merged dataframe == old dataframe. Nothing to save to disk.")
        else:
            merged_df.write_parquet(
                self._config.paths.parquet_archive,
                partition_by=["year", "month"],
                compression="zstd",
            )
        shutil.rmtree(buffer_processing_path)

    def _load_last_month_of_parquet_archive(self) -> pt.DataFrame[ProcessedEnvoyDataFrame]:
        """Load from disk.

        If there is no parquet on disk then return an empty dataframe.
        """
        is_empty = not any(self._config.paths.parquet_archive.iterdir())
        if is_empty:
            # Return empty DataFrame.
            log.info("The parquet archive is currently empty.")
            df = pl.DataFrame(schema=ProcessedEnvoyDataFrame.dtypes)
            return pt.DataFrame[ProcessedEnvoyDataFrame](df)

        df = pl.scan_parquet(self._config.paths.parquet_archive)

        # The Parquet archive uses monthly Hive partitions. Load the last month of data:
        last_date: date = df.select(pl.col("period_end_time").max()).collect().item().date()
        first_day_of_last_month = last_date.replace(day=1)
        df = df.filter(pl.col("period_end_time") >= first_day_of_last_month)
        df = df.collect()
        log.info(
            "Loaded %d rows from the parquet archive, starting at %s.",
            df.height,
            first_day_of_last_month.strftime("%Y-%m-%d"),
        )

        return pt.DataFrame[ProcessedEnvoyDataFrame](df)
