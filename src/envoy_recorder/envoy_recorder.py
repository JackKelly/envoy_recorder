import json
import time
from pathlib import Path
from typing import TypedDict

import polars as pl
import requests
import urllib3
from requests import Response

from envoy_recorder.config_loader import EnvoyRecorderConfig
from envoy_recorder.jsonl_to_dataframe import envoy_jsonl_to_dataframe
from envoy_recorder.logging import get_logger

log = get_logger(__name__)


class WrappedEnvoyData(TypedDict):
    """Wrap the Envoy data in a new JSON object that includes the retrieval
    time, to make it trivial to decide when to rotate the filenames."""

    retrieval_time: int  # Unix timestamp in seconds
    data: str  # The raw Enphase JSON


class EnvoyRecorder:
    def __init__(self) -> None:
        self.config = EnvoyRecorderConfig.load()

    def run(self) -> None:
        envoy_data = self.fetch_data_from_envoy()
        self.save_to_jsonl_live_file(envoy_data)
        if self.live_file_is_old_enough_for_conversion():
            new_filename = self.rename_live_file()
            append_to_parquet(new_filename)

    def fetch_data_from_envoy(self) -> WrappedEnvoyData:
        # Disable SSL Warnings because the Envoy uses self-signed certs.
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        token = self.config.envoy.token
        ip_address = self.config.envoy.ip_address
        headers = {"Authorization": f"Bearer {token}"}
        retrieval_time = round(time.time())
        url = f"http://{ip_address}/ivp/pdm/device_data"

        log.debug("Fetching data from %s...", url)
        response: Response = requests.get(url, headers=headers, verify=False, timeout=10)
        response.raise_for_status()
        data: str = response.text
        data = data.strip()
        log.debug("Successfully retrieved data from %s.", url)
        log.debug("First 50 characters of response text: '%s'", data[:50])

        return {"retrieval_time": retrieval_time, "data": data}

    def save_to_jsonl_live_file(self, data: WrappedEnvoyData):
        cache_dir: Path = self.config.paths.cache_dir
        if not cache_dir.exists():
            log.info(
                "Cache directory %s does not exist yet, so we will create it now.",
                cache_dir,
            )
            cache_dir.mkdir(parents=True)
        live_file: Path = self.config.paths.live_file
        log.debug("Appending JSON data to %s", live_file)
        json_string = json.dumps(data)
        with open(live_file, "a") as f:
            f.write(json_string + "\n")

    def live_file_is_old_enough_for_conversion(self) -> bool:
        live_file = self.config.paths.live_file
        retrieval_time = first_retrieval_time_in_jsonl_file(live_file)
        age_in_seconds = round(time.time()) - retrieval_time
        log.debug("Age of %s is %d seconds", live_file, age_in_seconds)
        assert age_in_seconds >= 0
        age_of_live_file_in_minutes = round(age_in_seconds / 60)
        return age_of_live_file_in_minutes > self.config.intervals.rotate_minutes

    def rename_live_file(self) -> Path:
        old_filename = self.config.paths.live_file
        cache_dir = self.config.paths.cache_dir
        retrieval_time = first_retrieval_time_in_jsonl_file(old_filename)
        new_filename = cache_dir / f"processing_{retrieval_time}.jsonl"
        log.info("Moving %s to %s", old_filename, new_filename)
        return old_filename.rename(new_filename)

    def append_to_parquet(self, jsonl_filename: Path) -> None:
        new_df = envoy_jsonl_to_dataframe(jsonl_filename)
        old_df = self.load_most_recent_parquet_partition()
        merged_df = pl.concat((old_df, new_df))
        # TODO(Jack):
        # - Figure out if the merged_df spans multiple months. Or does Polars handle this for us?
        # - Write the merged data back to disk.

    def load_most_recent_parquet_partition(self) -> pl.DataFrame:
        """Load from disk.

        If there is no parquet on disk then return an empty dataframe.
        """
        # TODO(Jack)


def first_retrieval_time_in_jsonl_file(live_file: Path) -> int:
    with live_file.open(mode="r") as f:
        first_line = f.readline()
    first_line_dict: WrappedEnvoyData = json.loads(first_line)
    return first_line_dict["retrieval_time"]
