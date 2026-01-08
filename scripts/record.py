import json
import time
from pathlib import Path
from typing import Any, TypedDict

import requests
import urllib3
from requests import Response

from envoy_recorder.config_loader import EnvoyRecorderConfig
from envoy_recorder.logging import get_logger

log = get_logger(__name__)


class WrappedEnvoyData(TypedDict):
    """Wrap the Envoy data in a new JSON object that includes the retrieval
    time, to make it trivial to decide when to rotate the filenames."""

    retrieval_time: int  # Unix timestamp in seconds
    data: dict[str, Any]  # The raw Enphase JSON


def fetch_data_from_envoy(config: EnvoyRecorderConfig) -> WrappedEnvoyData:
    # Disable SSL Warnings (Envoy uses self-signed certs)
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    token = config.envoy.token
    ip_address = config.envoy.ip_address
    headers = {"Authorization": f"Bearer {token}"}
    retrieval_time = round(time.time())
    url = f"http://{ip_address}/ivp/pdm/device_data"

    log.debug("Fetching data from %s...", url)
    response: Response = requests.get(url, headers=headers, verify=False, timeout=10)
    response.raise_for_status()
    try:
        data = response.json()
    except requests.exceptions.JSONDecodeError:
        log.exception(
            "Failed to decode JSON from Envoy! URL=%s. The raw text response from the Envoy is: '%s'",
            url,
            response.text,
        )
        raise
    else:
        log.debug("Successfully retrieved data from %s.", url)

    return {"retrieval_time": retrieval_time, "data": data}


def save(config: EnvoyRecorderConfig, data: WrappedEnvoyData):
    cache_dir: Path = config.paths.cache_dir
    if not cache_dir.exists():
        log.info(
            "Cache directory %s does not exist yet, so we will create it now.",
            cache_dir,
        )
        cache_dir.mkdir(parents=True)
    live_file: Path = config.paths.live_file
    log.debug("Appending JSON data to %s", live_file)
    json_string = json.dumps(data)
    with open(live_file, "a") as f:
        f.write(json_string + "\n")


def load_retrieval_time_from_file(live_file: Path) -> int:
    with live_file.open(mode="r") as f:
        first_line = f.readline()
    first_line_dict: WrappedEnvoyData = json.loads(first_line)
    return first_line_dict["retrieval_time"]


def rotate_if_necessary(config: EnvoyRecorderConfig):
    live_file = config.paths.live_file
    retrieval_time = load_retrieval_time_from_file(live_file)
    age_in_seconds = round(time.time()) - retrieval_time
    log.debug("Age of %s is %d seconds", live_file, age_in_seconds)
    assert age_in_seconds >= 0
    age_of_live_file_in_minutes = round(age_in_seconds / 60)

    if age_of_live_file_in_minutes > config.intervals.rotate_minutes:
        archive_filename = config.paths.cache_dir / f"archive_{retrieval_time}.jsonl"
        log.info("Moving %s to %s", live_file, archive_filename)
        live_file.rename(archive_filename)


def main() -> None:
    config = EnvoyRecorderConfig.load()
    envoy_data = fetch_data_from_envoy(config)
    save(config, envoy_data)
    rotate_if_necessary(config)


if __name__ == "__main__":
    try:
        main()
    except:
        log.exception("Exception raised in main()!")
        raise
