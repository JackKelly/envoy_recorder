import logging
import tomllib
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, IPvAnyAddress
from pydantic_settings import BaseSettings, SettingsConfigDict


class PathsConfig(BaseModel):
    # The live_buffer path will contain two directories: incoming and processing_<timestamp>
    live_buffer: Path = Path("./data/live_buffer")
    parquet_archive: Path = Path("./data/parquet_archive")
    storage_bucket: str  #  remote_name:bucket_name/path

    def create_directories(self) -> None:
        self.live_buffer_incoming.mkdir(parents=True, exist_ok=True)
        self.parquet_archive.mkdir(parents=True, exist_ok=True)

    @property
    def live_buffer_incoming(self) -> Path:
        return self.live_buffer / "incoming"


class IntervalsConfig(BaseModel):
    flush_buffer_every_n_minutes: int = 15


class EnvoyConfig(BaseModel):
    ip_address: IPvAnyAddress
    token: str


class LoggingConfig(BaseModel):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    log_file_for_record_script: Path | None = None


class EnvoyRecorderConfig(BaseSettings):
    model_config = SettingsConfigDict(env_nested_delimiter="__", env_prefix="ENVOY_RECORDER_")

    paths: PathsConfig = Field(default_factory=PathsConfig)
    intervals: IntervalsConfig = Field(default_factory=IntervalsConfig)
    envoy: EnvoyConfig
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    @classmethod
    def load(cls, path: str = "config.toml") -> EnvoyRecorderConfig:
        user_data = {}
        if Path(path).exists():
            with open(path, "rb") as f:
                user_data = tomllib.load(f)
        c = cls(**user_data)
        c._configure_logger()
        return c

    def _configure_logger(self) -> None:
        log = logging.getLogger()
        log.setLevel(self.logging.level)
        if self.logging.log_file_for_record_script:
            file_handler = logging.FileHandler(self.logging.log_file_for_record_script)
            log.addHandler(file_handler)
