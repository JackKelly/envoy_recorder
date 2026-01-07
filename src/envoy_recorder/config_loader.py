import logging
import tomllib
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, IPvAnyAddress
from pydantic_settings import BaseSettings, SettingsConfigDict


class PathsConfig(BaseModel):
    cache_dir: Path = Path("./solar_cache")
    live_file: Path = Path("./solar_cache/live.jsonl")


class IntervalsConfig(BaseModel):
    rotate_minutes: int = 15


class EnvoyConfig(BaseModel):
    ip_address: IPvAnyAddress
    token: str


class HuggingFaceConfig(BaseModel):
    repo_id: str
    token: str


class LoggingConfig(BaseModel):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    log_file_for_record_script: Path | None = None


class EnvoyRecorderConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_nested_delimiter="__", env_prefix="ENVOY_RECORDER_"
    )

    paths: PathsConfig = Field(default_factory=PathsConfig)
    intervals: IntervalsConfig = Field(default_factory=IntervalsConfig)
    envoy: EnvoyConfig
    hugging_face: HuggingFaceConfig
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    @classmethod
    def load(cls, path: str = "config.toml"):
        user_data = {}
        if Path(path).exists():
            with open(path, "rb") as f:
                user_data = tomllib.load(f)
        c = cls(**user_data)
        log = logging.getLogger()
        log.setLevel(c.logging.level)
        if c.logging.log_file_for_record_script:
            file_handler = logging.FileHandler(c.logging.log_file_for_record_script)
            log.addHandler(file_handler)
        return c
