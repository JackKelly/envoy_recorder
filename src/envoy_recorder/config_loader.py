import tomllib
from pathlib import Path

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


class EnvoyRecorderConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_nested_delimiter="__", env_prefix="ENVOY_RECORDER_"
    )

    paths: PathsConfig = Field(default_factory=PathsConfig)
    intervals: IntervalsConfig = Field(default_factory=IntervalsConfig)
    envoy: EnvoyConfig
    hugging_face: HuggingFaceConfig

    @classmethod
    def load(cls, path: str = "config.toml"):
        user_data = {}
        if Path(path).exists():
            with open(path, "rb") as f:
                user_data = tomllib.load(f)
        return cls(**user_data)
