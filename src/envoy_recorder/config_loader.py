import tomllib
from pathlib import Path

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


class PathsConfig(BaseModel):
    cache_dir: Path = Path("./solar_cache")
    live_file: Path = Path("./solar_cache/live.jsonl")


class IntervalsConfig(BaseModel):
    rotate_minutes: int = 15


class EnvoyConfig(BaseModel):
    ip_address: str
    token: str


class HuggingFaceConfig(BaseModel):
    repo_id: str
    token: str


class EnvoyRecorderConfig(BaseSettings):
    paths: PathsConfig = Field(default_factory=PathsConfig)
    intervals: IntervalsConfig = Field(default_factory=IntervalsConfig)
    envoy: EnvoyConfig = Field(default_factory=EnvoyConfig)
    hugging_face: HuggingFaceConfig = Field(default_factory=HuggingFaceConfig)

    @classmethod
    def load(cls, path: str = "config.toml"):
        user_data = {}
        if Path(path).exists():
            with open(path, "rb") as f:
                user_data = tomllib.load(f)
        return cls(**user_data)
