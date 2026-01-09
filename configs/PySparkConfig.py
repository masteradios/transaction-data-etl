
from dagster import Config
from pydantic import Field


class PySparkConfig(Config):
    master_url: str = Field(default="local[*]")
    python_script: str
    executor_memory: str = Field(default="4g")
    executor_cores: str = Field(default="2")