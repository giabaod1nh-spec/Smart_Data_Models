"""Validated isolated V1 configuration."""
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

class V1RawSettings(BaseSettings):
    model_config=SettingsConfigDict(env_prefix="DE_V1_RAW_",extra="ignore")
    bootstrap_servers: str="localhost:29092"
    topic: str="traffic.observations.v1"
    group_id: str="de-kafka-raw-v1"
    clickhouse_host: str="localhost"
    clickhouse_port: int=Field(default=8123,ge=1,le=65535)
    ledger_path: str="de/artifacts/kafka_raw_v1/ledger.sqlite3"
    batch_size: int=Field(default=500,gt=0,le=5000)
    flush_ms: int=Field(default=500,gt=0,le=5000)
    max_buffered_bytes: int=Field(default=33554432,gt=0)
    health_port: int=Field(default=8097,ge=1,le=65535)

    @model_validator(mode="after")
    def isolation(self):
        if self.topic!="traffic.observations.v1" or self.group_id!="de-kafka-raw-v1":
            raise ValueError("live V1 topic/group are fixed; use isolated replay for alternate groups")
        if "kafka_raw_v1" not in self.ledger_path:
            raise ValueError("ledger must be in kafka_raw_v1 namespace")
        return self

