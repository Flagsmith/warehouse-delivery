import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

KafkaAuthMode = Literal["scram", "none"]


class ConfigError(Exception):
    """Raised at startup, naming the environment variable that is missing or
    cannot be read, so a bad deployment fails before it touches Kafka."""


@dataclass(frozen=True)
class Config:
    kafka_bootstrap_servers: str
    kafka_auth: KafkaAuthMode
    kafka_username: str | None
    kafka_password: str | None
    redis_url: str
    warehouse_credentials_secret: str
    redis_cluster: bool = True
    events_topic: str = "external_warehouse_events"
    consumer_group: str = "warehouse-delivery"
    batch_max_records: int = 5000
    batch_max_wait_seconds: float = 5.0
    delivery_concurrency: int = 16

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Config:
        env = os.environ if env is None else env

        kafka_auth: KafkaAuthMode
        match env.get("KAFKA_AUTH", "scram"):
            case "scram":
                kafka_auth = "scram"
            case "none":
                kafka_auth = "none"
            case other:
                raise ConfigError(
                    f"Invalid value for KAFKA_AUTH: {other!r} (expected scram or none)"
                )
        if kafka_auth == "scram":
            kafka_username: str | None = _required(env, "KAFKA_USERNAME")
            kafka_password: str | None = _required(env, "KAFKA_PASSWORD")
        else:
            kafka_username = kafka_password = None

        return cls(
            kafka_bootstrap_servers=_required(env, "KAFKA_BOOTSTRAP_SERVERS"),
            kafka_auth=kafka_auth,
            kafka_username=kafka_username,
            kafka_password=kafka_password,
            redis_url=_required(env, "REDIS_URL"),
            warehouse_credentials_secret=_required(env, "WAREHOUSE_CREDENTIALS_SECRET"),
            redis_cluster=_bool(env, "REDIS_CLUSTER", cls.redis_cluster),
            events_topic=env.get("EXTERNAL_WAREHOUSE_TOPIC", cls.events_topic),
            consumer_group=env.get("KAFKA_CONSUMER_GROUP", cls.consumer_group),
            batch_max_records=_int(env, "BATCH_MAX_RECORDS", cls.batch_max_records),
            batch_max_wait_seconds=_int(env, "BATCH_MAX_WAIT_MS", 5000) / 1000,
            delivery_concurrency=_positive_int(
                env, "DELIVERY_CONCURRENCY", cls.delivery_concurrency
            ),
        )


def _required(env: Mapping[str, str], name: str) -> str:
    value = env.get(name)
    if not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


def _bool(env: Mapping[str, str], name: str, default: bool) -> bool:
    raw = env.get(name)
    if raw is None:
        return default
    if raw.lower() in ("true", "1", "yes"):
        return True
    if raw.lower() in ("false", "0", "no"):
        return False
    raise ConfigError(f"Invalid value for {name}: {raw!r} (expected true or false)")


def _int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(
            f"Invalid value for {name}: {raw!r} (expected an integer)"
        ) from exc


def _positive_int(env: Mapping[str, str], name: str, default: int) -> int:
    value = _int(env, name, default)
    if value < 1:
        raise ConfigError(
            f"Invalid value for {name}: {value} (expected a positive integer)"
        )
    return value
