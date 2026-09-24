from collections.abc import Iterable
from typing import Any

from confluent_kafka import KafkaError, KafkaException, Message

from warehouse_delivery.config import Config
from warehouse_delivery.events import Event

CLIENT_ID = "warehouse-delivery"
# If we go longer than this without asking Kafka for more messages, Kafka
# assumes we are dead and gives our partitions to another consumer. A batch
# holds events for many environments, inserted DELIVERY_CONCURRENCY at a time,
# so this has to cover the whole batch. With a 60 s insert timeout and about
# 10 s to connect, ten minutes allows for roughly eight rounds of stalled
# hosts, about 128 at the default concurrency, in the same batch; beyond that
# the commit fails, the process exits and the batch is redelivered.
MAX_POLL_INTERVAL_MS = 600_000
SESSION_TIMEOUT_MS = 45_000
# Well inside the poll interval, so a broker that never acknowledges a retry
# message fails the batch instead of stalling it.
PRODUCE_TIMEOUT_MS = 30_000


def consumer_settings(config: Config) -> dict[str, Any]:
    """Settings for the Kafka consumer. It connects to the same brokers with the
    same SASL/SCRAM login over TLS as the ingestion server.

    Only the loop tells Kafka which messages are done, after every customer in
    a batch has been handled. The client never does it on its own, because
    that could mark messages done that were never inserted."""
    return {
        **_connection_settings(config),
        "group.id": config.consumer_group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "max.poll.interval.ms": MAX_POLL_INTERVAL_MS,
        "session.timeout.ms": SESSION_TIMEOUT_MS,
        "partition.assignment.strategy": "cooperative-sticky",
    }


def producer_settings(config: Config) -> dict[str, Any]:
    return {
        **_connection_settings(config),
        "enable.idempotence": True,
        "delivery.timeout.ms": PRODUCE_TIMEOUT_MS,
    }


def _connection_settings(config: Config) -> dict[str, Any]:
    settings: dict[str, Any] = {
        "bootstrap.servers": config.kafka_bootstrap_servers,
        "client.id": CLIENT_ID,
    }
    if config.kafka_auth == "scram":
        settings.update(
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanisms": "SCRAM-SHA-512",
                "sasl.username": config.kafka_username,
                "sasl.password": config.kafka_password,
            }
        )
    else:
        settings["security.protocol"] = "PLAINTEXT"
    return settings


def events_from_messages(messages: Iterable[Message]) -> list[Event]:
    """Converts the messages from one poll into Events.

    Kafka can hand back an error in place of a message, for instance when all
    brokers are down. That is raised so the loop stops instead of marking the
    batch done. The end-of-partition marker is skipped: it is information,
    not an error."""
    events = []
    for message in messages:
        error = message.error()
        if error is not None:
            if error.code() == KafkaError._PARTITION_EOF:
                continue
            raise KafkaException(error)
        key = message.key()
        events.append(
            Event(
                key=_decode_key(key) if isinstance(key, bytes) else key,
                payload=message.value() or b"",
            )
        )
    return events


def _decode_key(key: bytes) -> str | None:
    """A client API key is plain ASCII. Anything that is not valid text was
    not put there by the ingestion server, so ignore it and let the
    environment_key inside the payload place the event instead of crashing
    the loop on every restart."""
    try:
        return key.decode()
    except UnicodeDecodeError:
        return None
