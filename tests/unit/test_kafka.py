from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException
from pytest_mock import MockerFixture

from warehouse_delivery import kafka
from warehouse_delivery.config import Config
from warehouse_delivery.events import Event

SCRAM = Config(
    kafka_bootstrap_servers="b-1.example:9096",
    kafka_auth="scram",
    kafka_username="delivery",
    kafka_password="hunter2",
    redis_url="rediss://redis.example:6379",
    warehouse_credentials_secret="secret",
)


def test_consumer_settings__scram__authenticates_like_the_ingestion_server() -> None:
    # Given / When
    settings = kafka.consumer_settings(SCRAM)

    # Then
    assert settings["bootstrap.servers"] == "b-1.example:9096"
    assert settings["security.protocol"] == "SASL_SSL"
    assert settings["sasl.mechanisms"] == "SCRAM-SHA-512"
    assert settings["sasl.username"] == "delivery"
    assert settings["sasl.password"] == "hunter2"
    assert settings["group.id"] == "warehouse-delivery"
    # Offsets are committed by the loop once a batch is settled
    assert settings["enable.auto.commit"] is False
    # The backlog since the API PR shipped is delivered on first start
    assert settings["auto.offset.reset"] == "earliest"
    # A whole batch of 60 s inserts must fit inside a poll interval
    assert settings["max.poll.interval.ms"] >= 600_000


def test_consumer_settings__auth_none__plaintext_without_credentials() -> None:
    # Given local brokers, as in tests and benchmarks
    config = Config(
        kafka_bootstrap_servers="localhost:9092",
        kafka_auth="none",
        kafka_username=None,
        kafka_password=None,
        redis_url="redis://localhost",
        warehouse_credentials_secret="secret",
        consumer_group="local",
    )

    # When
    settings = kafka.consumer_settings(config)

    # Then
    assert settings["security.protocol"] == "PLAINTEXT"
    assert "sasl.username" not in settings
    assert settings["group.id"] == "local"


def test_producer_settings__scram__authenticates_like_the_consumer() -> None:
    # Given / When
    producer = kafka.producer_settings(SCRAM)
    consumer = kafka.consumer_settings(SCRAM)

    # Then
    for name in (
        "bootstrap.servers",
        "security.protocol",
        "sasl.mechanisms",
        "sasl.username",
        "sasl.password",
    ):
        assert producer[name] == consumer[name]


def test_producer_settings__delivery_timeout__fits_inside_the_poll_interval() -> None:
    # Given / When
    settings = kafka.producer_settings(SCRAM)

    # Then a broker that never acknowledges fails the batch before Kafka
    # takes our partitions away
    assert settings["delivery.timeout.ms"] < kafka.MAX_POLL_INTERVAL_MS


def _message(
    mocker: MockerFixture,
    *,
    key: bytes | None,
    value: bytes | None,
    error: Any = None,
) -> Any:
    message = mocker.Mock()
    message.error.return_value = error
    message.key.return_value = key
    message.value.return_value = value
    return message


def test_events_from_messages__keyed_and_unkeyed__decodes_the_key(
    mocker: MockerFixture,
) -> None:
    # Given
    messages = [
        _message(mocker, key=b"acme", value=b'{"event":"a1"}'),
        _message(mocker, key=None, value=b'{"event":"a2"}'),
    ]

    # When
    events = kafka.events_from_messages(messages)

    # Then
    assert events == [
        Event(key="acme", payload=b'{"event":"a1"}'),
        Event(key=None, payload=b'{"event":"a2"}'),
    ]


def test_events_from_messages__partition_eof__skipped(mocker: MockerFixture) -> None:
    # Given the informational end-of-partition event
    eof = mocker.Mock()
    eof.code.return_value = KafkaError._PARTITION_EOF
    messages = [
        _message(mocker, key=None, value=b"", error=eof),
        _message(mocker, key=b"acme", value=b"{}"),
    ]

    # When
    events = kafka.events_from_messages(messages)

    # Then
    assert events == [Event(key="acme", payload=b"{}")]


def test_events_from_messages__broker_error__raises(mocker: MockerFixture) -> None:
    # Given a message carrying an error rather than an event
    error = KafkaError(KafkaError._ALL_BROKERS_DOWN)
    messages = [_message(mocker, key=None, value=b"", error=error)]

    # When / Then the loop stops rather than committing past it
    with pytest.raises(KafkaException):
        kafka.events_from_messages(messages)


def test_events_from_messages__key_not_text__key_ignored_so_payload_decides(
    mocker: MockerFixture,
) -> None:
    # Given a message whose key is bytes no text decoder accepts
    messages = [_message(mocker, key=b"\xff\xfe", value=b'{"environment_key":"acme"}')]

    # When
    events = kafka.events_from_messages(messages)

    # Then the loop does not crash, and the payload still places the event
    assert events == [Event(key=None, payload=b'{"environment_key":"acme"}')]
    assert events[0].environment_key == "acme"


def test_events_from_messages__no_value__payload_is_empty_bytes(
    mocker: MockerFixture,
) -> None:
    # Given a message with a key but no body
    messages = [_message(mocker, key=b"acme", value=None)]

    # When
    events = kafka.events_from_messages(messages)

    # Then
    assert events == [Event(key="acme", payload=b"")]
