from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException
from pytest_mock import MockerFixture
from pytest_structlog import StructuredLogCapture

from warehouse_delivery.events import Event
from warehouse_delivery.retries import KafkaRetryWriter

NOW = 1_758_000_000.123


@pytest.fixture()
def producer(mocker: MockerFixture) -> Any:
    producer = mocker.Mock()
    producer.flush.return_value = 0
    return producer


def _report_deliveries(producer: Any, error: KafkaError | None) -> None:
    """Makes flush report every produced message as delivered, or as failed
    with ``error``, as the real client does through on_delivery."""

    def flush(timeout: float) -> int:
        for call in producer.produce.call_args_list:
            call.kwargs["on_delivery"](error, None)
        return 0

    producer.flush.side_effect = flush


def test_write__events__one_message_each_keyed_by_environment_with_attempt_headers(
    producer: Any,
) -> None:
    # Given an event produced before the ingestion server keyed on environment
    writer = KafkaRetryWriter(producer, "retry", max_attempts=12, clock=lambda: NOW)
    events = [
        Event(key=None, payload=b'{"environment_key":"acme","event":"a1"}'),
        Event(key="acme", payload=b'{"environment_key":"acme","event":"a2"}'),
    ]

    # When
    writer.write("acme", events)
    writer.flush()

    # Then each payload goes out unchanged, keyed so the retry consumer can
    # place it without parsing
    headers = {"delivery_attempts": b"1", "failed_at_ms": b"1758000000123"}
    assert [
        (call.args, call.kwargs["key"], call.kwargs["value"], call.kwargs["headers"])
        for call in producer.produce.call_args_list
    ] == [
        (("retry",), b"acme", events[0].payload, headers),
        (("retry",), b"acme", events[1].payload, headers),
    ]


def test_flush__broker_rejects_a_message__raises(producer: Any) -> None:
    # Given the broker refuses a retry message
    _report_deliveries(producer, KafkaError(KafkaError._MSG_TIMED_OUT))
    writer = KafkaRetryWriter(producer, "retry", max_attempts=12)
    writer.write("acme", [Event(key="acme", payload=b"{}")])

    # When / Then the batch must not be committed
    with pytest.raises(KafkaException):
        writer.flush()


def test_flush__messages_still_unacknowledged__raises(producer: Any) -> None:
    # Given the broker has not answered by the flush timeout
    producer.flush.return_value = 1
    writer = KafkaRetryWriter(producer, "retry", max_attempts=12)
    writer.write("acme", [Event(key="acme", payload=b"{}")])

    # When / Then
    with pytest.raises(KafkaException, match="not acknowledged"):
        writer.flush()


def test_flush__after_a_failure__next_flush_starts_clean(producer: Any) -> None:
    # Given a flush that failed
    _report_deliveries(producer, KafkaError(KafkaError._MSG_TIMED_OUT))
    writer = KafkaRetryWriter(producer, "retry", max_attempts=12)
    writer.write("acme", [Event(key="acme", payload=b"{}")])
    with pytest.raises(KafkaException):
        writer.flush()

    # When the broker recovers
    producer.flush.side_effect = None

    # Then the old failure is not reported again
    writer.flush()


def test_write__event_from_retry_topic__attempts_counted_up(producer: Any) -> None:
    # Given an event that has already failed two retries
    writer = KafkaRetryWriter(producer, "retry", max_attempts=12, clock=lambda: NOW)

    # When its third retry fails too
    writer.write("acme", [Event(key="acme", payload=b"{}", attempts=2)])

    # Then
    assert producer.produce.call_args.kwargs["headers"] == {
        "delivery_attempts": b"3",
        "failed_at_ms": b"1758000000123",
    }


def test_write__retries_exhausted__dropped_and_logged(
    producer: Any,
    log: StructuredLogCapture,
) -> None:
    # Given one event on its last allowed retry and one with retries left
    writer = KafkaRetryWriter(producer, "retry", max_attempts=3)
    last = Event(key="acme", payload=b'{"event":"last"}', attempts=3)
    more = Event(key="acme", payload=b'{"event":"more"}', attempts=2)

    # When both fail
    writer.write("acme", [last, more])

    # Then only the one with retries left goes back to the retry topic
    assert [call.kwargs["value"] for call in producer.produce.call_args_list] == [
        more.payload
    ]
    assert log.has(
        "delivery.dropped",
        level="error",
        reason="retries_exhausted",
        environment__key="acme",
        events__count=1,
    )
