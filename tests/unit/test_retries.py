from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException
from pytest_mock import MockerFixture
from pytest_structlog import StructuredLogCapture

from warehouse_delivery.events import Event
from warehouse_delivery.retries import KafkaRetryWriter, RetryState, batch_due_at

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


def _retry_event(retry_number: int, failed_at_ms: int = 0) -> Event:
    return Event(
        key="acme",
        payload=b"{}",
        headers={
            "retry_number": str(retry_number).encode(),
            "failed_at_ms": str(failed_at_ms).encode(),
        },
    )


def test_retry_state__main_topic_event__not_a_retry() -> None:
    # Given / When
    state = RetryState.of(Event(key="acme", payload=b"{}"))

    # Then
    assert state == RetryState(retry_number=0, failed_at_ms=None)


def test_retry_state__headers_unreadable__treated_as_absent() -> None:
    # Given headers that were not written by the retry writer
    event = Event(
        key="acme",
        payload=b"{}",
        headers={"retry_number": b"many", "failed_at_ms": b"soon"},
    )

    # When / Then the loop does not crash on every restart, and it is due now
    assert RetryState.of(event) == RetryState()


def test_batch_due_at__retry_batch__delay_after_the_latest_failure() -> None:
    # Given / When
    due_at = batch_due_at(
        [_retry_event(1, failed_at_ms=1_000), _retry_event(1, failed_at_ms=2_000)],
        retry_delay_seconds=300,
    )

    # Then
    assert due_at == 302


def test_batch_due_at__main_topic_batch__none() -> None:
    # Given / When / Then
    assert batch_due_at([Event(key="acme", payload=b"{}")], 300) is None


def test_write__main_topic_event__written_as_the_first_retry(producer: Any) -> None:
    # Given
    writer = KafkaRetryWriter(producer, "retry", max_retries=12, clock=lambda: NOW)
    events = [
        Event(key=None, payload=b'{"environment_key":"acme","event":"a1"}'),
        Event(key="acme", payload=b'{"environment_key":"acme","event":"a2"}'),
    ]

    # When
    writer.write("acme", events)

    # Then each payload goes out unchanged, keyed by environment
    headers = {"retry_number": b"1", "failed_at_ms": b"1758000000123"}
    assert [
        (call.args, call.kwargs["key"], call.kwargs["value"], call.kwargs["headers"])
        for call in producer.produce.call_args_list
    ] == [
        (("retry",), b"acme", events[0].payload, headers),
        (("retry",), b"acme", events[1].payload, headers),
    ]


def test_write__retry_fails__written_as_the_next_retry(producer: Any) -> None:
    # Given
    writer = KafkaRetryWriter(producer, "retry", max_retries=12, clock=lambda: NOW)

    # When the second retry fails
    writer.write("acme", [_retry_event(2)])

    # Then
    assert producer.produce.call_args.kwargs["headers"] == {
        "retry_number": b"3",
        "failed_at_ms": b"1758000000123",
    }


def test_write__last_retry_fails__dropped_and_logged(
    producer: Any,
    log: StructuredLogCapture,
) -> None:
    # Given
    writer = KafkaRetryWriter(producer, "retry", max_retries=3)
    last, earlier = _retry_event(3), _retry_event(2)

    # When both fail
    writer.write("acme", [last, earlier])

    # Then only the one with retries left goes back to the retry topic
    assert producer.produce.call_count == 1
    assert producer.produce.call_args.kwargs["headers"]["retry_number"] == b"3"
    assert log.has(
        "delivery.dropped",
        level="error",
        reason="retries_exhausted",
        environment__key="acme",
        events__count=1,
    )


def test_flush__broker_rejects_a_message__raises(producer: Any) -> None:
    # Given the broker refuses a retry message
    _report_deliveries(producer, KafkaError(KafkaError._MSG_TIMED_OUT))
    writer = KafkaRetryWriter(producer, "retry", max_retries=12)
    writer.write("acme", [Event(key="acme", payload=b"{}")])

    # When / Then the batch must not be committed
    with pytest.raises(KafkaException):
        writer.flush()


def test_flush__messages_still_unacknowledged__raises(producer: Any) -> None:
    # Given the broker has not answered by the flush timeout
    producer.flush.return_value = 1
    writer = KafkaRetryWriter(producer, "retry", max_retries=12)
    writer.write("acme", [Event(key="acme", payload=b"{}")])

    # When / Then
    with pytest.raises(KafkaException, match="not acknowledged"):
        writer.flush()


def test_flush__after_a_failure__next_flush_starts_clean(producer: Any) -> None:
    # Given a flush that failed
    _report_deliveries(producer, KafkaError(KafkaError._MSG_TIMED_OUT))
    writer = KafkaRetryWriter(producer, "retry", max_retries=12)
    writer.write("acme", [Event(key="acme", payload=b"{}")])
    with pytest.raises(KafkaException):
        writer.flush()

    # When the broker recovers
    producer.flush.side_effect = None

    # Then the old failure is not reported again
    writer.flush()
