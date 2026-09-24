from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException
from pytest_mock import MockerFixture

from warehouse_delivery.events import Event
from warehouse_delivery.retries import KafkaRetryQueue

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


def test_put__events__one_message_each_keyed_by_environment_with_attempt_headers(
    producer: Any,
) -> None:
    # Given an event produced before the ingestion server keyed on environment
    queue = KafkaRetryQueue(producer, "retry", clock=lambda: NOW)
    events = [
        Event(key=None, payload=b'{"environment_key":"acme","event":"a1"}'),
        Event(key="acme", payload=b'{"environment_key":"acme","event":"a2"}'),
    ]

    # When
    queue.put("acme", events)
    queue.flush()

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
    queue = KafkaRetryQueue(producer, "retry")
    queue.put("acme", [Event(key="acme", payload=b"{}")])

    # When / Then the batch must not be committed
    with pytest.raises(KafkaException):
        queue.flush()


def test_flush__messages_still_unacknowledged__raises(producer: Any) -> None:
    # Given the broker has not answered by the flush timeout
    producer.flush.return_value = 1
    queue = KafkaRetryQueue(producer, "retry")
    queue.put("acme", [Event(key="acme", payload=b"{}")])

    # When / Then
    with pytest.raises(KafkaException, match="not acknowledged"):
        queue.flush()


def test_flush__after_a_failure__next_flush_starts_clean(producer: Any) -> None:
    # Given a flush that failed
    _report_deliveries(producer, KafkaError(KafkaError._MSG_TIMED_OUT))
    queue = KafkaRetryQueue(producer, "retry")
    queue.put("acme", [Event(key="acme", payload=b"{}")])
    with pytest.raises(KafkaException):
        queue.flush()

    # When the broker recovers
    producer.flush.side_effect = None

    # Then the old failure is not reported again
    queue.flush()
