from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException

from warehouse_delivery.events import Event
from warehouse_delivery.retries import KafkaRetryQueue

NOW = 1_758_000_000.123


class FakeProducer:
    """Records what is produced; flush reports each message as delivered, or
    as failed with ``delivery_error``, and leaves ``unacknowledged`` behind."""

    def __init__(
        self,
        *,
        delivery_error: KafkaError | None = None,
        unacknowledged: int = 0,
    ) -> None:
        self.produced: list[dict[str, Any]] = []
        self.delivery_error = delivery_error
        self.unacknowledged = unacknowledged
        self._pending: list[Any] = []

    def produce(self, topic: str, **kwargs: Any) -> None:
        self.produced.append({"topic": topic, **kwargs})
        self._pending.append(kwargs["on_delivery"])

    def flush(self, timeout: float) -> int:
        for on_delivery in self._pending:
            on_delivery(self.delivery_error, None)
        self._pending = []
        return self.unacknowledged


def test_put__events__one_message_each_keyed_by_environment_with_attempt_headers() -> (
    None
):
    # Given an event produced before the ingestion server keyed on environment
    producer = FakeProducer()
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
    assert [(m["topic"], m["key"], m["value"]) for m in producer.produced] == [
        ("retry", b"acme", events[0].payload),
        ("retry", b"acme", events[1].payload),
    ]
    assert producer.produced[0]["headers"] == {
        "delivery_attempts": b"1",
        "failed_at_ms": b"1758000000123",
    }


def test_flush__broker_rejects_a_message__raises() -> None:
    # Given the broker refuses a retry message
    producer = FakeProducer(delivery_error=KafkaError(KafkaError._MSG_TIMED_OUT))
    queue = KafkaRetryQueue(producer, "retry")
    queue.put("acme", [Event(key="acme", payload=b"{}")])

    # When / Then the batch must not be committed
    with pytest.raises(KafkaException):
        queue.flush()


def test_flush__messages_still_unacknowledged__raises() -> None:
    # Given the broker has not answered by the flush timeout
    producer = FakeProducer(unacknowledged=1)
    queue = KafkaRetryQueue(producer, "retry")
    queue.put("acme", [Event(key="acme", payload=b"{}")])

    # When / Then
    with pytest.raises(KafkaException, match="not acknowledged"):
        queue.flush()


def test_flush__after_a_failure__next_flush_starts_clean() -> None:
    # Given a flush that failed
    producer = FakeProducer(delivery_error=KafkaError(KafkaError._MSG_TIMED_OUT))
    queue = KafkaRetryQueue(producer, "retry")
    queue.put("acme", [Event(key="acme", payload=b"{}")])
    with pytest.raises(KafkaException):
        queue.flush()

    # When the broker recovers
    producer.delivery_error = None

    # Then the old failure is not reported again
    queue.flush()
