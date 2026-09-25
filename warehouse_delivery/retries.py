import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

import structlog
from confluent_kafka import KafkaError, KafkaException, Message

from warehouse_delivery.events import Event
from warehouse_delivery.kafka import PRODUCE_TIMEOUT_MS

logger = structlog.get_logger("warehouse")

RETRY_NUMBER_HEADER = "retry_number"
FAILED_AT_HEADER = "failed_at_ms"

# Longer than the producer's own delivery timeout, so flush only gives up once
# the client has reported every message as delivered or failed.
FLUSH_TIMEOUT_SECONDS = PRODUCE_TIMEOUT_MS / 1000 + 5


@dataclass(frozen=True)
class RetryState:
    """Where an event is in its retries, carried in the retry topic's message
    headers. An event from the main topic has no headers: its delivery is not
    a retry and nothing has failed yet."""

    retry_number: int = 0
    failed_at_ms: int | None = None

    @classmethod
    def of(cls, event: Event) -> RetryState:
        # A header we cannot read counts as absent, so the event is retried
        # straight away rather than crashing the loop on every restart.
        return cls(
            retry_number=_int_header(event, RETRY_NUMBER_HEADER) or 0,
            failed_at_ms=_int_header(event, FAILED_AT_HEADER),
        )

    def next_retry(self, failed_at_ms: int) -> RetryState:
        return RetryState(self.retry_number + 1, failed_at_ms)

    def to_headers(self) -> dict[str, bytes]:
        return {
            RETRY_NUMBER_HEADER: str(self.retry_number).encode(),
            FAILED_AT_HEADER: str(self.failed_at_ms).encode(),
        }


def batch_due_at(events: Sequence[Event], retry_delay_seconds: float) -> float | None:
    """When the batch may be retried: the delay after its latest failure. None
    for a batch from the main topic, which is due straight away."""
    failed_at = [
        state.failed_at_ms
        for state in map(RetryState.of, events)
        if state.failed_at_ms is not None
    ]
    if not failed_at:
        return None
    return max(failed_at) / 1000 + retry_delay_seconds


class RetryWriter(Protocol):
    def write(self, environment_key: str, events: Sequence[Event]) -> None:
        """Writes the events to be delivered again later, and drops those that
        have used up their retries. May be called from several threads at
        once."""

    def flush(self) -> None:
        """Returns once everything written is stored, or raises. Offsets must
        not be committed past events that were written but not stored."""


class KafkaRetryWriter:
    def __init__(
        self,
        producer: Any,
        topic: str,
        *,
        max_retries: int,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._producer = producer
        self._topic = topic
        self._max_retries = max_retries
        self._clock = clock
        self._errors: list[KafkaError] = []

    def write(self, environment_key: str, events: Sequence[Event]) -> None:
        failed_at_ms = round(self._clock() * 1000)
        dropped = 0
        for event in events:
            state = RetryState.of(event)
            if state.retry_number >= self._max_retries:
                dropped += 1
                continue
            # The payload goes out unchanged, so the same code reads either
            # topic. The key is always the environment, so an environment's
            # retries stay in order on one partition.
            self._producer.produce(
                self._topic,
                key=environment_key.encode(),
                value=event.payload,
                headers=state.next_retry(failed_at_ms).to_headers(),
                on_delivery=self._on_delivery,
            )
        if dropped:
            logger.error(
                "delivery.dropped",
                reason="retries_exhausted",
                environment__key=environment_key,
                events__count=dropped,
            )

    def flush(self) -> None:
        remaining = self._producer.flush(FLUSH_TIMEOUT_SECONDS)
        errors, self._errors = self._errors, []
        if errors:
            raise KafkaException(errors[0])
        if remaining:
            raise KafkaException(
                KafkaError(
                    KafkaError._TIMED_OUT,
                    f"{remaining} retry messages were not acknowledged",
                )
            )

    def _on_delivery(self, error: KafkaError | None, message: Message) -> None:
        # Called from flush on the caller's thread, never concurrently.
        if error is not None:
            self._errors.append(error)


def _int_header(event: Event, name: str) -> int | None:
    value = event.headers.get(name)
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None
