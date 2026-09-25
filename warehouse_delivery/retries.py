import time
from collections.abc import Callable, Sequence
from typing import Any, Protocol

import structlog
from confluent_kafka import KafkaError, KafkaException, Message

from warehouse_delivery.events import ATTEMPTS_HEADER, FAILED_AT_HEADER, Event
from warehouse_delivery.kafka import PRODUCE_TIMEOUT_MS

logger = structlog.get_logger("warehouse")

# Longer than the producer's own delivery timeout, so flush only gives up once
# the client has reported every message as delivered or failed.
FLUSH_TIMEOUT_SECONDS = PRODUCE_TIMEOUT_MS / 1000 + 5


class RetryWriter(Protocol):
    def write(self, environment_key: str, events: Sequence[Event]) -> None:
        """Writes an environment's events to be delivered again later. May be
        called from several threads at once."""

    def flush(self) -> None:
        """Returns once everything written is stored, or raises. Offsets must
        not be committed past events that were written but not stored."""


class KafkaRetryWriter:
    def __init__(
        self,
        producer: Any,
        topic: str,
        *,
        max_attempts: int,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._producer = producer
        self._topic = topic
        self._max_attempts = max_attempts
        self._clock = clock
        self._errors: list[KafkaError] = []

    def write(self, environment_key: str, events: Sequence[Event]) -> None:
        # The key and value are exactly those of the original message, so the
        # same code reads either topic.
        failed_at = str(round(self._clock() * 1000)).encode()
        exhausted = 0
        for event in events:
            # An event read with attempts == n has now failed n retries, on
            # top of its first delivery from the main topic.
            if event.attempts >= self._max_attempts:
                exhausted += 1
                continue
            self._producer.produce(
                self._topic,
                key=environment_key.encode(),
                value=event.payload,
                headers={
                    ATTEMPTS_HEADER: str(event.attempts + 1).encode(),
                    FAILED_AT_HEADER: failed_at,
                },
                on_delivery=self._on_delivery,
            )
        if exhausted:
            logger.error(
                "delivery.dropped",
                reason="retries_exhausted",
                environment__key=environment_key,
                events__count=exhausted,
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
