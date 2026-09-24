import time
from collections.abc import Callable, Sequence
from typing import Any, Protocol

from confluent_kafka import KafkaError, KafkaException, Message

from warehouse_delivery.events import Event
from warehouse_delivery.kafka import PRODUCE_TIMEOUT_MS

# Headers on every retry message, for the retry consumer to decide when to try
# again and when to give up. The key and value are exactly those of the
# original message, so the same code can read either topic.
ATTEMPTS_HEADER = "delivery_attempts"
FAILED_AT_HEADER = "failed_at_ms"

# Longer than the producer's own delivery timeout, so flush only gives up once
# the client has reported every message as delivered or failed.
FLUSH_TIMEOUT_SECONDS = PRODUCE_TIMEOUT_MS / 1000 + 5


class RetryQueue(Protocol):
    def put(self, environment_key: str, events: Sequence[Event]) -> None:
        """Queues an environment's events to be delivered again later. May be
        called from several threads at once."""

    def flush(self) -> None:
        """Returns once everything queued is stored, or raises. Offsets must
        not be committed past events that were queued but not stored."""


class KafkaRetryQueue:
    """Writes failed deliveries to the retry topic. Nothing reads that topic
    yet; the retry consumer comes in a later change."""

    def __init__(
        self,
        producer: Any,
        topic: str,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._producer = producer
        self._topic = topic
        self._clock = clock
        self._errors: list[KafkaError] = []

    def put(self, environment_key: str, events: Sequence[Event]) -> None:
        headers = {
            ATTEMPTS_HEADER: b"1",
            FAILED_AT_HEADER: str(round(self._clock() * 1000)).encode(),
        }
        for event in events:
            self._producer.produce(
                self._topic,
                key=environment_key.encode(),
                value=event.payload,
                headers=headers,
                on_delivery=self._on_delivery,
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
