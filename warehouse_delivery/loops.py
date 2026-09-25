import threading
import time
from collections.abc import Callable, Sequence
from typing import Any

from warehouse_delivery.delivery import DeliveryService
from warehouse_delivery.events import Event
from warehouse_delivery.kafka import events_from_messages


def run_delivery_loop(
    consumer: Any,
    delivery_service: DeliveryService,
    *,
    batch_max_records: int,
    batch_max_wait_seconds: float,
    retry_delay_seconds: float,
    stop: threading.Event,
    clock: Callable[[], float] = time.time,
) -> None:
    """Polls Kafka for a batch of messages, delivers them, tells Kafka they are
    done, and repeats until asked to stop.

    Kafka is only told after the whole batch is handled. If the process dies
    halfway through a batch, the same messages come back after a restart
    instead of being lost.

    Events from the retry topic are held until ``retry_delay_seconds`` after
    they last failed. Events from the main topic carry no failure time and are
    delivered straight away."""
    while not stop.is_set():
        messages = consumer.consume(batch_max_records, timeout=batch_max_wait_seconds)
        events = events_from_messages(messages)
        if not events:
            continue
        wait = _due_at(events, retry_delay_seconds) - clock()
        if wait > 0 and stop.wait(wait):
            # Told to stop while waiting: leave the batch uncommitted so it
            # comes back after the restart.
            return
        delivery_service.deliver_batch(events)
        consumer.commit(asynchronous=False)


def _due_at(events: Sequence[Event], retry_delay_seconds: float) -> float:
    failed_at_ms = max((e.failed_at_ms or 0 for e in events), default=0)
    return failed_at_ms / 1000 + retry_delay_seconds if failed_at_ms else 0.0
