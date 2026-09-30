import threading
import time
from collections.abc import Callable
from typing import Any

from warehouse_delivery.delivery import DeliveryService
from warehouse_delivery.kafka import events_from_messages
from warehouse_delivery.retries import batch_due_at


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
    """Polls Kafka for a batch of messages, waits until the batch is due,
    delivers it, tells Kafka it is done, and repeats until asked to stop.

    Kafka is only told after the whole batch is handled. If the process dies
    halfway through a batch, the same messages come back after a restart
    instead of being lost."""
    while not stop.is_set():
        messages = consumer.consume(batch_max_records, timeout=batch_max_wait_seconds)
        events = events_from_messages(messages)
        if not events:
            continue
        due_at = batch_due_at(events, retry_delay_seconds)
        if due_at is not None:
            # Capped at the delay, so a failure time in the future, from clock
            # skew or a bad header, cannot hold the loop past the poll interval.
            wait = min(due_at - clock(), retry_delay_seconds)
            if wait > 0:
                stopped = stop.wait(wait)
                if stopped:
                    return  # uncommitted, so the batch comes back after restart
        delivery_service.deliver_batch(events)
        consumer.commit(asynchronous=False)
