import threading
from typing import Any

from warehouse_delivery.delivery import DeliveryService
from warehouse_delivery.kafka import events_from_messages


def run_delivery_loop(
    consumer: Any,
    delivery_service: DeliveryService,
    *,
    batch_max_records: int,
    batch_max_wait_seconds: float,
    stop: threading.Event,
) -> None:
    """Polls Kafka for a batch of messages, delivers them, tells Kafka they are
    done, and repeats until asked to stop.

    Kafka is only told after the whole batch is handled. If the process dies
    halfway through a batch, the same messages come back after a restart
    instead of being lost."""
    while not stop.is_set():
        messages = consumer.consume(batch_max_records, timeout=batch_max_wait_seconds)
        events = events_from_messages(messages)
        if not events:
            continue
        delivery_service.deliver_batch(events)
        consumer.commit(asynchronous=False)
