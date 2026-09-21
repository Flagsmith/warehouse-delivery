import json
import threading
from typing import Any

import pytest
from pytest_mock import MockerFixture

from warehouse_delivery import loops
from warehouse_delivery.events import Event


def _event(environment_key: str, event_name: str) -> Event:
    payload = {"environment_key": environment_key, "event": event_name}
    return Event(key=environment_key, payload=json.dumps(payload).encode())


class FakeConsumer:
    """Hands out batches in turn, then asks the loop to stop."""

    def __init__(self, batches: list[list[Any]], stop: threading.Event) -> None:
        self._batches = batches
        self._stop = stop
        self.consume_args: list[tuple[int, float]] = []
        self.commits = 0

    def consume(self, num_messages: int, timeout: float) -> list[Any]:
        self.consume_args.append((num_messages, timeout))
        if not self._batches:
            self._stop.set()
            return []
        return self._batches.pop(0)

    def commit(self, asynchronous: bool) -> None:
        assert asynchronous is False
        self.commits += 1


def _message(mocker: MockerFixture, event: Event) -> Any:
    message = mocker.Mock()
    message.error.return_value = None
    message.key.return_value = event.key.encode() if event.key else None
    message.value.return_value = event.payload
    return message


def test_run_delivery_loop__batches__each_delivered_then_committed(
    mocker: MockerFixture,
) -> None:
    # Given two batches with an empty poll between them
    stop = threading.Event()
    first = [_event("acme", "a1")]
    second = [_event("acme", "a2"), _event("acme", "a3")]
    consumer = FakeConsumer(
        [
            [_message(mocker, r) for r in first],
            [],
            [_message(mocker, r) for r in second],
        ],
        stop,
    )
    delivery = mocker.Mock()

    # When
    loops.run_delivery_loop(
        consumer,
        delivery,
        batch_max_records=500,
        batch_max_wait_seconds=2.5,
        stop=stop,
    )

    # Then each batch is polled for as configured, delivered and committed on
    # its own
    assert set(consumer.consume_args) == {(500, 2.5)}
    assert delivery.deliver_batch.call_args_list == [((first,),), ((second,),)]
    assert consumer.commits == 2


def test_run_delivery_loop__delivery_raises__no_commit(mocker: MockerFixture) -> None:
    # Given a failure of our own mid-batch
    stop = threading.Event()
    consumer = FakeConsumer([[_message(mocker, _event("acme", "a1"))]], stop)
    delivery = mocker.Mock()
    delivery.deliver_batch.side_effect = RuntimeError("boom")

    # When / Then the loop stops and the offset stays where it was
    with pytest.raises(RuntimeError, match="boom"):
        loops.run_delivery_loop(
            consumer,
            delivery,
            batch_max_records=500,
            batch_max_wait_seconds=2.5,
            stop=stop,
        )
    assert consumer.commits == 0
