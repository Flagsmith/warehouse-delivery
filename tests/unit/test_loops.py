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
    message.headers.return_value = None
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
        retry_delay_seconds=300,
        stop=stop,
        heartbeat=mocker.Mock(),
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
            retry_delay_seconds=300,
            stop=stop,
            heartbeat=mocker.Mock(),
        )
    assert consumer.commits == 0


NOW = 1_758_000_000.0


def _retry_message(mocker: MockerFixture, failed_at: float) -> Any:
    message = mocker.Mock()
    message.error.return_value = None
    message.key.return_value = b"acme"
    message.value.return_value = b'{"environment_key":"acme"}'
    message.headers.return_value = [
        ("retry_number", b"1"),
        ("failed_at_ms", str(round(failed_at * 1000)).encode()),
    ]
    return message


def _stop_after_one_batch(mocker: MockerFixture, *, stopped_while_waiting: bool) -> Any:
    stop = mocker.Mock()
    stop.is_set.side_effect = [False, True]
    stop.wait.return_value = stopped_while_waiting
    return stop


def test_run_delivery_loop__retry_batch_not_due__waits_for_the_latest_then_delivers(
    mocker: MockerFixture,
) -> None:
    # Given two retry events that failed 200 s and 100 s ago
    consumer = mocker.Mock()
    consumer.consume.return_value = [
        _retry_message(mocker, NOW - 200),
        _retry_message(mocker, NOW - 100),
    ]
    delivery = mocker.Mock()
    stop = _stop_after_one_batch(mocker, stopped_while_waiting=False)

    # When
    loops.run_delivery_loop(
        consumer,
        delivery,
        batch_max_records=500,
        batch_max_wait_seconds=2.5,
        retry_delay_seconds=300,
        stop=stop,
        heartbeat=mocker.Mock(),
        clock=lambda: NOW,
    )

    # Then the batch waits until the later one is due
    stop.wait.assert_called_once_with(200)
    delivery.deliver_batch.assert_called_once()
    consumer.commit.assert_called_once_with(asynchronous=False)


def test_run_delivery_loop__retry_batch_already_due__delivered_without_waiting(
    mocker: MockerFixture,
) -> None:
    # Given a retry event that failed longer ago than the delay
    consumer = mocker.Mock()
    consumer.consume.return_value = [_retry_message(mocker, NOW - 301)]
    delivery = mocker.Mock()
    stop = _stop_after_one_batch(mocker, stopped_while_waiting=False)

    # When
    loops.run_delivery_loop(
        consumer,
        delivery,
        batch_max_records=500,
        batch_max_wait_seconds=2.5,
        retry_delay_seconds=300,
        stop=stop,
        heartbeat=mocker.Mock(),
        clock=lambda: NOW,
    )

    # Then
    stop.wait.assert_not_called()
    delivery.deliver_batch.assert_called_once()


def test_run_delivery_loop__stopped_while_waiting__batch_left_uncommitted(
    mocker: MockerFixture,
) -> None:
    # Given the process is told to stop while a retry batch is not yet due
    consumer = mocker.Mock()
    consumer.consume.return_value = [_retry_message(mocker, NOW - 10)]
    delivery = mocker.Mock()
    stop = _stop_after_one_batch(mocker, stopped_while_waiting=True)

    # When
    loops.run_delivery_loop(
        consumer,
        delivery,
        batch_max_records=500,
        batch_max_wait_seconds=2.5,
        retry_delay_seconds=300,
        stop=stop,
        heartbeat=mocker.Mock(),
        clock=lambda: NOW,
    )

    # Then it comes back after the restart
    delivery.deliver_batch.assert_not_called()
    consumer.commit.assert_not_called()


def test_run_delivery_loop__failure_time_in_the_future__wait_capped_at_the_delay(
    mocker: MockerFixture,
) -> None:
    # Given a failure time an hour ahead, from clock skew or a bad header
    consumer = mocker.Mock()
    consumer.consume.return_value = [_retry_message(mocker, NOW + 3600)]
    delivery = mocker.Mock()
    stop = _stop_after_one_batch(mocker, stopped_while_waiting=False)

    # When
    loops.run_delivery_loop(
        consumer,
        delivery,
        batch_max_records=500,
        batch_max_wait_seconds=2.5,
        retry_delay_seconds=300,
        stop=stop,
        heartbeat=mocker.Mock(),
        clock=lambda: NOW,
    )

    # Then the loop never waits long enough to be taken out of the group
    stop.wait.assert_called_once_with(300)


def test_run_delivery_loop__empty_polls__heartbeat_each_pass(
    mocker: MockerFixture,
) -> None:
    # Given an idle topic
    stop = threading.Event()
    consumer = FakeConsumer([[], []], stop)
    heartbeat = mocker.Mock()

    # When
    loops.run_delivery_loop(
        consumer,
        mocker.Mock(),
        batch_max_records=500,
        batch_max_wait_seconds=2.5,
        retry_delay_seconds=300,
        stop=stop,
        heartbeat=heartbeat,
    )

    # Then an idle loop still shows as alive
    assert heartbeat.call_count == len(consumer.consume_args)
