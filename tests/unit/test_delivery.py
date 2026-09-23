import json
import threading
from collections.abc import Callable, Sequence
from typing import Any

import pytest
from pytest_mock import MockerFixture
from pytest_structlog import StructuredLogCapture

from warehouse_delivery.connections import WarehouseConnection
from warehouse_delivery.delivery import DeliveryService, Outcome
from warehouse_delivery.errors import DeliveryError
from warehouse_delivery.events import Event

CONFIG = {
    "host": "ch.acme-corp.example",
    "port": 8443,
    "database": "acme_dwh",
    "username": "acme_svc",
    "secure": True,
}
ACME = WarehouseConnection(
    id=42,
    warehouse_type="clickhouse",
    config=CONFIG,
    credentials={"password": "hunter2"},
)
OTHER = WarehouseConnection(
    id=7,
    warehouse_type="clickhouse",
    config={**CONFIG, "host": "ch.other.example"},
    credentials={"password": "pw"},
)
NOW = 1_758_000_000.0
CONCURRENCY = 4


def _event(environment_key: str, event_name: str) -> Event:
    payload = {"environment_key": environment_key, "event": event_name}
    return Event(key=environment_key, payload=json.dumps(payload).encode())


class FakeConnections:
    def __init__(
        self, connections: dict[str, WarehouseConnection | None | Exception]
    ) -> None:
        self.connections = connections

    def get(self, environment_key: str) -> WarehouseConnection | None:
        connection = self.connections.get(environment_key)
        if isinstance(connection, Exception):
            raise connection
        return connection


class FakeWarehouse:
    """Stands in for an adapter: hands the rows to the ``insert`` mock along
    with the connection it was built for, so tests see which customer got what."""

    def __init__(self, connection: WarehouseConnection, insert: Any) -> None:
        self._connection = connection
        self._insert = insert

    def insert(self, rows: Sequence[bytes]) -> int:
        return int(self._insert(self._connection, rows))


def _warehouse_for(
    insert: Any,
) -> Callable[[WarehouseConnection], FakeWarehouse | None]:
    def warehouse_for(connection: WarehouseConnection) -> FakeWarehouse | None:
        if connection.warehouse_type != "clickhouse":
            return None
        return FakeWarehouse(connection, insert)

    return warehouse_for


class FakeStatusWriter:
    def __init__(self, error: Exception | None = None) -> None:
        self.writes: list[tuple[int, str, str | None]] = []
        self.error = error

    def write(self, connection_id: int, status: str, detail: str | None) -> None:
        if self.error is not None:
            raise self.error
        self.writes.append((connection_id, status, detail))


@pytest.fixture()
def status_writer() -> FakeStatusWriter:
    return FakeStatusWriter()


@pytest.fixture()
def insert(mocker: MockerFixture) -> Any:
    insert = mocker.Mock()
    insert.side_effect = lambda connection, rows: len(rows)
    return insert


def _delivery(
    connections: dict[str, WarehouseConnection | None | Exception],
    status_writer: FakeStatusWriter,
    insert: Any,
) -> DeliveryService:
    return DeliveryService(
        concurrency=CONCURRENCY,
        connections=FakeConnections(connections),
        status_writer=status_writer,
        warehouse_for=_warehouse_for(insert),
        clock=lambda: NOW,
    )


def test_deliver_batch__two_environments__one_insert_each_and_both_connected(
    status_writer: FakeStatusWriter,
    insert: Any,
    log: StructuredLogCapture,
) -> None:
    # Given a batch interleaving two customers' events
    events = [
        _event("acme", "a1"),
        _event("other", "o1"),
        _event("acme", "a2"),
    ]
    delivery = _delivery({"acme": ACME, "other": OTHER}, status_writer, insert)

    # When
    outcomes = delivery.deliver_batch(events)

    # Then each customer gets one insert holding their events in order
    assert outcomes == [
        Outcome("acme", "delivered", rows=2),
        Outcome("other", "delivered", rows=1),
    ]
    insert.assert_any_call(ACME, [events[0].payload, events[2].payload])
    insert.assert_any_call(OTHER, [events[1].payload])
    assert insert.call_count == 2
    assert sorted(status_writer.writes) == [
        (7, "connected", None),
        (42, "connected", None),
    ]
    assert log.has(
        "delivery.completed",
        environment__key="acme",
        connection__id=42,
        events__count=2,
        rows__count=2,
    )


def test_deliver_batch__unkeyed_events__grouped_by_payload_or_dropped(
    status_writer: FakeStatusWriter,
    insert: Any,
    log: StructuredLogCapture,
) -> None:
    # Given events produced before the sink keyed on the environment, one of
    # them with no environment in the payload either
    keyed = _event("acme", "a1")
    unkeyed = Event(key=None, payload=b'{"environment_key":"acme","event":"a2"}')
    orphan = Event(key=None, payload=b'{"event":"a3"}')
    garbage = Event(key=None, payload=b"not json")
    delivery = _delivery({"acme": ACME}, status_writer, insert)

    # When
    outcomes = delivery.deliver_batch([keyed, unkeyed, orphan, garbage])

    # Then the payload's environment is used, and the rest are dropped loudly
    assert outcomes == [Outcome("acme", "delivered", rows=2)]
    insert.assert_called_once_with(ACME, [keyed.payload, unkeyed.payload])
    assert log.has(
        "delivery.dropped",
        level="error",
        reason="no_environment_key",
        events__count=2,
    )


@pytest.mark.parametrize(
    "connection, reason",
    [
        pytest.param(None, "no_connection", id="connection-gone"),
        pytest.param(
            WarehouseConnection(
                id=9,
                warehouse_type="snowflake",
                config={"account_identifier": "acme"},
                credentials={},
            ),
            "unsupported_warehouse_type",
            id="snowflake",
        ),
    ],
)
def test_deliver_for_environment__nothing_to_deliver_to__drops_without_touching_status(
    connection: WarehouseConnection | None,
    reason: str,
    status_writer: FakeStatusWriter,
    insert: Any,
    log: StructuredLogCapture,
) -> None:
    # Given the connection was deleted, switched to Flagsmith, or is a type
    # no adapter exists for yet
    delivery = _delivery({"acme": connection}, status_writer, insert)

    # When
    outcome = delivery.deliver_for_environment("acme", [_event("acme", "a1")])

    # Then
    assert outcome == Outcome("acme", "dropped", reason=reason)
    insert.assert_not_called()
    assert status_writer.writes == []
    assert log.has("delivery.dropped", level="warning", reason=reason)


def test_deliver_batch__one_warehouse_rejects__marks_it_errored_and_delivers_the_rest(
    status_writer: FakeStatusWriter,
    insert: Any,
    log: StructuredLogCapture,
) -> None:
    # Given acme's warehouse refuses the credentials while other's accepts
    def insert_or_refuse(connection: WarehouseConnection, rows: Sequence[bytes]) -> int:
        if connection is ACME:
            raise DeliveryError("authentication", "Authentication failed.")
        return len(rows)

    insert.side_effect = insert_or_refuse
    delivery = _delivery({"acme": ACME, "other": OTHER}, status_writer, insert)

    # When
    outcomes = delivery.deliver_batch(
        [_event("acme", "a1"), _event("acme", "a2"), _event("other", "o1")]
    )

    # Then acme's connection shows the failure, its group is given up on, and
    # other's group is unaffected
    assert outcomes == [
        Outcome("acme", "failed", reason="authentication"),
        Outcome("other", "delivered", rows=1),
    ]
    assert sorted(status_writer.writes) == [
        (7, "connected", None),
        (42, "errored", "Authentication failed."),
    ]
    assert log.has(
        "delivery.failed",
        level="error",
        environment__key="acme",
        connection__id=42,
        events__count=2,
        failure__kind="authentication",
    )


def test_deliver_for_environment__credentials_undecryptable__marks_that_connection(
    status_writer: FakeStatusWriter,
    insert: Any,
    log: StructuredLogCapture,
) -> None:
    # Given the API and this service disagree on the credentials secret, so the
    # connection lookup fails but knows which connection it was reading
    delivery = _delivery(
        {
            "acme": DeliveryError(
                "stored_connection",
                "Stored credentials cannot be decrypted.",
                connection_id=42,
            )
        },
        status_writer,
        insert,
    )

    # When
    outcome = delivery.deliver_for_environment("acme", [_event("acme", "a1")])

    # Then the dashboard shows the problem rather than staying on "connected"
    assert outcome == Outcome("acme", "failed", reason="stored_connection")
    assert status_writer.writes == [
        (42, "errored", "Stored credentials cannot be decrypted.")
    ]
    insert.assert_not_called()
    assert log.has(
        "delivery.failed",
        level="error",
        connection__id=42,
        failure__kind="stored_connection",
    )


def test_deliver_for_environment__connection_unreadable__fails_with_only_a_log(
    status_writer: FakeStatusWriter,
    insert: Any,
    log: StructuredLogCapture,
) -> None:
    # Given the published connection is malformed beyond even its id
    delivery = _delivery(
        {
            "acme": DeliveryError(
                "stored_connection", "Stored connection details are incomplete."
            )
        },
        status_writer,
        insert,
    )

    # When
    outcome = delivery.deliver_for_environment("acme", [_event("acme", "a1")])

    # Then there is no connection id to hang a status on, so only the log says
    assert outcome == Outcome("acme", "failed", reason="stored_connection")
    assert status_writer.writes == []
    insert.assert_not_called()
    assert log.has("delivery.failed", level="error", failure__kind="stored_connection")


def test_deliver_for_environment__adapter_cannot_be_built__marks_the_connection(
    status_writer: FakeStatusWriter,
    insert: Any,
) -> None:
    # Given the stored connection is missing a detail such as the port
    def warehouse_for(connection: WarehouseConnection) -> FakeWarehouse | None:
        raise DeliveryError(
            "stored_connection", "Stored connection details are incomplete."
        )

    delivery = DeliveryService(
        concurrency=CONCURRENCY,
        connections=FakeConnections({"acme": ACME}),
        status_writer=status_writer,
        warehouse_for=warehouse_for,
        clock=lambda: NOW,
    )

    # When
    outcome = delivery.deliver_for_environment("acme", [_event("acme", "a1")])

    # Then the connection is known, so its status says what is missing
    assert outcome == Outcome("acme", "failed", reason="stored_connection")
    assert status_writer.writes == [
        (42, "errored", "Stored connection details are incomplete.")
    ]
    insert.assert_not_called()


def test_deliver_for_environment__status_write_fails__raises_as_our_own_failure(
    insert: Any,
) -> None:
    # Given Redis is down when the outcome is written
    status_writer = FakeStatusWriter(error=ConnectionError("redis down"))
    delivery = _delivery({"acme": ACME}, status_writer, insert)

    # When / Then the batch must not be committed as delivered
    with pytest.raises(ConnectionError, match="redis down"):
        delivery.deliver_for_environment("acme", [_event("acme", "a1")])


def test_deliver_for_environment__our_own_failure__raises_and_nothing_committed(
    status_writer: FakeStatusWriter,
    insert: Any,
) -> None:
    # Given a failure that is not the customer's: a bug, or Redis down
    insert.side_effect = RuntimeError("boom")
    delivery = _delivery({"acme": ACME}, status_writer, insert)

    # When / Then nothing is reported against the customer
    with pytest.raises(RuntimeError, match="boom"):
        delivery.deliver_for_environment("acme", [_event("acme", "a1")])
    assert status_writer.writes == []


def test_deliver_batch__two_environments__inserted_at_the_same_time(
    status_writer: FakeStatusWriter,
    insert: Any,
) -> None:
    # Given each insert waits until the other one has started, which can only
    # happen if they run side by side
    started = threading.Barrier(2, timeout=5)

    def insert_when_both_started(
        connection: WarehouseConnection, rows: Sequence[bytes]
    ) -> int:
        started.wait()
        return len(rows)

    insert.side_effect = insert_when_both_started
    delivery = _delivery({"acme": ACME, "other": OTHER}, status_writer, insert)

    # When
    outcomes = delivery.deliver_batch([_event("acme", "a1"), _event("other", "o1")])

    # Then
    assert outcomes == [
        Outcome("acme", "delivered", rows=1),
        Outcome("other", "delivered", rows=1),
    ]


def test_deliver_batch__our_own_failure_in_one_environment__raises(
    status_writer: FakeStatusWriter,
    insert: Any,
) -> None:
    # Given one insert hits a bug on our side while the other succeeds
    def insert_or_crash(connection: WarehouseConnection, rows: Sequence[bytes]) -> int:
        if connection is ACME:
            raise RuntimeError("boom")
        return len(rows)

    insert.side_effect = insert_or_crash
    delivery = _delivery({"acme": ACME, "other": OTHER}, status_writer, insert)

    # When / Then the batch must not be committed
    with pytest.raises(RuntimeError, match="boom"):
        delivery.deliver_batch([_event("acme", "a1"), _event("other", "o1")])
