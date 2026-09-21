import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass

import structlog

from warehouse_delivery import warehouses
from warehouse_delivery.connection_status import (
    CONNECTED,
    ERRORED,
    ConnectionStatusWriter,
)
from warehouse_delivery.connections import WarehouseConnection, WarehouseConnections
from warehouse_delivery.errors import DeliveryError
from warehouse_delivery.events import Event, group_by_environment
from warehouse_delivery.warehouses import Warehouse

logger = structlog.get_logger("warehouse")

DELIVERED = "delivered"
FAILED = "failed"
DROPPED = "dropped"


@dataclass(frozen=True)
class Outcome:
    environment_key: str
    result: str
    rows: int = 0
    reason: str | None = None


class DeliveryService:
    """Takes the messages from one Kafka poll and inserts them into each
    customer's warehouse, one customer at a time.

    If a customer's warehouse rejects their events, the connection shows as
    errored in the dashboard and those events are lost; there is no retry yet.
    If something fails on our side instead, the error is raised, nothing is
    marked done in Kafka, and the same messages are delivered again after a
    restart.

    This class never talks to Kafka or to a warehouse driver itself: the
    caller hands it messages, ``WarehouseConnections`` says where each customer's
    warehouse is, and a ``Warehouse`` does the insert."""

    def __init__(
        self,
        *,
        connections: WarehouseConnections,
        status_writer: ConnectionStatusWriter,
        warehouse_for: Callable[
            [WarehouseConnection], Warehouse | None
        ] = warehouses.warehouse_for,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._connections = connections
        self._status_writer = status_writer
        self._warehouse_for = warehouse_for
        self._clock = clock

    def deliver_batch(self, events: Sequence[Event]) -> list[Outcome]:
        groups, unattributable = group_by_environment(events)
        if unattributable:
            logger.error(
                "delivery.dropped",
                reason="no_environment_key",
                events__count=unattributable,
            )
        return [
            self.deliver_for_environment(environment_key, group)
            for environment_key, group in groups.items()
        ]

    def deliver_for_environment(
        self,
        environment_key: str,
        events: Sequence[Event],
    ) -> Outcome:
        log = logger.bind(environment__key=environment_key, events__count=len(events))
        connection: WarehouseConnection | None = None
        started = self._clock()
        try:
            connection = self._connections.get(environment_key)
            if connection is None:
                return self._drop(log, environment_key, "no_connection")
            log = log.bind(connection__id=connection.id)
            warehouse = self._warehouse_for(connection)
            if warehouse is None:
                return self._drop(log, environment_key, "unsupported_warehouse_type")
            rows = warehouse.insert([event.payload for event in events])
        except DeliveryError as error:
            # Something on the customer's side is wrong: their warehouse is
            # down, refused our login, has no events table, or the connection
            # details the API stored are unusable. Show the reason on their
            # connection in the dashboard when we know which connection it is,
            # carry on with the other customers in this batch, and let these
            # events go. There is no retry yet.
            connection_id = connection.id if connection else error.connection_id
            if connection_id is not None:
                log = log.bind(connection__id=connection_id)
                self._status_writer.write(connection_id, ERRORED, error.detail)
            log.error("delivery.failed", failure__kind=error.kind, exc_info=error)
            return Outcome(environment_key, FAILED, reason=error.kind)

        duration = self._clock() - started
        self._status_writer.write(connection.id, CONNECTED, None)
        log.info(
            "delivery.completed",
            rows__count=rows,
            bytes__count=sum(len(event.payload) for event in events),
            duration__ms=round(duration * 1000),
        )
        return Outcome(environment_key, DELIVERED, rows=rows)

    @staticmethod
    def _drop(log: structlog.BoundLogger, environment_key: str, reason: str) -> Outcome:
        log.warning("delivery.dropped", reason=reason)
        return Outcome(environment_key, DROPPED, reason=reason)
