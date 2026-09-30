import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial

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
from warehouse_delivery.retries import RetryWriter
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
    customer's warehouse, several customers at a time.

    If a customer's warehouse rejects their events, the connection shows as
    errored in the dashboard and the events are written for retry. If
    something fails on our side instead, the error is raised, nothing is
    marked done in Kafka, and the same messages are delivered again after a
    restart.

    This class never talks to Kafka or to a warehouse driver itself: the
    caller hands it messages, ``WarehouseConnections`` says where each customer's
    warehouse is, a ``Warehouse`` does the insert, and a ``RetryWriter`` keeps
    what failed."""

    def __init__(
        self,
        *,
        concurrency: int,
        connections: WarehouseConnections,
        status_writer: ConnectionStatusWriter,
        retry_writer: RetryWriter,
        warehouse_for: Callable[
            [WarehouseConnection], Warehouse | None
        ] = warehouses.warehouse_for,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._connections = connections
        self._status_writer = status_writer
        self._retry_writer = retry_writer
        self._warehouse_for = warehouse_for
        self._clock = clock
        self._concurrency = concurrency

    def deliver_batch(self, events: Sequence[Event]) -> list[Outcome]:
        groups, unattributable = group_by_environment(events)
        if unattributable:
            logger.error(
                "delivery.dropped",
                reason="no_environment_key",
                events__count=unattributable,
            )
        if not groups:
            return []
        # If one insert fails on our side, the error is raised only after the
        # other inserts have finished.
        with ThreadPoolExecutor(
            max_workers=min(self._concurrency, len(groups)),
            thread_name_prefix="delivery",
            # Worker threads start with an empty logging context. Carry the
            # caller's over, so their logs say which topic the batch came from.
            initializer=partial(
                structlog.contextvars.bind_contextvars,
                **structlog.contextvars.get_contextvars(),
            ),
        ) as executor:
            outcomes = list(
                executor.map(self.deliver_for_environment, groups, groups.values())
            )
        self._retry_writer.flush()
        return outcomes

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
            # carry on with the other customers in this batch, and write these
            # events to be tried again.
            connection_id = connection.id if connection else error.connection_id
            if connection_id is not None:
                log = log.bind(connection__id=connection_id)
                self._status_writer.write(connection_id, ERRORED, error.detail)
            self._retry_writer.write(environment_key, events)
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
