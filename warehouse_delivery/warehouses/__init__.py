from collections.abc import Callable, Sequence
from typing import Protocol

from warehouse_delivery.connections import WarehouseConnection
from warehouse_delivery.warehouses.clickhouse import ClickHouseWarehouse
from warehouse_delivery.warehouses.databricks import DatabricksWarehouse


class Warehouse(Protocol):
    def insert(self, rows: Sequence[bytes]) -> int:
        """Inserts the given JSON rows into the customer's warehouse in one
        request and returns how many rows were written.

        Raises ``DeliveryError`` if the customer's warehouse or their stored
        connection details are at fault. Any other exception means a bug or an
        outage on our side and is left to propagate."""


_ADAPTERS: dict[str, Callable[[WarehouseConnection], Warehouse]] = {
    "clickhouse": ClickHouseWarehouse.from_connection,
    "databricks": DatabricksWarehouse.from_connection,
}


def warehouse_for(connection: WarehouseConnection) -> Warehouse | None:
    """Picks the code that knows how to insert into this connection's kind of
    warehouse. Returns None for kinds not supported yet, such as Snowflake."""
    adapter = _ADAPTERS.get(connection.warehouse_type)
    return adapter(connection) if adapter is not None else None
