from warehouse_delivery.connections import WarehouseConnection
from warehouse_delivery.warehouses import warehouse_for
from warehouse_delivery.warehouses.clickhouse import ClickHouseWarehouse

CONFIG = {
    "host": "ch.acme-corp.example",
    "port": 8443,
    "database": "acme_dwh",
    "username": "acme_svc",
    "secure": True,
}


def test_warehouse_for__clickhouse_connection__builds_the_clickhouse_adapter() -> None:
    # Given
    connection = WarehouseConnection(
        id=42,
        warehouse_type="clickhouse",
        config=CONFIG,
        credentials={"password": "hunter2"},
    )

    # When
    warehouse = warehouse_for(connection)

    # Then
    assert warehouse == ClickHouseWarehouse(
        host="ch.acme-corp.example",
        port=8443,
        database="acme_dwh",
        username="acme_svc",
        password="hunter2",
        secure=True,
    )


def test_warehouse_for__type_without_an_adapter__returns_none() -> None:
    # Given a connection type the API accepts but nothing here delivers to yet
    connection = WarehouseConnection(
        id=9,
        warehouse_type="snowflake",
        config={"account_identifier": "acme"},
        credentials={},
    )

    # When / Then
    assert warehouse_for(connection) is None
