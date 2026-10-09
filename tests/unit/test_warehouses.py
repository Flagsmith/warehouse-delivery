from warehouse_delivery.connections import WarehouseConnection
from warehouse_delivery.warehouses import warehouse_for
from warehouse_delivery.warehouses.clickhouse import ClickHouseWarehouse
from warehouse_delivery.warehouses.databricks import DatabricksWarehouse

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


def test_warehouse_for__databricks_connection__builds_the_databricks_adapter() -> None:
    # Given
    connection = WarehouseConnection(
        id=7,
        warehouse_type="databricks",
        config={
            "host": "dbc-a1b2c3d4-e5f6.cloud.databricks.com",
            "warehouse_id": "abc123",
            "catalog": "main",
            "schema": "flagsmith",
            "workspace_id": "1234567890123456",
            "region": "us-west-2",
        },
        credentials={"client_id": "sp-id", "client_secret": "sp-secret"},
    )

    # When
    warehouse = warehouse_for(connection)

    # Then
    assert warehouse == DatabricksWarehouse(
        connection_id=7,
        host="dbc-a1b2c3d4-e5f6.cloud.databricks.com",
        workspace_id="1234567890123456",
        region="us-west-2",
        catalog="main",
        schema="flagsmith",
        client_id="sp-id",
        client_secret="sp-secret",
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
