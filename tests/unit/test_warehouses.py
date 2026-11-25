from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from warehouse_delivery.connections import WarehouseConnection
from warehouse_delivery.warehouses import warehouse_for
from warehouse_delivery.warehouses.clickhouse import ClickHouseWarehouse
from warehouse_delivery.warehouses.databricks import DatabricksWarehouse
from warehouse_delivery.warehouses.postgres import PostgresWarehouse
from warehouse_delivery.warehouses.snowflake import SnowflakeWarehouse

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


def test_warehouse_for__postgres_connection__builds_the_postgres_adapter() -> None:
    # Given
    connection = WarehouseConnection(
        id=11,
        warehouse_type="postgres",
        config={
            "host": "pg.acme-corp.example",
            "port": 5432,
            "database": "acme_dwh",
            "schema": "flagsmith_exp",
            "username": "flagsmith",
            "sslmode": "require",
        },
        credentials={"password": "hunter2"},
    )

    # When
    warehouse = warehouse_for(connection)

    # Then
    assert warehouse == PostgresWarehouse(
        connection_id=11,
        host="pg.acme-corp.example",
        port=5432,
        database="acme_dwh",
        schema="flagsmith_exp",
        username="flagsmith",
        password="hunter2",
        sslmode="require",
    )


def test_warehouse_for__snowflake_connection__builds_the_snowflake_adapter() -> None:
    # Given
    private_key = (
        rsa.generate_private_key(public_exponent=65537, key_size=2048)
        .private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        .decode()
    )
    connection = WarehouseConnection(
        id=13,
        warehouse_type="snowflake",
        config={
            "account_identifier": "ACME-PROD",
            "warehouse": "FLAGSMITH_WH",
            "database": "FLAGSMITH",
            "schema": "FLAGSMITH_EXP",
            "role": "FLAGSMITH_ROLE",
            "user": "FLAGSMITH_SERVICE",
        },
        credentials={"private_key": private_key},
    )

    # When
    warehouse = warehouse_for(connection)

    # Then
    assert warehouse == SnowflakeWarehouse(
        connection_id=13,
        account_identifier="ACME-PROD",
        database="FLAGSMITH",
        schema="FLAGSMITH_EXP",
        user="FLAGSMITH_SERVICE",
        private_key=private_key,
    )


def test_warehouse_for__type_without_an_adapter__returns_none() -> None:
    # Given a connection type nothing here delivers to
    connection = WarehouseConnection(
        id=9,
        warehouse_type="bigquery",
        config={"project_id": "acme"},
        credentials={},
    )

    # When / Then
    assert warehouse_for(connection) is None
