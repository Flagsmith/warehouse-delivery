import json

import pytest
from freezegun import freeze_time
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from warehouse_delivery.connections import (
    PostgresWarehouseConnections,
    WarehouseConnection,
)
from warehouse_delivery.crypto import fernet_from_secret
from warehouse_delivery.errors import DeliveryError

FERNET = fernet_from_secret("fernet-secret")
CONFIG = {
    "host": "ch.acme-corp.example",
    "port": 8443,
    "database": "acme_dwh",
    "username": "acme_svc",
    "secure": True,
}


@pytest.fixture()
def stored_connection(database_pool: ConnectionPool) -> None:
    with database_pool.connection() as database:
        database.execute(
            "INSERT INTO experimentation_delivery_connections "
            "VALUES (%s, %s, %s, %s, %s)",
            [
                "client-env-key",
                42,
                "clickhouse",
                Jsonb(CONFIG),
                FERNET.encrypt(json.dumps({"password": "hunter2"}).encode()).decode(),
            ],
        )


def test_warehouse_connection__repr__omits_the_credentials() -> None:
    # Given / When
    shown = repr(
        WarehouseConnection(
            id=42,
            warehouse_type="clickhouse",
            config=CONFIG,
            credentials={"password": "hunter2"},
        )
    )

    # Then
    assert "hunter2" not in shown


def test_postgres_connections_get__environment_with_connection__returns_it_decrypted(
    database_pool: ConnectionPool,
    stored_connection: None,
) -> None:
    # Given
    store = PostgresWarehouseConnections(database_pool, FERNET)

    # When
    connection = store.get("client-env-key")

    # Then
    assert connection == WarehouseConnection(
        id=42,
        warehouse_type="clickhouse",
        config=CONFIG,
        credentials={"password": "hunter2"},
    )


def test_postgres_connections_get__environment_without_connection__returns_none(
    database_pool: ConnectionPool,
) -> None:
    # Given an environment that sends events to Flagsmith's warehouse only
    store = PostgresWarehouseConnections(database_pool, FERNET)

    # When
    connection = store.get("client-env-key")

    # Then
    assert connection is None


def test_postgres_connections_get__deleted_while_cached__still_served_from_cache(
    database_pool: ConnectionPool,
    stored_connection: None,
) -> None:
    # Given a connection looked up once, then deleted
    store = PostgresWarehouseConnections(database_pool, FERNET)
    first = store.get("client-env-key")
    with database_pool.connection() as database:
        database.execute("DELETE FROM experimentation_delivery_connections")

    # When
    second = store.get("client-env-key")

    # Then
    assert second == first


def test_postgres_connections_get__deleted_and_cache_expired__returns_none(
    database_pool: ConnectionPool,
    stored_connection: None,
) -> None:
    # Given a connection looked up once, deleted, and cached for over a minute
    store = PostgresWarehouseConnections(database_pool, FERNET)
    with freeze_time() as frozen_time:
        store.get("client-env-key")
        with database_pool.connection() as database:
            database.execute("DELETE FROM experimentation_delivery_connections")
        frozen_time.tick(61)

        # When
        connection = store.get("client-env-key")

    # Then
    assert connection is None


def test_postgres_connections_get__undecryptable_credentials__raises_with_connection_id(
    database_pool: ConnectionPool,
    stored_connection: None,
) -> None:
    # Given credentials encrypted with a secret this service does not hold
    store = PostgresWarehouseConnections(
        database_pool, fernet_from_secret("other-secret")
    )

    # When / Then the id travels with the error so the dashboard can be updated
    with pytest.raises(DeliveryError) as excinfo:
        store.get("client-env-key")
    assert excinfo.value.detail == "Stored credentials cannot be decrypted."
    assert excinfo.value.connection_id == 42


def test_postgres_connections_get__no_stored_credentials__returns_empty_credentials(
    database_pool: ConnectionPool,
) -> None:
    # Given a warehouse type the API stores no credentials for
    with database_pool.connection() as database:
        database.execute(
            "INSERT INTO experimentation_delivery_connections "
            "VALUES (%s, %s, %s, %s, NULL)",
            ["client-env-key", 42, "snowflake", Jsonb({"account": "acme"})],
        )
    store = PostgresWarehouseConnections(database_pool, FERNET)

    # When
    connection = store.get("client-env-key")

    # Then
    assert connection is not None
    assert connection.credentials == {}


def test_postgres_connections_get__credentials_not_json__raises_with_connection_id(
    database_pool: ConnectionPool,
) -> None:
    # Given a token made with our secret that does not hold JSON
    with database_pool.connection() as database:
        database.execute(
            "INSERT INTO experimentation_delivery_connections "
            "VALUES (%s, %s, %s, %s, %s)",
            [
                "client-env-key",
                42,
                "clickhouse",
                Jsonb(CONFIG),
                FERNET.encrypt(b"not json").decode(),
            ],
        )
    store = PostgresWarehouseConnections(database_pool, FERNET)

    # When / Then
    with pytest.raises(DeliveryError) as excinfo:
        store.get("client-env-key")
    assert excinfo.value.detail == "Stored credentials cannot be decrypted."
    assert excinfo.value.connection_id == 42
