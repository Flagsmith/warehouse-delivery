import pytest
from psycopg_pool import ConnectionPool
from pytest_structlog import StructuredLogCapture

from warehouse_delivery.connection_status import PostgresConnectionStatusWriter


@pytest.fixture()
def warehouse_connection_id(database_pool: ConnectionPool) -> int:
    with database_pool.connection() as database:
        database.execute("INSERT INTO experimentation_warehouseconnection VALUES (42)")
    return 42


def test_postgres_status_writer_write__delivery_outcome__updates_connection_status(
    database_pool: ConnectionPool,
    warehouse_connection_id: int,
) -> None:
    # Given a connection whose last delivery failed
    writer = PostgresConnectionStatusWriter(database_pool)
    writer.write(warehouse_connection_id, "errored", "Authentication failed.")

    # When
    writer.write(warehouse_connection_id, "connected", None)

    # Then
    with database_pool.connection() as database:
        rows = database.execute(
            "SELECT connection_id, status, detail "
            "FROM experimentation_warehousedeliverystatus"
        ).fetchall()
    assert rows == [(warehouse_connection_id, "connected", None)]


def test_postgres_status_writer_write__connection_deleted__skips_and_logs(
    database_pool: ConnectionPool,
    log: StructuredLogCapture,
) -> None:
    # Given a connection deleted while the service still had it cached

    # When
    PostgresConnectionStatusWriter(database_pool).write(42, "connected", None)

    # Then
    assert log.has(
        "delivery_status.connection_missing", level="warning", connection__id=42
    )
