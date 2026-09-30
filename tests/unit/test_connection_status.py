from psycopg_pool import ConnectionPool

from warehouse_delivery.connection_status import PostgresConnectionStatusWriter


def test_postgres_status_writer_write__delivery_outcome__updates_connection_status(
    database_pool: ConnectionPool,
) -> None:
    # Given a connection whose last delivery failed
    writer = PostgresConnectionStatusWriter(database_pool)
    writer.write(42, "errored", "Authentication failed.")

    # When
    writer.write(42, "connected", None)

    # Then
    with database_pool.connection() as database:
        rows = database.execute(
            "SELECT connection_id, status, detail "
            "FROM experimentation_warehousedeliverystatus"
        ).fetchall()
    assert rows == [(42, "connected", None)]
