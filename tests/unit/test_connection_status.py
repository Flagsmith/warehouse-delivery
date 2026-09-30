import json

from psycopg_pool import ConnectionPool
from pytest_mock import MockerFixture

from warehouse_delivery.connection_status import (
    PostgresConnectionStatusWriter,
    RedisConnectionStatusWriter,
)


def test_write__outcome__lands_in_the_hash_the_api_drains(
    mocker: MockerFixture,
) -> None:
    # Given
    client = mocker.Mock()
    writer = RedisConnectionStatusWriter(client, clock=lambda: 1_758_000_000.0)

    # When
    writer.write(42, "errored", "Authentication failed.")

    # Then the entry sits under the connection id, for the API to apply
    client.hset.assert_called_once()
    hash_key, field, value = client.hset.call_args.args
    assert hash_key == "experimentation:warehouse_delivery_status"
    assert field == "42"
    assert json.loads(value) == {
        "status": "errored",
        "detail": "Authentication failed.",
        "at": 1_758_000_000.0,
    }


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
