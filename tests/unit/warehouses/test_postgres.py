import dataclasses
import json
import socket
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from pytest_mock import MockerFixture
from pytest_structlog import StructuredLogCapture

from warehouse_delivery import network
from warehouse_delivery.connections import WarehouseConnection
from warehouse_delivery.errors import DeliveryError
from warehouse_delivery.warehouses import postgres
from warehouse_delivery.warehouses.clickhouse import INSERT_TIMEOUT_SECONDS
from warehouse_delivery.warehouses.postgres import PostgresWarehouse

# The stored config plus the decrypted password.
DETAILS: dict[str, Any] = {
    "host": "pg.acme-corp.example",
    "port": 5432,
    "database": "acme_dwh",
    "schema": "flagsmith_exp",
    "username": "flagsmith",
    "sslmode": "require",
    "password": "hunter2",
}
WAREHOUSE = PostgresWarehouse(connection_id=7, **DETAILS)
EVENTS_DDL = """
CREATE SCHEMA {schema};
CREATE TABLE {schema}.events (
  environment_key TEXT NOT NULL,
  event           TEXT NOT NULL,
  feature_name    TEXT,
  timestamp       TIMESTAMPTZ NOT NULL,
  collected_at    TIMESTAMPTZ,
  identifier      TEXT NOT NULL,
  value           TEXT,
  traits          TEXT,
  metadata        TEXT,
  sdk_language    TEXT,
  sdk_version     TEXT
);
"""
EXPOSURE = {
    "environment_key": "env",
    "event": "$flag_exposure",
    "feature_name": "checkout",
    "timestamp": 1753000000123,
    "collected_at": 1753000001000,
    "identifier": "u1",
    "value": "variant_a",
    "traits": {"plan": "pro"},
    "metadata": {"page": "/cart"},
    "sdk_language": "python",
    "sdk_version": "5.0.0",
}
SENTENCES = {
    "authentication": "Authentication failed.",
    "missing_database": "Database does not exist.",
    "unreachable": "Could not connect to the host.",
    "missing_table": "Events table not found in the configured database. "
    "Run the setup SQL to create it.",
    "permission_denied": "Permission denied on the events table. "
    "The user needs SELECT and INSERT on it.",
    "schema_mismatch": "The events table's columns don't match the setup SQL.",
    "timeout": "The query timed out.",
    "rejected": "The PostgreSQL server rejected the request.",
}
MISSING = object()
END_SESSION = "PERFORM pg_terminate_backend(pg_backend_pid()); RETURN NEW;"
READ_ONLY = "RAISE EXCEPTION 'read only' USING ERRCODE = '25006';"
SLOW = "PERFORM pg_sleep(1); RETURN NEW;"


def _connection(details: dict[str, Any]) -> WarehouseConnection:
    config = {k: v for k, v in details.items() if v is not MISSING}
    password = config.pop("password", MISSING)
    credentials = {} if password is MISSING else {"password": password}
    return WarehouseConnection(
        id=7, warehouse_type="postgres", config=config, credentials=credentials
    )


def _payload(event: dict[str, Any]) -> bytes:
    return json.dumps({k: v for k, v in event.items() if v is not MISSING}).encode()


def _admin_execute(database_url: str, statement: str, schema: str) -> None:
    with psycopg.connect(database_url, autocommit=True) as admin:
        admin.execute(
            sql.SQL(statement).format(
                schema=sql.Identifier(schema),
                table=sql.Identifier(schema, "events"),
            )
        )


def _trigger(body: str) -> str:
    return (
        "CREATE FUNCTION {schema}.on_insert() RETURNS trigger LANGUAGE plpgsql "
        f"AS $$ BEGIN {body} END $$; CREATE TRIGGER on_insert BEFORE INSERT "
        "ON {table} FOR EACH ROW EXECUTE FUNCTION {schema}.on_insert();"
    )


def _events(database_url: str, schema: str) -> list[dict[str, Any]]:
    with psycopg.connect(database_url) as database:
        cursor = database.cursor(row_factory=psycopg.rows.dict_row)
        return cursor.execute(
            sql.SQL("SELECT * FROM {} ORDER BY timestamp").format(
                sql.Identifier(schema, "events")
            )
        ).fetchall()


@pytest.fixture()
def events_schema(database_url: str) -> Iterator[str]:
    schema = f"test_{uuid4().hex}"
    _admin_execute(database_url, EVENTS_DDL, schema)
    yield schema
    _admin_execute(database_url, "DROP SCHEMA IF EXISTS {schema} CASCADE", schema)


@pytest.fixture()
def target(database_url: str, events_schema: str) -> dict[str, Any]:
    """Details of the test database's events schema."""
    server: dict[str, Any] = conninfo_to_dict(database_url)
    return {
        "host": server["host"],
        "port": int(server.get("port", 5432)),
        "database": server["dbname"],
        "schema": events_schema,
        "username": server["user"],
        "sslmode": "disable",
        "password": server["password"],
    }


@pytest.fixture()
def warehouse(target: dict[str, Any]) -> PostgresWarehouse:
    return PostgresWarehouse.from_connection(_connection(target))


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({}, id="as-stored"),
        pytest.param({"host": "db"}, id="single-label-host"),
        pytest.param({"host": "my_db.internal"}, id="underscore-host"),
        pytest.param({"host": "10.0.0.5"}, id="ipv4"),
        pytest.param({"host": "2001:db8::1"}, id="ipv6"),
        pytest.param({"sslmode": "disable"}, id="ssl-disabled"),
    ],
)
def test_from_connection__valid_details__builds_the_warehouse(
    overrides: dict[str, Any],
) -> None:
    # Given / When
    warehouse = PostgresWarehouse.from_connection(_connection({**DETAILS, **overrides}))

    # Then
    assert warehouse == dataclasses.replace(WAREHOUSE, **overrides)
    assert "hunter2" not in repr(warehouse)


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"source": "deployment", "host": MISSING}, id="deployment"),
        pytest.param({"port": MISSING}, id="no-port"),
        pytest.param({"password": MISSING}, id="no-password"),
        pytest.param({"password": ""}, id="empty-password"),
        pytest.param({"password": "ab c\x00def"}, id="nul-in-password"),
        pytest.param({"username": "a\x00b"}, id="nul-in-username"),
        pytest.param({"database": ""}, id="empty-database"),
        pytest.param({"host": "/var/run/postgresql"}, id="socket-host"),
        pytest.param({"host": "a.example,b.example"}, id="multi-host"),
        pytest.param({"host": "pg host=evil"}, id="host-injection"),
        pytest.param({"port": "5432"}, id="port-as-text"),
        pytest.param({"port": True}, id="port-as-bool"),
        pytest.param({"port": 0}, id="port-zero"),
        pytest.param({"port": 65536}, id="port-too-high"),
        pytest.param({"schema": "a.b"}, id="dotted-schema"),
        pytest.param({"sslmode": "prefer"}, id="ssl-prefer"),
        pytest.param({"sslmode": "verify-full"}, id="ssl-verify-full"),
    ],
)
def test_from_connection__invalid_details__raises_stored_connection_failure(
    overrides: dict[str, Any],
) -> None:
    # Given
    connection = _connection({**DETAILS, **overrides})

    # When / Then
    with pytest.raises(DeliveryError) as excinfo:
        PostgresWarehouse.from_connection(connection)
    assert excinfo.value.kind == "stored_connection"
    assert excinfo.value.detail == "Stored connection details are incomplete."


def test_timeouts__worst_case__fits_inside_the_insert_timeout() -> None:
    # Given / When
    worst_case_ms = (
        postgres.CONNECT_TIMEOUT_SECONDS * 1000 + postgres.STATEMENT_TIMEOUT_MS
    )

    # Then
    assert worst_case_ms < INSERT_TIMEOUT_SECONDS * 1000


@pytest.mark.parametrize(
    "checked, pinned",
    [
        pytest.param(None, {}, id="blocking-off"),
        pytest.param("203.0.113.7", {"hostaddr": "203.0.113.7"}, id="pinned"),
    ],
)
def test_insert__connect__passes_the_stored_details_and_timeouts(
    mocker: MockerFixture,
    checked: str | None,
    pinned: dict[str, str],
) -> None:
    # Given
    checked_address = mocker.patch(
        "warehouse_delivery.warehouses.postgres.checked_address", return_value=checked
    )
    connect = mocker.patch("warehouse_delivery.warehouses.postgres.psycopg.connect")

    # When
    WAREHOUSE.insert([_payload(EXPOSURE)])

    # Then
    checked_address.assert_called_once_with("pg.acme-corp.example", 5432)
    connect.assert_called_once_with(
        **pinned,
        host="pg.acme-corp.example",
        port=5432,
        dbname="acme_dwh",
        user="flagsmith",
        password="hunter2",
        sslmode="require",
        application_name="flagsmith-warehouse-delivery",
        connect_timeout=10,
        tcp_user_timeout=30_000,
        keepalives_idle=10,
        keepalives_interval=5,
        keepalives_count=3,
    )


def test_insert__host_resolves_to_an_internal_address__raises_without_connecting(
    mocker: MockerFixture,
) -> None:
    # Given a host that now resolves to an internal address among public ones
    network.block_internal_hosts(True)
    mocker.patch(
        "warehouse_delivery.network.socket.getaddrinfo",
        return_value=[
            (socket.AF_INET, 1, 6, "", ("203.0.113.7", 5432)),
            (socket.AF_INET, 1, 6, "", ("10.0.0.5", 5432)),
        ],
    )
    connect = mocker.patch("warehouse_delivery.warehouses.postgres.psycopg.connect")

    # When
    try:
        with pytest.raises(DeliveryError) as excinfo:
            WAREHOUSE.insert([_payload(EXPOSURE)])
    finally:
        network.block_internal_hosts(False)

    # Then
    assert excinfo.value.kind == "stored_connection"
    assert excinfo.value.detail == "The host resolves to an internal address."
    connect.assert_not_called()


def test_insert__no_usable_rows__does_not_connect(mocker: MockerFixture) -> None:
    # Given
    connect = mocker.patch("warehouse_delivery.warehouses.postgres.psycopg.connect")

    # When / Then
    assert WAREHOUSE.insert([b"{not json"]) == 0
    connect.assert_not_called()


def test_insert__connection_string_unbuildable__raises_stored_connection_failure(
    mocker: MockerFixture,
) -> None:
    # Given
    mocker.patch(
        "warehouse_delivery.warehouses.postgres.psycopg.connect",
        side_effect=psycopg.ProgrammingError("unterminated quoted string"),
    )

    # When / Then
    with pytest.raises(DeliveryError) as excinfo:
        WAREHOUSE.insert([_payload(EXPOSURE)])
    assert excinfo.value.kind == "stored_connection"
    assert excinfo.value.__cause__ is None


def test_insert__events__copies_every_column_and_skips_unusable_rows(
    warehouse: PostgresWarehouse,
    database_url: str,
    events_schema: str,
    log: StructuredLogCapture,
) -> None:
    # Given / When
    written = warehouse.insert([_payload(EXPOSURE), b"{not json", b'["a list"]'])

    # Then
    assert written == 1
    assert _events(database_url, events_schema) == [
        {
            "environment_key": "env",
            "event": "$flag_exposure",
            "feature_name": "checkout",
            "timestamp": datetime(2025, 7, 20, 8, 26, 40, 123000, tzinfo=UTC),
            "collected_at": datetime(2025, 7, 20, 8, 26, 41, tzinfo=UTC),
            "identifier": "u1",
            "value": "variant_a",
            "traits": '{"plan":"pro"}',
            "metadata": '{"page":"/cart"}',
            "sdk_language": "python",
            "sdk_version": "5.0.0",
        }
    ]
    assert log.events == [
        {
            "level": "warning",
            "event": "rows.skipped",
            "connection__id": 7,
            "rows__count": 2,
        }
    ]


@pytest.mark.parametrize(
    "overrides, column, expected",
    [
        pytest.param({"value": 42}, "value", "42", id="integer"),
        pytest.param({"value": -9.99}, "value", "-9.99", id="float"),
        pytest.param({"value": True}, "value", "true", id="bool"),
        pytest.param({"value": None}, "value", None, id="null"),
        pytest.param({"value": [1, [True]]}, "value", "[1,[true]]", id="array"),
        pytest.param({"identifier": MISSING}, "identifier", "", id="no-identifier"),
        pytest.param(
            {"collected_at": MISSING}, "collected_at", None, id="no-collected"
        ),
        pytest.param({"feature_name": MISSING}, "feature_name", None, id="no-feature"),
        pytest.param({"identifier": "u\x001"}, "identifier", "u1", id="nul-stripped"),
        # JSON allows a lone surrogate but UTF-8 cannot encode it; left in, it
        # would fail the batch on every retry
        pytest.param({"value": "a\ud800b"}, "value", "a\ufffdb", id="lone-surrogate"),
    ],
)
def test_insert__event_field__stored_as_text(
    warehouse: PostgresWarehouse,
    database_url: str,
    events_schema: str,
    overrides: dict[str, Any],
    column: str,
    expected: str | None,
) -> None:
    # Given / When
    warehouse.insert([_payload({**EXPOSURE, **overrides})])

    # Then
    (row,) = _events(database_url, events_schema)
    assert row[column] == expected


@pytest.mark.parametrize(
    "statement, overrides, kind",
    [
        pytest.param(None, {"password": "wrong"}, "authentication", id="password"),
        pytest.param(None, {"database": "nope"}, "missing_database", id="database"),
        pytest.param(None, {"host": "127.0.0.1", "port": 1}, "unreachable", id="port"),
        pytest.param(_trigger(END_SESSION), {}, "unreachable", id="lost-mid-copy"),
        pytest.param("DROP TABLE {table}", {}, "missing_table", id="no-table"),
        pytest.param("DROP SCHEMA {schema} CASCADE", {}, "missing_table", id="schema"),
        pytest.param(_trigger(READ_ONLY), {}, "rejected", id="other-server-error"),
        pytest.param(_trigger(SLOW), {}, "timeout", id="statement-timeout"),
    ],
)
def test_insert__server_or_connection_fails__raises_its_delivery_error(
    target: dict[str, Any],
    database_url: str,
    events_schema: str,
    mocker: MockerFixture,
    statement: str | None,
    overrides: dict[str, Any],
    kind: str,
) -> None:
    # Given
    mocker.patch("warehouse_delivery.warehouses.postgres.STATEMENT_TIMEOUT_MS", 100)
    if statement is not None:
        _admin_execute(database_url, statement, events_schema)
    warehouse = PostgresWarehouse.from_connection(_connection({**target, **overrides}))

    # When / Then
    with pytest.raises(DeliveryError) as excinfo:
        warehouse.insert([_payload(EXPOSURE)])
    assert (excinfo.value.kind, excinfo.value.detail) == (kind, SENTENCES[kind])


@pytest.mark.parametrize(
    "statement, sqlstate, constraint",
    [
        pytest.param("ALTER TABLE {table} DROP COLUMN traits", "42703", None, id="col"),
        pytest.param(
            "ALTER TABLE {table} ALTER COLUMN value TYPE integer USING NULL",
            "22P02",
            None,
            id="column-type",
        ),
        pytest.param(
            "ALTER TABLE {table} ADD CONSTRAINT c CHECK (event <> '$flag_exposure')",
            "23514",
            "c",
            id="constraint",
        ),
    ],
)
def test_insert__table_does_not_match__raises_schema_mismatch_without_the_rows(
    warehouse: PostgresWarehouse,
    database_url: str,
    events_schema: str,
    log: StructuredLogCapture,
    statement: str,
    sqlstate: str,
    constraint: str | None,
) -> None:
    # Given a table that takes the first event but not the second
    _admin_execute(database_url, statement, events_schema)
    purchase = {**EXPOSURE, "event": "purchase", "value": 1}

    # When
    with pytest.raises(DeliveryError) as excinfo:
        warehouse.insert([_payload(purchase), _payload(EXPOSURE)])

    # Then nothing is written, and the end users' data Postgres quotes in its
    # error reaches neither the exception chain nor the logs
    assert excinfo.value.kind == "schema_mismatch"
    assert excinfo.value.detail == SENTENCES["schema_mismatch"]
    assert excinfo.value.__cause__ is None
    assert excinfo.value.__suppress_context__
    assert log.events == [
        {
            "level": "warning",
            "event": "rows.rejected",
            "connection__id": 7,
            "error__sqlstate": sqlstate,
            "error__constraint": constraint,
            "error__column": None,
        }
    ]
    assert _events(database_url, events_schema) == []


def test_insert__user_without_insert__raises_permission_denied(
    target: dict[str, Any],
    database_url: str,
    events_schema: str,
) -> None:
    # Given a user that can see the schema but not write to the table
    role = f"test_{uuid4().hex}"
    _admin_execute(
        database_url,
        f"CREATE ROLE {role} LOGIN PASSWORD 'x'; "
        f"GRANT USAGE ON SCHEMA {{schema}} TO {role}",
        events_schema,
    )
    warehouse = PostgresWarehouse.from_connection(
        _connection({**target, "username": role, "password": "x"})
    )

    # When
    try:
        with pytest.raises(DeliveryError) as excinfo:
            warehouse.insert([_payload(EXPOSURE)])
    finally:
        _admin_execute(
            database_url, f"DROP OWNED BY {role}; DROP ROLE {role}", events_schema
        )

    # Then
    assert excinfo.value.kind == "permission_denied"
    assert excinfo.value.detail == SENTENCES["permission_denied"]
