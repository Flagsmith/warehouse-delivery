import ipaddress
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import structlog
from psycopg import sql

from warehouse_delivery.connections import (
    INCOMPLETE_DETAIL,
    STORED_CONNECTION_FAILURE,
    WarehouseConnection,
)
from warehouse_delivery.errors import DeliveryError
from warehouse_delivery.network import checked_addresses
from warehouse_delivery.warehouses.rows import EventRow, parse_event

logger = structlog.get_logger("warehouse")

EVENTS_TABLE_NAME = "events"
APPLICATION_NAME = "flagsmith-warehouse-delivery"
COLUMNS = (
    "environment_key",
    "event",
    "feature_name",
    "timestamp",
    "collected_at",
    "identifier",
    "value",
    "traits",
    "metadata",
    "sdk_language",
    "sdk_version",
)
# The API accepts disable on self-hosted only, and checks that when the
# connection is saved.
SSL_MODES = frozenset({"require", "disable"})

# Their sum must stay under clickhouse.INSERT_TIMEOUT_SECONDS. The connect
# timeout applies to each address the host resolves to.
CONNECT_TIMEOUT_SECONDS = 10
STATEMENT_TIMEOUT_MS = 30_000
# statement_timeout is enforced by the server, so a network that drops our
# packets mid-COPY is caught by these instead.
TCP_USER_TIMEOUT_MS = 30_000
KEEPALIVES_IDLE_SECONDS = 10
KEEPALIVES_INTERVAL_SECONDS = 5
KEEPALIVES_COUNT = 3

_LABEL = r"[A-Za-z0-9_]([A-Za-z0-9_-]*[A-Za-z0-9_])?"
_HOSTNAME = re.compile(rf"{_LABEL}(\.{_LABEL})*")
_IDENTIFIER = re.compile(r"[A-Za-z0-9_]+")
_SURROGATE = re.compile("[\ud800-\udfff]")
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

UNREACHABLE_DETAIL = "Could not connect to the host."
AUTHENTICATION_DETAIL = "Authentication failed."
MISSING_DATABASE_DETAIL = "Database does not exist."
MISSING_TABLE_DETAIL = (
    "Events table not found in the configured database. Run the setup SQL to create it."
)
PERMISSION_DENIED_DETAIL = (
    "Permission denied on the events table. The user needs SELECT and INSERT on it."
)
SCHEMA_MISMATCH = "schema_mismatch"
SCHEMA_MISMATCH_DETAIL = "The events table's columns don't match the setup SQL."
TIMEOUT_DETAIL = "The query timed out."
REJECTED_DETAIL = "The PostgreSQL server rejected the request."

# libpq reports a failed connection without a SQLSTATE, so these are matched
# against its message. A server whose lc_messages is not English falls through
# to unreachable.
_AUTHENTICATION_FAILURE = re.compile(
    r'password authentication failed|no pg_hba\.conf entry|role ".*" does not exist'
)
_MISSING_DATABASE = re.compile(r'database ".*" does not exist')

UNDEFINED_TABLE = "42P01"
INVALID_SCHEMA_NAME = "3F000"
INSUFFICIENT_PRIVILEGE = "42501"
UNDEFINED_COLUMN = "42703"
QUERY_CANCELED = "57014"


@dataclass(frozen=True)
class PostgresWarehouse:
    """A customer's PostgreSQL events table. Each insert opens its own
    connection, as connections are not pooled across customers."""

    connection_id: int
    host: str
    port: int
    database: str
    schema: str
    username: str
    password: str = field(repr=False)
    sslmode: str

    @classmethod
    def from_connection(cls, connection: WarehouseConnection) -> PostgresWarehouse:
        """Re-validates the stored details: libpq reads a host starting with /
        as a socket directory and a comma-separated one as several hosts."""
        try:
            warehouse = cls(
                connection_id=connection.id,
                host=connection.config["host"],
                port=connection.config["port"],
                database=connection.config["database"],
                schema=connection.config["schema"],
                username=connection.config["username"],
                password=connection.credentials["password"],
                sslmode=connection.config["sslmode"],
            )
        except (KeyError, TypeError) as exc:
            raise DeliveryError(STORED_CONNECTION_FAILURE, INCOMPLETE_DETAIL) from exc
        if not warehouse._is_valid():
            raise DeliveryError(STORED_CONNECTION_FAILURE, INCOMPLETE_DETAIL)
        return warehouse

    def insert(self, rows: Sequence[bytes]) -> int:
        events = [event for row in rows if (event := parse_event(row)) is not None]
        if skipped := len(rows) - len(events):
            logger.warning(
                "rows.skipped",
                connection__id=self.connection_id,
                rows__count=skipped,
            )
        if not events:
            return 0
        try:
            connection = self._connect()
        except psycopg.OperationalError as error:
            raise _connect_error(error) from error
        except psycopg.Error:
            # psycopg could not build the connection string from the details.
            raise DeliveryError(STORED_CONNECTION_FAILURE, INCOMPLETE_DETAIL) from None
        with connection:
            try:
                with connection.transaction():
                    connection.execute(
                        f"SET LOCAL statement_timeout = {STATEMENT_TIMEOUT_MS}"
                    )
                    with connection.cursor().copy(self._copy_statement()) as copy:
                        for event in events:
                            copy.write_row(_copy_row(event))
            except psycopg.Error as error:
                delivery_error = _delivery_error(error)
                if delivery_error.kind != SCHEMA_MISMATCH:
                    raise delivery_error from error
                # Postgres quotes the failing row, end users' data included, so
                # only where it failed is logged.
                logger.warning(
                    "rows.rejected",
                    connection__id=self.connection_id,
                    error__sqlstate=error.sqlstate,
                    error__constraint=error.diag.constraint_name,
                    error__column=error.diag.column_name,
                )
                raise delivery_error from None
        return len(events)

    def _connect(self) -> psycopg.Connection:
        # libpq connects to hostaddr without resolving the host again, and
        # still uses the host for SNI. One host per address so each is tried.
        addresses = checked_addresses(self.host, self.port)
        pinned: dict[str, Any] = (
            {
                "hostaddr": ",".join(addresses),
                "host": ",".join([self.host] * len(addresses)),
            }
            if addresses
            else {"host": self.host}
        )
        return psycopg.connect(
            **pinned,
            port=self.port,
            dbname=self.database,
            user=self.username,
            password=self.password,
            sslmode=self.sslmode,
            application_name=APPLICATION_NAME,
            connect_timeout=CONNECT_TIMEOUT_SECONDS,
            tcp_user_timeout=TCP_USER_TIMEOUT_MS,
            keepalives_idle=KEEPALIVES_IDLE_SECONDS,
            keepalives_interval=KEEPALIVES_INTERVAL_SECONDS,
            keepalives_count=KEEPALIVES_COUNT,
        )

    def _copy_statement(self) -> sql.Composed:
        return sql.SQL("COPY {table} ({columns}) FROM STDIN").format(
            table=sql.Identifier(self.schema, EVENTS_TABLE_NAME),
            columns=sql.SQL(", ").join(map(sql.Identifier, COLUMNS)),
        )

    def _is_valid(self) -> bool:
        return (
            all(
                isinstance(value, str) and value and "\x00" not in value
                for value in (
                    self.host,
                    self.database,
                    self.schema,
                    self.username,
                    self.password,
                )
            )
            and _is_host(self.host)
            and isinstance(self.port, int)
            and not isinstance(self.port, bool)
            and 0 < self.port < 65536
            and bool(_IDENTIFIER.fullmatch(self.schema))
            and self.sslmode in SSL_MODES
        )


def _is_host(host: str) -> bool:
    if _HOSTNAME.fullmatch(host):
        return True
    try:
        ipaddress.ip_address(host)
    except ValueError:
        return False
    return True


def _copy_row(event: EventRow) -> tuple[str | datetime | None, ...]:
    values: dict[str, str | datetime | None] = {
        column: _text(value) if value is not None else None
        for column, value in {**event.not_null, **event.nullable}.items()
    }
    values["timestamp"] = _datetime(event.timestamp)
    values["collected_at"] = (
        _datetime(event.collected_at) if event.collected_at is not None else None
    )
    return tuple(values[column] for column in COLUMNS)


def _text(value: str) -> str:
    """Postgres text cannot hold NUL, and a lone surrogate, which JSON allows,
    cannot be encoded as UTF-8. Either would fail the whole COPY."""
    return _SURROGATE.sub("�", value.replace("\x00", ""))


def _datetime(micros: int) -> datetime:
    return _EPOCH + timedelta(microseconds=micros)


def _connect_error(error: psycopg.OperationalError) -> DeliveryError:
    message = str(error)
    if _AUTHENTICATION_FAILURE.search(message):
        return DeliveryError("authentication", AUTHENTICATION_DETAIL)
    if _MISSING_DATABASE.search(message):
        return DeliveryError("missing_database", MISSING_DATABASE_DETAIL)
    return DeliveryError("unreachable", UNREACHABLE_DETAIL)


def _delivery_error(error: psycopg.Error) -> DeliveryError:
    """Turns a server error into the label for the logs and the API's sentence
    for the dashboard. The server's own message goes to the logs only."""
    code = error.sqlstate
    if code is None or code.startswith(("08", "57P")):
        return DeliveryError("unreachable", UNREACHABLE_DETAIL)
    if code in (UNDEFINED_TABLE, INVALID_SCHEMA_NAME):
        return DeliveryError("missing_table", MISSING_TABLE_DETAIL)
    if code == INSUFFICIENT_PRIVILEGE:
        return DeliveryError("permission_denied", PERMISSION_DENIED_DETAIL)
    if code == UNDEFINED_COLUMN or code.startswith(("22", "23")):
        return DeliveryError(SCHEMA_MISMATCH, SCHEMA_MISMATCH_DETAIL)
    if code == QUERY_CANCELED:
        return DeliveryError("timeout", TIMEOUT_DETAIL)
    return DeliveryError("rejected", REJECTED_DETAIL)
