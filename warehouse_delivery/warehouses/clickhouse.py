import gzip
import typing
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import lru_cache

import clickhouse_connect
from clickhouse_connect import common as clickhouse_common
from clickhouse_connect.driver import httputil
from clickhouse_connect.driver.exceptions import (
    ClickHouseError,
    DatabaseError,
    OperationalError,
)
from clickhouse_connect.driver.summary import QuerySummary
from urllib3 import PoolManager

from warehouse_delivery.connections import (
    INCOMPLETE_DETAIL,
    STORED_CONNECTION_FAILURE,
    WarehouseConnection,
)
from warehouse_delivery.errors import DeliveryError

if typing.TYPE_CHECKING:
    from typing import Any

    from clickhouse_connect.driver.client import Client

# Customers run their own ClickHouse, and an old one does not have every
# setting we send. Left alone, clickhouse-connect passes an unknown setting on
# and the server refuses the whole insert; this makes it leave the setting out
# instead, so one set of settings works against every server version.
clickhouse_common.set_setting("invalid_setting_action", "drop")

EVENTS_TABLE_NAME = "events"
CONNECT_TIMEOUT_SECONDS = 10
# Far more than a healthy ClickHouse needs for a few thousand rows. Kept short
# because one Kafka batch can hold events for many customers, inserted a few
# at a time, and the whole batch must finish inside the consumer's poll
# interval (see kafka.MAX_POLL_INTERVAL_MS).
INSERT_TIMEOUT_SECONDS = 60

# Each event carries its timestamps as milliseconds since 1970, while the
# customer's events table stores them as DateTime64(3). Inserting the JSON
# straight into the table does not fail: ClickHouse silently turns every
# timestamp into 9999-12-31. Reading the JSON through input() with the
# timestamps declared as integers, and converting them in the SELECT, makes
# ClickHouse convert them properly, and the event bytes can be sent exactly
# as the ingestion server produced them.
INSERT_QUERY = (
    f"INSERT INTO {EVENTS_TABLE_NAME} "
    "(environment_key, event, feature_name, timestamp, collected_at, "
    "identifier, value, traits, metadata, sdk_language, sdk_version) "
    "SELECT environment_key, event, feature_name, "
    "fromUnixTimestamp64Milli(timestamp), fromUnixTimestamp64Milli(collected_at), "
    "identifier, value, traits, metadata, sdk_language, sdk_version "
    "FROM input('environment_key String, event String, feature_name String, "
    "timestamp Int64, collected_at Int64, identifier String, value String, "
    "traits String, metadata String, sdk_language String, sdk_version String') "
    "FORMAT JSONEachRow"
)

# An event's value is whatever the SDK sent: a number for a revenue
# conversion, true or false, a list. The customer's events table keeps value as
# text, and ClickHouse only accepts anything other than text there when these
# are on. ClickHouse 23 and later have them on already; before that they are
# off and the row is refused, so every insert asks for them.
INSERT_SETTINGS = {
    "input_format_json_read_numbers_as_strings": 1,
    "input_format_json_read_bools_as_strings": 1,
    "input_format_json_read_objects_as_strings": 1,
    "input_format_json_read_arrays_as_strings": 1,
}

# ClickHouse error codes that mean it could not fit our rows into the table: a
# value it could not parse, a value too large for its column, a constraint the
# row violated. Almost always the customer's events table does not match the
# schema in our setup SQL; an event missing a field the table requires would
# land here too.
SCHEMA_ERROR_CODES = frozenset(
    {
        6,  # CANNOT_PARSE_TEXT
        26,  # CANNOT_PARSE_QUOTED_STRING
        27,  # CANNOT_PARSE_INPUT_ASSERTION_FAILED
        38,  # CANNOT_PARSE_DATE
        41,  # CANNOT_PARSE_DATETIME
        53,  # TYPE_MISMATCH
        69,  # ARGUMENT_OUT_OF_BOUND
        72,  # CANNOT_PARSE_NUMBER
        117,  # INCORRECT_DATA
        469,  # VIOLATED_CONSTRAINT
    }
)
AUTHENTICATION_FAILED = 516
UNKNOWN_DATABASE = 81
UNKNOWN_TABLE = 60


@dataclass(frozen=True)
class ClickHouseWarehouse:
    """A customer's ClickHouse and how to insert into it. Uses the same HTTP
    interface, clickhouse-connect client and no-redirect connection pool the
    API uses when it verifies the connection, so what worked at save time
    works here."""

    host: str
    port: int
    database: str
    username: str
    password: str = field(repr=False)
    secure: bool = True

    @classmethod
    def from_connection(cls, connection: WarehouseConnection) -> ClickHouseWarehouse:
        """Builds the warehouse from the connection details the API stored. If
        a detail such as the port or password is missing, raises
        ``DeliveryError`` so the customer's connection shows what is wrong in
        the dashboard."""
        try:
            return cls(
                host=connection.config["host"],
                port=connection.config["port"],
                database=connection.config["database"],
                username=connection.config["username"],
                password=connection.credentials["password"],
                secure=connection.config["secure"],
            )
        except (KeyError, TypeError) as exc:
            raise DeliveryError(STORED_CONNECTION_FAILURE, INCOMPLETE_DETAIL) from exc

    def insert(self, rows: Sequence[bytes]) -> int:
        body = gzip.compress(b"\n".join(rows) + b"\n")
        try:
            with self._connect() as client:
                summary = client.command(
                    INSERT_QUERY,
                    data=body,
                    settings=INSERT_SETTINGS,
                    transport_settings={"Content-Encoding": "gzip"},
                )
        except ClickHouseError as error:
            raise _delivery_error(error) from error
        # ClickHouse answers an insert with an empty body and a header saying
        # how many rows it wrote, which clickhouse-connect returns as a
        # QuerySummary. If anything else comes back, count the rows we sent.
        if isinstance(summary, QuerySummary):
            return summary.written_rows
        return len(rows)

    @contextmanager
    def _connect(self) -> Iterator[Client]:
        client = clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
            secure=self.secure,
            connect_timeout=CONNECT_TIMEOUT_SECONDS,
            send_receive_timeout=INSERT_TIMEOUT_SECONDS,
            pool_mgr=_get_pool_manager(),
        )
        try:
            yield client
        finally:
            client.close()


def _delivery_error(error: ClickHouseError) -> DeliveryError:
    """Turns a driver exception into the label for the logs and the sentence
    for the dashboard.

    The sentences are the ones the API's describe_warehouse_error uses, so a
    customer reads the same words whether the problem was found when they
    saved the connection or when events were delivered. The driver's own
    message goes to the logs only, as it can contain hostnames and query
    text."""
    # OperationalError is a subclass of DatabaseError, so it has to be checked
    # first or the DatabaseError branch below would claim it.
    if isinstance(error, OperationalError):
        return DeliveryError("unreachable", "Could not connect to the host.")
    if isinstance(error, DatabaseError):
        if error.code == AUTHENTICATION_FAILED:
            return DeliveryError("authentication", "Authentication failed.")
        if error.code == UNKNOWN_DATABASE:
            return DeliveryError("missing_database", "Database does not exist.")
        if error.code == UNKNOWN_TABLE:
            return DeliveryError(
                "missing_table",
                "Events table not found in the configured database. "
                "Run the setup SQL to create it.",
            )
        if error.code in SCHEMA_ERROR_CODES:
            return DeliveryError(
                "schema_mismatch",
                "The warehouse rejected the events. Check that the events table "
                "matches the expected schema.",
            )
        return DeliveryError("rejected", "The ClickHouse server rejected the request.")
    # Anything else from the driver, such as an InterfaceError: it gave up
    # before ClickHouse answered, so there is no error code to go on.
    return DeliveryError("failed", "Connection failed.")


class _NoRedirectPoolManager(PoolManager):
    """Refuses HTTP redirects.

    When a connection is saved, the API checks that its host is not one of our
    own internal addresses. If we followed redirects, a host that passed that
    check could redirect our insert, events included, to an internal address
    the API never checked."""

    def urlopen(  # type: ignore[override]
        self,
        method: str,
        url: str,
        redirect: bool = True,
        **kwargs: Any,
    ) -> Any:
        kwargs["redirect"] = False
        return super().urlopen(method, url, **kwargs)


@lru_cache(maxsize=1)
def _get_pool_manager() -> PoolManager:
    # One pool for the whole process. It keeps connections open per host, so
    # consecutive inserts into the same warehouse reuse a connection instead of
    # reconnecting each time.
    return _NoRedirectPoolManager(**httputil.get_pool_manager_options())
