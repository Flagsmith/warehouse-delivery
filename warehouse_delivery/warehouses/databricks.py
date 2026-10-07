import json
import math
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

import structlog
from zerobus.sdk.shared import (
    RecordType,
    StreamConfigurationOptions,
    TableProperties,
    ZerobusException,
)
from zerobus.sdk.sync import ZerobusSdk

from warehouse_delivery.connections import (
    INCOMPLETE_DETAIL,
    STORED_CONNECTION_FAILURE,
    WarehouseConnection,
)
from warehouse_delivery.errors import DeliveryError

logger = structlog.get_logger("warehouse")

EVENTS_TABLE_NAME = "events"
APPLICATION_NAME = "flagsmith-warehouse-delivery"
HOST_SUFFIXES = ("cloud.databricks.com", "azuredatabricks.net", "gcp.databricks.com")

_LABEL = r"[a-z0-9]([a-z0-9-]*[a-z0-9])?"
_HOSTNAME = re.compile(rf"{_LABEL}(\.{_LABEL})+")
_WORKSPACE_ID = re.compile(r"[0-9]+")
_REGION = re.compile(r"[a-z0-9-]+")
_IDENTIFIER = re.compile(r"[A-Za-z0-9_]+")

# Their sum must stay under clickhouse.INSERT_TIMEOUT_SECONDS.
CREATE_TIMEOUT_MS = 15_000
FLUSH_TIMEOUT_MS = 25_000
PAUSED_CLOSE_TIMEOUT_MS = 5_000
SDK_SHUTDOWN_MS = 1_100

# Range of a Delta TIMESTAMP, 0001-01-01 to 9999-12-31, in microseconds.
MIN_TIMESTAMP_MICROS = -62_135_596_800_000_000
MAX_TIMESTAMP_MICROS = 253_402_300_799_999_999

NOT_NULL_COLUMNS = ("environment_key", "event", "identifier")
NULLABLE_COLUMNS = (
    "feature_name",
    "value",
    "traits",
    "metadata",
    "sdk_language",
    "sdk_version",
)

SCHEMA_MISMATCH_DETAIL = (
    "The warehouse rejected the events. Check that the events table "
    "matches the expected schema."
)
ALL_APIS_DETAIL = (
    "The service principal secret must allow all APIs. "
    "Generate a new secret with the All APIs scope."
)

# SDK exceptions carry only a message, so they are classified by these
# markers, first match wins.
_ERROR_MARKERS: tuple[tuple[tuple[str, ...], str, str], ...] = (
    (("are not assigned to the client",), "authentication", ALL_APIS_DETAIL),
    (
        ("invalid_authorization_details",),
        "permission_denied",
        "The events table is missing or the service principal lacks access to "
        "it. Run the setup SQL.",
    ),
    (
        ("The caller does not have permission",),
        "permission_denied",
        "Permission denied on the events table.",
    ),
    (
        ("Some requested entity was not found",),
        "missing_table",
        "Events table not found in the configured database. "
        "Run the setup SQL to create it.",
    ),
    (
        (
            "Token fetch failed: Network error",
            "Failed to open a channel",
            "Failed to establish TLS connection",
            "Connection timeout",
            "Deadline expired before operation could complete",
            "The service is currently unavailable",
        ),
        "unreachable",
        "Could not connect to the host.",
    ),
    (
        (
            "Specified UC token is in invalid format",
            "The request does not have valid authentication credentials",
        ),
        "authentication",
        "Authentication failed.",
    ),
    (
        (
            "Client specified an invalid argument",
            "Invalid argument:",
            "schema does not match the target table",
        ),
        "schema_mismatch",
        SCHEMA_MISMATCH_DETAIL,
    ),
    (
        (
            "The specified Zerobus endpoint is in invalid format",
            "Specified UC endpoint is in invalid format",
            "Specified UC table name is invalid",
        ),
        STORED_CONNECTION_FAILURE,
        INCOMPLETE_DETAIL,
    ),
)


@dataclass(frozen=True)
class DatabricksWarehouse:
    """A customer's Databricks events table, written to through Zerobus Ingest."""

    connection_id: int
    host: str
    workspace_id: str
    region: str
    catalog: str
    schema: str
    client_id: str
    client_secret: str = field(repr=False)

    @classmethod
    def from_connection(cls, connection: WarehouseConnection) -> DatabricksWarehouse:
        """Re-validates the stored details before any of them reaches a URL."""
        try:
            warehouse = cls(
                connection_id=connection.id,
                host=connection.config["host"].lower(),
                workspace_id=str(connection.config["workspace_id"]),
                region=connection.config["region"],
                catalog=connection.config["catalog"],
                schema=connection.config["schema"],
                client_id=connection.credentials["client_id"],
                client_secret=connection.credentials["client_secret"],
            )
        except (KeyError, TypeError, AttributeError) as exc:
            raise DeliveryError(STORED_CONNECTION_FAILURE, INCOMPLETE_DETAIL) from exc
        if not warehouse._is_valid():
            raise DeliveryError(STORED_CONNECTION_FAILURE, INCOMPLETE_DETAIL)
        return warehouse

    @property
    def server_endpoint(self) -> str:
        suffix = next(s for s in HOST_SUFFIXES if self.host.endswith(f".{s}"))
        return f"https://{self.workspace_id}.zerobus.{self.region}.{suffix}"

    @property
    def unity_catalog_url(self) -> str:
        return f"https://{self.host}"

    @property
    def table_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{EVENTS_TABLE_NAME}"

    def insert(self, rows: Sequence[bytes]) -> int:
        records = [record for row in rows if (record := map_row(row)) is not None]
        if skipped := len(rows) - len(records):
            logger.warning(
                "rows.skipped",
                connection__id=self.connection_id,
                rows__count=skipped,
            )
        if not records:
            return 0
        sdk = _get_sdk(self.server_endpoint, self.unity_catalog_url)
        try:
            stream = sdk.create_stream(
                client_id=self.client_id,
                client_secret=self.client_secret,
                table_properties=TableProperties(self.table_name),
                options=_stream_options(),
            )
            try:
                stream.ingest_records_nowait(records)
            finally:
                # close() flushes, so it is the one wait for acknowledgements.
                stream.close()
        except ZerobusException as error:
            cause = None if self.client_secret in str(error) else error
            raise _delivery_error(error) from cause
        return len(records)

    def _is_valid(self) -> bool:
        return (
            all(
                isinstance(value, str)
                for value in (
                    self.host,
                    self.region,
                    self.catalog,
                    self.schema,
                    self.client_id,
                    self.client_secret,
                )
            )
            and bool(_HOSTNAME.fullmatch(self.host))
            and self.host.endswith(tuple(f".{s}" for s in HOST_SUFFIXES))
            and bool(_WORKSPACE_ID.fullmatch(self.workspace_id))
            and bool(_REGION.fullmatch(self.region))
            and bool(_IDENTIFIER.fullmatch(self.catalog))
            and bool(_IDENTIFIER.fullmatch(self.schema))
            and bool(self.client_id)
            and bool(self.client_secret)
        )


def map_row(payload: bytes) -> str | None:
    """Returns None for an event that cannot be made valid, since one invalid
    record fails the whole stream."""
    try:
        event = json.loads(payload)
    except ValueError:
        return None
    if not isinstance(event, dict):
        return None
    timestamp = _micros(event.get("timestamp"))
    collected_at = _micros(event.get("collected_at"))
    if timestamp is None:
        timestamp = collected_at
    if timestamp is None:
        return None
    record: dict[str, str | int] = {
        column: _text(event.get(column)) or "" for column in NOT_NULL_COLUMNS
    }
    record["timestamp"] = timestamp
    if collected_at is not None:
        record["collected_at"] = collected_at
    for column in NULLABLE_COLUMNS:
        if (text := _text(event.get(column))) is not None:
            record[column] = text
    return json.dumps(record, separators=(",", ":"))


def _micros(milliseconds: Any) -> int | None:
    if isinstance(milliseconds, bool) or not isinstance(milliseconds, int | float):
        return None
    if not math.isfinite(milliseconds):
        return None
    micros = int(milliseconds * 1000)
    if not MIN_TIMESTAMP_MICROS <= micros <= MAX_TIMESTAMP_MICROS:
        return None
    return micros


def _text(value: Any) -> str | None:
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, separators=(",", ":"))


def _stream_options() -> StreamConfigurationOptions:
    return StreamConfigurationOptions(
        record_type=RecordType.JSON,
        recovery=False,
        recovery_timeout_ms=CREATE_TIMEOUT_MS,
        server_lack_of_ack_timeout_ms=FLUSH_TIMEOUT_MS,
        flush_timeout_ms=FLUSH_TIMEOUT_MS,
        stream_paused_max_wait_time_ms=PAUSED_CLOSE_TIMEOUT_MS,
        callback_max_wait_time_ms=0,
    )


def _delivery_error(error: ZerobusException) -> DeliveryError:
    message = str(error)
    for markers, kind, detail in _ERROR_MARKERS:
        if any(marker in message for marker in markers):
            return DeliveryError(kind, detail)
    return DeliveryError("rejected", "The Databricks workspace rejected the request.")


@lru_cache(maxsize=64)
def _get_sdk(server_endpoint: str, unity_catalog_url: str) -> ZerobusSdk:
    # Each SDK owns an async runtime; its token cache is keyed by credentials.
    return ZerobusSdk(
        server_endpoint, unity_catalog_url, application_name=APPLICATION_NAME
    )
