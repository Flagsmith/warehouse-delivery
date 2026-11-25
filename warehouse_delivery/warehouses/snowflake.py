import base64
import hashlib
import json
import re
import threading
import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from urllib.parse import urlencode
from uuid import uuid4

import structlog
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from urllib3 import BaseHTTPResponse, PoolManager, Timeout
from urllib3.exceptions import HTTPError, ReadTimeoutError

from warehouse_delivery.connections import (
    INCOMPLETE_DETAIL,
    STORED_CONNECTION_FAILURE,
    WarehouseConnection,
)
from warehouse_delivery.errors import DeliveryError
from warehouse_delivery.warehouses.rows import parse_event

logger = structlog.get_logger("warehouse")

EVENTS_TABLE_NAME = "EVENTS"
APPLICATION_NAME = "flagsmith-warehouse-delivery"
HOST_SUFFIX = "snowflakecomputing.com"
JWT_BEARER_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer"

_ACCOUNT_IDENTIFIER = re.compile(r"[A-Za-z0-9]+-[A-Za-z0-9_-]+")
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")
_LABEL = r"[a-z0-9]([a-z0-9-]*[a-z0-9])?"
_HOSTNAME = re.compile(rf"{_LABEL}(\.{_LABEL})+")
_SCOPED_TOKEN = re.compile(r"[A-Za-z0-9._-]+")
_SNOWFLAKE_CODE = re.compile(r"[A-Za-z0-9_]+")
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# Their sum must stay under clickhouse.INSERT_TIMEOUT_SECONDS.
HOSTNAME_TIMEOUT_SECONDS = 10
TOKEN_TIMEOUT_SECONDS = 10
APPEND_TIMEOUT_SECONDS = 30

# Snowpipe Streaming rejects a request over 4 MB.
MAX_PAYLOAD_BYTES = 3 * 1024 * 1024

JWT_LIFETIME_SECONDS = 300
SCOPED_TOKEN_LIFETIME_SECONDS = 3600
TOKEN_REFRESH_MARGIN_SECONDS = 300

UNREACHABLE_DETAIL = "Could not connect to the host."
AUTHENTICATION_DETAIL = "Authentication failed."
MISSING_DATABASE_DETAIL = "Database does not exist."
MISSING_TABLE_DETAIL = (
    "Events table not found in the configured database. Run the setup SQL to create it."
)
PERMISSION_DENIED_DETAIL = "Permission denied on the events table."
TIMEOUT_DETAIL = "The query timed out."
REJECTED_DETAIL = "The Snowflake account rejected the request."

MISSING_DATABASE_CODE = "ERR_DATABASE_DOES_NOT_EXIST_OR_NOT_AUTHORIZED"
MISSING_TABLE_CODES = frozenset(
    {
        "ERR_SCHEMA_DOES_NOT_EXIST_OR_NOT_AUTHORIZED",
        "ERR_TABLE_DOES_NOT_EXIST_NOT_AUTHORIZED",
    }
)


@dataclass(frozen=True)
class SnowflakeWarehouse:
    """A customer's Snowflake events table, appended to through the Snowpipe
    Streaming REST API with key-pair authentication."""

    connection_id: int
    account_identifier: str
    database: str
    schema: str
    user: str
    private_key: str = field(repr=False)

    @classmethod
    def from_connection(cls, connection: WarehouseConnection) -> SnowflakeWarehouse:
        """Re-validates the stored details before any of them reaches a URL."""
        try:
            warehouse = cls(
                connection_id=connection.id,
                account_identifier=connection.config["account_identifier"],
                database=connection.config["database"],
                schema=connection.config["schema"],
                user=connection.config["user"],
                private_key=connection.credentials["private_key"],
            )
        except (KeyError, TypeError) as exc:
            raise DeliveryError(STORED_CONNECTION_FAILURE, INCOMPLETE_DETAIL) from exc
        if not warehouse._is_valid():
            raise DeliveryError(STORED_CONNECTION_FAILURE, INCOMPLETE_DETAIL)
        _load_private_key(warehouse.private_key)
        return warehouse

    @property
    def account_host(self) -> str:
        account = self.account_identifier.lower().replace("_", "-")
        return f"{account}.{HOST_SUFFIX}"

    @property
    def qualified_user(self) -> str:
        return f"{self.account_identifier.upper()}.{self.user.upper()}"

    @property
    def rows_path(self) -> str:
        return (
            f"/v2/streaming/data/databases/{self.database.upper()}"
            f"/schemas/{self.schema.upper()}/tables/{EVENTS_TABLE_NAME}/rows"
        )

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
        ingest_host = _get_ingest_host(self)
        token = _get_token(self, ingest_host)
        deadline = time.monotonic() + APPEND_TIMEOUT_SECONDS
        for chunk in _chunks(records):
            self._append(ingest_host, token, chunk, deadline)
        return len(records)

    def _append(
        self, ingest_host: str, token: str, chunk: list[str], deadline: float
    ) -> None:
        remaining_seconds = deadline - time.monotonic()
        if remaining_seconds <= 0:
            raise DeliveryError("timeout", TIMEOUT_DETAIL)
        query = urlencode({"requestId": str(uuid4()), "retryCount": 0})
        try:
            response = _request(
                "POST",
                f"https://{ingest_host}{self.rows_path}?{query}",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/x-ndjson",
                    "Accept": "application/json",
                    "User-Agent": APPLICATION_NAME,
                },
                body="".join(chunk).encode(),
                timeout=remaining_seconds,
            )
            if response.status == 200:
                return
            if response.status == 401:
                with _tokens_lock:
                    _tokens.pop(_token_key(self, ingest_host), None)
            raise _refused(self, "append.refused", response)
        except DeliveryError as error:
            if error.kind == "unreachable":
                with _ingest_hosts_lock:
                    _ingest_hosts.pop(self.account_host, None)
            raise

    def _is_valid(self) -> bool:
        return (
            all(
                isinstance(value, str)
                for value in (
                    self.account_identifier,
                    self.database,
                    self.schema,
                    self.user,
                    self.private_key,
                )
            )
            and bool(_ACCOUNT_IDENTIFIER.fullmatch(self.account_identifier))
            and bool(_IDENTIFIER.fullmatch(self.database))
            and bool(_IDENTIFIER.fullmatch(self.schema))
            and bool(_IDENTIFIER.fullmatch(self.user))
        )


def map_row(payload: bytes) -> str | None:
    """Returns the event as one NDJSON line, or None for an event that cannot
    be made valid, which Snowflake would not accept."""
    row = parse_event(payload)
    if row is None:
        return None
    record = {**row.not_null, "timestamp": _epoch_micros_to_iso8601(row.timestamp)}
    if row.collected_at is not None:
        record["collected_at"] = _epoch_micros_to_iso8601(row.collected_at)
    for column, value in row.nullable.items():
        if value is not None:
            record[column] = value
    line = json.dumps(record, separators=(",", ":")) + "\n"
    # json.dumps escapes non-ASCII, so the length is the size in bytes.
    return line if len(line) <= MAX_PAYLOAD_BYTES else None


def _epoch_micros_to_iso8601(micros: int) -> str:
    moment = _EPOCH + timedelta(microseconds=micros)
    return moment.isoformat(timespec="microseconds")


def _chunks(records: list[str]) -> Iterator[list[str]]:
    chunk: list[str] = []
    size = 0
    for record in records:
        if chunk and size + len(record) > MAX_PAYLOAD_BYTES:
            yield chunk
            chunk, size = [], 0
        chunk.append(record)
        size += len(record)
    if chunk:
        yield chunk


@lru_cache(maxsize=64)
def _load_private_key(pem: str) -> RSAPrivateKey:
    # Checking an RSA key takes tens of milliseconds, so each is loaded once.
    try:
        private_key = serialization.load_pem_private_key(pem.encode(), password=None)
    except (ValueError, TypeError):
        raise DeliveryError(STORED_CONNECTION_FAILURE, INCOMPLETE_DETAIL) from None
    if not isinstance(private_key, RSAPrivateKey):
        raise DeliveryError(STORED_CONNECTION_FAILURE, INCOMPLETE_DETAIL)
    return private_key


def _keypair_jwt(warehouse: SnowflakeWarehouse) -> str:
    private_key = _load_private_key(warehouse.private_key)
    public_key = private_key.public_key().public_bytes(
        serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    fingerprint = base64.b64encode(hashlib.sha256(public_key).digest()).decode()
    now = int(time.time())
    header: dict[str, str | int] = {"alg": "RS256", "typ": "JWT"}
    claims: dict[str, str | int] = {
        "iss": f"{warehouse.qualified_user}.SHA256:{fingerprint}",
        "sub": warehouse.qualified_user,
        "iat": now,
        "exp": now + JWT_LIFETIME_SECONDS,
    }
    signing_input = f"{_base64url_json(header)}.{_base64url_json(claims)}"
    signature = private_key.sign(
        signing_input.encode(), padding.PKCS1v15(), hashes.SHA256()
    )
    return f"{signing_input}.{_base64url(signature)}"


def _base64url_json(value: dict[str, str | int]) -> str:
    return _base64url(json.dumps(value, separators=(",", ":")).encode())


def _base64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _keypair_headers(warehouse: SnowflakeWarehouse) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {_keypair_jwt(warehouse)}",
        "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
        "Accept": "application/json",
        "User-Agent": APPLICATION_NAME,
    }


_ingest_hosts: dict[str, str] = {}
_ingest_hosts_lock = threading.Lock()


def _get_ingest_host(warehouse: SnowflakeWarehouse) -> str:
    with _ingest_hosts_lock:
        cached = _ingest_hosts.get(warehouse.account_host)
    if cached is not None:
        return cached
    response = _request(
        "GET",
        f"https://{warehouse.account_host}/v2/streaming/hostname",
        headers=_keypair_headers(warehouse),
        timeout=HOSTNAME_TIMEOUT_SECONDS,
    )
    if response.status != 200:
        raise _refused(warehouse, "hostname.refused", response)
    ingest_host = (
        response.data.decode(errors="replace").strip().lower().replace("_", "-")
    )
    if not (
        _HOSTNAME.fullmatch(ingest_host) and ingest_host.endswith(f".{HOST_SUFFIX}")
    ):
        raise DeliveryError("unreachable", UNREACHABLE_DETAIL)
    with _ingest_hosts_lock:
        _ingest_hosts[warehouse.account_host] = ingest_host
    return ingest_host


_TokenKey = tuple[str, str, bytes, str]
_tokens: dict[_TokenKey, tuple[str, float]] = {}
_tokens_lock = threading.Lock()


def _token_key(warehouse: SnowflakeWarehouse, ingest_host: str) -> _TokenKey:
    private_key_digest = hashlib.sha256(warehouse.private_key.encode()).digest()
    return (
        warehouse.account_host,
        warehouse.qualified_user,
        private_key_digest,
        ingest_host,
    )


def _get_token(warehouse: SnowflakeWarehouse, ingest_host: str) -> str:
    key = _token_key(warehouse, ingest_host)
    now = time.monotonic()
    with _tokens_lock:
        cached = _tokens.get(key)
    if cached is not None and cached[1] > now:
        return cached[0]
    token = _exchange_token(warehouse, ingest_host)
    with _tokens_lock:
        for stale in [k for k, (_, expiry) in _tokens.items() if expiry <= now]:
            del _tokens[stale]
        _tokens[key] = (
            token,
            now + SCOPED_TOKEN_LIFETIME_SECONDS - TOKEN_REFRESH_MARGIN_SECONDS,
        )
    return token


def _exchange_token(warehouse: SnowflakeWarehouse, ingest_host: str) -> str:
    response = _request(
        "POST",
        f"https://{warehouse.account_host}/oauth/token",
        headers={
            **_keypair_headers(warehouse),
            "Content-Type": "application/x-www-form-urlencoded",
        },
        body=urlencode({"grant_type": JWT_BEARER_GRANT_TYPE, "scope": ingest_host}),
        timeout=TOKEN_TIMEOUT_SECONDS,
    )
    if response.status != 200:
        raise _refused(warehouse, "token.refused", response)
    token = response.data.decode(errors="replace").strip()
    if not _SCOPED_TOKEN.fullmatch(token):
        raise _rejected()
    return token


def _request(
    method: str,
    url: str,
    *,
    headers: dict[str, str],
    timeout: float,
    body: str | bytes | None = None,
) -> BaseHTTPResponse:
    try:
        return _get_pool_manager().request(
            method,
            url,
            body=body,
            headers=headers,
            timeout=Timeout(total=timeout),
            retries=False,
            redirect=False,
        )
    except ReadTimeoutError as exc:
        raise DeliveryError("timeout", TIMEOUT_DETAIL) from exc
    except HTTPError as exc:
        raise DeliveryError("unreachable", UNREACHABLE_DETAIL) from exc


def _refused(
    warehouse: SnowflakeWarehouse, log_event: str, response: BaseHTTPResponse
) -> DeliveryError:
    snowflake_code = _snowflake_code(response.data)
    logger.warning(
        log_event,
        connection__id=warehouse.connection_id,
        http__status=response.status,
        snowflake__code=snowflake_code,
    )
    if response.status == 401:
        return DeliveryError("authentication", AUTHENTICATION_DETAIL)
    if response.status == 403:
        return DeliveryError("permission_denied", PERMISSION_DENIED_DETAIL)
    if snowflake_code == MISSING_DATABASE_CODE:
        return DeliveryError("missing_database", MISSING_DATABASE_DETAIL)
    if snowflake_code in MISSING_TABLE_CODES:
        return DeliveryError("missing_table", MISSING_TABLE_DETAIL)
    if response.status == 404:
        return DeliveryError("unreachable", UNREACHABLE_DETAIL)
    return _rejected()


def _snowflake_code(body: bytes) -> str | None:
    try:
        code = json.loads(body).get("code")
    except (ValueError, AttributeError):
        return None
    return code if isinstance(code, str) and _SNOWFLAKE_CODE.fullmatch(code) else None


def _rejected() -> DeliveryError:
    return DeliveryError("rejected", REJECTED_DETAIL)


@lru_cache(maxsize=1)
def _get_pool_manager() -> PoolManager:
    return PoolManager()
