import time
from dataclasses import dataclass, field
from typing import Any, Protocol

from cryptography.fernet import Fernet, InvalidToken
from psycopg_pool import ConnectionPool

from warehouse_delivery.crypto import decrypt_json
from warehouse_delivery.errors import DeliveryError

CACHE_TTL_SECONDS = 60.0

CONNECTION_QUERY = """
SELECT connection_id, warehouse_type, config, credentials
FROM experimentation_delivery_connections
WHERE client_api_key = %s
"""

# Label used when the problem is the stored connection itself rather than the
# customer's warehouse: the credentials will not decrypt, or a detail such as
# the port is missing.
STORED_CONNECTION_FAILURE = "stored_connection"
INCOMPLETE_DETAIL = "Stored connection details are incomplete."
UNDECRYPTABLE_DETAIL = "Stored credentials cannot be decrypted."


@dataclass(frozen=True)
class WarehouseConnection:
    id: int
    warehouse_type: str
    config: dict[str, Any]
    credentials: dict[str, Any] = field(repr=False)


class WarehouseConnections(Protocol):
    def get(self, environment_key: str) -> WarehouseConnection | None:
        """The warehouse connection for an environment, or None when the
        environment sends events to Flagsmith's own warehouse only."""


class PostgresWarehouseConnections:
    def __init__(self, pool: ConnectionPool, fernet: Fernet) -> None:
        self._pool = pool
        self._fernet = fernet
        self._cache: dict[str, tuple[float, WarehouseConnection]] = {}

    def get(self, environment_key: str) -> WarehouseConnection | None:
        now = time.monotonic()
        cached = self._cache.get(environment_key)
        if cached is not None and cached[0] > now:
            return cached[1]
        connection = self._read(environment_key)
        if connection is not None:
            self._cache[environment_key] = (now + CACHE_TTL_SECONDS, connection)
        return connection

    def _read(self, environment_key: str) -> WarehouseConnection | None:
        with self._pool.connection() as database:
            row = database.execute(CONNECTION_QUERY, [environment_key]).fetchone()
        if row is None:
            return None
        connection_id, warehouse_type, config, encrypted = row
        try:
            credentials = decrypt_json(self._fernet, encrypted) if encrypted else {}
        except (InvalidToken, ValueError) as exc:
            raise DeliveryError(
                STORED_CONNECTION_FAILURE,
                UNDECRYPTABLE_DETAIL,
                connection_id=connection_id,
            ) from exc
        return WarehouseConnection(
            id=connection_id,
            warehouse_type=warehouse_type,
            config=config or {},
            credentials=credentials if isinstance(credentials, dict) else {},
        )
