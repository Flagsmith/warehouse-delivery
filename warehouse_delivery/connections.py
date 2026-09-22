import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

from cryptography.fernet import Fernet, InvalidToken

from warehouse_delivery.crypto import decrypt_json
from warehouse_delivery.errors import DeliveryError

# The API's sync_environment_ingestion task writes one entry under this prefix
# for every environment that has an external warehouse connection. The rest of
# the key is the environment's client API key, the same value the ingestion
# server puts on each Kafka message, so a message leads straight to its
# connection.
CONNECTION_KEY_PREFIX = "experimentation:environment_warehouses:"
CACHE_TTL_SECONDS = 60.0

# Label used when the problem is the stored connection itself rather than the
# customer's warehouse: the Redis entry is malformed, the credentials will not
# decrypt, or a detail such as the port is missing.
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


class RedisWarehouseConnections:
    """Looks up an environment's warehouse connection in Redis and remembers it
    for a minute, so a batch does not ask Redis once per environment and a
    connection the customer deletes stops being looked up within a minute.

    Finding no connection is not remembered. The events are being dropped
    meanwhile, and the API may be about to publish it, so it is worth asking
    Redis again on the next batch rather than dropping another minute of
    events."""

    def __init__(
        self,
        client: Any,
        fernet: Fernet,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client = client
        self._fernet = fernet
        self._clock = clock
        self._cache: dict[str, tuple[float, WarehouseConnection | None]] = {}

    def get(self, environment_key: str) -> WarehouseConnection | None:
        now = self._clock()
        cached = self._cache.get(environment_key)
        if cached is not None and cached[0] > now:
            return cached[1]
        connection = self._read(environment_key)
        if connection is not None:
            self._cache[environment_key] = (now + CACHE_TTL_SECONDS, connection)
        return connection

    def _read(self, environment_key: str) -> WarehouseConnection | None:
        raw = self._client.get(f"{CONNECTION_KEY_PREFIX}{environment_key}")
        if raw is None:
            return None
        try:
            document = json.loads(raw)
            connection_id = int(document["connection_id"])
            warehouse_type = str(document["warehouse_type"])
            config = document["config"]
            encrypted = document.get("credentials")
            if not isinstance(config, dict):
                raise TypeError("config is not an object")
            if encrypted is not None and not isinstance(encrypted, str):
                raise TypeError("credentials is not a string")
        except (ValueError, KeyError, TypeError) as exc:
            # The API wrote something we cannot read, so the bug is on our
            # side. Raising DeliveryError instead of crashing keeps the other
            # customers in the batch moving. Without a readable connection id
            # there is nothing to mark errored in the dashboard, so the log
            # is the only trace.
            raise DeliveryError(STORED_CONNECTION_FAILURE, INCOMPLETE_DETAIL) from exc
        try:
            credentials = decrypt_json(self._fernet, encrypted) if encrypted else {}
        except (InvalidToken, ValueError) as exc:
            # InvalidToken: the API encrypted these with a different
            # WAREHOUSE_CREDENTIALS_SECRET than ours, or the value is not a
            # Fernet token. ValueError: it decrypted, but not to JSON. The
            # connection id is known here, so the dashboard can say so.
            raise DeliveryError(
                STORED_CONNECTION_FAILURE,
                UNDECRYPTABLE_DETAIL,
                connection_id=connection_id,
            ) from exc
        return WarehouseConnection(
            id=connection_id,
            warehouse_type=warehouse_type,
            config=config,
            credentials=credentials if isinstance(credentials, dict) else {},
        )
