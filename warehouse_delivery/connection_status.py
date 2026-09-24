import json
import time
from collections.abc import Callable
from typing import Any, Protocol

# Every connection's latest outcome sits in this one Redis hash, under its
# connection id. The API reads it when it shows connections, and removes an
# entry when the connection's details change or the connection goes. One key
# means the API reads everything in a single call instead of searching the
# whole Redis cluster for keys.
STATUS_HASH_KEY = "experimentation:warehouse_delivery_status"
CONNECTED = "connected"
ERRORED = "errored"


class ConnectionStatusWriter(Protocol):
    def write(self, connection_id: int, status: str, detail: str | None) -> None:
        """Records whether the last insert for this connection worked and, if
        not, the sentence the customer should see."""


class RedisConnectionStatusWriter:
    """Writes the outcome to Redis for the API to show on the connection. The
    connection lives in the API's Postgres, and only the API writes there."""

    def __init__(self, client: Any, *, clock: Callable[[], float] = time.time) -> None:
        self._client = client
        self._clock = clock

    def write(self, connection_id: int, status: str, detail: str | None) -> None:
        entry = {"status": status, "detail": detail, "at": self._clock()}
        self._client.hset(STATUS_HASH_KEY, str(connection_id), json.dumps(entry))
