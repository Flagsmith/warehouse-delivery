from typing import Protocol

from psycopg_pool import ConnectionPool

CONNECTED = "connected"
ERRORED = "errored"

STATUS_UPSERT = """
INSERT INTO experimentation_warehousedeliverystatus
    (connection_id, status, detail, recorded_at)
VALUES (%s, %s, %s, now())
ON CONFLICT (connection_id) DO UPDATE SET
    status = EXCLUDED.status,
    detail = EXCLUDED.detail,
    recorded_at = EXCLUDED.recorded_at
"""


class ConnectionStatusWriter(Protocol):
    def write(self, connection_id: int, status: str, detail: str | None) -> None:
        """Records whether the last insert for this connection worked and, if
        not, the sentence the customer should see."""


class PostgresConnectionStatusWriter:
    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    def write(self, connection_id: int, status: str, detail: str | None) -> None:
        with self._pool.connection() as database:
            database.execute(STATUS_UPSERT, [connection_id, status, detail])
