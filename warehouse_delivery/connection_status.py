from typing import Protocol

import structlog
from psycopg.errors import ForeignKeyViolation
from psycopg_pool import ConnectionPool

from warehouse_delivery.database import transaction

logger = structlog.get_logger("warehouse")

CONNECTED = "connected"
ERRORED = "errored"

STATUS_UPSERT = """
INSERT INTO experimentation_warehousedeliverystatus
    (connection_id, status, detail, updated_at)
VALUES (%s, %s, %s, now())
ON CONFLICT (connection_id) DO UPDATE SET
    status = EXCLUDED.status,
    detail = EXCLUDED.detail,
    updated_at = EXCLUDED.updated_at
"""


class ConnectionStatusWriter(Protocol):
    def write(self, connection_id: int, status: str, detail: str | None) -> None:
        """Records whether the last insert for this connection worked and, if
        not, the sentence the customer should see."""


class PostgresConnectionStatusWriter:
    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    def write(self, connection_id: int, status: str, detail: str | None) -> None:
        try:
            with transaction(self._pool) as database:
                database.execute(STATUS_UPSERT, [connection_id, status, detail])
        except ForeignKeyViolation:
            logger.warning(
                "delivery_status.connection_missing", connection__id=connection_id
            )
