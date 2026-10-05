from collections.abc import Iterator
from contextlib import contextmanager

from psycopg import Connection
from psycopg_pool import ConnectionPool

STATEMENT_TIMEOUT = "2s"


@contextmanager
def transaction(pool: ConnectionPool) -> Iterator[Connection]:
    """A transaction whose statements are cancelled after STATEMENT_TIMEOUT, so
    a database that stops answering fails the delivery instead of hanging it.
    `SET LOCAL` rather than a connection option, which PgBouncer rejects."""
    with pool.connection() as connection, connection.transaction():
        connection.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
        yield connection
