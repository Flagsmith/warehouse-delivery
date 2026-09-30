import os
from collections.abc import Iterator
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg_pool import ConnectionPool


@pytest.fixture()
def database_pool() -> Iterator[ConnectionPool]:
    database_url = os.environ["DATABASE_URL"]
    schema = sql.Identifier(f"test_{uuid4().hex}")
    with psycopg.connect(database_url, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(schema))
    pool = ConnectionPool(
        database_url,
        kwargs={"options": f"-c search_path={schema.as_string()}"},
        min_size=1,
        max_size=2,
    )
    with pool.connection() as database:
        database.execute(
            """
            CREATE TABLE experimentation_delivery_connections (
                client_api_key text PRIMARY KEY,
                connection_id integer NOT NULL,
                warehouse_type text NOT NULL,
                config jsonb,
                credentials text
            )
            """
        )
        database.execute(
            """
            CREATE TABLE experimentation_warehousedeliverystatus (
                connection_id integer PRIMARY KEY,
                status text NOT NULL,
                detail text,
                recorded_at timestamptz NOT NULL
            )
            """
        )
    yield pool
    pool.close()
    with psycopg.connect(database_url, autocommit=True) as admin:
        admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(schema))
