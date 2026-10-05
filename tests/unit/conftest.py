import os
from collections.abc import Iterator
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg_pool import ConnectionPool


@pytest.fixture()
def database_url() -> str:
    return os.environ["DATABASE_URL"]


@pytest.fixture()
def database_pool(database_url: str) -> Iterator[ConnectionPool]:
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
            "CREATE TABLE experimentation_warehouseconnection (id integer PRIMARY KEY)"
        )
        database.execute(
            """
            CREATE TABLE experimentation_warehousedeliverystatus (
                connection_id integer PRIMARY KEY
                    REFERENCES experimentation_warehouseconnection (id),
                status text NOT NULL,
                detail text,
                updated_at timestamptz NOT NULL
            )
            """
        )
    yield pool
    pool.close()
    with psycopg.connect(database_url, autocommit=True) as admin:
        admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(schema))
