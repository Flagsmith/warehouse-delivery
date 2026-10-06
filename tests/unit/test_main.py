import threading

import psycopg
import pytest
from psycopg_pool import PoolTimeout
from pytest_mock import MockerFixture

from warehouse_delivery.__main__ import build_database_pool, run_loops
from warehouse_delivery.config import Config


def test_build_database_pool__database_unreachable__raises(
    mocker: MockerFixture,
) -> None:
    # Given a database URL nothing listens on
    mocker.patch("warehouse_delivery.__main__.DATABASE_CONNECT_TIMEOUT_SECONDS", 0.5)
    config = Config(
        kafka_bootstrap_servers="localhost:9092",
        kafka_auth="none",
        kafka_username=None,
        kafka_password=None,
        database_url="postgresql://localhost:1/flagsmith",
        warehouse_credentials_secret="secret",
    )

    # When / Then
    with pytest.raises(PoolTimeout):
        build_database_pool(config)


def test_build_database_pool__idle_connection_terminated__replaced(
    database_url: str,
) -> None:
    # Given a pooled connection the server has since terminated
    config = Config(
        kafka_bootstrap_servers="localhost:9092",
        kafka_auth="none",
        kafka_username=None,
        kafka_password=None,
        database_url=database_url,
        warehouse_credentials_secret="secret",
    )
    pool = build_database_pool(config)
    with pool.connection() as database:
        backend_pid = database.info.backend_pid
    with psycopg.connect(database_url, autocommit=True) as admin:
        admin.execute("SELECT pg_terminate_backend(%s)", (backend_pid,))

    # When
    with pool.connection() as database:
        result = database.execute("SELECT 1").fetchone()

    # Then
    assert result == (1,)
    pool.close()


def test_run_loops__one_loop_raises__others_stopped_and_error_raised() -> None:
    # Given one loop that fails and one that runs until told to stop
    stop = threading.Event()

    def failing() -> None:
        raise RuntimeError("postgres down")

    told_to_stop: list[bool] = []

    def until_stopped() -> None:
        told_to_stop.append(stop.wait(timeout=5))

    # When / Then
    with pytest.raises(RuntimeError, match="postgres down"):
        run_loops([failing, until_stopped], stop)
    assert told_to_stop == [True]
