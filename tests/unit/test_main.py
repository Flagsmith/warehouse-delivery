import threading

import pytest

from warehouse_delivery.__main__ import build_database_pool, run_loops
from warehouse_delivery.config import Config


def test_build_database_pool__connection_from_pool__has_two_second_statement_timeout(
    database_url: str,
) -> None:
    # Given
    config = Config(
        kafka_bootstrap_servers="b",
        kafka_auth="none",
        kafka_username=None,
        kafka_password=None,
        database_url=database_url,
        warehouse_credentials_secret="s",
    )

    # When
    with build_database_pool(config) as pool, pool.connection() as database:
        statement_timeout = database.execute("SHOW statement_timeout").fetchone()

    # Then
    assert statement_timeout == ("2s",)


def test_run_loops__one_loop_raises__others_stopped_and_error_raised() -> None:
    # Given one loop that fails and one that runs until told to stop
    stop = threading.Event()

    def failing() -> None:
        raise RuntimeError("redis down")

    told_to_stop: list[bool] = []

    def until_stopped() -> None:
        told_to_stop.append(stop.wait(timeout=5))

    # When / Then
    with pytest.raises(RuntimeError, match="redis down"):
        run_loops([failing, until_stopped], stop)
    assert told_to_stop == [True]
