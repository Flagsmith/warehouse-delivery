import threading

import pytest
from pytest_mock import MockerFixture

from warehouse_delivery.__main__ import build_redis_client, run_loops
from warehouse_delivery.config import Config


@pytest.mark.parametrize(
    "cluster, patched",
    [
        pytest.param(True, "RedisCluster", id="elasticache-cluster"),
        pytest.param(False, "Redis", id="single-node"),
    ],
)
def test_build_redis_client__cluster_flag__picks_the_client_class(
    cluster: bool,
    patched: str,
    mocker: MockerFixture,
) -> None:
    # Given
    from_url = mocker.patch(f"warehouse_delivery.__main__.{patched}.from_url")
    config = Config(
        kafka_bootstrap_servers="b",
        kafka_auth="none",
        kafka_username=None,
        kafka_password=None,
        redis_url="rediss://redis.example:6379",
        warehouse_credentials_secret="s",
        redis_cluster=cluster,
    )

    # When
    client = build_redis_client(config)

    # Then
    assert client is from_url.return_value
    from_url.assert_called_once_with(
        "rediss://redis.example:6379", socket_timeout=2.0, socket_keepalive=True
    )


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
