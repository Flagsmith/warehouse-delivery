import pytest
from pytest_mock import MockerFixture

from warehouse_delivery.__main__ import build_redis_client
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
