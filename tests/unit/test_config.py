import pytest

from warehouse_delivery.config import Config, ConfigError

REQUIRED = {
    "KAFKA_BOOTSTRAP_SERVERS": "b-1.example:9096,b-2.example:9096",
    "KAFKA_USERNAME": "delivery",
    "KAFKA_PASSWORD": "hunter2",
    "REDIS_URL": "rediss://redis.example:6379",
    "WAREHOUSE_CREDENTIALS_SECRET": "fernet-secret",
}


def test_config_from_env__only_required_set__fills_defaults() -> None:
    # Given only the variables with no default

    # When
    config = Config.from_env(REQUIRED)

    # Then the connection details are taken as given and the rest default
    assert config.kafka_bootstrap_servers == "b-1.example:9096,b-2.example:9096"
    assert config.kafka_auth == "scram"
    assert config.kafka_username == "delivery"
    assert config.kafka_password == "hunter2"
    assert config.redis_url == "rediss://redis.example:6379"
    assert config.warehouse_credentials_secret == "fernet-secret"
    assert config.events_topic == "external_warehouse_events"
    assert config.consumer_group == "warehouse-delivery"
    assert config.batch_max_records == 5000
    assert config.batch_max_wait_seconds == 5.0
    assert config.redis_cluster is True
    assert config.delivery_concurrency == 16


def test_config_from_env__overrides_set__parses_them() -> None:
    # Given every optional variable set, the wait in milliseconds as the task
    # definition will carry it
    env = {
        **REQUIRED,
        "EXTERNAL_WAREHOUSE_TOPIC": "topic",
        "KAFKA_CONSUMER_GROUP": "group",
        "BATCH_MAX_RECORDS": "100",
        "BATCH_MAX_WAIT_MS": "250",
        "REDIS_CLUSTER": "false",
        "DELIVERY_CONCURRENCY": "4",
    }

    # When
    config = Config.from_env(env)

    # Then
    assert config.redis_cluster is False
    assert config.events_topic == "topic"
    assert config.consumer_group == "group"
    assert config.batch_max_records == 100
    assert config.batch_max_wait_seconds == 0.25
    assert config.delivery_concurrency == 4


@pytest.mark.parametrize(
    "missing, expected_message",
    [
        pytest.param(
            "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_BOOTSTRAP_SERVERS", id="brokers"
        ),
        pytest.param("KAFKA_USERNAME", "KAFKA_USERNAME", id="scram-username"),
        pytest.param("KAFKA_PASSWORD", "KAFKA_PASSWORD", id="scram-password"),
        pytest.param("REDIS_URL", "REDIS_URL", id="redis"),
        pytest.param(
            "WAREHOUSE_CREDENTIALS_SECRET", "WAREHOUSE_CREDENTIALS_SECRET", id="secret"
        ),
    ],
)
def test_config_from_env__required_variable_missing__raises_naming_it(
    missing: str,
    expected_message: str,
) -> None:
    # Given
    env = {name: value for name, value in REQUIRED.items() if name != missing}

    # When / Then
    with pytest.raises(ConfigError, match=expected_message):
        Config.from_env(env)


def test_config_from_env__auth_none__needs_no_credentials() -> None:
    # Given local brokers with no authentication, as in tests and benchmarks
    env = {
        name: value
        for name, value in REQUIRED.items()
        if name not in ("KAFKA_USERNAME", "KAFKA_PASSWORD")
    } | {"KAFKA_AUTH": "none"}

    # When
    config = Config.from_env(env)

    # Then
    assert config.kafka_auth == "none"
    assert config.kafka_username is None
    assert config.kafka_password is None


@pytest.mark.parametrize(
    "name, value",
    [
        pytest.param("KAFKA_AUTH", "iam", id="auth-mode"),
        pytest.param("BATCH_MAX_RECORDS", "lots", id="integer"),
        pytest.param("REDIS_CLUSTER", "maybe", id="boolean"),
        pytest.param("DELIVERY_CONCURRENCY", "0", id="concurrency-zero"),
    ],
)
def test_config_from_env__invalid_value__raises_naming_variable(
    name: str,
    value: str,
) -> None:
    # Given
    env = {**REQUIRED, name: value}

    # When / Then
    with pytest.raises(ConfigError, match=name):
        Config.from_env(env)
