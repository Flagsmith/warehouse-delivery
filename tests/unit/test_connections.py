import json
from typing import Any

import pytest
from pytest_mock import MockerFixture

from warehouse_delivery.connections import (
    RedisWarehouseConnections,
    WarehouseConnection,
)
from warehouse_delivery.crypto import fernet_from_secret
from warehouse_delivery.errors import DeliveryError

FERNET = fernet_from_secret("fernet-secret")
CONFIG = {
    "host": "ch.acme-corp.example",
    "port": 8443,
    "database": "acme_dwh",
    "username": "acme_svc",
    "secure": True,
}


def _document(**overrides: Any) -> bytes:
    document: dict[str, Any] = {
        "connection_id": 42,
        "warehouse_type": "clickhouse",
        "config": CONFIG,
        "credentials": FERNET.encrypt(
            json.dumps({"password": "hunter2"}).encode()
        ).decode(),
    }
    document.update(overrides)
    return json.dumps(document).encode()


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def test_redis_connections_get__document_present__returns_decrypted_connection(
    mocker: MockerFixture,
) -> None:
    # Given the API has published the environment's connection
    client = mocker.Mock()
    client.get.return_value = _document()
    store = RedisWarehouseConnections(client, FERNET, clock=Clock())

    # When
    connection = store.get("client-env-key")

    # Then the connection carries the plain credentials, read from the key the
    # API writes
    client.get.assert_called_once_with(
        "experimentation:environment_warehouses:client-env-key"
    )
    assert connection == WarehouseConnection(
        id=42,
        warehouse_type="clickhouse",
        config=CONFIG,
        credentials={"password": "hunter2"},
    )


def test_redis_connections_get__document_present__second_lookup_served_from_cache(
    mocker: MockerFixture,
) -> None:
    # Given an environment whose connection the API has published
    client = mocker.Mock()
    client.get.return_value = _document()
    store = RedisWarehouseConnections(client, FERNET, clock=Clock())

    # When it is looked up twice inside the cache window
    first = store.get("client-env-key")
    second = store.get("client-env-key")

    # Then Redis is asked once, however many environments a batch holds
    assert first == second
    client.get.assert_called_once()


def test_redis_connections_get__no_document__asked_again_on_the_next_lookup(
    mocker: MockerFixture,
) -> None:
    # Given an environment whose connection the API has not published yet, so
    # its events are being dropped
    client = mocker.Mock()
    client.get.return_value = None
    store = RedisWarehouseConnections(client, FERNET, clock=Clock())

    # When it is looked up twice inside the cache window
    first = store.get("client-env-key")
    second = store.get("client-env-key")

    # Then Redis is asked both times, so delivery starts as soon as the API
    # publishes the connection rather than up to a minute later
    assert first is None
    assert second is None
    assert client.get.call_count == 2


def test_redis_connections_get__ttl_elapsed__reads_redis_again(
    mocker: MockerFixture,
) -> None:
    # Given a cached connection and a connection change published since
    clock = Clock()
    client = mocker.Mock()
    client.get.side_effect = [_document(), _document(connection_id=43)]
    store = RedisWarehouseConnections(client, FERNET, clock=clock)
    first = store.get("client-env-key")

    # When the TTL has passed
    clock.now += 60.0
    second = store.get("client-env-key")

    # Then the new document is picked up
    assert first is not None and first.id == 42
    assert second is not None and second.id == 43
    assert client.get.call_count == 2


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param(b"not json", id="not-json"),
        pytest.param(b'{"warehouse_type": "clickhouse"}', id="no-connection-id"),
        pytest.param(_document(connection_id="forty-two"), id="bad-connection-id"),
        pytest.param(_document(config="host=ch"), id="config-not-object"),
    ],
)
def test_redis_connections_get__malformed_document__raises_stored_connection_failure(
    raw: bytes,
    mocker: MockerFixture,
) -> None:
    # Given a document the API would never write
    client = mocker.Mock()
    client.get.return_value = raw
    store = RedisWarehouseConnections(client, FERNET, clock=Clock())

    # When / Then the customer's connection shows what is wrong in the
    # dashboard, rather than their events vanishing silently
    with pytest.raises(DeliveryError) as excinfo:
        store.get("client-env-key")
    assert excinfo.value.kind == "stored_connection"
    assert excinfo.value.detail == "Stored connection details are incomplete."


def test_redis_connections_get__other_secret__raises_stored_connection_failure(
    mocker: MockerFixture,
) -> None:
    # Given credentials encrypted with a secret this service does not hold
    client = mocker.Mock()
    client.get.return_value = _document()
    store = RedisWarehouseConnections(
        client, fernet_from_secret("other-secret"), clock=Clock()
    )

    # When / Then
    with pytest.raises(DeliveryError, match="cannot be decrypted") as excinfo:
        store.get("client-env-key")
    assert excinfo.value.kind == "stored_connection"


def test_redis_connections_get__no_credentials__yields_empty_credentials(
    mocker: MockerFixture,
) -> None:
    # Given a connection type the API stores no credentials for
    client = mocker.Mock()
    client.get.return_value = _document(warehouse_type="snowflake", credentials=None)
    store = RedisWarehouseConnections(client, FERNET, clock=Clock())

    # When
    connection = store.get("client-env-key")

    # Then
    assert connection is not None
    assert connection.warehouse_type == "snowflake"
    assert connection.credentials == {}


def test_warehouse_connection__repr__omits_the_credentials() -> None:
    # Given / When
    shown = repr(
        WarehouseConnection(
            id=42,
            warehouse_type="clickhouse",
            config=CONFIG,
            credentials={"password": "hunter2"},
        )
    )

    # Then
    assert "hunter2" not in shown


@pytest.mark.parametrize(
    "credentials",
    [
        pytest.param({"password": "hunter2"}, id="object-instead-of-token"),
        pytest.param(42, id="number"),
    ],
)
def test_redis_connections_get__credentials_not_a_string__raises_incomplete(
    credentials: Any,
    mocker: MockerFixture,
) -> None:
    # Given a document whose credentials field is not a Fernet token string
    client = mocker.Mock()
    client.get.return_value = _document(credentials=credentials)
    store = RedisWarehouseConnections(client, FERNET, clock=Clock())

    # When / Then the loop does not crash on it
    with pytest.raises(DeliveryError) as excinfo:
        store.get("client-env-key")
    assert excinfo.value.kind == "stored_connection"
    assert excinfo.value.detail == "Stored connection details are incomplete."


def test_redis_connections_get__token_decrypts_to_non_json__raises_undecryptable(
    mocker: MockerFixture,
) -> None:
    # Given a token made with our secret that does not hold JSON
    client = mocker.Mock()
    client.get.return_value = _document(
        credentials=FERNET.encrypt(b"not json").decode()
    )
    store = RedisWarehouseConnections(client, FERNET, clock=Clock())

    # When / Then
    with pytest.raises(DeliveryError) as excinfo:
        store.get("client-env-key")
    assert excinfo.value.detail == "Stored credentials cannot be decrypted."
    assert excinfo.value.connection_id == 42


def test_redis_connections_get__other_secret__error_names_the_connection(
    mocker: MockerFixture,
) -> None:
    # Given credentials encrypted with a secret this service does not hold
    client = mocker.Mock()
    client.get.return_value = _document()
    store = RedisWarehouseConnections(
        client, fernet_from_secret("other-secret"), clock=Clock()
    )

    # When / Then the id travels with the error so the dashboard can be updated
    with pytest.raises(DeliveryError) as excinfo:
        store.get("client-env-key")
    assert excinfo.value.connection_id == 42
