import json

from pytest_mock import MockerFixture

from warehouse_delivery.connection_status import RedisConnectionStatusWriter


def test_write__outcome__lands_in_the_hash_the_api_drains(
    mocker: MockerFixture,
) -> None:
    # Given
    client = mocker.Mock()
    writer = RedisConnectionStatusWriter(client, clock=lambda: 1_758_000_000.0)

    # When
    writer.write(42, "errored", "Authentication failed.")

    # Then the entry sits under the connection id, for the API to apply
    client.hset.assert_called_once()
    hash_key, field, value = client.hset.call_args.args
    assert hash_key == "experimentation:warehouse_delivery_status"
    assert field == "42"
    assert json.loads(value) == {
        "status": "errored",
        "detail": "Authentication failed.",
        "at": 1_758_000_000.0,
    }
