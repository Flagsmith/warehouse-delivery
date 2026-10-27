import socket
from collections.abc import Iterator
from typing import Any

import pytest
from pytest_mock import MockerFixture

from warehouse_delivery import network
from warehouse_delivery.errors import DeliveryError


@pytest.fixture()
def blocking() -> Iterator[None]:
    network.block_internal_hosts(True)
    yield
    network.block_internal_hosts(False)


def _resolves_to(mocker: MockerFixture, *addresses: str) -> Any:
    return mocker.patch(
        "warehouse_delivery.network.socket.getaddrinfo",
        return_value=[
            (socket.AF_INET6 if ":" in a else socket.AF_INET, 1, 6, "", (a, 443))
            for a in addresses
        ],
    )


def test_checked_address__blocking_off__does_not_resolve(mocker: MockerFixture) -> None:
    # Given
    getaddrinfo = _resolves_to(mocker, "10.0.0.5")

    # When / Then
    assert network.checked_address("db.internal", 5432) is None
    getaddrinfo.assert_not_called()


@pytest.mark.usefixtures("blocking")
def test_checked_address__public_addresses__returns_the_first(
    mocker: MockerFixture,
) -> None:
    # Given
    getaddrinfo = _resolves_to(mocker, "2606:4700::1111", "1.1.1.1")

    # When / Then
    assert network.checked_address("pg.acme-corp.example", 5432) == "2606:4700::1111"
    getaddrinfo.assert_called_once_with(
        "pg.acme-corp.example", 5432, type=socket.SOCK_STREAM
    )


@pytest.mark.usefixtures("blocking")
@pytest.mark.parametrize(
    "addresses",
    [
        pytest.param(["127.0.0.1"], id="loopback"),
        pytest.param(["10.0.0.5"], id="private"),
        pytest.param(["169.254.169.254"], id="link-local"),
        pytest.param(["100.64.0.1"], id="shared"),
        pytest.param(["240.0.0.1"], id="reserved"),
        pytest.param(["224.0.0.1"], id="multicast"),
        pytest.param(["0.0.0.0"], id="unspecified"),
        pytest.param(["::1"], id="ipv6-loopback"),
        pytest.param(["fe80::1%lo0"], id="ipv6-link-local-scoped"),
        pytest.param(["::ffff:10.0.0.5"], id="ipv4-mapped-private"),
        pytest.param(["1.1.1.1", "10.0.0.5"], id="mixed-records"),
    ],
)
def test_checked_address__any_internal_address__raises(
    mocker: MockerFixture,
    addresses: list[str],
) -> None:
    # Given
    _resolves_to(mocker, *addresses)

    # When / Then
    with pytest.raises(DeliveryError) as excinfo:
        network.checked_address("pg.acme-corp.example", 5432)
    assert excinfo.value.kind == "stored_connection"
    assert excinfo.value.detail == "The host resolves to an internal address."


@pytest.mark.usefixtures("blocking")
def test_checked_address__unresolvable__left_to_the_driver(
    mocker: MockerFixture,
) -> None:
    # Given
    mocker.patch(
        "warehouse_delivery.network.socket.getaddrinfo",
        side_effect=socket.gaierror("not known"),
    )

    # When / Then
    assert network.checked_address("nowhere.invalid", 5432) is None
