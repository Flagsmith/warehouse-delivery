import socket
from typing import Any

import pytest
from pytest_mock import MockerFixture

from warehouse_delivery import network
from warehouse_delivery.errors import DeliveryError

INTERNAL = object()


@pytest.mark.parametrize(
    "blocking, resolved, expected",
    [
        pytest.param(False, ["10.0.0.5"], None, id="blocking-off"),
        pytest.param(True, socket.gaierror("not known"), None, id="unresolvable"),
        pytest.param(
            True, ["2606:4700::1111", "1.1.1.1"], "2606:4700::1111", id="public"
        ),
        pytest.param(True, ["127.0.0.1"], INTERNAL, id="loopback"),
        pytest.param(True, ["10.0.0.5"], INTERNAL, id="private"),
        pytest.param(True, ["169.254.169.254"], INTERNAL, id="link-local"),
        pytest.param(True, ["100.64.0.1"], INTERNAL, id="shared"),
        pytest.param(True, ["::ffff:10.0.0.5"], INTERNAL, id="ipv4-mapped-private"),
        pytest.param(True, ["1.1.1.1", "10.0.0.5"], INTERNAL, id="mixed-records"),
    ],
)
def test_checked_address__resolved_addresses__decide_where_to_connect(
    mocker: MockerFixture,
    blocking: bool,
    resolved: list[str] | Exception,
    expected: Any,
) -> None:
    # Given
    mocker.patch.object(network, "_block_internal_hosts", False)  # restored after
    network.block_internal_hosts(blocking)
    getaddrinfo = mocker.patch(
        "warehouse_delivery.network.socket.getaddrinfo",
        side_effect=resolved if isinstance(resolved, Exception) else None,
        return_value=[
            (socket.AF_INET6 if ":" in a else socket.AF_INET, 1, 6, "", (a, 5432))
            for a in (resolved if isinstance(resolved, list) else [])
        ],
    )

    # When / Then
    if expected is INTERNAL:
        with pytest.raises(DeliveryError) as excinfo:
            network.checked_address("pg.acme-corp.example", 5432)
        assert excinfo.value.kind == "stored_connection"
        assert excinfo.value.detail == "The host resolves to an internal address."
    else:
        assert network.checked_address("pg.acme-corp.example", 5432) == expected
    assert getaddrinfo.called is blocking
