import gzip
from typing import Any

import pytest
from clickhouse_connect import common as clickhouse_common
from clickhouse_connect.driver.exceptions import (
    DatabaseError,
    InterfaceError,
    OperationalError,
)
from clickhouse_connect.driver.summary import QuerySummary
from pytest_mock import MockerFixture
from urllib3 import PoolManager

from warehouse_delivery.connections import WarehouseConnection
from warehouse_delivery.errors import DeliveryError
from warehouse_delivery.warehouses import clickhouse
from warehouse_delivery.warehouses.clickhouse import ClickHouseWarehouse

CONFIG = {
    "host": "ch.acme-corp.example",
    "port": 8443,
    "database": "acme_dwh",
    "username": "acme_svc",
    "secure": True,
}
WAREHOUSE = ClickHouseWarehouse(
    host="ch.acme-corp.example",
    port=8443,
    database="acme_dwh",
    username="acme_svc",
    password="hunter2",
    secure=True,
)
# Two events exactly as the ingestion server produced them.
ROWS = [
    b'{"environment_key":"env","event":"$flag_exposure","timestamp":1753000000000}',
    b'{"environment_key":"env","event":"purchase","timestamp":1753000000001}',
]


@pytest.fixture()
def get_client(mocker: MockerFixture) -> Any:
    get_client = mocker.patch(
        "warehouse_delivery.warehouses.clickhouse.clickhouse_connect.get_client"
    )
    get_client.return_value.command.return_value = QuerySummary({"written_rows": "2"})
    return get_client


def test_from_connection__complete_details__reads_config_and_password() -> None:
    # Given
    connection = WarehouseConnection(
        id=42,
        warehouse_type="clickhouse",
        config=CONFIG,
        credentials={"password": "hunter2"},
    )

    # When / Then
    assert ClickHouseWarehouse.from_connection(connection) == WAREHOUSE


@pytest.mark.parametrize(
    "config, credentials",
    [
        pytest.param(
            {k: v for k, v in CONFIG.items() if k != "port"},
            {"password": "x"},
            id="missing-port",
        ),
        pytest.param(CONFIG, {}, id="missing-password"),
    ],
)
def test_from_connection__incomplete_details__raises_stored_connection_failure(
    config: dict[str, Any],
    credentials: dict[str, Any],
) -> None:
    # Given
    connection = WarehouseConnection(
        id=42,
        warehouse_type="clickhouse",
        config=config,
        credentials=credentials,
    )

    # When / Then the batch is parked and the dashboard says why
    with pytest.raises(DeliveryError) as excinfo:
        ClickHouseWarehouse.from_connection(connection)
    assert excinfo.value.kind == "stored_connection"
    assert excinfo.value.detail == "Stored connection details are incomplete."


def test_insert__rows__connects_as_the_api_does_and_posts_one_gzipped_block(
    get_client: Any,
) -> None:
    # Given

    # When
    written = WAREHOUSE.insert(ROWS)

    # Then the stored details are used as-is, over a pool that refuses
    # redirects so a permitted host cannot bounce the events elsewhere
    assert written == 2
    get_client.assert_called_once_with(
        host="ch.acme-corp.example",
        port=8443,
        username="acme_svc",
        password="hunter2",
        database="acme_dwh",
        secure=True,
        connect_timeout=10,
        send_receive_timeout=60,
        pool_mgr=clickhouse._get_pool_manager(),
    )
    assert isinstance(
        get_client.call_args.kwargs["pool_mgr"], clickhouse._NoRedirectPoolManager
    )
    # And the rows go through input() so ClickHouse converts the epoch
    # milliseconds, as one gzipped block of the untouched bytes
    client = get_client.return_value
    client.command.assert_called_once()
    (query,) = client.command.call_args.args
    assert query.startswith(
        "INSERT INTO events (environment_key, event, feature_name, "
    )
    assert (
        "fromUnixTimestamp64Milli(timestamp), fromUnixTimestamp64Milli(collected_at)"
        in query
    )
    assert "FROM input('environment_key String, event String, " in query
    assert query.endswith("FORMAT JSONEachRow")
    kwargs = client.command.call_args.kwargs
    assert kwargs["transport_settings"] == {"Content-Encoding": "gzip"}
    assert gzip.decompress(kwargs["data"]) == b"\n".join(ROWS) + b"\n"
    # And an event whose value is a number, a bool or a list is accepted into
    # the table's text column instead of being refused by an older ClickHouse
    assert kwargs["settings"] == {
        "input_format_json_read_numbers_as_strings": 1,
        "input_format_json_read_bools_as_strings": 1,
        "input_format_json_read_objects_as_strings": 1,
        "input_format_json_read_arrays_as_strings": 1,
    }
    client.close.assert_called_once_with()


def test_insert__unexpected_response_body__falls_back_to_batch_size(
    get_client: Any,
) -> None:
    # Given a server that answers an insert with a body instead of a summary
    get_client.return_value.command.return_value = ""

    # When
    written = WAREHOUSE.insert([b"{}", b"{}", b"{}"])

    # Then
    assert written == 3


@pytest.mark.parametrize(
    "error, kind, detail",
    [
        pytest.param(
            OperationalError("HTTPSConnectionPool: Max retries exceeded"),
            "unreachable",
            "Could not connect to the host.",
            id="unreachable",
        ),
        pytest.param(
            DatabaseError("Code: 516. DB::Exception: nope", code=516),
            "authentication",
            "Authentication failed.",
            id="bad-auth",
        ),
        pytest.param(
            DatabaseError("Code: 81. DB::Exception: no database", code=81),
            "missing_database",
            "Database does not exist.",
            id="missing-database",
        ),
        pytest.param(
            DatabaseError("Code: 60. DB::Exception: no table", code=60),
            "missing_table",
            "Events table not found in the configured database. "
            "Run the setup SQL to create it.",
            id="missing-table",
        ),
        pytest.param(
            DatabaseError("Code: 469. DB::Exception: constraint", code=469),
            "schema_mismatch",
            "The warehouse rejected the events. Check that the events table "
            "matches the expected schema.",
            id="schema-mismatch",
        ),
        pytest.param(
            DatabaseError("Code: 241. DB::Exception: memory limit", code=241),
            "rejected",
            "The ClickHouse server rejected the request.",
            id="other-server-error",
        ),
        pytest.param(
            InterfaceError("unsupported setting"),
            "failed",
            "Connection failed.",
            id="driver-gave-up",
        ),
    ],
)
def test_insert__driver_error__raises_delivery_error_with_dashboard_detail(
    error: Exception,
    kind: str,
    detail: str,
    get_client: Any,
) -> None:
    # Given the insert fails on the customer's side
    get_client.return_value.command.side_effect = error

    # When / Then the customer sees the API's wording, the logs keep the driver's
    with pytest.raises(DeliveryError) as excinfo:
        WAREHOUSE.insert(ROWS)
    assert (excinfo.value.kind, excinfo.value.detail) == (kind, detail)
    assert excinfo.value.__cause__ is error
    get_client.return_value.close.assert_called_once_with()


def test_insert__connection_refused__raises_unreachable(get_client: Any) -> None:
    # Given the client cannot even be opened
    get_client.side_effect = OperationalError("connection refused")

    # When / Then
    with pytest.raises(DeliveryError) as excinfo:
        WAREHOUSE.insert(ROWS)
    assert excinfo.value.kind == "unreachable"


def test_insert__our_own_failure__propagates_and_still_closes_the_client(
    get_client: Any,
) -> None:
    # Given a failure that is not the customer's
    get_client.return_value.command.side_effect = RuntimeError("boom")

    # When / Then it is not dressed up as a delivery error, and the pooled
    # connection is still released
    with pytest.raises(RuntimeError, match="boom"):
        WAREHOUSE.insert(ROWS)
    get_client.return_value.close.assert_called_once_with()


def test_driver__settings_the_server_does_not_have__left_out_of_the_request() -> None:
    # Given a customer on a ClickHouse too old to know the settings above

    # When / Then the driver leaves them out rather than sending them, which
    # that server would answer by refusing the whole insert
    assert clickhouse_common.get_setting("invalid_setting_action") == "drop"


def test_click_house_warehouse__repr__omits_the_password() -> None:
    # Given / When
    shown = repr(WAREHOUSE)

    # Then
    assert "hunter2" not in shown
    assert "ch.acme-corp.example" in shown


def test_no_redirect_pool_manager__urlopen__refuses_to_follow_redirects(
    mocker: MockerFixture,
) -> None:
    # Given a manager asked to follow redirects, as clickhouse-connect's own
    # request path does
    urlopen = mocker.patch.object(PoolManager, "urlopen")
    manager = clickhouse._NoRedirectPoolManager()

    # When
    manager.urlopen("POST", "https://ch.acme-corp.example/", redirect=True)

    # Then the redirect is refused: a permitted host must not be able to bounce
    # the request, and its event payload, to an unchecked address
    assert urlopen.call_args.kwargs["redirect"] is False
