import json
import traceback
from collections.abc import Iterator
from typing import Any

import pytest
from pytest_mock import MockerFixture
from pytest_structlog import StructuredLogCapture
from zerobus.sdk.shared import (
    NonRetriableException,
    RecordType,
    ZerobusException,
)

from warehouse_delivery.connections import WarehouseConnection
from warehouse_delivery.errors import DeliveryError
from warehouse_delivery.warehouses import databricks
from warehouse_delivery.warehouses.clickhouse import INSERT_TIMEOUT_SECONDS
from warehouse_delivery.warehouses.databricks import DatabricksWarehouse, map_row

CONFIG = {
    "host": "dbc-a1b2c3d4-e5f6.cloud.databricks.com",
    "warehouse_id": "abc123",
    "catalog": "main",
    "schema": "flagsmith",
    "workspace_id": "1234567890123456",
    "region": "us-west-2",
}
CREDENTIALS = {"client_id": "sp-id", "client_secret": "sp-secret"}
WAREHOUSE = DatabricksWarehouse(
    connection_id=7,
    host="dbc-a1b2c3d4-e5f6.cloud.databricks.com",
    workspace_id="1234567890123456",
    region="us-west-2",
    catalog="main",
    schema="flagsmith",
    client_id="sp-id",
    client_secret="sp-secret",
)
ROWS = [
    b'{"environment_key":"env","event":"$flag_exposure","identifier":"u1",'
    b'"timestamp":1753000000000}',
    b'{"environment_key":"env","event":"purchase","identifier":"u2",'
    b'"timestamp":1753000000001,"value":9.99}',
]


def _connection(config: dict[str, Any], credentials: dict[str, Any]) -> Any:
    return WarehouseConnection(
        id=7, warehouse_type="databricks", config=config, credentials=credentials
    )


@pytest.fixture(autouse=True)
def clear_sdk_cache() -> Iterator[None]:
    databricks._get_sdk.cache_clear()
    yield
    databricks._get_sdk.cache_clear()


@pytest.fixture()
def sdk_class(mocker: MockerFixture) -> Any:
    return mocker.patch(
        "warehouse_delivery.warehouses.databricks.ZerobusSdk", autospec=True
    )


@pytest.fixture()
def stream(sdk_class: Any) -> Any:
    return sdk_class.return_value.create_stream.return_value


def test_from_connection__complete_details__reads_config_and_credentials() -> None:
    # Given
    connection = _connection(CONFIG, CREDENTIALS)

    # When / Then
    assert DatabricksWarehouse.from_connection(connection) == WAREHOUSE


@pytest.mark.parametrize(
    "config, credentials",
    [
        pytest.param(
            {k: v for k, v in CONFIG.items() if k != "workspace_id"},
            CREDENTIALS,
            id="missing-workspace-id",
        ),
        pytest.param(
            {k: v for k, v in CONFIG.items() if k != "region"},
            CREDENTIALS,
            id="missing-region",
        ),
        pytest.param(CONFIG, {"client_id": "sp-id"}, id="missing-secret"),
        pytest.param({**CONFIG, "host": None}, CREDENTIALS, id="null-host"),
        pytest.param({**CONFIG, "catalog": 1}, CREDENTIALS, id="catalog-not-text"),
        pytest.param(CONFIG, {**CREDENTIALS, "client_secret": ""}, id="empty-secret"),
    ],
)
def test_from_connection__incomplete_details__raises_stored_connection_failure(
    config: dict[str, Any],
    credentials: dict[str, Any],
) -> None:
    # Given
    connection = _connection(config, credentials)

    # When / Then
    with pytest.raises(DeliveryError) as excinfo:
        DatabricksWarehouse.from_connection(connection)
    assert excinfo.value.kind == "stored_connection"
    assert excinfo.value.detail == "Stored connection details are incomplete."


@pytest.mark.parametrize(
    "override",
    [
        pytest.param({"host": "databricks.attacker.example"}, id="foreign-host"),
        pytest.param({"host": "cloud.databricks.com"}, id="bare-suffix"),
        pytest.param(
            {"host": "https://dbc-1.cloud.databricks.com"}, id="host-with-scheme"
        ),
        pytest.param(
            {"host": "evil.example/x.cloud.databricks.com"}, id="host-with-path"
        ),
        pytest.param(
            {"host": "dbc-1.cloud.databricks.com.attacker.example"},
            id="suffix-not-at-end",
        ),
        pytest.param({"workspace_id": "123abc"}, id="workspace-id-not-digits"),
        pytest.param({"workspace_id": True}, id="workspace-id-bool"),
        pytest.param({"region": "us-west-2.attacker"}, id="region-with-dot"),
        pytest.param({"region": "US-WEST-2"}, id="region-uppercase"),
        pytest.param({"catalog": "main; DROP"}, id="catalog-not-identifier"),
        pytest.param({"schema": "a.b"}, id="schema-with-dot"),
    ],
)
def test_from_connection__invalid_details__raises_stored_connection_failure(
    override: dict[str, Any],
) -> None:
    # Given
    connection = _connection({**CONFIG, **override}, CREDENTIALS)

    # When / Then
    with pytest.raises(DeliveryError) as excinfo:
        DatabricksWarehouse.from_connection(connection)
    assert excinfo.value.kind == "stored_connection"


def test_from_connection__numeric_workspace_id_and_mixed_case_host__normalised() -> (
    None
):
    # Given
    connection = _connection(
        {
            **CONFIG,
            "host": "DBC-A1B2C3D4-E5F6.cloud.databricks.com",
            "workspace_id": 1234567890123456,
        },
        CREDENTIALS,
    )

    # When / Then
    assert DatabricksWarehouse.from_connection(connection) == WAREHOUSE


@pytest.mark.parametrize(
    "host, region, server_endpoint",
    [
        pytest.param(
            "dbc-a1b2c3d4-e5f6.cloud.databricks.com",
            "us-west-2",
            "https://1234567890123456.zerobus.us-west-2.cloud.databricks.com",
            id="aws",
        ),
        pytest.param(
            "adb-1234567890123456.7.azuredatabricks.net",
            "westeurope",
            "https://1234567890123456.zerobus.westeurope.azuredatabricks.net",
            id="azure",
        ),
        pytest.param(
            "1234567890123456.7.gcp.databricks.com",
            "us-central1",
            "https://1234567890123456.zerobus.us-central1.gcp.databricks.com",
            id="gcp",
        ),
    ],
)
def test_databricks_warehouse__per_cloud__builds_zerobus_and_workspace_urls(
    host: str,
    region: str,
    server_endpoint: str,
) -> None:
    # Given
    connection = _connection({**CONFIG, "host": host, "region": region}, CREDENTIALS)

    # When
    warehouse = DatabricksWarehouse.from_connection(connection)

    # Then
    assert warehouse.server_endpoint == server_endpoint
    assert warehouse.unity_catalog_url == f"https://{host}"
    assert warehouse.table_name == "main.flagsmith.events"


@pytest.mark.parametrize(
    "event, expected",
    [
        pytest.param(
            {
                "environment_key": "env",
                "event": "purchase",
                "identifier": "u1",
                "timestamp": 1753000000123,
                "collected_at": 1753000001000,
                "feature_name": "checkout",
                "sdk_language": "python",
            },
            {
                "environment_key": "env",
                "event": "purchase",
                "identifier": "u1",
                "timestamp": 1753000000123000,
                "collected_at": 1753000001000000,
                "feature_name": "checkout",
                "sdk_language": "python",
            },
            id="milliseconds-to-microseconds",
        ),
        pytest.param(
            {"event": "e", "timestamp": 1753000000123.5},
            {
                "environment_key": "",
                "event": "e",
                "identifier": "",
                "timestamp": 1753000000123500,
            },
            id="fractional-milliseconds",
        ),
        pytest.param(
            {"event": "e", "timestamp": "yesterday", "collected_at": 1753000001000},
            {
                "environment_key": "",
                "event": "e",
                "identifier": "",
                "timestamp": 1753000001000000,
                "collected_at": 1753000001000000,
            },
            id="timestamp-falls-back-to-collected-at",
        ),
        pytest.param(
            {"environment_key": None, "timestamp": 1},
            {"environment_key": "", "event": "", "identifier": "", "timestamp": 1000},
            id="not-null-columns-default-to-empty",
        ),
        pytest.param(
            {
                "event": "e",
                "identifier": 42,
                "timestamp": 1,
                "value": 9.99,
                "traits": {"plan": "pro", "seats": 3},
                "metadata": [1, "a"],
                "sdk_version": True,
                "feature_name": None,
            },
            {
                "environment_key": "",
                "event": "e",
                "identifier": "42",
                "timestamp": 1000,
                "value": "9.99",
                "traits": '{"plan":"pro","seats":3}',
                "metadata": '[1,"a"]',
                "sdk_version": "true",
            },
            id="non-string-values-become-json",
        ),
        pytest.param(
            {"event": "e", "timestamp": 1, "ip": "1.2.3.4", "extra": {"a": 1}},
            {"environment_key": "", "event": "e", "identifier": "", "timestamp": 1000},
            id="unknown-keys-dropped",
        ),
    ],
)
def test_map_row__event__projects_the_table_columns(
    event: dict[str, Any],
    expected: dict[str, Any],
) -> None:
    # Given
    payload = json.dumps(event).encode()

    # When
    record = map_row(payload)

    # Then
    assert record is not None
    assert json.loads(record) == expected


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(b"{not json", id="unparseable"),
        pytest.param(b"\xff\xfe", id="not-utf8"),
        pytest.param(b'["a list"]', id="not-an-object"),
        pytest.param(b'{"event":"e"}', id="no-timestamp"),
        pytest.param(b'{"event":"e","timestamp":true}', id="bool-timestamp"),
        pytest.param(b'{"event":"e","timestamp":1e30}', id="timestamp-out-of-range"),
        pytest.param(b'{"event":"e","timestamp":NaN}', id="nan-timestamp"),
    ],
)
def test_map_row__unusable_event__returns_none(payload: bytes) -> None:
    # Given / When / Then
    assert map_row(payload) is None


def test_insert__rows__opens_a_stream_sends_every_record_and_closes_it(
    sdk_class: Any,
    stream: Any,
    mocker: MockerFixture,
) -> None:
    # Given
    calls = mocker.Mock()
    calls.attach_mock(sdk_class.return_value.create_stream, "create_stream")
    calls.attach_mock(stream.ingest_records_nowait, "ingest_records_nowait")
    calls.attach_mock(stream.close, "close")

    # When
    written = WAREHOUSE.insert(ROWS)

    # Then
    assert written == 2
    sdk_class.assert_called_once_with(
        "https://1234567890123456.zerobus.us-west-2.cloud.databricks.com",
        "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com",
        application_name="flagsmith-warehouse-delivery",
    )
    assert [call[0] for call in calls.mock_calls] == [
        "create_stream",
        "ingest_records_nowait",
        "close",
    ]
    stream.flush.assert_not_called()
    kwargs = sdk_class.return_value.create_stream.call_args.kwargs
    assert kwargs["table_properties"].table_name == "main.flagsmith.events"
    assert (kwargs["client_id"], kwargs["client_secret"]) == ("sp-id", "sp-secret")
    options = kwargs["options"]
    assert options.record_type == RecordType.JSON
    assert options.recovery is False
    (records,) = stream.ingest_records_nowait.call_args.args
    assert [json.loads(record) for record in records] == [
        {
            "environment_key": "env",
            "event": "$flag_exposure",
            "identifier": "u1",
            "timestamp": 1753000000000000,
        },
        {
            "environment_key": "env",
            "event": "purchase",
            "identifier": "u2",
            "timestamp": 1753000000001000,
            "value": "9.99",
        },
    ]


def test_insert__unusable_rows__skips_and_logs_them_without_contents(
    stream: Any,
    log: StructuredLogCapture,
) -> None:
    # Given
    rows = [*ROWS, b"{not json", b'{"event":"secret-event"}']

    # When
    written = WAREHOUSE.insert(rows)

    # Then
    assert written == 2
    assert len(stream.ingest_records_nowait.call_args.args[0]) == 2
    assert log.events == [
        {
            "level": "warning",
            "event": "rows.skipped",
            "connection__id": 7,
            "rows__count": 2,
        }
    ]


def test_insert__every_row_unusable__opens_no_stream(sdk_class: Any) -> None:
    # Given / When
    written = WAREHOUSE.insert([b"{not json"])

    # Then
    assert written == 0
    sdk_class.assert_not_called()


def test_insert__same_workspace__reuses_one_sdk(sdk_class: Any, stream: Any) -> None:
    # Given / When
    WAREHOUSE.insert(ROWS)
    WAREHOUSE.insert(ROWS)

    # Then
    sdk_class.assert_called_once()
    assert sdk_class.return_value.create_stream.call_count == 2


def test_insert__ingest_fails__closes_the_stream_and_raises(stream: Any) -> None:
    # Given
    error = NonRetriableException("Stream is closed: code: 'Internal error'")
    stream.ingest_records_nowait.side_effect = error

    # When / Then
    with pytest.raises(DeliveryError) as excinfo:
        WAREHOUSE.insert(ROWS)
    assert excinfo.value.__cause__ is error
    stream.close.assert_called_once_with()


def test_insert__acknowledgement_fails_on_close__raises_delivery_error(
    stream: Any,
) -> None:
    # Given
    stream.close.side_effect = ZerobusException(
        "Stream is closed: code: 'The service is currently unavailable'"
    )

    # When / Then
    with pytest.raises(DeliveryError) as excinfo:
        WAREHOUSE.insert(ROWS)
    assert excinfo.value.kind == "unreachable"


def test_insert__our_own_failure__propagates_and_still_closes_the_stream(
    stream: Any,
) -> None:
    # Given
    stream.ingest_records_nowait.side_effect = RuntimeError("boom")

    # When / Then
    with pytest.raises(RuntimeError, match="boom"):
        WAREHOUSE.insert(ROWS)
    stream.close.assert_called_once_with()


@pytest.mark.parametrize(
    "error, kind, detail",
    [
        pytest.param(
            NonRetriableException(
                "Specified UC token is in invalid format: Client error (403): "
                '{"error":"access_denied","request_id":"c58174b0","error_description"'
                ":\"Scopes 'all-apis' are not assigned to the client d9c616b1\"}."
            ),
            "authentication",
            "The service principal secret must allow all APIs. "
            "Generate a new secret with the All APIs scope.",
            id="secret-without-all-apis",
        ),
        pytest.param(
            NonRetriableException(
                "Specified UC token is in invalid format: Client error (401): "
                '{"error":"invalid_client","error_description":"Client '
                'authentication failed"}.'
            ),
            "authentication",
            "Authentication failed.",
            id="bad-client-secret",
        ),
        pytest.param(
            NonRetriableException(
                "Failed to create stream: code: 'The request does not have valid "
                'authentication credentials\', message: "Invalid token audience".'
            ),
            "authentication",
            "Authentication failed.",
            id="grpc-unauthenticated-wrong-workspace-id",
        ),
        pytest.param(
            ZerobusException(
                "Token fetch failed: Network error: error sending request for url"
            ),
            "unreachable",
            "Could not connect to the host.",
            id="token-endpoint-unreachable",
        ),
        pytest.param(
            ZerobusException("Failed to open a channel: transport error."),
            "unreachable",
            "Could not connect to the host.",
            id="channel",
        ),
        pytest.param(
            ZerobusException("Failed to establish TLS connection."),
            "unreachable",
            "Could not connect to the host.",
            id="tls",
        ),
        pytest.param(
            ZerobusException("Connection timeout: flush timed out"),
            "unreachable",
            "Could not connect to the host.",
            id="timeout",
        ),
        pytest.param(
            ZerobusException(
                "Failed to create stream: code: 'Deadline expired before operation "
                'could complete\', message: "Stream creation timed out".'
            ),
            "unreachable",
            "Could not connect to the host.",
            id="deadline",
        ),
        pytest.param(
            ZerobusException(
                "Failed to create stream: code: 'The service is currently "
                'unavailable\', message: "dns error", source: '
                'tonic::transport::Error(Transport, ConnectError("dns error")).'
            ),
            "unreachable",
            "Could not connect to the host.",
            id="unavailable",
        ),
        pytest.param(
            NonRetriableException(
                "Failed to create stream: code: 'Some requested entity was not "
                'found\', message: "Table not found".'
            ),
            "missing_table",
            "Events table not found in the configured database. "
            "Run the setup SQL to create it.",
            id="grpc-not-found",
        ),
        pytest.param(
            NonRetriableException(
                "Specified UC token is in invalid format: Client error (401): "
                '{"error":"invalid_authorization_details","request_id":"3e1d0593",'
                '"error_description":"User is not authorized to the requested '
                'authorizations"}.'
            ),
            "permission_denied",
            "The events table is missing or the service principal lacks access "
            "to it. Run the setup SQL.",
            id="missing-object-or-grant-at-token-mint",
        ),
        pytest.param(
            NonRetriableException(
                "Failed to create stream: code: 'The caller does not have "
                "permission to execute the specified operation', message: "
                '"Authorization token is missing MODIFY privilege for a table '
                'main.flagsmith.events. Error Code: 3, Error State: 0.".'
            ),
            "permission_denied",
            "Permission denied on the events table.",
            id="grpc-permission-denied",
        ),
        pytest.param(
            NonRetriableException(
                "Stream is closed: code: 'Client specified an invalid argument', "
                'message: "Record decoder/encoder error: unrecognized field name '
                "'ip' at line 1 column 285. Error Code: 4044, Error State: 3.\""
            ),
            "schema_mismatch",
            "The warehouse rejected the events. Check that the events table "
            "matches the expected schema.",
            id="invalid-record",
        ),
        pytest.param(
            NonRetriableException(
                "Invalid argument: Record decoder/encoder error: unrecognized field "
                "name 'ip' at line 1 column 285. Error Code: 4044, Error State: 3.."
            ),
            "schema_mismatch",
            "The warehouse rejected the events. Check that the events table "
            "matches the expected schema.",
            id="sdk-invalid-argument",
        ),
        pytest.param(
            NonRetriableException(
                "Specified UC table name is invalid: Table name must have exactly "
                "3 parts."
            ),
            "stored_connection",
            "Stored connection details are incomplete.",
            id="invalid-table-name",
        ),
        pytest.param(
            NonRetriableException(
                "Failed to create stream: code: 'The system is not in a state "
                "required for the operation's execution', message: \"Default "
                'storage is not supported".'
            ),
            "rejected",
            "The Databricks workspace rejected the request.",
            id="other",
        ),
    ],
)
def test_insert__sdk_error__raises_delivery_error_with_dashboard_detail(
    error: Exception,
    kind: str,
    detail: str,
    sdk_class: Any,
) -> None:
    # Given
    sdk_class.return_value.create_stream.side_effect = error

    # When / Then
    with pytest.raises(DeliveryError) as excinfo:
        WAREHOUSE.insert(ROWS)
    assert (excinfo.value.kind, excinfo.value.detail) == (kind, detail)
    assert excinfo.value.__cause__ is error


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(
            NonRetriableException(
                "Failed to create stream: code: 'The request does not have valid "
                'authentication credentials\', message: "Invalid token".'
            ),
            id="message-without-secret",
        ),
        pytest.param(
            NonRetriableException(
                "Specified UC token is in invalid format: Client error (401): "
                "client_secret=sp-secret rejected."
            ),
            id="message-repeating-secret",
        ),
    ],
)
def test_insert__sdk_error__secret_never_reaches_the_error_or_the_logs(
    error: Exception,
    sdk_class: Any,
    log: StructuredLogCapture,
) -> None:
    # Given
    sdk_class.return_value.create_stream.side_effect = error

    # When
    with pytest.raises(DeliveryError) as excinfo:
        WAREHOUSE.insert([*ROWS, b"{not json"])

    # Then
    logged = "".join(traceback.format_exception(excinfo.value))
    assert "sp-secret" not in logged
    assert "sp-secret" not in repr(excinfo.value)
    assert "sp-secret" not in repr(log.events)
    assert log.events


def test_stream_options__worst_case__fits_inside_the_insert_timeout() -> None:
    # Given
    options = databricks._stream_options()

    # When
    worst_case_ms = (
        options.recovery_timeout_ms
        + max(options.flush_timeout_ms, options.server_lack_of_ack_timeout_ms)
        + databricks.PAUSED_CLOSE_TIMEOUT_MS
        + databricks.SDK_SHUTDOWN_MS
    )

    # Then
    assert options.recovery is False
    assert options.recovery_timeout_ms == databricks.CREATE_TIMEOUT_MS
    assert options.flush_timeout_ms == databricks.FLUSH_TIMEOUT_MS
    assert worst_case_ms < INSERT_TIMEOUT_SECONDS * 1000


def test_databricks_warehouse__repr__omits_the_secret() -> None:
    # Given / When
    shown = repr(WAREHOUSE)

    # Then
    assert "sp-secret" not in shown
    assert "dbc-a1b2c3d4-e5f6.cloud.databricks.com" in shown
