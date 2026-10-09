import dataclasses
import json
import traceback
from collections.abc import Iterator
from typing import Any
from urllib.parse import parse_qs

import pytest
from pytest_mock import MockerFixture
from pytest_structlog import StructuredLogCapture
from urllib3 import HTTPResponse, PoolManager
from urllib3.exceptions import NewConnectionError
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
GET_POOL_MANAGER = databricks._get_pool_manager
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
def clear_caches() -> Iterator[None]:
    databricks._get_sdk.cache_clear()
    databricks._tokens.clear()
    yield
    databricks._get_sdk.cache_clear()
    databricks._tokens.clear()


@pytest.fixture(autouse=True)
def token_endpoint(mocker: MockerFixture) -> Any:
    pool = mocker.patch(
        "warehouse_delivery.warehouses.databricks._get_pool_manager"
    ).return_value
    pool.request.return_value = HTTPResponse(
        body=b'{"access_token":"minted-token","expires_in":3600}', status=200
    )
    return pool


@pytest.fixture()
def sdk_class(mocker: MockerFixture) -> Any:
    return mocker.patch(
        "warehouse_delivery.warehouses.databricks.ZerobusSdk", autospec=True
    )


@pytest.fixture()
def stream(sdk_class: Any) -> Any:
    return sdk_class.return_value.create_stream.return_value


@pytest.mark.parametrize(
    "host, region, server_endpoint",
    [
        pytest.param(
            "DBC-A1B2C3D4-E5F6.cloud.databricks.com",
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
def test_from_connection__valid_details__builds_the_workspace_urls(
    host: str,
    region: str,
    server_endpoint: str,
) -> None:
    # Given
    config = {
        **CONFIG,
        "host": host,
        "region": region,
        "workspace_id": 1234567890123456,
    }

    # When
    warehouse = DatabricksWarehouse.from_connection(_connection(config, CREDENTIALS))

    # Then
    assert warehouse.server_endpoint == server_endpoint
    assert warehouse.unity_catalog_url == f"https://{host.lower()}"
    assert warehouse.table_name == "main.flagsmith.events"
    assert "sp-secret" not in repr(warehouse)


@pytest.mark.parametrize(
    "config, credentials",
    [
        pytest.param(
            {k: v for k, v in CONFIG.items() if k != "region"},
            CREDENTIALS,
            id="missing-region",
        ),
        pytest.param({**CONFIG, "host": None}, CREDENTIALS, id="null-host"),
        pytest.param(CONFIG, {**CREDENTIALS, "client_secret": ""}, id="empty-secret"),
        pytest.param(
            {**CONFIG, "host": "dbc-1.cloud.databricks.com.attacker.example"},
            CREDENTIALS,
            id="foreign-host",
        ),
        pytest.param(
            {**CONFIG, "host": "evil.example/x.cloud.databricks.com"},
            CREDENTIALS,
            id="host-with-path",
        ),
        pytest.param({**CONFIG, "workspace_id": "123abc"}, CREDENTIALS, id="workspace"),
        pytest.param({**CONFIG, "region": "us-west-2.evil"}, CREDENTIALS, id="region"),
        pytest.param({**CONFIG, "schema": "a.b"}, CREDENTIALS, id="schema"),
    ],
)
def test_from_connection__invalid_details__raises_stored_connection_failure(
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
    "event, expected",
    [
        pytest.param(
            {
                "environment_key": "env",
                "event": "purchase",
                "identifier": "u1",
                "timestamp": 1753000000123.5,
                "collected_at": 1753000001000,
                "feature_name": "checkout",
            },
            {
                "environment_key": "env",
                "event": "purchase",
                "identifier": "u1",
                "timestamp": 1753000000123500,
                "collected_at": 1753000001000000,
                "feature_name": "checkout",
            },
            id="milliseconds-to-microseconds",
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
            {
                "event": "e",
                "identifier": 42,
                "timestamp": 1,
                "value": 9.99,
                "traits": {"plan": "pro"},
                "feature_name": None,
                "ip": "1.2.3.4",
            },
            {
                "environment_key": "",
                "event": "e",
                "identifier": "42",
                "timestamp": 1000,
                "value": "9.99",
                "traits": '{"plan":"pro"}',
            },
            id="non-string-values-become-json",
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
        pytest.param(b'["a list"]', id="not-an-object"),
        pytest.param(b'{"event":"e","timestamp":true}', id="bool-timestamp"),
        pytest.param(b'{"event":"e","timestamp":NaN}', id="nan-timestamp"),
        pytest.param(b'{"event":"e","timestamp":1e30}', id="timestamp-out-of-range"),
    ],
)
def test_map_row__unusable_event__returns_none(payload: bytes) -> None:
    # Given / When / Then
    assert map_row(payload) is None


def test_insert__rows__sends_every_usable_record_then_closes_the_stream(
    sdk_class: Any,
    stream: Any,
    mocker: MockerFixture,
    log: StructuredLogCapture,
) -> None:
    # Given
    calls = mocker.Mock()
    calls.attach_mock(sdk_class.return_value.create_stream, "create_stream")
    calls.attach_mock(stream.ingest_records_offset, "ingest_records_offset")
    calls.attach_mock(stream.close, "close")

    # When
    written = WAREHOUSE.insert([*ROWS, b"{not json"])

    # Then
    assert written == 2
    assert [call[0] for call in calls.mock_calls] == [
        "create_stream",
        "ingest_records_offset",
        "close",
    ]
    kwargs = sdk_class.return_value.create_stream.call_args.kwargs
    assert kwargs["table_properties"].table_name == "main.flagsmith.events"
    assert kwargs["options"].record_type == RecordType.JSON
    assert kwargs["headers_provider"].get_headers() == [
        ("authorization", "Bearer minted-token"),
        ("x-databricks-zerobus-table-name", "main.flagsmith.events"),
    ]
    (records,) = stream.ingest_records_offset.call_args.args
    assert [json.loads(record)["identifier"] for record in records] == ["u1", "u2"]
    assert log.events == [
        {
            "level": "warning",
            "event": "rows.skipped",
            "connection__id": 7,
            "rows__count": 1,
        }
    ]


def test_insert__every_row_unusable__opens_no_stream(sdk_class: Any) -> None:
    # Given / When
    written = WAREHOUSE.insert([b"{not json"])

    # Then
    assert written == 0
    sdk_class.assert_not_called()


@pytest.mark.parametrize(
    "error, raised",
    [
        pytest.param(ZerobusException("Connection timeout"), DeliveryError, id="sdk"),
        pytest.param(RuntimeError("boom"), RuntimeError, id="ours"),
    ],
)
def test_insert__ingest_fails__still_closes_the_stream(
    error: Exception,
    raised: type[Exception],
    stream: Any,
) -> None:
    # Given
    stream.ingest_records_offset.side_effect = error

    # When / Then
    with pytest.raises(raised):
        WAREHOUSE.insert(ROWS)
    stream.close.assert_called_once_with()


@pytest.mark.parametrize(
    "message, kind, detail",
    [
        pytest.param(
            "Failed to create stream: code: 'The request does not have valid "
            'authentication credentials\', message: "Invalid token audience".',
            "authentication",
            "Authentication failed.",
            id="unauthenticated",
        ),
        pytest.param(
            "Failed to create stream: code: 'The service is currently unavailable', "
            'message: "dns error".',
            "unreachable",
            "Could not connect to the host.",
            id="unavailable",
        ),
        pytest.param(
            "Failed to create stream: code: 'Some requested entity was not found', "
            'message: "Table not found".',
            "missing_table",
            "Events table not found in the configured database. "
            "Run the setup SQL to create it.",
            id="not-found",
        ),
        pytest.param(
            "Failed to create stream: code: 'The caller does not have permission "
            'to execute the specified operation\', message: "missing MODIFY".',
            "permission_denied",
            "Permission denied on the events table.",
            id="permission-denied",
        ),
        pytest.param(
            "Stream is closed: code: 'Client specified an invalid argument', "
            "message: \"Record decoder/encoder error: unrecognized field name 'ip'\".",
            "schema_mismatch",
            "The warehouse rejected the events. Check that the events table "
            "matches the expected schema.",
            id="invalid-record",
        ),
        pytest.param(
            "Specified UC table name is invalid: Table name must have exactly 3 parts.",
            "stored_connection",
            "Stored connection details are incomplete.",
            id="invalid-table-name",
        ),
        pytest.param(
            "Invalid argument: Ingest payload too large: 10485761 bytes exceeds the "
            "configured limit of 10420224 bytes.",
            "rejected",
            "The Databricks workspace rejected the request.",
            id="payload-too-large",
        ),
        pytest.param(
            "Failed to create stream: code: 'The system is not in a state required "
            "for the operation's execution'.",
            "rejected",
            "The Databricks workspace rejected the request.",
            id="other",
        ),
    ],
)
def test_insert__sdk_error__raises_delivery_error_with_dashboard_detail(
    message: str,
    kind: str,
    detail: str,
    sdk_class: Any,
) -> None:
    # Given
    error = NonRetriableException(message)
    sdk_class.return_value.create_stream.side_effect = error

    # When / Then
    with pytest.raises(DeliveryError) as excinfo:
        WAREHOUSE.insert(ROWS)
    assert (excinfo.value.kind, excinfo.value.detail) == (kind, detail)
    assert excinfo.value.__cause__ is error


@pytest.mark.parametrize("credential", ["sp-secret", "minted-token"])
def test_insert__sdk_error_repeats_a_credential__keeps_it_out_of_the_error(
    credential: str,
    sdk_class: Any,
) -> None:
    # Given
    sdk_class.return_value.create_stream.side_effect = NonRetriableException(
        f"Failed to create stream: {credential}."
    )

    # When
    with pytest.raises(DeliveryError) as excinfo:
        WAREHOUSE.insert(ROWS)

    # Then
    assert credential not in "".join(traceback.format_exception(excinfo.value))


def test_insert__token__minted_with_sql_scope_for_the_events_table(
    token_endpoint: Any,
    stream: Any,
) -> None:
    # Given / When
    WAREHOUSE.insert(ROWS)

    # Then
    method, url = token_endpoint.request.call_args.args
    assert (method, url) == (
        "POST",
        "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com/oidc/v1/token",
    )
    kwargs = token_endpoint.request.call_args.kwargs
    assert kwargs["headers"]["authorization"] == "Basic c3AtaWQ6c3Atc2VjcmV0"
    form = parse_qs(kwargs["body"])
    assert form["scope"] == ["sql"]
    assert form["resource"] == [
        "api://databricks/workspaces/1234567890123456/zerobusDirectWriteApi"
    ]
    assert json.loads(form["authorization_details"][0]) == [
        {
            "type": "unity_catalog_privileges",
            "privileges": ["USE CATALOG"],
            "object_type": "CATALOG",
            "object_full_path": "main",
        },
        {
            "type": "unity_catalog_privileges",
            "privileges": ["USE SCHEMA"],
            "object_type": "SCHEMA",
            "object_full_path": "main.flagsmith",
        },
        {
            "type": "unity_catalog_privileges",
            "privileges": ["SELECT", "MODIFY"],
            "object_type": "TABLE",
            "object_full_path": "main.flagsmith.events",
            "operations": ["zerobuswrite"],
        },
    ]


@pytest.mark.parametrize(
    "second, mints",
    [
        pytest.param(WAREHOUSE, 1, id="same-connection"),
        pytest.param(
            dataclasses.replace(WAREHOUSE, client_secret="rotated"),
            2,
            id="rotated-secret",
        ),
        pytest.param(dataclasses.replace(WAREHOUSE, schema="other"), 2, id="table"),
    ],
)
def test_insert__second_insert__reuses_the_token_only_for_the_same_credentials(
    second: DatabricksWarehouse,
    mints: int,
    token_endpoint: Any,
    stream: Any,
) -> None:
    # Given / When
    WAREHOUSE.insert(ROWS)
    second.insert(ROWS)

    # Then
    assert token_endpoint.request.call_count == mints


@pytest.mark.parametrize("invalidate", [False, True], ids=["near-expiry", "invalid"])
def test_insert__token_expiring_or_invalidated__mints_a_new_one(
    invalidate: bool,
    token_endpoint: Any,
    sdk_class: Any,
    stream: Any,
) -> None:
    # Given
    if not invalidate:
        token_endpoint.request.return_value = HTTPResponse(
            body=b'{"access_token":"minted-token","expires_in":300}', status=200
        )
    WAREHOUSE.insert(ROWS)
    if invalidate:
        sdk_class.return_value.create_stream.call_args.kwargs[
            "headers_provider"
        ].invalidate()

    # When
    WAREHOUSE.insert(ROWS)

    # Then
    assert token_endpoint.request.call_count == 2


@pytest.mark.parametrize(
    "response, kind, detail",
    [
        pytest.param(
            HTTPResponse(
                body=b'{"error":"access_denied","error_description":"Scopes '
                b"'sql' are not assigned to the client d9c616b1\"}",
                status=403,
            ),
            "authentication",
            "The service principal secret must allow the sql scope. "
            "Generate a new secret with the sql scope.",
            id="secret-without-sql-scope",
        ),
        pytest.param(
            HTTPResponse(body=b'{"error":"invalid_authorization_details"}', status=401),
            "permission_denied",
            "The events table is missing or the service principal lacks access "
            "to it. Run the setup SQL.",
            id="missing-object-or-grant",
        ),
        pytest.param(
            HTTPResponse(body=b'{"error":"invalid_client"}', status=401),
            "authentication",
            "Authentication failed.",
            id="bad-client-secret",
        ),
        pytest.param(
            HTTPResponse(body=b"upstream error", status=503),
            "rejected",
            "The Databricks workspace rejected the request.",
            id="server-error",
        ),
        pytest.param(
            HTTPResponse(body=b"<html>not json</html>", status=200),
            "rejected",
            "The Databricks workspace rejected the request.",
            id="unexpected-success-body",
        ),
        pytest.param(
            NewConnectionError(None, "dns error"),  # type: ignore[arg-type]
            "unreachable",
            "Could not connect to the host.",
            id="unreachable",
        ),
    ],
)
def test_insert__token_endpoint_refuses__raises_without_opening_a_stream(
    response: HTTPResponse | Exception,
    kind: str,
    detail: str,
    token_endpoint: Any,
    sdk_class: Any,
) -> None:
    # Given
    if isinstance(response, Exception):
        token_endpoint.request.side_effect = response
    else:
        token_endpoint.request.return_value = response

    # When / Then
    with pytest.raises(DeliveryError) as excinfo:
        WAREHOUSE.insert(ROWS)
    assert (excinfo.value.kind, excinfo.value.detail) == (kind, detail)
    sdk_class.return_value.create_stream.assert_not_called()


def test_insert__token_endpoint_refuses__logs_status_and_oauth_error_only(
    token_endpoint: Any,
    log: StructuredLogCapture,
) -> None:
    # Given
    token_endpoint.request.return_value = HTTPResponse(
        body=b'{"error":"invalid_client","error_description":"sp-id is unknown"}',
        status=401,
    )

    # When
    with pytest.raises(DeliveryError):
        WAREHOUSE.insert(ROWS)

    # Then
    assert log.events == [
        {
            "level": "warning",
            "event": "token.refused",
            "connection__id": 7,
            "http__status": 401,
            "oauth__error": "invalid_client",
        }
    ]


def test_get_pool_manager__called_twice__returns_one_pool() -> None:
    # Given / When
    first, second = GET_POOL_MANAGER(), GET_POOL_MANAGER()

    # Then
    assert isinstance(first, PoolManager)
    assert first is second


def test_stream_options__worst_case__fits_inside_the_insert_timeout() -> None:
    # Given
    options = databricks._stream_options()

    # When
    worst_case_ms = (
        databricks.MINT_TIMEOUT_SECONDS * 1000
        + options.recovery_timeout_ms
        + max(options.flush_timeout_ms, options.server_lack_of_ack_timeout_ms)
        + databricks.PAUSED_CLOSE_TIMEOUT_MS
        + databricks.SDK_SHUTDOWN_MS
    )

    # Then
    assert options.recovery is False
    assert worst_case_ms < INSERT_TIMEOUT_SECONDS * 1000
