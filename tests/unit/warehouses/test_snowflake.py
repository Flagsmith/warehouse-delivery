import base64
import dataclasses
import hashlib
import json
import traceback
from collections.abc import Iterator
from typing import Any
from urllib.parse import parse_qs, urlsplit
from uuid import UUID

import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, padding, rsa
from freezegun import freeze_time
from pytest_mock import MockerFixture
from pytest_structlog import StructuredLogCapture
from urllib3 import HTTPResponse, PoolManager
from urllib3.exceptions import (
    ConnectTimeoutError,
    NewConnectionError,
    ReadTimeoutError,
)

from warehouse_delivery.connections import WarehouseConnection
from warehouse_delivery.errors import DeliveryError
from warehouse_delivery.warehouses import snowflake
from warehouse_delivery.warehouses.clickhouse import INSERT_TIMEOUT_SECONDS
from warehouse_delivery.warehouses.snowflake import SnowflakeWarehouse, map_row

PRIVATE_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
PRIVATE_KEY_PEM = PRIVATE_KEY.private_bytes(
    serialization.Encoding.PEM,
    serialization.PrivateFormat.PKCS8,
    serialization.NoEncryption(),
).decode()
OTHER_PRIVATE_KEY_PEM = (
    rsa.generate_private_key(public_exponent=65537, key_size=2048)
    .private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    .decode()
)
CONFIG = {
    "account_identifier": "ACME-PROD",
    "warehouse": "FLAGSMITH_WH",
    "database": "flagsmith",
    "schema": "flagsmith_exp",
    "role": "FLAGSMITH_ROLE",
    "user": "flagsmith_service",
}
CREDENTIALS = {"private_key": PRIVATE_KEY_PEM}
WAREHOUSE = SnowflakeWarehouse(
    connection_id=7,
    account_identifier="ACME-PROD",
    database="flagsmith",
    schema="flagsmith_exp",
    user="flagsmith_service",
    private_key=PRIVATE_KEY_PEM,
)
GET_POOL_MANAGER = snowflake._get_pool_manager
ACCOUNT_HOST = "acme-prod.snowflakecomputing.com"
INGEST_HOST = "uw43389.ingest.cdgach.snowflakecomputing.com"
ROWS_URL = (
    f"https://{INGEST_HOST}/v2/streaming/data/databases/FLAGSMITH"
    "/schemas/FLAGSMITH_EXP/tables/EVENTS/rows"
)
ROWS = [
    b'{"environment_key":"env","event":"$flag_exposure","identifier":"u1",'
    b'"timestamp":1753000000000}',
    b'{"environment_key":"env","event":"purchase","identifier":"u2",'
    b'"timestamp":1753000000001,"value":9.99}',
]


def _connection(config: dict[str, Any], credentials: dict[str, Any]) -> Any:
    return WarehouseConnection(
        id=7, warehouse_type="snowflake", config=config, credentials=credentials
    )


def _request_name(url: str) -> str:
    path = urlsplit(url).path
    if path == "/v2/streaming/hostname":
        return "hostname"
    if path == "/oauth/token":
        return "token"
    return "rows"


def _requests(pool: Any, name: str) -> list[Any]:
    return [
        call
        for call in pool.request.call_args_list
        if _request_name(call.args[1]) == name
    ]


def _jwt_claims(authorization: str) -> dict[str, Any]:
    header, claims, signature = authorization.removeprefix("Bearer ").split(".")
    PRIVATE_KEY.public_key().verify(
        _base64url_decode(signature),
        f"{header}.{claims}".encode(),
        padding.PKCS1v15(),
        hashes.SHA256(),
    )
    assert json.loads(_base64url_decode(header)) == {"alg": "RS256", "typ": "JWT"}
    claims_json: dict[str, Any] = json.loads(_base64url_decode(claims))
    return claims_json


def _base64url_decode(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


@pytest.fixture(autouse=True)
def clear_caches() -> Iterator[None]:
    snowflake._load_private_key.cache_clear()
    snowflake._ingest_hosts.clear()
    snowflake._tokens.clear()
    yield
    snowflake._load_private_key.cache_clear()
    snowflake._ingest_hosts.clear()
    snowflake._tokens.clear()


@pytest.fixture()
def answers() -> dict[str, HTTPResponse | Exception]:
    return {
        "hostname": HTTPResponse(
            body=b"UW43389.ingest.cdgach.snowflakecomputing.com", status=200
        ),
        "token": HTTPResponse(body=b"scoped-token", status=200),
        "rows": HTTPResponse(
            body=b'{"status_code":0,"message":"","next_continuation_token":""}',
            status=200,
        ),
    }


@pytest.fixture(autouse=True)
def pool(mocker: MockerFixture, answers: dict[str, HTTPResponse | Exception]) -> Any:
    pool = mocker.patch(
        "warehouse_delivery.warehouses.snowflake._get_pool_manager"
    ).return_value

    def answer(method: str, url: str, **kwargs: Any) -> HTTPResponse:
        result = answers[_request_name(url)]
        if isinstance(result, Exception):
            raise result
        return result

    pool.request.side_effect = answer
    return pool


@pytest.mark.parametrize(
    "account_identifier, account_host, qualified_user",
    [
        pytest.param(
            "XMIPIIN-IF45918",
            "xmipiin-if45918.snowflakecomputing.com",
            "XMIPIIN-IF45918.FLAGSMITH_SERVICE",
            id="organisation-account",
        ),
        pytest.param(
            "acme-prod_eu",
            "acme-prod-eu.snowflakecomputing.com",
            "ACME-PROD_EU.FLAGSMITH_SERVICE",
            id="underscore-in-account-name",
        ),
    ],
)
def test_from_connection__valid_details__builds_the_account_urls(
    account_identifier: str,
    account_host: str,
    qualified_user: str,
) -> None:
    # Given
    config = {**CONFIG, "account_identifier": account_identifier}

    # When
    warehouse = SnowflakeWarehouse.from_connection(_connection(config, CREDENTIALS))

    # Then
    assert warehouse == dataclasses.replace(
        WAREHOUSE, account_identifier=account_identifier
    )
    assert warehouse.account_host == account_host
    assert warehouse.qualified_user == qualified_user
    assert warehouse.rows_path == (
        "/v2/streaming/data/databases/FLAGSMITH/schemas/FLAGSMITH_EXP"
        "/tables/EVENTS/rows"
    )
    assert "PRIVATE KEY" not in repr(warehouse)


@pytest.mark.parametrize(
    "config, credentials",
    [
        pytest.param(
            {k: v for k, v in CONFIG.items() if k != "user"},
            CREDENTIALS,
            id="missing-user",
        ),
        pytest.param({**CONFIG, "database": None}, CREDENTIALS, id="null-database"),
        pytest.param(CONFIG, {}, id="missing-private-key"),
        pytest.param(
            {**CONFIG, "account_identifier": "acme"}, CREDENTIALS, id="locator"
        ),
        pytest.param(
            {**CONFIG, "account_identifier": "acme-prod.eu-west-1"},
            CREDENTIALS,
            id="dotted-account",
        ),
        pytest.param(
            {**CONFIG, "account_identifier": "acme-prod/evil"},
            CREDENTIALS,
            id="account-with-path",
        ),
        pytest.param({**CONFIG, "schema": "a.b"}, CREDENTIALS, id="dotted-schema"),
        pytest.param({**CONFIG, "database": '"Flagsmith"'}, CREDENTIALS, id="quoted"),
        pytest.param({**CONFIG, "user": "1user"}, CREDENTIALS, id="leading-digit"),
        pytest.param(CONFIG, {"private_key": 42}, id="key-not-text"),
        pytest.param(CONFIG, {"private_key": "not a key"}, id="key-not-pem"),
        pytest.param(
            CONFIG,
            {
                "private_key": PRIVATE_KEY.private_bytes(
                    serialization.Encoding.PEM,
                    serialization.PrivateFormat.PKCS8,
                    serialization.BestAvailableEncryption(b"passphrase"),
                ).decode()
            },
            id="encrypted-key",
        ),
        pytest.param(
            CONFIG,
            {
                "private_key": ec.generate_private_key(ec.SECP256R1())
                .private_bytes(
                    serialization.Encoding.PEM,
                    serialization.PrivateFormat.PKCS8,
                    serialization.NoEncryption(),
                )
                .decode()
            },
            id="not-rsa",
        ),
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
        SnowflakeWarehouse.from_connection(connection)
    assert excinfo.value.kind == "stored_connection"
    assert excinfo.value.detail == "Stored connection details are incomplete."


def test_from_connection__unloadable_key__keeps_it_out_of_the_error() -> None:
    # Given
    private_key = PRIVATE_KEY_PEM.replace("MII", "secret-key-material", 1)

    # When
    with pytest.raises(DeliveryError) as excinfo:
        SnowflakeWarehouse.from_connection(
            _connection(CONFIG, {"private_key": private_key})
        )

    # Then
    assert "secret-key-material" not in "".join(
        traceback.format_exception(excinfo.value)
    )


@pytest.mark.parametrize(
    "event, expected",
    [
        pytest.param(
            {
                "environment_key": "env",
                "event": "purchase",
                "identifier": "u1",
                "timestamp": 1794825015123.456,
                "collected_at": 1794825016000,
                "feature_name": "checkout",
            },
            {
                "environment_key": "env",
                "event": "purchase",
                "identifier": "u1",
                "timestamp": "2026-11-16T10:30:15.123456+00:00",
                "collected_at": "2026-11-16T10:30:16.000000+00:00",
                "feature_name": "checkout",
            },
            id="milliseconds-to-utc-iso-8601",
        ),
        pytest.param(
            {"event": "e", "timestamp": "yesterday", "collected_at": 1794825016000},
            {
                "environment_key": "",
                "event": "e",
                "identifier": "",
                "timestamp": "2026-11-16T10:30:16.000000+00:00",
                "collected_at": "2026-11-16T10:30:16.000000+00:00",
            },
            id="timestamp-falls-back-to-collected-at",
        ),
        pytest.param(
            {
                "event": "e",
                "identifier": 42,
                "timestamp": -1,
                "value": 9.99,
                "traits": {"plan": "pro"},
                "feature_name": None,
                "ip": "1.2.3.4",
            },
            {
                "environment_key": "",
                "event": "e",
                "identifier": "42",
                "timestamp": "1969-12-31T23:59:59.999000+00:00",
                "value": "9.99",
                "traits": '{"plan":"pro"}',
            },
            id="non-string-values-become-json",
        ),
    ],
)
def test_map_row__event__projects_the_table_columns_as_one_ndjson_line(
    event: dict[str, Any],
    expected: dict[str, Any],
) -> None:
    # Given
    payload = json.dumps(event).encode()

    # When
    line = map_row(payload)

    # Then
    assert line is not None
    assert line.endswith("\n") and line.count("\n") == 1
    assert json.loads(line) == expected


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(b"{not json", id="unparseable"),
        pytest.param(b'{"event":"e","timestamp":true}', id="bool-timestamp"),
        pytest.param(b'{"event":"e","timestamp":1e30}', id="timestamp-out-of-range"),
    ],
)
def test_map_row__unusable_event__returns_none(payload: bytes) -> None:
    # Given / When / Then
    assert map_row(payload) is None


def test_insert__rows__discovers_the_host_exchanges_a_token_and_appends_ndjson(
    pool: Any,
    log: StructuredLogCapture,
) -> None:
    # Given / When
    written = WAREHOUSE.insert([*ROWS, b"{not json"])

    # Then
    assert written == 2
    assert [
        (call.args[0], _request_name(call.args[1]))
        for call in pool.request.call_args_list
    ] == [("GET", "hostname"), ("POST", "token"), ("POST", "rows")]
    for call in pool.request.call_args_list:
        assert call.kwargs["redirect"] is False
        assert call.kwargs["retries"] is False
        assert call.kwargs["headers"]["Accept"] == "application/json"
    (append,) = _requests(pool, "rows")
    url, query = append.args[1].split("?")
    assert url == ROWS_URL
    params = parse_qs(query)
    assert params["retryCount"] == ["0"]
    assert UUID(params["requestId"][0]).version == 4
    assert append.kwargs["headers"]["Authorization"] == "Bearer scoped-token"
    assert append.kwargs["headers"]["Content-Type"] == "application/x-ndjson"
    body = append.kwargs["body"].decode()
    assert body.endswith("\n")
    assert [json.loads(line)["identifier"] for line in body.splitlines()] == [
        "u1",
        "u2",
    ]
    assert log.events == [
        {
            "level": "warning",
            "event": "rows.skipped",
            "connection__id": 7,
            "rows__count": 1,
        }
    ]


@freeze_time("2026-11-16T10:30:00Z")
def test_insert__keypair_requests__send_a_signed_jwt_for_the_account_user(
    pool: Any,
) -> None:
    # Given
    public_key = PRIVATE_KEY.public_key().public_bytes(
        serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    fingerprint = base64.b64encode(hashlib.sha256(public_key).digest()).decode()

    # When
    WAREHOUSE.insert(ROWS)

    # Then
    (hostname,) = _requests(pool, "hostname")
    (token,) = _requests(pool, "token")
    assert hostname.args[1] == f"https://{ACCOUNT_HOST}/v2/streaming/hostname"
    assert token.args[1] == f"https://{ACCOUNT_HOST}/oauth/token"
    for call in (hostname, token):
        headers = call.kwargs["headers"]
        assert headers["X-Snowflake-Authorization-Token-Type"] == "KEYPAIR_JWT"
        assert _jwt_claims(headers["Authorization"]) == {
            "iss": f"ACME-PROD.FLAGSMITH_SERVICE.SHA256:{fingerprint}",
            "sub": "ACME-PROD.FLAGSMITH_SERVICE",
            "iat": 1794825000,
            "exp": 1794825300,
        }
    assert token.kwargs["headers"]["Content-Type"] == (
        "application/x-www-form-urlencoded"
    )
    assert parse_qs(token.kwargs["body"]) == {
        "grant_type": ["urn:ietf:params:oauth:grant-type:jwt-bearer"],
        "scope": [INGEST_HOST],
    }


def test_insert__records_over_the_payload_limit__appends_them_in_chunks(
    pool: Any,
    mocker: MockerFixture,
) -> None:
    # Given
    first, second = (map_row(row) for row in ROWS)
    assert first is not None and second is not None
    limit = len(first) + len(second)
    mocker.patch.object(snowflake, "MAX_PAYLOAD_BYTES", limit)
    third = ROWS[0].replace(b'"u1"', b'"u3"')
    oversized = json.dumps({**json.loads(ROWS[0]), "value": "x" * limit}).encode()

    # When
    written = WAREHOUSE.insert([*ROWS, oversized, third])

    # Then
    assert written == 3
    appends = _requests(pool, "rows")
    assert [
        [json.loads(line)["identifier"] for line in call.kwargs["body"].splitlines()]
        for call in appends
    ] == [["u1", "u2"], ["u3"]]
    assert (
        len({parse_qs(call.args[1].split("?")[1])["requestId"][0] for call in appends})
        == 2
    )


def test_insert__every_row_unusable__sends_no_request(pool: Any) -> None:
    # Given / When
    written = WAREHOUSE.insert([b"{not json"])

    # Then
    assert written == 0
    pool.request.assert_not_called()


@pytest.mark.parametrize(
    "second, hostname_requests, token_requests",
    [
        pytest.param(WAREHOUSE, 1, 1, id="same-connection"),
        pytest.param(
            dataclasses.replace(WAREHOUSE, schema="other"), 1, 1, id="other-table"
        ),
        pytest.param(
            dataclasses.replace(WAREHOUSE, private_key=OTHER_PRIVATE_KEY_PEM),
            1,
            2,
            id="rotated-key",
        ),
        pytest.param(
            dataclasses.replace(WAREHOUSE, user="other_user"), 1, 2, id="other-user"
        ),
        pytest.param(
            dataclasses.replace(WAREHOUSE, account_identifier="ACME-STAGING"),
            2,
            2,
            id="other-account",
        ),
    ],
)
def test_insert__second_insert__reuses_the_host_per_account_and_token_per_key(
    second: SnowflakeWarehouse,
    hostname_requests: int,
    token_requests: int,
    pool: Any,
) -> None:
    # Given / When
    WAREHOUSE.insert(ROWS)
    second.insert(ROWS)

    # Then
    assert len(_requests(pool, "hostname")) == hostname_requests
    assert len(_requests(pool, "token")) == token_requests
    assert len(_requests(pool, "rows")) == 2


def test_insert__token_near_expiry__exchanges_a_new_one(pool: Any) -> None:
    # Given
    with freeze_time() as frozen_time:
        WAREHOUSE.insert(ROWS)
        frozen_time.tick(3300)

        # When
        WAREHOUSE.insert(ROWS)

    # Then
    assert len(_requests(pool, "hostname")) == 1
    assert len(_requests(pool, "token")) == 2


def test_insert__append_unauthorised__exchanges_a_new_token_next_time(
    pool: Any,
    answers: dict[str, HTTPResponse | Exception],
) -> None:
    # Given
    answers["rows"] = HTTPResponse(body=b'{"code":"390303"}', status=401)
    with pytest.raises(DeliveryError):
        WAREHOUSE.insert(ROWS)

    # When
    with pytest.raises(DeliveryError):
        WAREHOUSE.insert(ROWS)

    # Then
    assert len(_requests(pool, "token")) == 2


@pytest.mark.parametrize(
    "rows_answer, hostname_requests",
    [
        pytest.param(
            NewConnectionError(None, "dns error"),  # type: ignore[arg-type]
            2,
            id="connection-failed",
        ),
        pytest.param(
            HTTPResponse(body=b"<html>not found</html>", status=404),
            2,
            id="unknown-path",
        ),
        pytest.param(
            HTTPResponse(
                body=b'{"code":"ERR_TABLE_DOES_NOT_EXIST_NOT_AUTHORIZED"}', status=404
            ),
            1,
            id="missing-table",
        ),
    ],
)
def test_insert__append_fails__looks_the_ingest_host_up_again_only_when_unreachable(
    rows_answer: HTTPResponse | Exception,
    hostname_requests: int,
    pool: Any,
    answers: dict[str, HTTPResponse | Exception],
) -> None:
    # Given
    answers["rows"] = rows_answer
    with pytest.raises(DeliveryError):
        WAREHOUSE.insert(ROWS)

    # When
    with pytest.raises(DeliveryError):
        WAREHOUSE.insert(ROWS)

    # Then
    assert len(_requests(pool, "hostname")) == hostname_requests


@pytest.mark.parametrize(
    "request_name, answer, kind, detail",
    [
        pytest.param(
            "hostname",
            HTTPResponse(body=b'{"code":"390144"}', status=401),
            "authentication",
            "Authentication failed.",
            id="hostname-bad-jwt",
        ),
        pytest.param(
            "token",
            HTTPResponse(body=b'{"code":"390144"}', status=401),
            "authentication",
            "Authentication failed.",
            id="token-bad-jwt",
        ),
        pytest.param(
            "rows",
            HTTPResponse(body=b'{"code":"390303"}', status=401),
            "authentication",
            "Authentication failed.",
            id="rows-bad-token",
        ),
        pytest.param(
            "hostname",
            HTTPResponse(
                body=b"<!DOCTYPE html><title>File not Found</title>", status=404
            ),
            "unreachable",
            "Could not connect to the host.",
            id="unknown-account",
        ),
        pytest.param(
            "rows",
            HTTPResponse(
                body=b'{"code":"ERR_TABLE_DOES_NOT_EXIST_NOT_AUTHORIZED","message":""}',
                status=404,
            ),
            "missing_table",
            "Events table not found in the configured database. "
            "Run the setup SQL to create it.",
            id="missing-table",
        ),
        pytest.param(
            "rows",
            HTTPResponse(
                body=b'{"code":"ERR_SCHEMA_DOES_NOT_EXIST_OR_NOT_AUTHORIZED"}',
                status=404,
            ),
            "missing_table",
            "Events table not found in the configured database. "
            "Run the setup SQL to create it.",
            id="missing-schema",
        ),
        pytest.param(
            "rows",
            HTTPResponse(
                body=b'{"code":"ERR_DATABASE_DOES_NOT_EXIST_OR_NOT_AUTHORIZED"}',
                status=404,
            ),
            "missing_database",
            "Database does not exist.",
            id="missing-database",
        ),
        pytest.param(
            "rows",
            HTTPResponse(body=b"{}", status=403),
            "permission_denied",
            "Permission denied on the events table.",
            id="forbidden",
        ),
        pytest.param(
            "rows",
            HTTPResponse(body=b"slow down", status=429),
            "rejected",
            "The Snowflake account rejected the request.",
            id="throttled",
        ),
        pytest.param(
            "token",
            HTTPResponse(body=b"upstream error", status=503),
            "rejected",
            "The Snowflake account rejected the request.",
            id="server-error",
        ),
        pytest.param(
            "hostname",
            HTTPResponse(body=b"<html>moved</html>", status=302),
            "rejected",
            "The Snowflake account rejected the request.",
            id="redirect",
        ),
        pytest.param(
            "token",
            HTTPResponse(body=b'{"token": "x"}', status=200),
            "rejected",
            "The Snowflake account rejected the request.",
            id="unexpected-token-body",
        ),
        pytest.param(
            "hostname",
            HTTPResponse(body=b"ingest.attacker.example", status=200),
            "unreachable",
            "Could not connect to the host.",
            id="foreign-ingest-host",
        ),
        pytest.param(
            "hostname",
            HTTPResponse(body=b"evil.example/x.snowflakecomputing.com", status=200),
            "unreachable",
            "Could not connect to the host.",
            id="ingest-host-with-path",
        ),
        pytest.param(
            "hostname",
            NewConnectionError(None, "dns error"),  # type: ignore[arg-type]
            "unreachable",
            "Could not connect to the host.",
            id="connection-failed",
        ),
        pytest.param(
            "token",
            ConnectTimeoutError(None, "connect timed out"),
            "unreachable",
            "Could not connect to the host.",
            id="connect-timeout",
        ),
        pytest.param(
            "rows",
            ReadTimeoutError(None, "", "read timed out"),  # type: ignore[arg-type]
            "timeout",
            "The query timed out.",
            id="read-timeout",
        ),
    ],
)
def test_insert__snowflake_refuses__raises_delivery_error_and_stops(
    request_name: str,
    answer: HTTPResponse | Exception,
    kind: str,
    detail: str,
    pool: Any,
    answers: dict[str, HTTPResponse | Exception],
) -> None:
    # Given
    answers[request_name] = answer

    # When / Then
    with pytest.raises(DeliveryError) as excinfo:
        WAREHOUSE.insert(ROWS)
    assert (excinfo.value.kind, excinfo.value.detail) == (kind, detail)
    assert _request_name(pool.request.call_args.args[1]) == request_name


def test_insert__snowflake_refuses__logs_status_and_snowflake_code_only(
    answers: dict[str, HTTPResponse | Exception],
    log: StructuredLogCapture,
) -> None:
    # Given
    answers["rows"] = HTTPResponse(
        body=b'{"code":"ERR_TABLE_DOES_NOT_EXIST_NOT_AUTHORIZED","message":"EVENTS"}',
        status=404,
    )

    # When
    with pytest.raises(DeliveryError):
        WAREHOUSE.insert(ROWS)

    # Then
    assert log.events == [
        {
            "level": "warning",
            "event": "append.refused",
            "connection__id": 7,
            "http__status": 404,
            "snowflake__code": "ERR_TABLE_DOES_NOT_EXIST_NOT_AUTHORIZED",
        }
    ]


def test_insert__appends_outlast_their_budget__raises_timeout_before_the_next_chunk(
    pool: Any,
    answers: dict[str, HTTPResponse | Exception],
    mocker: MockerFixture,
) -> None:
    # Given
    mocker.patch.object(snowflake, "MAX_PAYLOAD_BYTES", 200)
    with freeze_time() as frozen_time:
        rows_answer = answers["rows"]

        def slow_append(method: str, url: str, **kwargs: Any) -> Any:
            if _request_name(url) != "rows":
                return answers[_request_name(url)]
            frozen_time.tick(snowflake.APPEND_TIMEOUT_SECONDS)
            return rows_answer

        pool.request.side_effect = slow_append

        # When
        with pytest.raises(DeliveryError) as excinfo:
            WAREHOUSE.insert(ROWS)

    # Then
    assert (excinfo.value.kind, excinfo.value.detail) == (
        "timeout",
        "The query timed out.",
    )
    (append,) = _requests(pool, "rows")
    assert append.kwargs["timeout"].total == snowflake.APPEND_TIMEOUT_SECONDS


def test_insert__our_own_failure__propagates_unwrapped(
    answers: dict[str, HTTPResponse | Exception],
) -> None:
    # Given
    answers["rows"] = RuntimeError("boom")

    # When / Then
    with pytest.raises(RuntimeError, match="boom"):
        WAREHOUSE.insert(ROWS)


@freeze_time()
def test_timeouts__worst_case__fits_inside_the_insert_timeout(pool: Any) -> None:
    # Given
    WAREHOUSE.insert(ROWS)

    # When
    worst_case_seconds = sum(
        call.kwargs["timeout"].total for call in pool.request.call_args_list
    )

    # Then
    assert worst_case_seconds == (
        snowflake.HOSTNAME_TIMEOUT_SECONDS
        + snowflake.TOKEN_TIMEOUT_SECONDS
        + snowflake.APPEND_TIMEOUT_SECONDS
    )
    assert worst_case_seconds < INSERT_TIMEOUT_SECONDS


def test_get_pool_manager__called_twice__returns_one_pool() -> None:
    # Given / When
    first, second = GET_POOL_MANAGER(), GET_POOL_MANAGER()

    # Then
    assert isinstance(first, PoolManager)
    assert first is second
