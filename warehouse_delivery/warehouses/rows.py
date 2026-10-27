import json
from dataclasses import dataclass
from typing import Any

# Range of a Delta TIMESTAMP, 0001-01-01 to 9999-12-31, in microseconds.
MIN_TIMESTAMP_MICROS = -62_135_596_800_000_000
MAX_TIMESTAMP_MICROS = 253_402_300_799_999_999

NOT_NULL_COLUMNS = ("environment_key", "event", "identifier")
NULLABLE_COLUMNS = (
    "feature_name",
    "value",
    "traits",
    "metadata",
    "sdk_language",
    "sdk_version",
)


@dataclass(frozen=True)
class EventRow:
    """An event's columns as the warehouses that take typed rows store them:
    timestamps in microseconds, everything else as text."""

    timestamp: int
    collected_at: int | None
    not_null: dict[str, str]
    nullable: dict[str, str | None]


def parse_event(payload: bytes) -> EventRow | None:
    """Returns None for an event that cannot be made valid: unparseable, not an
    object, or without a usable timestamp or collected_at."""
    try:
        event = json.loads(payload)
    except ValueError:
        return None
    if not isinstance(event, dict):
        return None
    timestamp = _epoch_millis_to_micros(event.get("timestamp"))
    collected_at = _epoch_millis_to_micros(event.get("collected_at"))
    if timestamp is None:
        timestamp = collected_at
    if timestamp is None:
        return None
    return EventRow(
        timestamp=timestamp,
        collected_at=collected_at,
        not_null={
            column: _as_column_text(event.get(column)) or ""
            for column in NOT_NULL_COLUMNS
        },
        nullable={
            column: _as_column_text(event.get(column)) for column in NULLABLE_COLUMNS
        },
    )


def _epoch_millis_to_micros(milliseconds: Any) -> int | None:
    if isinstance(milliseconds, bool) or not isinstance(milliseconds, int | float):
        return None
    try:
        micros = int(milliseconds * 1000)
    except (OverflowError, ValueError):
        # NaN, infinity, or a float that overflows once scaled.
        return None
    if not MIN_TIMESTAMP_MICROS <= micros <= MAX_TIMESTAMP_MICROS:
        return None
    return micros


def _as_column_text(value: Any) -> str | None:
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, separators=(",", ":"))
