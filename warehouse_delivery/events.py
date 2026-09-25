import json
from collections.abc import Sequence
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Event:
    """One experiment event as it arrived from Kafka. Topic, partition, offset
    and timestamp are left behind because nothing here reads them."""

    key: str | None
    payload: bytes
    headers: dict[str, bytes | None] = field(default_factory=dict)

    @property
    def environment_key(self) -> str | None:
        """Which environment this message belongs to.

        The ingestion server will set the environment's key as the Kafka
        message key. Until it does, and for messages produced before then, the
        same value is read from the environment_key field of the JSON payload.
        None when neither is present."""
        if self.key:
            return self.key
        try:
            key = json.loads(self.payload).get("environment_key")
        except (ValueError, AttributeError):
            return None
        return key if isinstance(key, str) and key else None


def group_by_environment(
    events: Sequence[Event],
) -> tuple[dict[str, list[Event]], int]:
    """Splits a batch into one list per environment, keeping each environment's
    messages in the order they arrived. Also counts the messages that name no
    environment at all, so the caller can log how many were thrown away."""
    groups: dict[str, list[Event]] = {}
    unattributable = 0
    for event in events:
        environment_key = event.environment_key
        if environment_key is None:
            unattributable += 1
            continue
        groups.setdefault(environment_key, []).append(event)
    return groups, unattributable
