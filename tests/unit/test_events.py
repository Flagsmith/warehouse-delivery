import pytest

from warehouse_delivery.events import Event


@pytest.mark.parametrize(
    "event, expected",
    [
        pytest.param(
            Event(key="acme", payload=b'{"environment_key":"other"}'),
            "acme",
            id="message-key-wins",
        ),
        pytest.param(
            Event(key=None, payload=b'{"environment_key":"acme"}'),
            "acme",
            id="payload-fallback",
        ),
        pytest.param(
            Event(key="", payload=b'{"environment_key":"acme"}'),
            "acme",
            id="empty-key-falls-back",
        ),
        pytest.param(
            Event(key=None, payload=b'{"environment_key":42}'),
            None,
            id="payload-key-not-a-string",
        ),
        pytest.param(
            Event(key=None, payload=b'{"environment_key":""}'),
            None,
            id="payload-key-empty",
        ),
        pytest.param(Event(key=None, payload=b"not json"), None, id="not-json"),
        pytest.param(Event(key=None, payload=b"[1, 2]"), None, id="not-an-object"),
        pytest.param(Event(key=None, payload=b""), None, id="empty-payload"),
    ],
)
def test_event_environment_key__key_and_payload_variants__resolves_or_none(
    event: Event,
    expected: str | None,
) -> None:
    # Given / When / Then
    assert event.environment_key == expected
