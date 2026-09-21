class DeliveryError(Exception):
    """A customer's events could not be inserted for a reason on the customer's
    side: their warehouse is unreachable, refused our login, has no events
    table, rejected the rows, or the connection details the API stored are
    unusable. Failures on our side, such as Redis being down or a bug, are
    never wrapped in this.

    ``kind`` is a short label for the logs, such as ``authentication``.
    ``detail`` is the sentence the customer sees against their connection in
    the dashboard, so it must not contain hostnames or raw error text from the
    driver. ``connection_id`` is set when the raiser knows which connection
    failed but the caller has no ``WarehouseConnection`` in hand, as when the
    stored credentials will not decrypt, so the dashboard can still be
    updated."""

    def __init__(
        self, kind: str, detail: str, *, connection_id: int | None = None
    ) -> None:
        super().__init__(detail)
        self.kind = kind
        self.detail = detail
        self.connection_id = connection_id
