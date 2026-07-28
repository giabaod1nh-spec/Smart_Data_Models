"""Domain exceptions for DE-1 webhook."""


class EnvelopeValidationError(ValueError):
    """Notification envelope failed minimal DE-1 validation."""


class ClickHouseUnavailableError(Exception):
    """ClickHouse connection or server unreachable."""


class ClickHouseTimeoutError(Exception):
    """ClickHouse operation exceeded configured timeout."""
