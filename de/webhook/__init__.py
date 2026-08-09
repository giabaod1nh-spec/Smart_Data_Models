"""DE-1 webhook package — Orion notification raw ingestion.

LEGACY: Raw v1 Orion Subscription → HTTP → ClickHouse ``raw_ngsi_notifications``.
Retirement: cutover gate K-6b; package removal K-8.
Not part of the final Kafka → Raw v2 historical backbone.
"""

LEGACY = True
RETIREMENT_PHASE = "K-8"
CUTOVER_GATE = "K-6b"
LEGACY_CONTRACT = "Orion Notification Delivery 1.0.0"
