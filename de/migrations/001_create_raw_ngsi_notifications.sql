CREATE DATABASE IF NOT EXISTS smart_traffic;

CREATE TABLE IF NOT EXISTS smart_traffic.raw_ngsi_notifications
(
    ingestion_id       UUID,
    notification_id    String,
    subscription_id    String,
    payload_hash       String,
    contract_version   String,
    source_type        LowCardinality(String) DEFAULT 'ORION_NOTIFICATION',
    received_at        DateTime64(3, 'UTC'),
    notified_at        Nullable(DateTime64(3, 'UTC')),
    entity_count       UInt16,
    payload_size_bytes UInt32,
    payload_raw        String CODEC(ZSTD(3)),
    ingestion_status   LowCardinality(String) DEFAULT 'STORED',
    source_ip          String DEFAULT '',
    request_id         String DEFAULT ''
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(received_at)
ORDER BY (received_at, ingestion_id);
