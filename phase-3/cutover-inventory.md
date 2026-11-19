# V2 cutover inventory, 2026-11-19

The V2 Outbox source was `Visualize/artifacts/kafka_outbox/outbox.sqlite3`. Its verified backup is `Visualize/artifacts/kafka_outbox/archives/outbox-v2-2026-11-19.sqlite3`. The backup has 93,247 ACKED rows, zero pending rows, and passed `PRAGMA integrity_check`. The exact SHA-256 is recorded in `cutover-inventory.json`.

Before shutdown, Kafka consumer offsets on `traffic.entity-events.v2` were:

- `de-kafka-raw-v2`: partitions 0/1/2 at 51602/20733/20912, all lag zero.
- `projector-k5-production`: partitions 0/1/2 at 51602/20723/20902; lag 0/10/10.

The stopped V2 services are `orion-projector`, `de-kafka-raw-consumer`, `de-bronze-processor`, `de-silver-processor`, and `de-gold-runtime`. The Kafka broker and historical V2 topics were retained. The local V1 topic is `traffic.observations.v1`; the V1 Outbox is a separate `v1-outbox.sqlite3` file.
