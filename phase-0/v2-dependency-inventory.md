# V2 dependency inventory

```text
TraCI entity producer → SQLite Outbox → traffic.entity-events.v2 (3 partitions)
  ├→ Orion Projector [projector-k5-production] → Orion-LD → Spring realtime/API → dashboard
  └→ DE Raw [de-kafka-raw-v2] → ClickHouse Raw → Bronze → Silver → Gold → analytics/API
```

The two bracketed groups were observed in the running broker. The downstream ClickHouse workers read prior tables/checkpoints, not the Kafka topic directly in the inspected runtime. Dashboard/API dependency is through Orion/analytics outputs; no direct browser Kafka consumer was identified.

| Component | Topic or input | Contract dependency | Impact of removing V2 |
|---|---|---|---|
| `Visualize/tools/projector_live_consumer.py` | `traffic.entity-events.v2`, group `projector-k5-production` | Accepts `TrafficEntityObserved` and `TrafficSimulationRunStarted`; one event carries one entity | Must migrate to v1 or be retired at cutover. |
| `de/kafka_raw/main.py` / `de/kafka_raw/validator.py` | Same topic, group `de-kafka-raw-v2` | Validates v2 JSON schemas; stores Raw v2 or quarantine | New v1 events cannot enter the same historical path unchanged. Needs a v1 ingestion design before v2 producer removal. |
| `de/bronze` | Raw v2 and quarantine tables | Parses and normalizes v2 payload/metadata | Needs downstream mapping from v1 Raw; no direct Kafka subscription. |
| `de/silver` | Bronze tables and topic allowlist | Allowlist includes `traffic.entity-events.v2` | Must change lineage/allowlist and metric mapping if v1 replaces v2. |
| `de/gold_runtime` | Silver tables | Derived KPI semantics | May need versioned metric/lineage review after Silver changes. |
| Spring and dashboard | Orion current entities and analytics APIs | Indirectly depend on stable entity IDs and data freshness | Preserve current entity model or coordinate API changes. |

Source evidence: `de/kafka_raw/config.py` defaults to v2 topic/group and v2 schemas; `de/silver/config.py` has a v2 topic allowlist; `docs/shared/PHASE1_BOUNDARIES.md` describes the current Raw → Bronze → Silver → Gold chain. Full v2 producer retirement affects analytics even if the Orion Projector migration succeeds. Phase 1 must assign an owner and target contract to this path; “ClickHouse optimization out of scope” does not remove this migration dependency.

No other active broker consumer groups were observed at capture time. Archived Compose files, test harnesses and repair tools reference v2, but are not evidence of active production consumers. Their rollback/maintenance disposition remains a cutover decision.
