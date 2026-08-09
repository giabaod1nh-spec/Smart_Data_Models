# Compatibility — Kafka Event Delivery Contract 2.0.0

| Change | Rule |
|--------|------|
| Add **optional** field | Backward-compatible |
| Add **required** field | Breaking |
| Change type / rename / remove field | Breaking |
| Unknown additive field | Consumers **MUST accept** (ignore or store) |
| Unsupported `contractVersion` | Quarantine |
| Major breaking change | New schema version and/or **new topic** |
| Rollout | Prefer consumer ready before producer for additive changes |
| Schema Registry | Future enhancement; đồ án uses JSON Schema in-repo |

## Named contracts

- **NGSI-LD Entity Contract 1.0.0** — Orion entity payloads / Server
- **Kafka Event Delivery Contract 2.0.0** — this Kafka envelope
- **Legacy Orion Notification Delivery Contract 1.0.0** — webhook Notification

Do not refer to these generically as “Contract v2”.

## Canonical JSON (locked)

Shared module: [`contracts/canonical_json.py`](../canonical_json.py)

```text
json.dumps(obj, sort_keys=True, ensure_ascii=True, separators=(",", ":"))
→ UTF-8 → SHA-256 hex (64)
```

- Sort object keys
- Preserve array order
- Compact separators; no extra whitespace

`entityPayloadHash` hashes the **inner entity only**.
