# Temporary Notification Receiver (pre-DE)

**Purpose:** Verify Orion-LD Subscription delivery only.

| Allowed | Forbidden |
|---------|-----------|
| Receive HTTP POST | Parse / transform NGSI |
| Log requests | Database inserts |
| Save raw JSON under `integration/captured/` | ETL / Bronze / Analytics |
| Return **204** | Acting as production DE webhook |

## Run standalone

```bash
python integration/receiver/app.py --port 18080
```

Endpoint: `POST /webhook/ngsi` → 204

This component is **temporary infrastructure** for the Orion delivery verification gate.
It is **not** part of the Data Engineering pipeline.
