# Orion Delivery Verification Runbook

Pre-DE gateway. Proves Subscription delivery to a **temporary** receiver.

## Anti-false-pass rules (mandatory)

1. **Subscription create = HTTP 201 only**  
   Fail on 200/204/409. Check `Location` or known `id`, then **GET** subscription → 200.

2. **Delivery success ≠ local 204 alone**  
   Require: (a) receiver recorded ≥1 POST body; (b) GET subscription shows `notification.status` is **not** `failed` (expect `ok` when Orion exposes it).

3. **Subscription ID hygiene**  
   Use `urn:ngsi-ld:Subscription:verify-<uuid>` per run, **or** `--delete-first` with a fixed id. Never leave stale subscriptions that can fire into a new run.

## Prerequisites

- Docker Desktop running
- `docker compose up -d mongo-db orion` (`fiware/orion-ld:1.7.1`)
- SUMO / TraCI available for Visualize short run
- Python deps: `requests`, `jsonschema`, `pytest`

## Environment

| Variable | Default | Meaning |
|----------|---------|---------|
| `ORION_URL` | `http://localhost:1026` | Context Broker |
| `ORION_NOTIFY_HOST` | `host.docker.internal` | Host Orion container uses to reach receiver |
| `ORION_NOTIFY_URL` | (derived) | Full override of notify URI |
| `VERIFY_PUBLISH_CYCLES` | `3` | Minimum publish cycles |
| `VERIFY_MAX_SIM_SEC` | `30` | Cap simulation time for short run |

## Commands

```bash
# Terminal A (optional standalone receiver)
python integration/receiver/app.py --port 18080

# Terminal B — full gate (starts in-process receiver)
pytest integration/test_orion_delivery_verification.py -v

# Manual subscription create (unique id)
python integration/scripts/register_subscription.py \
  --notify-uri http://host.docker.internal:18080/webhook/ngsi
```

## PASS criteria

See plan acceptance checklist: multi-tick Orion updates, GET attribute delta, 201+GET sub, receiver recorded, Orion status not failed, captured file validates against `contracts/delivery/notification.schema.json`.
