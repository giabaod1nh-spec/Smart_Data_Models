# Orion Subscription Delivery Verification (pre-DE gateway)

Temporary gate proving:

```text
SUMO → Publisher → Orion-LD → Subscription → Temporary Receiver
```

**Not** a Data Engineering milestone (no Bronze/Silver/Gold/ClickHouse/Kafka).

## Layout

| Path | Role |
|------|------|
| [`receiver/`](receiver/) | Temporary 204 receiver (log + save raw only) |
| [`scripts/register_subscription.py`](scripts/register_subscription.py) | Create sub with **201** + GET verify |
| [`test_orion_delivery_verification.py`](test_orion_delivery_verification.py) | Full harness (anti-false-pass) |
| [`captured/`](captured/) | Live evidence JSON |

Contract SoT (untouched): [`../contracts/delivery/`](../contracts/delivery/)

Implementation runbook: [`../docs/implementation/orion_delivery_verification.md`](../docs/implementation/orion_delivery_verification.md)

## Quick start

```bash
docker compose up -d mongo-db orion
pytest integration/test_orion_delivery_verification.py -v
```

Orion down ⇒ test **FAILS** (no skip).
