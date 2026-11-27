# E-03 — Isolated sizing và durability I/O envelope

Python 3.14, SQLite 3.50.4, disposable local DB, no services, sequential. Preflight 2.5 GB memory và 500 MB free scratch. CPU ngay dưới timer resolution ghi 0, không suy ra zero cost. Workload synthetic 5 intersections với 3/3/4/3/4 approaches, cadence 1 Hz, 60/300 slots. Mỗi direction sample [approach index, V=10,H=2,speed=36 km/h,q=(slot+index)%20,occupancy=12.5]. Projection sorted by slot/intersection, compact deterministic JSON. Oracle: count 10n/2n, weighted 288n, queue P95 18 m cho cả hai n do 20-value cycle, rollup cộng counts, 23 rows (17+5+1). SHA-256 giữ nguyên khi reserialize/reopen. Scratch transaction ghi cả hai BLOB và result hash, 20 lần mỗi size, WAL synchronous FULL, autocheckpoint off để đo burst, TRUNCATE checkpoint rồi reopen/read/hash.

| Measurement | 60s | 300s |
|---|---:|---:|
| Intersection slots / direction inputs | 300 / 1,020 | 1,500 / 5,100 |
| Projection bytes / bytes mỗi intersection slot | 22,541 / 75.14 | 113,901 / 75.93 |
| Compact computed rows / bytes | 23 / 833 | 23 / 904 |
| Peak traced Python heap (bao gồm codec) | 244,268 B | 1,244,492 B |
| CPU projection / serialize+hash / compute | <resolution / 15.6ms / <resolution | 15.6ms / 46.9ms / <resolution |
| SQLite median / max commit | 0.610 / 1.007ms | 1.134 / 2.466ms |
| DB+WAL bytes trước checkpoint | 4,096 + 667,472 | 4,096 + 2,492,632 |
| DB bytes sau checkpoint | 499,712 | 2,314,240 |
| Checkpoint / reopen+read+verify | 2.94 / 3.98ms | 9.69 / 3.75ms |

Result hashes 60: 1d5ed6d81d333624d379320af8c2b20915c8c9633b1b276eb38e5ce844029b3a. 300: ec1463de1682f7178d877d352314050a7ab95d5141fabdb53cb172505828274d. Reproduce with recipe above and temporary phase4b-sizing-diagnostic-0ccf206.py. These are measured fixture costs, no concurrent writer/reader, no full row manifest/lineage/strings/source receipt, no ClickHouse network, no process RSS. Checkpoint burst ≠ steady-state WAL growth. One transaction with two payloads measures storage floor only; deployment has separate seal, compute/result and publish transactions.

Budget model: reserve `pending_count × bounded(projection + result + manifest + metadata)` plus already durable rows, WAL/checkpoint headroom, Core free-space reserve and recovery grace. In-flight RSS includes `Core page + reducer + codec coexistence + compute projection/result + publish batch/decode + replay`; bound each term and total live/replay concurrency. Cap aggregate pending bytes separately from count, test conflict/malicious cardinality guard at hard member limit without retaining duplicates. At 1×, 1,440×22,541 + 288×113,901 = 65,262,528 projection bytes/day at this constant fixture shape, before cleanup and revisions. Per-day result payload floor ≈1,440×833 + 288×904 = 1,459,872 B. No retention/day assumptions for official workload. Clean up published projection only after recovery/replay constraints; retention and max in-flight budgets need measurements first.

CH expected 23 logical rows per result in fixture. Estimate write/read-back rows proportional to attempted revisions and reconciliation. Manifest+keys add bytes beyond fixture payload. Partial retry exact read examined set plus inserts for missing keys, never rerun compute for same payload. Batch cap count and bytes, measure CH per-grain writes and query/read-back separately. Replay multiplies window work only for admitted scope and follows live reserve, never size from raw source step count alone.
## Provisional isolated profile ISOLATED_FIXTURE_V1

These limits are test inputs, not production thresholds. Validate on startup: relationships between caps, memory reserve, WAL, max pending bytes and shared fixture process count. Override only in an explicitly approved isolated profile, record effective config/digest with evidence. Reject malformed/unbounded configs. Normal/upper-limit/fault workload must log expected result and fail closed. Production profile and numeric limits require later approval.

| Guard | Fixture default and rationale |
|---|---|
| Population and work | 5 participants, 17 approaches, 300 ordinals, ≤1,500 intersection slot claims, ≤5,100 direction slots. Max 3,000 verified members including duplicate/conflict stress. Beyond max hold before claim when count known or abort bounded stream. |
| Source and projection | Existing Core public feed entry limits unchanged. Additionally limit one Business candidate to 64 MiB cumulative encoded receipt bytes and 128 MiB cumulative decoded projection inputs, check per-member bounded decode before expansion; these are synthetic caps, validate with fixture shape. Max canonical projection 256 KiB (over 2× E-03 113,901 B). |
| Result and manifest | 32 KiB canonical result, 32 KiB manifest, up to 23 expected row keys in default fixture. Per-publish write batch ≤23 rows and ≤64 KiB including envelope. No hidden truncation. |
| Pending and in flight | Max 16 obligations counting claims/held durable payloads, max 8 MiB pending/reserved payload bytes and metadata charged separately. One Core reducer, one compute and one publication job; one bounded reader request in measurement. Per-lane budget 1 dispatch or completion transition per Core tick, no network waits on coordinator; check CPU/RSS and deadlines at work quantum. |
| Storage and WAL | Scratch only, 512 MiB preflight free disk at minimum, reserve at least 32 MiB WAL/checkpoint headroom and 64 MiB protected Core disk within approved scratch budget. Check DB+WAL/free before admission and writer activity; attempt checkpoint in bounded maintenance lane; stop intake before protected reserve breach. |
| Retry, wall budget | Max 4 attempts per phase, exponential bounded 0.25/0.5/1s fixture delay, 2s per simulated CH call and 60s per isolated fixture run unless owner sets explicit different bounded scenario. Deterministic fixture clock, production jitter/budgets need approval. |
| Retention | No timed deletion of pending, blocked or required recovery state. For tests cleanup terminal/orphan only after explicit fence/reference check and grace/cancellation, use exact IDs and run-level scratch teardown. Live retention windows need independent approval. |

Record high-water member/slot counts, cumulative encoded/decoded bytes, payload+manifest bytes, reserved and actual storage, pending bytes/count and oldest age, reservation denials, per-lane quanta, reopen/recovery cost, WAL and process RSS. At cap test same-stamp denial → no requalification loop, credit release → admitted drain, restart stale lease → recoverable work, and eligible work behind an inadmissible head. Integrated acceptance measures from Outbox/Kafka lag through Raw/Bronze/Silver and Core/Business to verified freshness. Use same fidelity/source configuration for comparisons, both durations, Realtime and bounded drain, no numeric throughput SLA here.
