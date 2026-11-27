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
