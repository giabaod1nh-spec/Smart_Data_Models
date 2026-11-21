# Phase 3 proposed acceptance matrix

Status: planning artifact only. No result below is claimed PASS. Every executed case must store source identity and machine-readable evidence under `de/artifacts/silver_v1/acceptance/<run-id>/`. Common evidence includes JUnit XML, frozen run manifest/catalog hashes, source Bronze snapshot/high-waters, Silver cursor/obligation snapshot, ClickHouse schema and row content export, health/resource sample, and command/image IDs.

Independent oracles are separate from production builders: (1) cardinality from archived approach map and observed valid event set; (2) values from original canonical Bronze envelopes; (3) availability/presence from original event JSON; (4) lineage from Bronze delivery intents; (5) invalidation from Bronze current identity + invalidation journal. A row count alone never passes a case.

| ID | Case / fixture or workload | Execution | Expected results and required assertions | Evidence |
|---|---|---|---|---|
| A01 | Full five-node 120-cycle run; manifest participants A/B/C/D/M; directions 3/3/4/3/4 | Actual SUMO→Outbox→Kafka→Raw→Bronze→Silver runtime | From verified events: 600 intersection, 2,040 traffic, 2,040 signal logical facts, 4,680 total; each identity/field/lineage matches source oracle; counts are derived, not coded constants | run report, source/fact identity diff, archive inventory, health/time series |
| A02 | Subset participant run | Deterministic archived fixture + worker | Only declared participating intersections; no absent-node dimensions/facts; exact direction counts | source-to-fact oracle |
| A03 | Intersection with fewer than four declared directions | 3-approach fixture | Exactly three traffic and three signal rows for event; no N/E/S/W filler | identity query and independent approach set |
| A04 | Multiple run generations/topology digests | Two archived catalogs for same network/run IDs as permitted by fixture | Dimensions/bindings keyed by generation/digest; no cross-version merge | dimension key export |
| A05 | Topology digest mismatch | Mutated archived fixture, never active config | No analytically valid facts; integrity status and no cursor pass | archive error and zero-valid query |
| A06 | Missing expected direction | Valid event altered versus catalog | Reject event facts as valid; exact missing direction recorded | payload/catalog diff, quarantine/integrity record |
| A07 | Unexpected direction | Event altered versus catalog | Reject all event output as valid; no widened topology dimension | payload/catalog diff |
| A08 | Missing/malformed manifest or catalog | Offline malformed fixtures | Missing/transient read error retries without cursor; malformed/identity mismatch permanent integrity failure; no current topology fallback | retry and fault state |
| A09 | Temporarily unavailable archive | Permission/rename/open fault injection restored in finally | No checkpoint advance while unavailable; worker recovers after restore | cursor before/during/after and health |
| A10 | Measured zero | Contract valid event with numeric zero for representative required metrics | Zero persists as numeric zero; availability is measured/absent; no default flag | source/value oracle |
| A11 | `null + UNAVAILABLE` | `unavailable_synthetic.json` / normalized valid fixture | NULL plus UNAVAILABLE retained for each metric; never zero | field-level round trip |
| A12 | `null + NOT_APPLICABLE` | synthetic contract-valid fixture | NULL plus NOT_APPLICABLE retained, distinguishable from unavailable | field-level round trip |
| A13 | Absent optional metrics | Remove optional keys from contract-valid fixture | Key absence retained in payload JSON/presence representation; typed projection NULL; never zero/default/prior | JSON key-presence diff |
| A14 | Partial intersection totals | Source with one or more unavailable constituent values | Total NULL and PARTIAL_TOTAL/quality evidence; no partial sum published as complete | independent total oracle |
| A15 | Optional top-level state absent | Omit optional timing/spillback/context values | NULL/absent as contract declares; required fields remain exact | field-level source diff |
| A16 | Direction-specific availability | Two directions with different status on same metric | Each status/value independently preserved; no intersection-wide overwrite | per-direction oracle |
| A17 | Scenario changes during run | Events before/after node scenario change | Per-event scenario preserved; run profile remains separate; no dimension overwrite | timeline comparison |
| A18 | Duplicate semantic observation / physical deliveries | Bronze duplicate dispositions fixture/runtime | One logical fact set; all source delivery lineage retained; no duplicate logical identity | source intents and deduped fact reconciliation |
| A19 | Late/out-of-order cycles | Reordered accepted event changes | All valid cycles retained, sorted by event coordinates in output query; cursor follows journal sequence, not cycle sequence | source/event set comparison |
| A20 | Conflict before Silver consumption | Bronze state already CONFLICTED before bootstrap/change read | No valid facts emitted; conflict/invalidation accounted; cursor advances only when terminal state obligations durable | current authority and output diff |
| A21 | Conflict after Silver materialization | Accept E, materialize, then submit changed same-ID delivery to Bronze | Silver local validity changes; all direction and intersection facts for E absent from canonical Gold-facing reader; CH tombstone has both raw IDs/hashes | before/after canonical reader, invalidation provenance |
| A22 | Invalidation mirror pending | Fail CH insert after durable local suppression | Canonical reader suppresses E immediately; invalidation backlog/readiness reports pending; retry content reconciles without duplicate | query during failure and after recovery |
| A23 | Crash/restart with pending obligation | Terminate at each frozen intent boundary | Startup recovers exact bytes/rows; no checkpoint skip, duplicate logical facts, or validity restoration | process exit, SQLite state, CH diff |
| A24 | Partial ClickHouse persistence | Inject after dimensions/facts/tombstone subsets | Reconciliation completes missing frozen rows, detects changed content as integrity error | target-by-target expected/actual report |
| A25 | Lost insert response | CH write succeeds, client response dropped | Retry discovers exact content and completes; no false failure or duplicate logical result | fault trace and reconciliation report |
| A26 | Retry/checkpoint consistency and CAS race | Concurrent cursor attempt, source appends during fixed high-water | Exactly one CAS wins; no cursor beyond durable obligations or captured safe high-water | cursor generations and journal inventory |
| A27 | Authority outage and incomplete CH mirror | Pause/deny Bronze SQLite, then hide/corrupt CH observation mirror | Authority outage blocks progress and fails closed; missing/mismatched mirror never accepted; recovery reprocesses | HTTP health/cursor/mirror report |
| A28 | Historical replay | Frozen replay manifest and archived old run | Uses historical run archive; explicit source high-waters; current-state cutoff caveat; no invented as-of claim | manifest/archive hash/report |
| A29 | Replay/live isolation | Replay and live process same events/conflict independently | Separate SQLite state and CH replay tables; replay cannot change live cursor, facts, or validity | before/after live and replay inventories |
| A30 | V2 upgrade preservation | Existing V2 sentinel rows and exact SHOW CREATE, then 008 | 004 V2 table schemas/data unchanged; V1 additive objects present | before/after schema and sentinel hashes |
| A31 | Clean install and repeat migration | Empty database, run migration twice | Every V1 live/replay table present; second run no error/schema drift; worker starts only after migration | migration logs/schema inventory |
| A32 | Actual SUMO-to-Silver runtime | Isolated profile with shared Kafka/CH/Raw/Bronze and mounted archive | ACK/source/Bronze identities reconcile to Silver; actual worker READY; no synthetic substitution | full runtime acceptance bundle |
| A33 | Gold-facing canonical reader correctness | Query through supported reader, not raw table | Valid facts only, nullable values/presence/availability/lineage; conflicted/pending/unknown validity suppressed; fixed high-water pagination | independent API/query oracle |
| A34 | Single owner, dependency restart and low-memory operation | Double-start attempt; CH restart/outage; sustained workload under configured Docker limits | One lock owner; no cursor loss; readiness truthful; backlog recovers; resource and latency limits explicitly reported | health samples, container stats, lock error |

## Objective exit gates

1. Every scenario A01–A34 is PASS with evidence, or an explicitly approved scope change records the exclusion. No inferred pass.
2. Full workload cardinalities match archive-derived independent oracle and exact row identities; values, availability, lineage, hashes and validity reconcile independently.
3. Zero measured-zero corruption, zero unavailable-to-zero conversion, zero fabricated direction rows, zero unexplained Bronze events, and zero known-conflicted IDs exposed by canonical reader.
4. Clean install, V2 upgrade and repeat migration preserve historical V2 state; worker starts only after all target schemas exist.
5. Crash/restart, partial/lost-response writes, source outage, mirror outage and replay isolation pass with cursor/validity invariants.
6. Actual runtime reaches READY and converges with bounded pending obligations; resource/backlog/latency results are measured for the supported local Windows/Docker Desktop profile.
7. Phase 4 handover names the canonical reader, invalidation cursor/overlay, source cutoff limitation, null/availability model, lineage and archive retention obligations.

