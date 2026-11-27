# Stage A blockers and dependencies

| Blocker | Severity | Evidence / status | Affected work | Mitigation and resolution owner |
|---|---|---|---|---|
| G-ENTRY: Minimum paced evidence ENVIRONMENT BLOCKED | Critical | `performance/acceptance-result.md`; paced run stopped on 2026-11-25 at 206.05 simulation seconds due to memory floor; no full two-duration/drain/oracle boundary | Stage A runtime qualification; any conditional entry | Preserve evidence, resolve environment cause under performance owner; do not rerun or reuse retained fixture without separate authorization |
| G-COV: no approved expected slot schedule | Critical | backend interval default and actual canonical sim timestamp, but no verified slot identity/alignment; Core coverage UNKNOWN | Official business publication, strict coverage, metrics/serving | SF-07 capture and manifest/producer contract proposal; source + DE + business owners approve |
| G-SRC: occupancy normalization | Critical cho occupancy/pressure | Inbound/outbound E2 sai với percent nhỏ. Lane paths cần probe theo installed version. | Occupancy, score và spillback evidence tùy path | SF-01 và source version boundary được duyệt |
| G-QUEUE: mixed queue methods lack provenance | High | E2 nonzero branch vs vehicle-based estimate fallback; no proven method compatibility | Official queue P95, queue-dependent reference calibration và comparison | Source fixture study; choose compatible definition or represent separately |
| G-REV: no approved business revision/publication protocol | Critical | Core has input proof generations, but no business publication manifest/current-effective transaction across CH+SQLite | All business writes and latest serving | Approve failure matrix and implement staged immutable output + one family publication fence |
| G-METRIC: formulas and calibration remain review candidate | High | Business Spec formulas proposed; pressure weights/thresholds need independent calibration | Metric engine and official score results | Approve registry, or defer dependent metrics without blocking independent ones |
| G-EXP: experiment metadata/protocol incomplete | High for comparison | run manifest exists, but complete demand/schedule/controller/analysis metadata and comparable cohorts unproven | Official Fixed-Time vs Adaptive and scenario comparison | Source/AI owner metadata contract plus independent run protocol |
| Master Plan vs implementation status drift | Medium/high governance | Master Plan snapshot still describes V1 DE unimplemented; current acceptance describes Phase 4A done and capacity blocked | Approval routing and Stage A/B/C ownership | Owner reconciles authoritative plan before approving decomposition |
| Waiting time/throughput semantics | Medium, optional | no canonical completed-trip waiting; stop-line crossing not represented; AI `avg_waiting` count-valued | Optional experiment metrics only | Research independently; do not block core metrics |

## Phạm vi blocker

G-ENTRY hiện chặn production business implementation, gồm minimum gate unresolved hoặc cần approved amendment. G-COV/G-REV chặn official publication. G-SRC/G-CAL chặn occupancy/pressure-dependent outputs; G-QUEUE chặn queue-dependent results, impact calibration chỉ khi queue dùng làm reference. G-EXP chặn official comparisons. Optional research chặn riêng sản phẩm adopt. G-PERF và Phase 5 blockers chặn operational/cutover acceptance. Preparation read-only reviewer work và separately scoped fixtures có approval riêng.
## Dependencies bổ sung

- R02: cần authoritative lifecycle/schedule/slot linkage được verify qua public Silver. No eligible official KPI nếu historical runs thiếu evidence.
- R01/R04/R06/R09: numeric bounds và protected reserve, upstream retention, v2 schema rehearsal, CPU/I/O coexistence và durable payload sizing chờ owner cùng benchmark.
- R07/R08: approve SQLite writer/serving adapter, CH read consistency, cleanup grace và frozen replay source nếu cần historical concurrent with live. Không hạ replay boundary checks để mở gate.
## Preparation readiness

Planning candidate can be reviewed now. UP-01 and official G-COV block full coverage; UP-02 blocks affected unit/queue metrics. D-A14 authoritative amendment plus A-P03/04/05 applicable approvals gate isolated implementation. G-REV and measurable reserve/authority approval gate publication. G-PERF and operational acceptance require representative paced both-duration evidence, realtime impact, independent durable oracle and bounded drain. Retained memory stop remains ENVIRONMENT BLOCKED. [Readiness](final-design-closure.md).
## Freeze status and unlock actions

- Isolated A-I01a/A-I02/03: effective [authority amendment](conditional-entry-proposal.md), scoped owner authorization, applicable fixture/SQLite or CH/G-MART/G-REV reviews. Nothing implemented here.
- Full integration: UP-01 metadata contract proof/A-I02 consumer hook, source metric-specific UP-02A/B and pressure/experiment gates as applicable, existing full-entry evidence. The minimum scope to start pure A-I01a does not require upstream implementation.
- Live/operations: G-COV/G-REV, retained representative G-ENTRY and G-PERF evidence, CH consistency/reader adapter, production budgets and operational approvals still required. Concurrent replay deferred without immutable compatible source.
