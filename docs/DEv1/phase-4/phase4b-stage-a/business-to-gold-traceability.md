# Stage A business-to-Gold traceability

[Business specification](../phase4b-business-spec/README.md) defines the requirements. [Acceptance](acceptance.md) retains AC-01–40, SA-01–19, FA-01–30 and FZ-01–11 as the governing suite. [Execution status](execution-status.md) records results and authorizations. All reader names below refer to the [logical Gold serving contract](serving-contract.md).

The current evidence consists of the A-I01a independent arithmetic and slot fixtures, the UP-01 disposable Outbox and Silver authority fixture, the UP-02 raw evidence and revision fixtures, the A-I02 SQLite metadata/handoff rehearsal, and the A-I03 publication and ClickHouse adapter tests with a physical-row test double. The full acceptance cases remain open. A-I04, A-I05, A-I06 and A-I07 have their respective entry gates.

## Metric lineage

| ID and KPI | Source and semantics | Grain, Gold columns and sufficient statistics | Reader, quality and revision | Package, acceptance and evidence |
|---|---|---|---|---|
| M-01 Moving Speed | `vehicle_count` V, `waiting_vehicle_count` H, `average_speed_kmh` v from moving vehicles | Direction, intersection, network × W. `moving_speed`, `weighted_speed=Σv(V−H)`, `moving_count=Σ(V−H)` | Selected Window KPI, Historical Trends, Intersection Detail, Experiment Run Comparison. Zero eligible moving exposure gives N/A. | A-I01a fixture verified, AC-01/08/09. Official materialization open. |
| M-02 Stopped Ratio | V/H with `0 ≤ H ≤ V` | All three grains × W. `stopped_ratio`, `stopped_count=ΣH`, `ratio_vehicle_count=ΣV` | Same readers. All stopped with measured V>0 gives 100%; zero exposure gives N/A. | A-I01a fixture verified, AC-02/08/09. Official materialization open. |
| M-03 Queue Mean/P95/Max and worst approach | `queue_length_m`, method/scope digest, coverage | Direction × W exact nearest-rank P95 over qualified samples, `queue_mean`, `queue_max`, `queue_p95`, `queue_sum` plus counts. Intersection/network use max of direction P95, `worst_queue_direction` deterministic. | Selected Window KPI, Ranking, Trends, Detail, Experiments. Queue methods and scope must match before comparison; ties stable by archived IDs. | A-I01a math fixture and UP-02B method fixture, AC-03/09. Official G-QUEUE open. |
| M-04 Mean Occupancy | Separate Silver-verified measurement ID, occupancy method and raw percentage components linked to V1; legacy `occupancy_pct` retains its original semantics | Direction × W occupancy sum and valid/expected count; planned mean field | Detail and Ranking. Unknown legacy semantics cannot qualify the corrected result. | UP-02A known vectors and verified public Silver sidecar receipts, AC-04. G-SRC open. |
| M-05 Traffic Pressure | Joint applicable M-02/M-04; calibration definition and unrounded class | Direction score/components and intersection max score/argmax in planned Gold rows | Ranking, Heatmap, Detail, Trends. Versioned calibration and joint coverage required. | A-P04/A-I01, AC-05/08. G-CAL open. |
| M-06 High-Pressure Intersections | Approved threshold applied to M-05 for every expected intersection | Network × W planned high count, qualifying IDs and population count | Selected Window KPI and adjacent delta. Incomplete scores suppress the result. | A-I01/A-I04, AC-06/23. G-CAL and G-COV open. |
| M-07 Observed Vehicle Load | Aligned V across qualified slots and archived scope | All three grains × W. `observed_load`, `load_vehicle_count`, `time_slots` | Trends, Detail, Experiments. Expected slots stay independent of the observations. | A-I01a fixture verified, AC-07/09/26. Official coverage open. |
| M-08 Persistence and Change | Effective M-05 per window, previous adjacent compatible revision and expected gaps | Intersection × range derived from effective rows. Expected/valid/high counts, longest/current streak, peak and adjacent delta | Persistence Summary. Missing windows break streaks and suppress full-range publication under strict coverage. | A-I04, AC-27/28. G-CAL and reader evidence open. |
| M-09 Estimated Stopped Vehicle Time | H weighted by observed interval under approved sampling semantics | Optional direction or intersection × W. Exposure numerator and interval/coverage proof candidate | Research only. Completion-based waiting uses separate source evidence. | D-05/A-P02; AC-37/38 conditional. Deferred research. |

`W` is a full half-open 60 s or 300 s window with explicit archived run, topology, definition and revision identity. The metric columns named in backticks exist in the fixture payload. A-I03 stores and verifies exact row bytes against a durable manifest through its target interface. A physical ClickHouse mart and a serving reader remain open. Every eligible reader must revalidate the effective generation and publication epoch on its selected revisions. Plans and formulas are in [Metric contract](metric-contract.md), [Mart design](mart-design.md) and [Coverage contract](coverage-contract.md).

## Dashboard and experiment outputs

| Business requirement | Gold data and statistics | Logical reader and quality behavior | Package and acceptance | Actual evidence / readiness |
|---|---|---|---|---|
| O-01 Historic run selection, latest/pin, window navigator and duration/range | Run manifest/archive, topology, available range, published pointer, expected window timeline at 60/300 | Historical Run Catalog, Latest Published Window and Selected Window KPI. Pinned selections retain lineage and report invalidated revisions. | A-I04, AC-20–22 | Fixture windows verify durations. Historical reader pending. |
| O-02 Four Network KPI cards and previous-window deltas | Network × W M-06/M-01/M-02/M-03. Previous adjacent compatible effective revision and same-window denominators | Selected Window KPI. Suppress incompatible deltas, preserve absolute units and ratio percentage points. | A-I04, AC-23 | Three card metrics have fixture arithmetic. G-CAL and reader pending. |
| O-03 Priority and diagnosis | Intersection scores and separate worst-pressure/worst-queue IDs, direction statistics, sampled context | Intersection Ranking. Stable ties and explicit unknown diagnostics. | A-I04, AC-24 | Fixture worst-queue IDs. G-CAL and ranking pending. |
| O-04 Pressure Heatmap | Intersection × W effective M-05/class, sampled context and expected gaps | Pressure Heatmap. Same effective score as ranking and stable archived order. | A-I04, AC-25 | G-CAL and heatmap query pending. |
| O-05 Performance Trends | Intersection × W time series of M-01/02/03/07 and optional M-05 | Historical Trends. Explicit gaps, metric applicability and effective fence. | A-I04, AC-26 | Core metric fixture values; gap-aware serving pending. |
| O-06 Bottleneck Persistence | M-08 range statistics over effective intersection windows | Persistence Summary. Full-range scope and streak boundaries require qualified coverage. | A-I04, AC-27 | G-CAL and persistence query pending. |
| O-07 Data Quality and Publication | Expected/valid counts, structural slot dispositions, source finality, manifest and current revision | Catalog, timeline and selection. Report quality and publication as distinct statuses. | A-I04, AC-11–14/28/39 | Fixture quality and SQLite effective pointer evidence. G-COV and reader pending. |
| D-01 Direction Performance/Pressure and trends | Archived direction × W M-01–05 and M-07, queue mean/max, independent pressure and queue IDs | Intersection Detail and Historical Trends. Immutable topology membership and same-point revision. | A-I04, AC-29 | A-I01a direction and rollup values. Occupancy, pressure and reader pending. |
| D-02 Queue Storage Utilization | Sampled storage fraction, geometry version, valid sample count, exact P95 and length scope | Intersection Detail. Withhold unavailable or incompatible inputs. | P1/A-I04, AC-30 | Adoption and geometry reference evidence pending. |
| D-03 Sampled Signal Operation | Signal color, eligible sample share and aligned conditional queues | Intersection Detail. Preserve sampled semantics and joint availability. | P1/A-I04, AC-31 | Adoption and oracle pending. |
| D-04 Incident, Spillback and Local Context | Canonical intersection flags/pressure, observed incidents, scenario and mode, per-window context | Intersection Detail. Show sampled evidence with source version and local scope. | P1/A-I04, AC-32 | Spillback conversion and V1/Silver provenance fixture. Aggregation and reader pending. |
| D-05 Extended metrics | Independent completed trips/crossings, waiting trajectories, optional M-09 and event version | Diagnostic investigation, future optional Detail/Experiments. Historical fields remain unsupported if absent. | A-P02, AC-36–38 | Deferred research and explicit adoption. |
| E-01 Experiment design and run catalog | Run pair metadata, topology, seed, demand, warm-up, range, cadence, controller and metric versions | Experiment Run Comparison. PASS/FAIL/UNKNOWN comparability from pinned metadata. | A-I05, AC-33 | G-EXP and reader pending. |
| E-02 Paired Performance Summary | Nonoverlapping aligned windows. Sum M-01/02/07 sufficient statistics. Max window-level M-03. | Experiment Run Comparison. Matched range/coverage and explicit absolute or eligible relative change. | A-I05, AC-34 | Sufficient statistics in fixtures. Paired reader pending. |
| E-03 Comparative Trends | Effective per-run metrics on identical boundaries and scopes | Experiment Run Comparison. Preserve missing intervals and fail closed on incompatible definitions. | A-I05, AC-35 | Pending paired oracle and reader. |
| E-04 Change by Intersection | Per-intersection E-02 values and baseline/candidate delta | Experiment Run Comparison. Stable archived intersection identity and identical qualified range. | A-I05, AC-35 | Pending paired oracle and reader. |
| E-05 Repeated Trials | Compatible pairs, fixed trial protocol and declared seeds | Research protocol for paired distributions, trial count and interpretation. | G-EXP research; AC-33–35 apply to each pair | Deferred. |

## Acceptance evidence by ID

| IDs | Evidence and outstanding requirement |
|---|---|
| AC-01, AC-02, AC-07, AC-08, AC-09 | A-I01a independent weighted, empty, alignment and rollup fixtures. AC-08 pressure applicability remains open under G-CAL. |
| AC-03 | Exact fixture P95 and worst queue. Official source qualification and replay open. |
| AC-04 | Four detector/lane fixture paths and Silver verification of sidecar raw components, values and original V1 payload hash. Instrumented source oracle and G-SRC open. |
| AC-05, AC-06 | Calibration authority, holdout and high-pressure population count open. |
| AC-10, AC-11, AC-12, AC-13, AC-14 | Fixture window/population, lease-bound producer checkpoint, Silver delayed outcome boundary and separate Gold change/invalidation-gated cursor. Physical source-to-Gold proof, topology/lifecycle integration and official coverage open. |
| AC-15 | Deterministic fixture dedup and SQLite handoff rehearsal. Official replay and durability chain open. |
| AC-16, AC-17, AC-18, AC-19 | SQLite generation/fence and restart/partial target fixtures. Physical ClickHouse faults, late correction and rebuild acceptance open. |
| AC-20, AC-21, AC-22, AC-23, AC-24, AC-25, AC-26, AC-27, AC-28, AC-29 | O-01–07 and D-01 above. Bounded reader, official revision validation and range/pin semantics open. |
| AC-30, AC-31, AC-32 | D-02–04 above. P1 acceptance applies on explicit adoption. |
| AC-33, AC-34, AC-35 | E-01–04 above. G-EXP and matched run evidence open. |
| AC-36, AC-37, AC-38 | D-05 above. Optional trajectory truth, waiting and mixed-version tests apply on adoption. Existing V1 compatibility was exercised independently. |
| AC-39 | The isolated pointer rejects stale generation. Bounded multi-read fence and stale response handling open. |
| AC-40 | Isolated source and Realtime measurements only. Full-entry performance gate and PA-01–12 evidence remain open. |
# Functional checkpoint evidence

- M-01/02/03/04/07 and O-01/02/05 have an isolated runnable aggregation and Reader path. Weighted speed, stopped ratio and observed load preserve sufficient statistics. Per-direction queue and occupancy use only verified measurement evidence with method and coverage. Network and intersection queue P95 use the worst direction. The independent oracle supplies expected values for five 60-second windows and one 300-second window from the retained real SUMO capture. Physical value and lineage comparison remains open.
- O-03/04/06 expose queue diagnostics, pinned windows and pressure availability. Official ranking, heatmap values, bottleneck persistence and the high-pressure KPI require G-CAL. M-08 has adjacent-window change for published physical metrics with gap handling.
- Experiments retain pinned manifests, treatment evidence and matched effective Gold window comparison. Actual Adaptive execution and acceptance are **DEFERRED_EXTERNAL_DEPENDENCY** by owner decision on 2026-11-27 because trained checkpoints are unavailable. The current milestone prioritizes the four KPI cards and O-03/04/05 Analytics Overview datasets, with M-01–08, G-CAL and actual Fixed-Time physical verification still required.
- The [execution status](execution-status.md) records the implementation, disposable verification commands, physical blockers and deferred performance requirements.
