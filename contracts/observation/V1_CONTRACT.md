# IntersectionObservation V1

## Scope and source review

One event is the complete **declared domain state** of one logical intersection
at one sampling cycle. It is not a per-vehicle event or a delta. The current
`traffic_observation.py` copied the whole internal snapshot, its JSON Schema
allowed arbitrary nested properties, and it generated `capturedAt` when the
builder ran. The source mapper also indexed metrics that the old schema treated
as optional. This contract fixes the wire boundary; Phase 2 will supply an
immutable capture and Phase 4 will build an independent V1 projector.

## Wire structure

The envelope is defined by `intersection-observation-v1.schema.json`. Its
`payload` contains identity, simulation state, one color and measurement set
per physical approach, and the current incident list. Optional fields carry
signal timing, spillback, context, and anomaly information. Every payload and
nested object is closed to unknown wire properties. Direction names and vehicle
class names are domain keys, not unrestricted extension bags.

The builder explicitly selects declared fields. Extra keys in an *internal*
snapshot are ignored; extra keys on the *wire* are rejected. There is no V1
extension object. A new domain field requires an intentional schema, builder,
and documentation change. Complete state means every required field is present
and every applicable optional field has the meaning specified in
`V1_METRIC_CONTRACT.md`.

## Metadata and ordering

| Field | Meaning |
|---|---|
| `simulationRunId` | One simulation execution; restart creates a new ID. |
| `intersectionId` | Stable logical node such as A, B, C, D, M. |
| `scenarioId` | Scenario at this node at capture time; can differ between nodes. Run manifest demand profile is separate. |
| `simulationTimeSeconds` | SUMO clock in seconds, not wall time. |
| `simulationStep` | Count of completed SUMO simulation steps in this run. |
| `cycleSequence` | Monotonic observation sampling-cycle number within one run, shared by all sampled nodes in that cycle. |
| `capturedAt` | UTC wall-clock timestamp at source snapshot capture, supplied by Phase 2. Never synthesized by the contract builder. |
| `topologyDigest` | `v1:sha256:<64 lowercase hex>` of the archived projection-relevant catalog. |
| `schemaVersion` | Integer `1`; the revised V1 wire format supersedes the unused draft V1 helper. |

`payload.node_id`, `simulation_run_id`, `simulation_time_sec`, and `scenario`
must equal their envelope counterparts. A scenario change takes effect in the
first captured event after the change. Replay preserves all original fields.
Kafka partition key remains `simulationRunId:intersectionId`; this preserves
per-intersection order but does not imply cross-partition global order.

## Identity, hash, and retry

`eventId = SHA256(UTF8("1|" + runId + "|" + intersectionId + "|" +
decimal(cycleSequence)))`, lowercase hexadecimal. This deliberately retains
the existing identity formula. The producer must never reuse a cycle sequence
for different data within the same run and node.

`payloadHash = SHA256(UTF8(canonical_json(payload)))`. Canonical JSON means
Python `json.dumps` with `sort_keys=True`, `ensure_ascii=False`,
`allow_nan=False`, and separators `(',', ':')`. JSON object order does not
matter; array order does. JSON integer `1` and decimal `1.0` have different
canonical bytes, so producers must retain the declared number types. The hash
includes only `payload`, excluding all envelope fields and `payloadHash`.
Other-language producers need the same numeric and Unicode test vectors before
claiming byte-compatible hashes.

The original serialized event is frozen. A retry carries the same `eventId`,
`payloadHash`, `capturedAt`, and all other fields. Matching ID and hash is an
idempotent duplicate only if the entire frozen event matches. Same ID with a
different hash or capture timestamp is an identity conflict. The current
Outbox rejects any duplicate ID; Phase 3 will implement the persistence rule.

## Validation boundary

JSON Schema validates structure, types, ranges, enums, and date-time with an
explicit `FormatChecker`. Python adds event identity, payload hash, finite
numbers, envelope/payload agreement, direction/color agreement, and metric
availability consistency. Neither validator consults a catalog archive.
`resolve_v1_catalog` is a separate injected contextual check. A missing
archive may be retryable; a resolved archive with another digest is an
integrity error. Phase 2 must archive the run catalog before first V1 publish.

The active V2 publisher, V2 Projector, shared `build_all_entities()`, legacy
`topology_hash`, run manifest, and DE topology contract retain their existing
behavior in Phase 1.
