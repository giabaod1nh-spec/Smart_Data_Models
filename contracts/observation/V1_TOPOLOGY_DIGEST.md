# V1 topology identity

`topologyDigest` is separate from the existing `topology_hash`. The old hash
and `run_manifest.topology_hash` are unchanged. The V1 digest is
`v1:sha256:<64 lowercase hex>`, calculated by `contracts/topology/v1_digest.py`.

The helper selects `PROJECTION_FIELDS`: catalog schema version, selected signal
programs and their hash, phase indexes, junction types, primary TLS, TLS
groups, reverse TLS mapping, grouped axes, node locations, approach/sensing/
outgoing edges, inter-node links, edges, connections, and movements by
approach. It excludes the machine-relative `source_net` path, legacy
`topology_hash`, and explanatory lane-convention note. Changing any included
projection field changes the digest; JSON object key order does not. Array
order is preserved, including TLS group order (primary TLS first). Missing
included fields are invalid. `null` is serialized distinctly from omission.
Canonicalization is UTF-8 JSON with sorted object keys, no NaN/Infinity,
unescaped Unicode, and compact separators. Integers and decimals retain their
JSON representation. The digest version must change if inclusion or
canonicalization rules change.

The future resolver takes `(simulationRunId, topologyDigest)` and reads only
the catalog archived under that run. Its result carries the run ID so a
wrong-run archive is detectable. It recomputes the digest over the archived
catalog. It must never fall back to global `config.py` or the newest generated
catalog. The Phase 1 helper specifies this boundary; Phase 4 provides the
actual filesystem or service adapter.

`validate_observation_in_catalog` additionally checks that the logical node
exists and that the event contains exactly its archived approach directions.
This contextual check runs after local wire/semantic validation.

Missing or temporarily unreachable archives raise `CatalogUnavailableError`
and are retryable. A resolved archive for another run, a malformed catalog,
or a digest mismatch raises `CatalogIntegrityError` and blocks projection.
The run catalog must be durably archived before Phase 3 publishes its first
V1 observation. This is a Phase 2 publishing precondition. The current
run-manifest/DE contract continues to compare its legacy hashes as before.
