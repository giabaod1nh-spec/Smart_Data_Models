# V1 observation fixtures

`valid_code_derived.json` is generated from the checked-in contract test
factory and the current generated topology catalog. It is **code-derived**,
not a captured same-cycle SUMO/V2 pair. `unavailable_synthetic.json` is a
synthetic contract edge case showing a required metric becoming unavailable.
`invalid_unknown_field_synthetic.json` is a synthetic wire rejection case.

The focused tests construct other invalid cases (metadata, numeric values,
hashes, catalog mismatch) in memory. Phase 0 did not capture a current
same-cycle source snapshot and V2 event. These fixtures are not cutover parity
evidence.
