# Expected Observation and coverage contract

**Mandatory design gate G-COV.** The Business Spec's strict 100% applicable coverage is a proposed owner direction. It must be made precise and approved before any official publication. The examples 300/1,020 samples per 60s and 1,500/5,100 per 300s assume five intersections, 17 approaches and exactly 1Hz aligned sampling; they are not runtime constants or approved denominators.

## Authoritative expected population

For each immutable run generation and topology digest, derive participants and applicable approaches from verified run manifest/archive. Define expected sampling cadence and alignment from producer configuration plus measured event times/steps. Use simulation time and the approved precision; do not derive expected counts from records received. Define slot identity independently (e.g., an approved manifest schedule/sequence mapping), including start offset, first/last partial eligibility, delayed and missing events, duplicate delivery, terminal status and tolerance. Do not round 0.01s / 60.04s observations to a convenient slot without approved mapping. Gold's current epoch-zero `[start,end)` membership and fractional timestamp handling are Core invariants unless owner approves an amendment.

The current source defaults to `OBSERVATION_INTERVAL_SEC=1.0`, tests elapsed simulation time against the last observation, records rounded observation simulation time and step, but does not expose a verified expected-slot schedule. Therefore slot contract is **not available yet**. The observation-count table in Business Spec is a conditional example only.

## Per-metric, per-grain accounting

Persist or reproducibly derive counts for `expected`, `received`, `valid`, `unavailable`, `not_applicable`, `missing`, `invalid/conflicted`, plus population and slot-set identity. Enforce conservation: each expected metric slot is classified exactly once; transport duplicates are deduplicated by semantic event identity before counts. Coverage is `valid / expected_applicable`, while NOT_APPLICABLE is included only if independently specified as not part of that metric's population. Never relabel missing as not-applicable to reach 100%.

| State | Meaning | Numeric value |
|---|---|---|
| Measured zero | valid observation measured value 0 | zero, contributes to valid count |
| NULL + UNAVAILABLE | applicable metric could not be measured | no numeric contribution; lowers coverage |
| NULL + NOT_APPLICABLE | contract says metric does not apply to this slot/entity | excluded only by approved applicability rule and recorded separately |
| Missing | expected observation not received | not zero; lowers coverage |
| Invalid/conflicted | observation rejected or source identity in conflict | not valid; affected result suppressed/reprocessed |
| INCOMPLETE | expected population not fully valid | no official result under strict policy |
| N/A | complete applicable population, but formula denominator is zero or there is no applicable event | no numeric result, reason included |
| REPROCESSING | prior result fenced after change/invalidation | not serve prior revision as current |
| PUBLISHED | one verified current business revision is eligible to serve | values and manifest share one revision fence |

## Strict publication policy proposal

For an official metric result at grain G, require all expected applicable slots and participants for that metric to be valid, identity-consistent and in the same input generation/definition. Coverage is metric-specific: missing speed does not by itself suppress queue P95; missing an applicable participant suppresses a network metric whose population includes it. A complete zero-vehicle window differs from an absent observation. A missing expected slot is never converted to zero. Only the result family for which all required inputs are met may be published; dependent metrics can be unavailable independently.

Terminal `RUN COMPLETE`, passed window end, Core `COMPLETE`, or an empty feed do not establish business completeness. The public feed currently lacks authoritative lifecycle/finality. Until a source contract supplies it, expose Core `UNKNOWN` / not-final and do not claim closed-run completeness. No provisional aggregates are in scope.

## Acceptance cases

For every metric and grain: complete measured zero; full coverage with zero denominator; one missing sample; one explicit unavailable; approved not-applicable member; invalid/conflicted event; missing intersection; changed topology; scenario change; late delivery after publication; terminal incomplete window. Assert exact counts and reason codes, metric-specific suppression, and no fabricated zero or denominator shrinkage. Independent oracle must construct expected population from manifest/topology and schedule fixture, not from Gold rows.

