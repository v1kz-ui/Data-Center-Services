# Phase 5 Scoring Engine

## Purpose

Phase 5 converts `pending_scoring` parcels into `scored` parcels with complete
factor, bonus, confidence, and provenance outputs.

## Runtime Flow

1. Resolve the requested `score_run`.
2. Re-check metro freshness before scoring begins.
3. Resolve the scoring profile by explicit name or the single active effective
   profile.
4. Validate that the active profile has exactly ten factor rows and a 100-point
   total budget.
5. Clear prior score-factor, bonus, and provenance rows for the run so reruns are
   deterministic.
6. Reset previously scored parcels back to `pending_scoring` for replay.
7. Score every `pending_scoring` parcel.
8. Validate factor and bonus cardinality.
9. Mark the run `completed` only when no parcel remains in a pending state.

## Factor Mechanics

Each profile factor produces exactly one `score_factor_detail` row per scored
parcel. The current implementation expects normalized evidence in
`source_evidence.attribute_name` using these conventions:

- measured: `f##_measured` or `f##_score`
- manual: `f##_manual`
- proxy: `f##_proxy`
- heuristic: `f##_heuristic`

Numeric values may be provided as `0-1` normalized decimals or `0-100`
percent-style numbers. The engine clamps the normalized result to `0-1` before
applying the profile factor budget.

## Bonus Mechanics

Each active bonus produces exactly one `score_bonus_detail` row per scored parcel,
including rows where the bonus does not apply. The current implementation expects
boolean evidence using these conventions:

- measured: `b##_measured` or `b##_applies`
- manual: `b##_manual`
- proxy: `b##_proxy`
- heuristic: `b##_heuristic`

Truthy values apply the configured bonus points; false or missing values produce a
zero-point bonus row.

## Direct-Over-Proxy Precedence

The runtime uses a fixed evidence-precedence order:

1. `measured`
2. `manual`
3. `proxy`
4. `heuristic`

This precedence controls both the chosen factor or bonus input and the evidence
quality used by the confidence model.
