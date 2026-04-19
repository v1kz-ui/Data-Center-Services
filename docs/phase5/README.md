# Phase 5 Scoring Foundation Package

This folder contains the Phase 5 foundation package for deterministic parcel
scoring, confidence calculation, provenance capture, and score-run profile
tracking.

## Included Artifacts

- `scoring_engine.md`
- `confidence_and_provenance.md`
- `score_explanations.md`
- `migration_rehearsal.md`
- `phase5_foundation_report.md`

## Foundation Outcome

The repo now includes a working scoring runtime that:

- resolves and validates the active scoring profile,
- scores all `pending_scoring` parcels within a run,
- writes exact factor and bonus cardinality per scored parcel,
- computes parcel-level viability and confidence,
- records factor-input provenance with retry-safe behavior,
- completes the run when no pending parcel states remain, and
- stores the scoring profile name used by the run for auditability.

The current factor and bonus mechanics use documented evidence-key conventions
because the repo still carries placeholder catalog semantics for `F01-F10` and
`B01-B05`.
