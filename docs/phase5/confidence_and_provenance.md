# Phase 5 Confidence and Provenance

## Confidence Model

Confidence is calculated from factor-level evidence quality weights and stored on
each scored parcel as `confidence_score`.

Default weights:

- `measured = 1.00`
- `manual = 0.90`
- `proxy = 0.60`
- `heuristic = 0.30`
- `missing = 0.00`

The runtime multiplies each factor budget by the selected evidence-quality weight,
then sums those weighted contributions across the ten-factor profile. Because the
default profile totals 100 points, the resulting confidence naturally resolves to a
`0-100` score.

## Provenance Writes

For each scored factor, the service writes one or more `score_factor_input` rows
based on the evidence candidates discovered for that factor. This captures:

- direct and proxy candidates when both are present,
- the raw input value used for scoring review, and
- the evidence-quality label attached to each candidate.

## Retry Safety

Scoring reruns are deterministic because the service:

- deletes prior `score_factor_detail` rows for the run,
- deletes prior `score_bonus_detail` rows for the run,
- deletes prior `score_factor_input` provenance rows for the run, and
- resets previously scored parcels back to `pending_scoring` before replaying.

This keeps the unique provenance constraint intact without leaving duplicate rows
behind after reruns.

## Profile Tracking

Phase 5 adds `score_run.profile_name` so every run retains the scoring-profile name
used at execution time, even if the active profile changes later.
