# Status And Failure Codes Baseline

## Score Batch Statuses

| Status | Meaning | Allowed Next State |
| --- | --- | --- |
| `building` | batch exists and required runs are not yet fully validated | `failed`, `completed` |
| `failed` | one or more required runs failed or validation failed | terminal unless rerun policy is defined |
| `completed` | all required runs completed and validations passed | `active` |
| `active` | completed batch is published to readers | terminal for publication state |

## Score Run Statuses

| Status | Meaning | Allowed Next State |
| --- | --- | --- |
| `running` | run is in progress | `failed`, `completed` |
| `failed` | run stopped before valid completion | terminal unless retry creates a new attempt |
| `completed` | run finished and passed completion checks | terminal |

## Parcel Evaluation Statuses

| Status | Meaning |
| --- | --- |
| `prefiltered_band` | parcel rejected by corridor or band rule |
| `prefiltered_size` | parcel rejected by acreage or threshold rule |
| `pending_exclusion_check` | parcel still needs exclusion checks |
| `pending_scoring` | parcel cleared for scoring |
| `excluded` | parcel failed one or more hard exclusions |
| `scored` | parcel has complete factor, bonus, and confidence outputs |

## Initial Failure Codes

| Code | Meaning | Typical Owner |
| --- | --- | --- |
| `STALE_SOURCE` | required source exists but freshness exceeded allowed cadence | `Ingestion Agent` |
| `MISSING_SOURCE` | required source snapshot not found | `Ingestion Agent` |
| `SOURCE_LOAD_ERROR` | source ingestion failed before canonical publish | `Ingestion Agent` |
| `VALIDATION_FAILURE` | cardinality, pending-state, or contract validation failed | `QA Agent` / owner agent |
| `EVALUATION_FAILURE` | parcel evaluation stage failed | `GIS Agent` |
| `SCORING_FAILURE` | factor, bonus, or confidence stage failed | `Scoring Agent` |
| `ACTIVATION_VALIDATION_FAILURE` | batch failed publication checks | `API Agent` |
| `MANUAL_CANCELLED` | operator intentionally stopped the run | `Ops-Release Agent` |

## Guardrails

- A batch cannot become `active` unless it first reaches `completed`.
- A run cannot become `completed` if pending parcel states remain.
- Failure codes must be preserved in run history and surfaced to operators.
- Any new status or failure code requires updates to the SDD, tests, and this baseline.

