# Target Repository Structure

## Top Level

```text
apps/
  api/
    src/
workers/
  orchestrator/
    src/
  ingestion/
    src/
  evaluation/
    src/
  scoring/
    src/
db/
  migrations/
  seeds/
tests/
  unit/
  integration/
  e2e/
infra/
configs/
scripts/
docs/
  phase1/
```

## Module Responsibilities

- `apps/api`: user-facing and admin APIs backed only by the active batch
- `workers/orchestrator`: batch and run lifecycle, scheduling, activation logic
- `workers/ingestion`: source loading, validation, freshness evaluation, quarantine handling
- `workers/evaluation`: metro scoping, filtering, exclusion logic, evaluation metrics
- `workers/scoring`: factor scoring, bonus scoring, confidence, provenance, scoring validations
- `db/migrations`: forward and rollback migration assets
- `db/seeds`: seed files for catalogs, profiles, and controlled reference data
- `tests/unit`: small deterministic logic tests
- `tests/integration`: database and service integration tests
- `tests/e2e`: full flow tests across batches and APIs

