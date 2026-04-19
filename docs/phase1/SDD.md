# KIO Site Finder Phase 1 Software Design Description

## 1. Design Overview

This document describes the component-level design for the Phase 1 rewrite. It translates the architecture into concrete data structures, service behavior, validation rules, and interface contracts.

## 2. Primary Design Principles

- One authoritative batch publication model
- One parcel evaluation record per parcel per score run
- One factor-detail row per factor and one bonus-detail row per bonus
- Deterministic rerun behavior
- Explicit source freshness and failure handling

## 3. Data Model Design

### 3.1 Reference Tables

| Table | Purpose | Notes |
| --- | --- | --- |
| `metro_catalog` | Approved metro definitions | Includes active flag and boundary metadata |
| `county_catalog` | County-to-metro mapping | Supports score-run scoping |
| `source_catalog` | Required source inventory | Includes cadence and `block_refresh` |
| `source_interface` | Contract per source feed | Schema, owner, validation notes |
| `factor_catalog` | Approved `F01-F10` definitions | Semantics owned by business |
| `bonus_catalog` | Approved `B01-B05` definitions | Includes max bonus points |
| `scoring_profile` | Versioned scoring profiles | Effective dating and status |
| `scoring_profile_factor` | Profile factor budgets | Sum of active factors must equal 100 |

### 3.2 Operational Tables

| Table | Purpose | Key Columns |
| --- | --- | --- |
| `source_snapshot` | One record per source load | `source_id`, `metro_id`, `snapshot_ts`, `status`, `checksum` |
| `raw_parcels` | Canonical parcel store | `parcel_id`, `county_fips`, geometry, lineage |
| `parcel_rep_point` | Persisted representative point | `parcel_id`, `rep_point_geom` |
| `score_batch` | System refresh container | `batch_id`, `status`, `expected_metros`, `completed_metros`, `activated_at` |
| `score_run` | Metro-scoped scoring run | `run_id`, `batch_id`, `metro_id`, `status`, `failure_reason`, `started_at`, `completed_at` |
| `parcel_evaluations` | Parcel lifecycle per run | `run_id`, `parcel_id`, `status`, `viability_score`, `confidence_score` |
| `parcel_exclusion_events` | Rule-level exclusions | `run_id`, `parcel_id`, `exclusion_code`, `exclusion_reason` |
| `score_factor_detail` | One row per factor | `run_id`, `parcel_id`, `factor_id`, `points_awarded`, `rationale` |
| `score_factor_input` | Provenance inputs per factor | `run_id`, `parcel_id`, `factor_id`, `input_name`, `input_value`, `evidence_quality` |
| `score_bonus_detail` | One row per bonus | `run_id`, `parcel_id`, `bonus_id`, `applied`, `points_awarded` |

### 3.3 Constraints

- `score_batch.status` in `('building','failed','completed','active')`
- `score_run.status` in `('running','failed','completed')`
- `parcel_evaluations.status` in `('prefiltered_band','prefiltered_size','pending_exclusion_check','pending_scoring','scored','excluded')`
- unique `score_factor_detail(run_id, parcel_id, factor_id)`
- unique `score_bonus_detail(run_id, parcel_id, bonus_id)`
- unique `score_factor_input(run_id, parcel_id, factor_id, input_name)`
- active scoring profile must include exactly ten factor rows
- sum of `scoring_profile_factor.max_points` for the active profile equals 100

## 4. Partitioning and Indexing

### 4.1 Partitioning

- `raw_parcels` partitioned by `county_fips` to match metro/county filtering patterns
- optional time-based partitioning for `source_snapshot` if volume warrants it

### 4.2 Required Indexes

- `raw_parcels(county_fips, parcel_id)`
- spatial index on `raw_parcels.geom`
- spatial index on `parcel_rep_point.rep_point_geom`
- `parcel_evaluations(run_id, status)`
- `score_factor_detail(run_id, parcel_id)`
- `score_bonus_detail(run_id, parcel_id)`
- `score_factor_input(run_id, parcel_id, factor_id, input_name)`
- `score_run(batch_id, metro_id, status)`

## 5. Service Design

### 5.1 Ingestion Service

Inputs:

- external source file, API response, or data feed

Processing:

1. validate interface contract,
2. write source snapshot metadata,
3. load staging data,
4. transform into canonical tables,
5. compute representative point,
6. publish load status.

Outputs:

- canonical source data,
- `source_snapshot` audit record,
- quality findings and alert events.

### 5.2 Freshness Gate Service

Inputs:

- metro ID,
- required source list,
- latest successful source snapshots.

Processing:

1. determine required sources for the metro,
2. compare latest snapshot age against allowed cadence,
3. fail if any `block_refresh` source is stale, missing, or errored,
4. store gate result against the batch and run.

Outputs:

- pass/fail decision,
- actionable failure reason.

### 5.3 Evaluation Service

Inputs:

- `score_run`,
- raw parcels for in-scope counties,
- evaluation configuration.

Processing:

1. create `parcel_evaluations` rows for all in-scope parcels,
2. apply band filter with `rep_point`,
3. apply size filter,
4. apply hard exclusion rules,
5. write exclusion events,
6. promote survivors to `pending_scoring`.

Outputs:

- parcel statuses,
- exclusion details,
- evaluation metrics.

### 5.4 Scoring Service

Inputs:

- `pending_scoring` parcels,
- scoring profile,
- factor and bonus catalogs,
- source-derived evidence.

Processing:

1. resolve evidence per factor,
2. prefer direct evidence when available,
3. calculate factor points,
4. write factor details,
5. write factor input provenance,
6. calculate bonus rows for all `B01-B05`,
7. compute `viability_score`,
8. compute `confidence_score`,
9. mark parcel as `scored`.

Outputs:

- full score detail,
- provenance,
- parcel-level scores and confidence.

### 5.5 Batch Orchestrator

Inputs:

- refresh request,
- approved metro list.

Processing:

1. create a new `score_batch`,
2. create one `score_run` per metro,
3. execute freshness gate,
4. trigger evaluation and scoring,
5. validate completion invariants,
6. activate the batch only if all required runs completed and none failed.

Outputs:

- batch status,
- activation record,
- operator notifications.

## 6. State Models

### 6.1 Score Batch State

- `building`: batch created, runs not all complete
- `failed`: one or more required runs failed
- `completed`: all required runs complete and validations passed
- `active`: completed batch published for reads

### 6.2 Parcel Evaluation State

- `prefiltered_band`
- `prefiltered_size`
- `pending_exclusion_check`
- `pending_scoring`
- `excluded`
- `scored`

Transition rules:

- a parcel may only reach `scored` from `pending_scoring`
- a parcel may not remain in `pending_exclusion_check` or `pending_scoring` when the run completes
- `confidence_score` is required when status = `scored` and null otherwise

## 7. Validation Rules

- `VR-001`: freshness gate must pass before any score details are written
- `VR-002`: every completed run must have zero parcels in pending states
- `VR-003`: every scored parcel must have ten factor rows
- `VR-004`: every scored parcel must have five bonus rows
- `VR-005`: a run cannot be marked completed if factor or bonus cardinality validation fails
- `VR-006`: active batch reads must resolve to one and only one activated batch
- `VR-007`: duplicate factor input writes are prevented through uniqueness and upsert behavior

## 8. API Design

### 8.1 Reader APIs

- `GET /api/v1/parcels`
  - Returns scored parcels from the active batch only
  - Supports metro, county, acreage, score range, and status filters

- `GET /api/v1/parcels/{parcelId}`
  - Returns parcel detail, factor detail, bonus detail, confidence, and provenance summary from the active batch

- `GET /api/v1/batches/active`
  - Returns active batch metadata and activation timestamp

### 8.2 Operator APIs

- `GET /api/v1/admin/runs`
  - Returns run status, counts, failure reasons, and timestamps

- `GET /api/v1/admin/sources/freshness`
  - Returns source freshness status by metro

- `POST /api/v1/admin/runs/{runId}/retry`
  - Retries a failed run if source prerequisites are met

- `POST /api/v1/admin/batches/{batchId}/activate`
  - Allowed only for a completed batch that passes validation checks

## 9. Pseudocode

```text
for each refresh_request:
    batch = create_score_batch()
    for each metro in approved_metros:
        run = create_score_run(batch, metro)
        if freshness_gate_fails(metro):
            fail_run(run, "STALE_SOURCE")
            fail_batch(batch)
            continue

        evaluate_all_parcels(run, metro)
        score_pending_parcels(run)

        if has_pending_rows(run) or fails_cardinality_checks(run):
            fail_run(run, "VALIDATION_FAILURE")
            fail_batch(batch)
        else:
            complete_run(run)

    if every_required_run_completed(batch) and no_failed_runs(batch):
        activate_batch(batch)
```

## 10. Error Handling

- stale or missing critical source: fail run before scoring
- malformed source record: quarantine row/file and flag source snapshot
- duplicate provenance write: upsert instead of duplicate insert
- partial scoring failure: fail run and keep prior active batch unchanged
- activation validation failure: keep batch non-active and notify operators

## 11. Design Completion Criteria

The design is complete when:

- every requirement in the SRS maps to a component, table, or interface,
- every major pipeline stage has explicit inputs, outputs, and validations,
- every read surface is scoped to the active batch,
- the design supports repeatable testing for the audit-critical scenarios.
