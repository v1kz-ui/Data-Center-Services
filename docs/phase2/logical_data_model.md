# Phase 2 Logical Data Model

## Objective

Define the canonical schema groups that support Phase 3 through Phase 6 implementation
without relying on the structural shortcuts called out in the audit memo.

## Entity Groups

### Territory And Scope

- `metro_catalog`: approved metro baseline
- `county_catalog`: county-to-metro mapping

### Source Governance

- `source_catalog`: approved source inventory
- `source_interface`: source contract definition
- `source_snapshot`: per-source, per-metro load history

### Canonical Parcel Foundation

- `raw_parcels`: canonical parcel store with acreage, lineage, and geometry payload
- `parcel_rep_point`: persisted representative-point artifact for later evaluation rules

### Scoring Configuration

- `factor_catalog`
- `bonus_catalog`
- `scoring_profile`
- `scoring_profile_factor`

### Runtime Containers

- `score_batch`
- `score_run`

### Evaluation Lifecycle

- `parcel_evaluations`
- `parcel_exclusion_events`

### Scoring And Provenance

- `score_factor_detail`
- `score_factor_input`
- `score_bonus_detail`

## Key Rules

- one `score_batch` contains many `score_run` rows
- one `score_run` may evaluate many parcels
- one parcel may have one evaluation row per run
- one scored parcel must ultimately resolve to ten factor rows and five bonus rows
- factor input provenance is duplicate-protected by `(run_id, parcel_id, factor_id, input_name)`
- the scoring profile remains normalized through `scoring_profile_factor`

## Physical Design Notes

- the repo-safe foundation stores geometry payloads as `geometry_wkt` and `rep_point_wkt`
  so migrations and tests remain portable in local and CI contexts
- the logical geometry model still anticipates PostGIS-backed execution in later pipeline work
- migration-safe checks are used for nonnegative counts, point allocations, and scored-confidence rules

