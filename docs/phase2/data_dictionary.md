# Phase 2 Data Dictionary

## Reference Tables

| Table | Primary Key | Purpose | Key Constraints |
| --- | --- | --- | --- |
| `metro_catalog` | `metro_id` | approved metro baseline | unique `display_name` |
| `county_catalog` | `county_fips` | county-to-metro mapping | FK to `metro_catalog` |
| `source_catalog` | `source_id` | approved source inventory | source identifier is stable |
| `source_interface` | `interface_id` | source interface contract | unique `(source_id, interface_name)` |
| `factor_catalog` | `factor_id` | factor baseline `F01-F10` | ordinal ordering |
| `bonus_catalog` | `bonus_id` | bonus baseline `B01-B05` | nonnegative `max_points` by policy |
| `scoring_profile` | `profile_id` | versioned scoring profile | unique `profile_name` |
| `scoring_profile_factor` | `profile_factor_id` | factor budgets per profile | unique `(profile_id, factor_id)` and `(profile_id, ordinal)` |

## Operational Tables

| Table | Primary Key | Purpose | Key Constraints |
| --- | --- | --- | --- |
| `source_snapshot` | `snapshot_id` | source-load audit record per metro | nonnegative `row_count`; indexed by source/metro/time |
| `raw_parcels` | `parcel_id` | canonical parcel store | nonnegative `acreage`; indexed by county and metro |
| `parcel_rep_point` | `parcel_id` | representative-point artifact | one row per parcel |
| `score_batch` | `batch_id` | refresh container | `completed_metros <= expected_metros` |
| `score_run` | `run_id` | metro-scoped run | unique `(batch_id, metro_id)` |
| `parcel_evaluations` | `evaluation_id` | parcel status per run | unique `(run_id, parcel_id)` |
| `parcel_exclusion_events` | `exclusion_event_id` | exclusion rule audit | indexed by `(run_id, parcel_id)` |
| `score_factor_detail` | `factor_detail_id` | factor result per parcel/run/factor | unique `(run_id, parcel_id, factor_id)` |
| `score_factor_input` | `factor_input_id` | provenance input per factor | unique `(run_id, parcel_id, factor_id, input_name)` |
| `score_bonus_detail` | `bonus_detail_id` | bonus result per parcel/run/bonus | unique `(run_id, parcel_id, bonus_id)` |

## Status Domains

| Domain | Values |
| --- | --- |
| `scoring_profile_status` | `draft`, `active`, `retired` |
| `score_batch_status` | `building`, `failed`, `completed`, `active` |
| `score_run_status` | `running`, `failed`, `completed` |
| `source_snapshot_status` | `success`, `failed`, `quarantined` |
| `parcel_evaluation_status` | `prefiltered_band`, `prefiltered_size`, `pending_exclusion_check`, `pending_scoring`, `scored`, `excluded` |

## Seed Baseline

- metros: `DFW`, `AUS`, `PHX`, `LAS`
- counties: baseline county map for the four engineering metros
- sources: `PARCEL`, `ZONING`, `FLOOD`, `UTILITY`, `MARKET`
- one active scoring profile: `default_v1`

