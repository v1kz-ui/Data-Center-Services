# KIO Site Finder Master Phase Summary

## Planning Assumptions

- The clean rewrite program is derived from the v1.6 audit memo.
- The authoritative master plan uses eight delivery phases with ten full sprints per phase.
- Each sprint is planned as ten business days.
- This document is the executive phase summary; the sprint-by-sprint breakdown lives in `master_delivery_plan.md`, the execution controls live in `delivery_controls_pack.md`, and the per-agent operating model lives in `agent_execution_playbook.md`.
- Brownsville remains out of scope until parcel, zoning, and source catalog coverage exist.
- The business definitions of factors `F01-F10` and bonuses `B01-B05` are inherited from the existing scoring catalog; Phase 1 standardizes the mechanics, controls, and traceability around them.

## Program Objective

Deliver a production-ready rewrite of KIO Site Finder that:

- replaces patch-stack ambiguity with one authoritative implementation,
- uses activated batch semantics instead of per-metro partial refreshes,
- blocks scoring when critical sources are stale,
- stores full scoring provenance and confidence,
- removes legacy dependency on `candidate_parcels_v`, and
- supports auditable parcel evaluation, scoring, and reporting for the approved pilot metros.

## Delivery Model

- Total duration: 80 sprints
- Sprint length: 10 business days
- Recommended agent set: `Orchestrator Agent`, `Requirements Agent`, `Architecture Agent`, `Schema Agent`, `Ingestion Agent`, `GIS Agent`, `Scoring Agent`, `API Agent`, `QA Agent`, `Ops-Release Agent`
- Definition of done for every sprint:
  - code complete,
  - reviewed architecture and schema changes,
  - updated documentation,
  - traceability to requirements,
  - automated tests passing for the sprint scope.

## Phase 1: Scope Closure and Delivery Foundation

### Goal

Turn the audit memo into one executable backlog, one delivery baseline, and one authoritative Phase 1 scope.

### Backlog Items

1. Consolidate the v1.6 audit decisions into a product backlog and decision register.
2. Confirm pilot metros and remove any references to unsupported metros.
3. Publish the initial source catalog and interface ownership list.
4. Stand up the repository structure, branching policy, and CI pipeline skeleton.
5. Define environment strategy for local, integration, staging, and production.
6. Create the traceability matrix linking requirements to design, code, and tests.
7. Define the canonical run statuses, parcel statuses, and failure codes.
8. Create the initial release plan, RAID log, and governance calendar.
9. Draft seed acceptance criteria for batch, freshness, scoring, and auditability.
10. Run a sprint review with architecture sign-off for the rewrite baseline.

### Exit Criteria

- Approved Phase 1 scope baseline
- Approved delivery plan and traceability structure
- CI scaffold and delivery operating model in place

## Phase 2: Data Model Rewrite

### Goal

Implement the canonical schema that removes invalid validation patterns and supports the batch-safe pipeline.

### Backlog Items

1. Create `score_batch` and `score_run` schemas with explicit lifecycle states.
2. Replace JSON budget validation with normalized `scoring_profile_factor`.
3. Define `factor_catalog` and `bonus_catalog` seed structures for `F01-F10` and `B01-B05`.
4. Define `parcel_evaluations` and `parcel_exclusion_events`.
5. Define `score_factor_detail`, `score_factor_input`, and `score_bonus_detail`.
6. Add uniqueness and foreign key rules for provenance and cardinality safety.
7. Partition `raw_parcels` by `county_fips` or another query-aligned key.
8. Persist and index `rep_point` for representative-point geospatial predicates.
9. Create migration scripts, rollback scripts, and schema versioning controls.
10. Complete schema review, data dictionary review, and DBA sign-off.

### Exit Criteria

- Canonical schema approved and migration-ready
- Data dictionary published
- All invalid v1.5 modeling patterns removed

## Phase 3: Ingestion and Freshness Controls

### Goal

Build source-aware ingestion and the freshness gate that can stop scoring before bad runs occur.

### Backlog Items

1. Create `source_catalog`, `source_interface`, and `source_snapshot` tables.
2. Build ingestion adapters for each approved Phase 1 source.
3. Implement source load audit logging with row counts and checksum metadata.
4. Implement freshness policy evaluation by metro and source.
5. Enforce `block_refresh` behavior before scoring starts.
6. Add ingestion validation for geometry, mandatory attributes, and county mapping.
7. Build quarantine handling for failed or malformed source loads.
8. Create source health dashboards and alert thresholds.
9. Add scheduler hooks and re-run controls for ingestion jobs.
10. Execute integration tests covering stale, missing, and corrected source states.

### Exit Criteria

- All Phase 1 sources can be loaded into staging/canonical tables
- Freshness gate implemented and testable
- Source quality telemetry visible to operators

## Phase 4: Candidate Evaluation and Exclusion Pipeline

### Goal

Score every parcel through a controlled evaluation state machine before scoring begins.

### Backlog Items

1. Implement metro-to-county resolution for every score run.
2. Evaluate all raw parcels in scope and write initial `parcel_evaluations` rows.
3. Apply representative-point band filtering for inclusion corridor logic.
4. Apply parcel size filtering and write prefiltered statuses.
5. Apply hard exclusions and store exclusion event detail.
6. Promote survivors to `pending_scoring` only after exclusion checks complete.
7. Remove the legacy `candidate_parcels_v` dependency from all workflows.
8. Add idempotent re-run behavior for evaluation stages.
9. Add metrics for evaluated, filtered, excluded, and promoted parcel counts.
10. Demonstrate end-to-end evaluation for one pilot metro in staging.

### Exit Criteria

- Evaluation pipeline writes all required statuses and audit events
- No dependency remains on the deprecated candidate view
- Pipeline can be re-run safely

## Phase 5: Scoring, Confidence, and Provenance

### Goal

Implement deterministic factor scoring, bonus scoring, confidence scoring, and scoring provenance.

### Backlog Items

1. Implement the single-pass base score calculation across `F01-F10`.
2. Implement additive bonus handling across `B01-B05` with a 100-point cap.
3. Implement confidence weights for measured, manual, proxy, heuristic default, and missing evidence.
4. Enforce direct-evidence precedence over proxy evidence where both exist.
5. Write exactly one `score_factor_detail` record per factor per scored parcel.
6. Write one or more `score_factor_input` rows per applicable factor input with uniqueness constraints.
7. Write exactly one `score_bonus_detail` row per bonus, including `applied = FALSE` records.
8. Compute and store `viability_score`, `confidence_score`, and score rationale.
9. Add completion invariants for factor/bonus cardinality and no pending states.
10. Run scoring regression tests against seeded reference scenarios.

### Exit Criteria

- Deterministic scoring and confidence engine implemented
- Provenance is complete and duplicate-safe
- Cardinality checks pass for scored parcels

## Phase 6: Batch Activation and Read Model

### Goal

Make the system visible to users only through completed, activated batches and supported read APIs.

### Backlog Items

1. Implement batch aggregation and activation rules across all required metros.
2. Prevent read models from exposing partially refreshed batches.
3. Build active-batch-backed views and materialized query tables.
4. Implement parcel search, scored parcel detail, and batch status APIs.
5. Implement cross-metro reporting using activated batch semantics only.
6. Rework any border corridor or comparative queries to use a single active batch.
7. Implement failed-batch handling and operator recovery workflow.
8. Add audit endpoints for run status, freshness status, and batch activation history.
9. Tune indexes and query plans for pilot-metro scale.
10. Complete integration testing for hybrid-batch prevention and active-batch reads.

### Exit Criteria

- No user query can see a mixed old/new metro dataset
- Batch activation is auditable
- Read APIs support the Phase 1 reporting scope

## Phase 7: Operations, Admin, and User Acceptance Readiness

### Goal

Prepare the system for controlled user validation, operational support, and defect triage.

### Backlog Items

1. Build an admin dashboard for source health, runs, and batch activation.
2. Build operator controls for retry, cancel, and rerun actions.
3. Expose parcel score explanations and provenance in the admin experience.
4. Add role-based access control for operator and read-only users.
5. Add structured logging, tracing, and alerting for all major pipeline stages.
6. Publish runbooks for ingestion failure, stale sources, and failed batch recovery.
7. Set up UAT environments and seeded sample data.
8. Train business users on run status, score interpretation, and audit exports.
9. Triage defects from UAT and fold fixes back into regression testing.
10. Lock the Phase 1 release candidate scope and cut UAT sign-off.

### Exit Criteria

- UAT users can exercise the system with support materials
- Admin and operator workflows exist
- Release candidate is stabilized

## Phase 8: Hardening, Cutover, and Launch

### Goal

Finish hardening, validate production readiness, and execute the Phase 1 launch.

### Backlog Items

1. Execute the full end-to-end regression suite for all Phase 1 scenarios.
2. Complete data reconciliation against approved sample outputs.
3. Execute performance and volume tests for pilot-metro throughput targets.
4. Run failover, recovery, and restart drills for batch and ingestion services.
5. Validate backup, restore, and audit-retention procedures.
6. Complete security review, secrets rotation, and least-privilege validation.
7. Approve production cutover checklist and rollback decision tree.
8. Execute production deployment and initial controlled batch run.
9. Monitor launch metrics and resolve launch defects within hypercare.
10. Close Phase 1 with metrics, lessons learned, and Phase 2 recommendations.

### Exit Criteria

- Production deployment completed
- Initial active batch published successfully
- Hypercare and Phase 2 backlog agreed

## Program Milestones

| Milestone | Target Sprint | Outcome |
| --- | --- | --- |
| M1 | Phase 2 | Canonical schema and delivery baseline approved |
| M2 | Phase 4 | Evaluation pipeline functional in staging |
| M3 | Phase 6 | Activated-batch read model operational |
| M4 | Phase 8 | Production go-live and hypercare |

## Key Risks and Mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Source volatility or missing coverage | Blocks scoring or reduces confidence | Enforce source catalog governance and freshness gates |
| Geospatial data inconsistencies | Causes false exclusions or wrong parcel scoring | Validate geometry, county mapping, and representative-point generation |
| Mixed-batch exposure | Users see inconsistent cross-metro results | Enforce `score_batch` activation and active-batch-only reads |
| Duplicate provenance writes on retry | Inflates evidence history and breaks audits | Add uniqueness constraints and upsert logic |
| Scope creep from legacy patch stack | Delays delivery and reintroduces contradictions | Maintain a single authoritative backlog and spec set |
