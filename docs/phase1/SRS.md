# KIO Site Finder Phase 1 Software Requirements Specification

## 1. Purpose

This Software Requirements Specification defines the Phase 1 requirements for the clean rewrite of KIO Site Finder. The goal is to deliver an auditable, batch-safe, source-aware parcel scoring platform that replaces the inconsistent v1.3-v1.5 patch stack with one authoritative implementation baseline.

## 2. Scope

Phase 1 covers:

- canonical ingestion of approved parcel, zoning, environmental, utility, and market-supporting sources,
- metro-scoped parcel evaluation and scoring,
- source freshness gating,
- active-batch-based data publication,
- score explanation, provenance, and confidence,
- operator visibility, audit, and controlled reporting APIs.

Phase 1 does not cover:

- unsupported metros such as Brownsville until source coverage is formally added,
- consumer-grade public portal capabilities,
- machine-learning-based score optimization,
- automated redefinition of business factor semantics beyond the approved factor catalog.

## 3. Product Overview

KIO Site Finder evaluates raw parcels within approved metro counties, applies pre-score filtering and exclusion rules, calculates factor and bonus scores, computes confidence, and publishes only fully completed metro batches to user-facing read models.

## 4. Stakeholders and Users

- Business sponsor: approves scoring policy, launch readiness, and metro scope
- Product owner: owns backlog, acceptance, and release scope
- Data engineering team: owns ingestion, quality, and canonical data stores
- Backend engineering team: owns scoring services, APIs, and orchestration
- GIS/data analysts: validate parcel, zoning, and geospatial logic
- Operations users: monitor runs, diagnose failures, and trigger reruns
- Read-only business users: search parcels, inspect scores, and export results

## 5. Assumptions and Dependencies

- Approved factor IDs `F01-F10` and bonus IDs `B01-B05` already exist in the business scoring catalog.
- Phase 1 sources are explicitly listed in the source catalog and mapped to interface definitions.
- PostgreSQL with PostGIS is the system of record for Phase 1 scoring data.
- Only one authoritative active batch may be exposed to user-facing reads at any time.

## 6. Functional Requirements

### 6.1 Source Catalog and Freshness

- `FR-001`: The system shall maintain a source catalog containing source ID, owner, refresh cadence, block-refresh flag, metro coverage, and target storage objects.
- `FR-002`: The system shall log every source ingestion as a source snapshot with load time, source version, row counts, checksum, and status.
- `FR-003`: The system shall evaluate freshness per source and metro before starting a score run.
- `FR-004`: The system shall block a score run when any required source marked `block_refresh = TRUE` is stale, missing, or errored.
- `FR-005`: The system shall record the reason a freshness gate failed and expose it to operator users.
- `FR-006`: The system shall support source quarantine for malformed loads without deleting prior successful source snapshots.

### 6.2 Parcel Evaluation Pipeline

- `FR-007`: The system shall resolve each score run to an approved metro and its associated counties.
- `FR-008`: The system shall evaluate all raw parcels in scope and write one `parcel_evaluations` row per parcel per score run.
- `FR-009`: The system shall support parcel statuses `prefiltered_band`, `prefiltered_size`, `pending_exclusion_check`, `pending_scoring`, `scored`, and `excluded`.
- `FR-010`: The system shall apply representative-point geospatial logic using persisted `rep_point` geometries.
- `FR-011`: The system shall record one or more exclusion events for parcels rejected by exclusion rules.
- `FR-012`: The system shall not depend on `candidate_parcels_v` or any equivalent deprecated candidate view.
- `FR-013`: The system shall support idempotent reruns of evaluation stages for the same score run.
- `FR-014`: The system shall verify that no parcel remains in a pending state when a score run is marked completed.

### 6.3 Scoring and Confidence

- `FR-015`: The system shall compute `V_base` as the sum of factor scores for `F01-F10`.
- `FR-016`: The system shall enforce that the active scoring profile allocates factor budgets summing to 100 points.
- `FR-017`: The system shall compute `V_final` as the base score plus bonuses `B01-B05`, capped at 100.
- `FR-018`: The system shall write exactly one factor-detail row per parcel, score run, and factor ID.
- `FR-019`: The system shall write exactly one bonus-detail row per parcel, score run, and bonus ID, including rows where the bonus is not applied.
- `FR-020`: The system shall write one or more factor-input provenance rows when a factor uses multiple evidence inputs.
- `FR-021`: The system shall enforce uniqueness on factor-input provenance to prevent duplicate writes on retries.
- `FR-022`: The system shall prioritize direct evidence over proxy evidence when both are available for the same scoring decision.
- `FR-023`: The system shall compute confidence using configured evidence quality weights and store the resulting score with each scored parcel.

### 6.4 Batch and Publication Semantics

- `FR-024`: The system shall create one `score_batch` for a system-wide refresh attempt and one `score_run` per metro.
- `FR-025`: The system shall support score run statuses `running`, `failed`, and `completed`.
- `FR-026`: The system shall activate a batch only after all required metro runs complete successfully.
- `FR-027`: The system shall expose parcel search and reporting data only from the latest activated batch.
- `FR-028`: The system shall preserve historical batches and their activation history for audit purposes.

### 6.5 Reporting and Interfaces

- `FR-029`: The system shall provide an API to search scored parcels within the active batch.
- `FR-030`: The system shall provide an API to retrieve parcel score detail, factor detail, bonus detail, confidence, and provenance summary.
- `FR-031`: The system shall provide an operator API or admin view for run status, source freshness, and batch activation history.
- `FR-032`: The system shall support export of parcel scoring results and audit metadata for approved users.
- `FR-033`: The system shall support cross-metro reporting using one activated batch context only.

### 6.6 Security and Administration

- `FR-034`: The system shall authenticate users through the enterprise identity provider used by KIO.
- `FR-035`: The system shall authorize actions using role-based access control with at least `admin`, `operator`, and `reader` roles.
- `FR-036`: The system shall audit all administrative actions that change run state, retry jobs, or activate batches.

## 7. Data Requirements

- `DR-001`: The system shall store canonical parcel identifiers, geometry, county, metro, acreage, and source lineage.
- `DR-002`: The system shall store one persisted representative point per parcel for evaluation predicates.
- `DR-003`: The system shall store factor catalog and bonus catalog reference data as normalized reference tables.
- `DR-004`: The system shall store scoring profile versions and effective dates.
- `DR-005`: The system shall store all run, batch, and source records with immutable creation timestamps.
- `DR-006`: The system shall retain historical score outputs and provenance for audit and reproducibility.

## 8. Nonfunctional Requirements

- `NFR-001`: The system shall support pilot-metro scoring completion within the daily batch window approved by operations.
- `NFR-002`: The system shall maintain transactional integrity for score writes and batch activation updates.
- `NFR-003`: The system shall prevent users from seeing mixed old/new metro results during a system refresh.
- `NFR-004`: The system shall provide deterministic scoring results given the same source snapshots and scoring profile.
- `NFR-005`: The system shall log all failures with enough context for root-cause analysis.
- `NFR-006`: The system shall support rollback to the previously active batch if a newly built batch fails validation before activation.
- `NFR-007`: The system shall encrypt secrets and sensitive configuration at rest and in transit.
- `NFR-008`: The system shall provide monitoring and alerts for ingestion failures, stale sources, failed runs, and failed activations.
- `NFR-009`: The system shall be maintainable through modular components, documented interfaces, and versioned database migrations.
- `NFR-010`: The system shall support automated tests for unit, integration, data quality, and end-to-end acceptance scenarios.
- `NFR-011`: The system shall expose lineage and evidence traceability sufficient for audit review.
- `NFR-012`: The system shall preserve availability of the active read model during new batch processing.

## 9. External Interface Requirements

### 9.1 Source Interfaces

- Source adapters shall load provider data into staging without mutating prior successful loads.
- Each source interface shall specify expected schema, cadence, ownership, and validation rules.

### 9.2 Database Interface

- PostgreSQL/PostGIS shall be the authoritative scoring and read-store platform for Phase 1.
- All schema changes shall be deployed through versioned migrations.

### 9.3 Application Interfaces

- Internal batch orchestration shall call ingestion, evaluation, scoring, and validation services through authenticated service interfaces.
- User-facing APIs shall be read-only against the active batch except for authorized admin operations.

## 10. Acceptance Criteria Summary

Phase 1 is accepted when:

- all approved sources load and pass freshness validation,
- completed score runs contain no pending parcels,
- every scored parcel has exactly ten factor rows and five bonus rows,
- users can query only the latest activated batch,
- operator and audit functions are available,
- regression, integration, and launch-readiness tests pass.
