# KIO Site Finder Delivery Controls Pack

## 1. Purpose

This delivery controls pack turns the master plan into an execution control system. It adds:

- named owner agents instead of staffing assumptions,
- dependency control between every sprint,
- milestone dates by sprint,
- governance rules intended to make the build predictable, testable, and high-confidence.

The detailed operating instructions for each delivery agent live in `agent_execution_playbook.md`.

## 2. Baseline Calendar Assumptions

- Baseline program start date: `2026-04-20`
- Sprint cadence: ten working days per sprint
- Calendar model: weekends and U.S. federal holidays are treated as non-working days
- Company shutdown weeks, procurement delays, and approval pauses are not yet modeled
- If any phase gate slips by more than five working days, the remaining sprint dates must be re-baselined
- A sprint is not complete until code, tests, documentation, and exit evidence are all present

## 3. Agent Operating Model

| Agent | Primary Ownership | Mandatory Output |
| --- | --- | --- |
| `Orchestrator Agent` | backlog sequencing, phase gating, dependency management, release readiness | approved sprint and phase closure evidence |
| `Requirements Agent` | SRS, acceptance criteria, traceability, change control | requirement baselines and trace matrix updates |
| `Architecture Agent` | SAD, SDD, ADRs, boundary decisions, review standards | architecture decisions and approved design updates |
| `Schema Agent` | database schema, migrations, reference data, data performance fundamentals | versioned migrations and schema validation evidence |
| `Ingestion Agent` | source adapters, staging, canonical loads, freshness logic | source load pipelines and source-quality checks |
| `GIS Agent` | representative-point logic, evaluation pipeline, exclusions, spatial rules | geospatial evaluation outputs and spatial validation evidence |
| `Scoring Agent` | factor scoring, bonus scoring, confidence, provenance logic | deterministic scoring outputs and scoring rationale |
| `API Agent` | batch publication, read model, search/detail APIs, exports | active-batch-safe interfaces and API verification |
| `QA Agent` | test strategy, automation, regression, acceptance certification | test packs, regression runs, and sign-off evidence |
| `Ops-Release Agent` | environments, CI/CD, observability, RBAC, cutover, hypercare | deployment readiness and operational runbooks |

## 4. Agent Assumptions For A Perfect Build

- One primary owner agent is assigned to every sprint and every critical artifact.
- A non-owner agent must verify any critical change before a phase gate closes.
- The `Architecture Agent` and `Requirements Agent` are the only agents allowed to approve requirement or boundary changes.
- The `Schema Agent` is the sole authority for migrations and database contract changes.
- The `GIS Agent` is the sole authority for representative-point logic, spatial filtering, and exclusion geometry behavior.
- The `Scoring Agent` is the sole authority for factor math, bonus math, confidence logic, and provenance shape.
- The `API Agent` is the sole authority for active-batch publication logic and user-facing read contracts.
- The `QA Agent` must maintain oracle datasets, automated acceptance checks, and regression evidence for every phase gate.
- The `Ops-Release Agent` must own environment parity, secrets handling, monitoring, and cutover rehearsal evidence.
- No agent may merge a change that alters business behavior without updating linked requirements, design notes, and tests.
- No sprint may close with undocumented assumptions; unresolved issues must be converted into decision records or explicit risks.
- Every phase gate requires dual control: builder-owner evidence plus independent `QA Agent` verification.
- Every retryable job path must be validated before the owning phase can close.
- Every production-affecting change must preserve the previously active batch until the new batch is validated.

## 5. Ownership And Control Rules

| Control Area | Owner Agent | Secondary Gate Agent |
| --- | --- | --- |
| backlog sequencing and dependencies | `Orchestrator Agent` | `Requirements Agent` |
| requirement IDs and acceptance rules | `Requirements Agent` | `QA Agent` |
| architecture changes and design exceptions | `Architecture Agent` | `Orchestrator Agent` |
| schema and migration changes | `Schema Agent` | `Architecture Agent` |
| source ingestion and freshness behavior | `Ingestion Agent` | `QA Agent` |
| geospatial filtering and exclusions | `GIS Agent` | `QA Agent` |
| scoring and confidence math | `Scoring Agent` | `QA Agent` |
| APIs, exports, and read-model behavior | `API Agent` | `QA Agent` |
| security, operations, and release readiness | `Ops-Release Agent` | `Orchestrator Agent` |
| phase and milestone sign-off | `Orchestrator Agent` | `QA Agent` |

## 6. Phase Gate Dates

| Gate | Date | Meaning |
| --- | --- | --- |
| `G1` | `2026-09-10` | Phase 1 complete |
| `G2` | `2027-02-05` | Phase 2 complete |
| `G3` | `2027-06-30` | Phase 3 complete |
| `G4` | `2027-11-23` | Phase 4 complete |
| `G5` | `2028-04-18` | Phase 5 complete |
| `G6` | `2028-09-11` | Phase 6 complete |
| `G7` | `2029-02-06` | Phase 7 complete |
| `G8` | `2029-06-29` | Phase 8 complete and program close |

## 7. Sprint Control Calendar

### 7.1 Phase 1 Controls

| Sprint | Start | End | Owner Agent | Depends On | Milestone / Exit Artifact |
| --- | --- | --- | --- | --- | --- |
| `P1-S01` | `2026-04-20` | `2026-05-01` | `Orchestrator Agent` | `None` | Rewrite charter and baseline backlog approved |
| `P1-S02` | `2026-05-04` | `2026-05-15` | `Orchestrator Agent` | `P1-S01` | Governance model, RACI, and decision cadence approved |
| `P1-S03` | `2026-05-18` | `2026-06-01` | `Requirements Agent` | `P1-S01, P1-S02` | Metro scope and source-scope boundary approved |
| `P1-S04` | `2026-06-02` | `2026-06-15` | `Ops-Release Agent` | `P1-S01, P1-S02` | Repo standards and CI skeleton operational |
| `P1-S05` | `2026-06-16` | `2026-06-30` | `Ops-Release Agent` | `P1-S04` | Environment and platform topology approved |
| `P1-S06` | `2026-07-01` | `2026-07-15` | `Requirements Agent` | `P1-S03, P1-S05` | Requirement baseline and acceptance IDs signed off |
| `P1-S07` | `2026-07-16` | `2026-07-29` | `Architecture Agent` | `P1-S06` | Initial architecture baseline signed off |
| `P1-S08` | `2026-07-30` | `2026-08-12` | `QA Agent` | `P1-S06, P1-S07` | Traceability matrix and test strategy approved |
| `P1-S09` | `2026-08-13` | `2026-08-26` | `Orchestrator Agent` | `P1-S02, P1-S08` | RAID, release controls, and escalation model active |
| `P1-S10` | `2026-08-27` | `2026-09-10` | `Orchestrator Agent` | `P1-S06, P1-S07, P1-S08, P1-S09` | Phase 1 gate approved |

### 7.2 Phase 2 Controls

| Sprint | Start | End | Owner Agent | Depends On | Milestone / Exit Artifact |
| --- | --- | --- | --- | --- | --- |
| `P2-S01` | `2026-09-11` | `2026-09-24` | `Architecture Agent` | `P1-S10` | Logical data model approved |
| `P2-S02` | `2026-09-25` | `2026-10-08` | `Schema Agent` | `P2-S01, P1-S06` | Normalized scoring-profile model approved |
| `P2-S03` | `2026-10-09` | `2026-10-23` | `Schema Agent` | `P2-S01, P1-S07` | Canonical parcel schema approved |
| `P2-S04` | `2026-10-26` | `2026-11-06` | `Schema Agent` | `P2-S01, P1-S07` | Batch and run lifecycle schema validated |
| `P2-S05` | `2026-11-09` | `2026-11-23` | `Schema Agent` | `P2-S03, P2-S04` | Evaluation and exclusion schema validated |
| `P2-S06` | `2026-11-24` | `2026-12-08` | `Schema Agent` | `P2-S02, P2-S04, P2-S05` | Score detail and provenance schema validated |
| `P2-S07` | `2026-12-09` | `2026-12-22` | `GIS Agent` | `P2-S03` | Representative-point and spatial index design approved |
| `P2-S08` | `2026-12-23` | `2027-01-07` | `Schema Agent` | `P2-S02, P2-S06, P2-S07` | Migration framework and seed-data scripts operational |
| `P2-S09` | `2027-01-08` | `2027-01-22` | `QA Agent` | `P2-S08` | Migration rehearsal and rollback tests pass |
| `P2-S10` | `2027-01-25` | `2027-02-05` | `Architecture Agent` | `P2-S09` | Data dictionary, ERDs, and Phase 2 gate approved |

### 7.3 Phase 3 Controls

| Sprint | Start | End | Owner Agent | Depends On | Milestone / Exit Artifact |
| --- | --- | --- | --- | --- | --- |
| `P3-S01` | `2027-02-08` | `2027-02-22` | `Ingestion Agent` | `P2-S10, P1-S04` | Shared ingestion framework operational |
| `P3-S02` | `2027-02-23` | `2027-03-08` | `Ingestion Agent` | `P3-S01, P2-S03` | Parcel source ingestion validated |
| `P3-S03` | `2027-03-09` | `2027-03-22` | `Ingestion Agent` | `P3-S01, P2-S03` | Zoning and land-use ingestion validated |
| `P3-S04` | `2027-03-23` | `2027-04-05` | `Ingestion Agent` | `P3-S01, P2-S10` | Blocking-source wave 1 validated |
| `P3-S05` | `2027-04-06` | `2027-04-19` | `Ingestion Agent` | `P3-S04` | Blocking-source wave 2 validated |
| `P3-S06` | `2027-04-20` | `2027-05-03` | `Ingestion Agent` | `P3-S01, P2-S10` | Utility and hosting-capacity feeds available |
| `P3-S07` | `2027-05-04` | `2027-05-17` | `Ingestion Agent` | `P3-S01, P2-S10` | Market and proxy datasets available |
| `P3-S08` | `2027-05-18` | `2027-06-01` | `Ingestion Agent` | `P3-S02, P3-S04, P3-S06` | Quarantine and source-quality workflow proven |
| `P3-S09` | `2027-06-02` | `2027-06-15` | `Ingestion Agent` | `P3-S04, P3-S05, P3-S08` | Freshness gate passes core tests |
| `P3-S10` | `2027-06-16` | `2027-06-30` | `QA Agent` | `P3-S03, P3-S08, P3-S09` | Cross-source integration and Phase 3 gate approved |

### 7.4 Phase 4 Controls

| Sprint | Start | End | Owner Agent | Depends On | Milestone / Exit Artifact |
| --- | --- | --- | --- | --- | --- |
| `P4-S01` | `2027-07-01` | `2027-07-15` | `GIS Agent` | `P3-S10` | Metro and county scoping service validated |
| `P4-S02` | `2027-07-16` | `2027-07-29` | `GIS Agent` | `P4-S01` | One evaluation row per in-scope parcel proven |
| `P4-S03` | `2027-07-30` | `2027-08-12` | `GIS Agent` | `P2-S07, P4-S02` | Representative-point band filter validated |
| `P4-S04` | `2027-08-13` | `2027-08-26` | `GIS Agent` | `P4-S02` | Size and threshold prefilters validated |
| `P4-S05` | `2027-08-27` | `2027-09-10` | `GIS Agent` | `P4-S02, P3-S10` | Exclusion rule wave 1 validated |
| `P4-S06` | `2027-09-13` | `2027-09-24` | `GIS Agent` | `P4-S05` | Exclusion rule wave 2 validated |
| `P4-S07` | `2027-09-27` | `2027-10-08` | `GIS Agent` | `P4-S01, P4-S02` | Deprecated candidate view fully removed |
| `P4-S08` | `2027-10-12` | `2027-10-25` | `GIS Agent` | `P4-S03, P4-S06, P4-S07` | Evaluation reruns and restart checkpoints proven |
| `P4-S09` | `2027-10-26` | `2027-11-08` | `QA Agent` | `P4-S08` | Evaluation reconciliation reports approved |
| `P4-S10` | `2027-11-09` | `2027-11-23` | `QA Agent` | `P4-S03, P4-S06, P4-S09` | End-to-end evaluation acceptance and Phase 4 gate approved |

### 7.5 Phase 5 Controls

| Sprint | Start | End | Owner Agent | Depends On | Milestone / Exit Artifact |
| --- | --- | --- | --- | --- | --- |
| `P5-S01` | `2027-11-24` | `2027-12-08` | `Scoring Agent` | `P4-S10, P2-S10` | Scoring runtime framework operational |
| `P5-S02` | `2027-12-09` | `2027-12-22` | `Scoring Agent` | `P5-S01, P1-S07` | Factor framework and rule hooks approved |
| `P5-S03` | `2027-12-23` | `2028-01-07` | `Scoring Agent` | `P5-S02, P3-S10` | Factor wave 1 regression passes |
| `P5-S04` | `2028-01-10` | `2028-01-24` | `Scoring Agent` | `P5-S03` | Factor wave 2 regression passes |
| `P5-S05` | `2028-01-25` | `2028-02-07` | `Scoring Agent` | `P5-S04` | Factor wave 3 regression passes |
| `P5-S06` | `2028-02-08` | `2028-02-22` | `Scoring Agent` | `P5-S02` | Bonus engine and cap logic validated |
| `P5-S07` | `2028-02-23` | `2028-03-07` | `Scoring Agent` | `P5-S05, P5-S06` | Confidence model validated |
| `P5-S08` | `2028-03-08` | `2028-03-21` | `Scoring Agent` | `P5-S05, P5-S07` | Provenance and explanation outputs approved |
| `P5-S09` | `2028-03-22` | `2028-04-04` | `QA Agent` | `P5-S08` | Oracle datasets and scoring regression pack pass |
| `P5-S10` | `2028-04-05` | `2028-04-18` | `QA Agent` | `P5-S06, P5-S07, P5-S09` | Scoring certification and Phase 5 gate approved |

### 7.6 Phase 6 Controls

| Sprint | Start | End | Owner Agent | Depends On | Milestone / Exit Artifact |
| --- | --- | --- | --- | --- | --- |
| `P6-S01` | `2028-04-19` | `2028-05-02` | `API Agent` | `P5-S10, P3-S09` | Batch orchestrator core proven in integration |
| `P6-S02` | `2028-05-03` | `2028-05-16` | `API Agent` | `P6-S01` | Failure and retry pathways validated |
| `P6-S03` | `2028-05-17` | `2028-05-31` | `API Agent` | `P6-S01, P2-S04` | Active-batch read model prevents mixed data exposure |
| `P6-S04` | `2028-06-01` | `2028-06-14` | `API Agent` | `P6-S03, P5-S10` | Parcel search API approved |
| `P6-S05` | `2028-06-15` | `2028-06-29` | `API Agent` | `P6-S03, P5-S10` | Parcel detail API approved |
| `P6-S06` | `2028-06-30` | `2028-07-14` | `API Agent` | `P6-S04, P6-S05` | Export and reporting interfaces validated |
| `P6-S07` | `2028-07-17` | `2028-07-28` | `API Agent` | `P6-S06` | Cross-metro and border-corridor logic validated |
| `P6-S08` | `2028-07-31` | `2028-08-11` | `API Agent` | `P6-S01, P6-S03` | Activation history and rollback controls approved |
| `P6-S09` | `2028-08-14` | `2028-08-25` | `API Agent` | `P6-S03, P6-S04, P6-S05` | Read-model performance target met |
| `P6-S10` | `2028-08-28` | `2028-09-11` | `QA Agent` | `P6-S06, P6-S07, P6-S09` | API and activation integration suite passes and Phase 6 gate approved |

### 7.7 Phase 7 Controls

| Sprint | Start | End | Owner Agent | Depends On | Milestone / Exit Artifact |
| --- | --- | --- | --- | --- | --- |
| `P7-S01` | `2028-09-12` | `2028-09-25` | `Ops-Release Agent` | `P6-S10` | Admin monitoring dashboard available |
| `P7-S02` | `2028-09-26` | `2028-10-10` | `Ops-Release Agent` | `P7-S01` | Operator retry, cancel, and rerun controls validated |
| `P7-S03` | `2028-10-11` | `2028-10-24` | `Ops-Release Agent` | `P1-S05, P6-S10` | Authentication and RBAC controls pass |
| `P7-S04` | `2028-10-25` | `2028-11-07` | `Ops-Release Agent` | `P1-S04, P6-S10` | Logging, tracing, and alerts approved |
| `P7-S05` | `2028-11-08` | `2028-11-22` | `Ops-Release Agent` | `P7-S01, P7-S04` | Audit evidence and export packages validated |
| `P7-S06` | `2028-11-24` | `2028-12-07` | `Ops-Release Agent` | `P7-S02, P7-S04, P7-S05` | Runbooks and support SOPs approved |
| `P7-S07` | `2028-12-08` | `2028-12-21` | `Ops-Release Agent` | `P7-S03, P7-S04, P7-S06` | UAT environment and scenario packs ready |
| `P7-S08` | `2028-12-22` | `2029-01-08` | `QA Agent` | `P7-S07` | UAT wave 1 completed with defect log |
| `P7-S09` | `2029-01-09` | `2029-01-23` | `QA Agent` | `P7-S08` | UAT wave 2 completed and priority defects closed |
| `P7-S10` | `2029-01-24` | `2029-02-06` | `Orchestrator Agent` | `P7-S03, P7-S05, P7-S06, P7-S09` | Release candidate gate approved |

### 7.8 Phase 8 Controls

| Sprint | Start | End | Owner Agent | Depends On | Milestone / Exit Artifact |
| --- | --- | --- | --- | --- | --- |
| `P8-S01` | `2029-02-07` | `2029-02-21` | `QA Agent` | `P7-S10, P5-S10, P6-S10` | Full regression and reconciliation pass |
| `P8-S02` | `2029-02-22` | `2029-03-07` | `QA Agent` | `P8-S01` | Volume and performance evidence approved |
| `P8-S03` | `2029-03-08` | `2029-03-21` | `Ops-Release Agent` | `P7-S06, P8-S01` | Backup, restore, and DR rehearsal succeed |
| `P8-S04` | `2029-03-22` | `2029-04-04` | `Ops-Release Agent` | `P7-S03, P7-S04` | Security hardening and sign-off complete |
| `P8-S05` | `2029-04-05` | `2029-04-18` | `Ops-Release Agent` | `P8-S01, P8-S03, P8-S04` | Cutover rehearsal 1 passed |
| `P8-S06` | `2029-04-19` | `2029-05-02` | `Ops-Release Agent` | `P8-S05` | Cutover rehearsal 2 passed |
| `P8-S07` | `2029-05-03` | `2029-05-16` | `Ops-Release Agent` | `P8-S06, P7-S10` | Go-live approval and production checklist complete |
| `P8-S08` | `2029-05-17` | `2029-05-31` | `Ops-Release Agent` | `P8-S07` | Production deployment and first active batch complete |
| `P8-S09` | `2029-06-01` | `2029-06-14` | `Ops-Release Agent` | `P8-S08` | Hypercare stabilization exit criteria met |
| `P8-S10` | `2029-06-15` | `2029-06-29` | `Orchestrator Agent` | `P8-S09` | Program closure and Phase 2 transition approved |

## 8. Critical Dependency Chains

- Governance chain: `P1-S01 -> P1-S06 -> P1-S07 -> P1-S10`
- Data foundation chain: `P2-S01 -> P2-S06 -> P2-S08 -> P2-S10`
- Source readiness chain: `P3-S01 -> P3-S04 -> P3-S08 -> P3-S09 -> P3-S10`
- Evaluation chain: `P4-S01 -> P4-S02 -> P4-S05 -> P4-S08 -> P4-S10`
- Scoring chain: `P5-S01 -> P5-S02 -> P5-S05 -> P5-S07 -> P5-S10`
- Publication chain: `P6-S01 -> P6-S03 -> P6-S06 -> P6-S10`
- Operational readiness chain: `P7-S01 -> P7-S04 -> P7-S07 -> P7-S10`
- Launch chain: `P8-S01 -> P8-S05 -> P8-S07 -> P8-S08 -> P8-S10`

## 9. Perfect-Build Control Checklist

- No sprint closes without updated linked requirements, design notes, and test evidence.
- No phase closes without a passing regression subset owned by the `QA Agent`.
- No data-contract change merges without `Schema Agent` ownership and `Architecture Agent` review.
- No scoring-rule change merges without `Scoring Agent` ownership and regression evidence.
- No geospatial-rule change merges without `GIS Agent` validation and representative-point proof.
- No publication change merges unless active-batch isolation tests pass.
- No operational readiness claim is accepted without runbooks, alerts, and recovery drills.
- No launch approval is granted without two successful cutover rehearsals and one clean regression pass.
