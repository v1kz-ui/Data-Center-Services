# KIO Site Finder Master Delivery Plan

## 1. Planning Model

- Program structure: 8 phases
- Sprint structure: 10 full sprints per phase
- Sprint duration: 10 business days
- Total planned sprints: 80
- Total planned delivery length: 800 business days, not including contingency buffers or organizational hold periods
- Planning method: rolling-wave planning with detailed sprint planning inside each phase and re-baselining at each phase gate
- Authoritative references: `SRS.md`, `SAD.md`, `SDD.md`, `test_cases.md`, `test_scripts.md`, `delivery_controls_pack.md`, and `agent_execution_playbook.md`

## 2. Master Delivery Objective

Deliver a production-grade KIO Site Finder rewrite that is operationally safe, architecturally coherent, auditable at parcel level, and publishable only through completed activated batches.

## 3. Phase Map

| Phase | Name | Primary Outcome |
| --- | --- | --- |
| 1 | Program Mobilization and Governance | Approved scope, backlog, architecture baseline, governance, and delivery controls |
| 2 | Canonical Data and Schema Foundation | Production-ready schema, migrations, partitioning, and baseline reference data |
| 3 | Source Ingestion and Freshness | Canonical source loads, freshness enforcement, and source-quality controls |
| 4 | Parcel Evaluation and Exclusions | Repeatable metro evaluation pipeline with auditable filtering and exclusion handling |
| 5 | Scoring, Confidence, and Provenance | Deterministic factor/bonus scoring with evidence traceability and confidence |
| 6 | Batch Activation, Read Model, and APIs | Active-batch publication, user APIs, and reporting interfaces |
| 7 | Operations, Security, and UAT | Operator tooling, support readiness, RBAC, observability, and UAT sign-off |
| 8 | Hardening, Cutover, Launch, and Closure | Performance, security, launch execution, hypercare, and Phase 2 transition |

## 4. Cross-Phase Governance Workstreams

- Product management: backlog ownership, change control, acceptance, and release scope
- Architecture: design reviews, schema governance, integration decisions, and technical debt control
- QA and traceability: requirement mapping, test automation, regression growth, and acceptance evidence
- DevOps and platform: environments, CI/CD, deployment automation, observability, and recovery readiness
- Data governance: source ownership, data contracts, quality thresholds, and catalog stewardship
- Change management: training, runbooks, release readiness, and business adoption

## 5. Phase 1: Program Mobilization and Governance

**Objective**

Create the delivery machine: scope, decisions, governance, environments, requirements baseline, and execution controls.

**Entry Gate**

- Audit findings accepted as the rewrite baseline
- Sponsor approval to proceed with the clean rewrite

**Sprint Plan**

| Sprint | Focus | Key Build Outcomes | Exit Gate |
| --- | --- | --- | --- |
| `P1-S01` | Kickoff and baseline reset | Convert the audit memo into a single program backlog, identify unresolved decisions, and define the rewrite charter | Sponsor and product owner approve the charter |
| `P1-S02` | Stakeholder alignment | Publish stakeholder map, RACI, decision cadence, and phase review structure | Governance calendar and ownership matrix approved |
| `P1-S03` | Scope and metro closure | Confirm pilot metros, remove unsupported scope, and lock the initial source-coverage envelope | Phase 1 scope baseline approved |
| `P1-S04` | Repository and SDLC setup | Stand up repo standards, branching rules, PR templates, CI skeleton, and coding conventions | Delivery toolchain operational |
| `P1-S05` | Environment and platform strategy | Define local, integration, staging, and production patterns plus secrets and configuration approach | Platform topology approved |
| `P1-S06` | Requirement baseline | Publish first-pass SRS, acceptance criteria, and requirement IDs tied to the audit corrections | Requirements baseline signed off |
| `P1-S07` | Architectural baseline | Publish initial SAD and major architecture decisions for batch, scoring, and geospatial design | Architecture review board sign-off |
| `P1-S08` | QA and traceability foundation | Build requirement-to-design-to-test trace matrix and draft the core test strategy | Traceability model approved |
| `P1-S09` | Release and risk controls | Create RAID log, release governance, dependency register, and escalation model | Delivery controls active |
| `P1-S10` | Phase gate review | Run integrated planning review, refine dependencies, and freeze the mobilization outputs | Phase 1 exit gate approved |

**Phase Exit Criteria**

- Program governance is active
- Scope, requirements, and architecture baselines exist
- Delivery tooling and environments are defined

## 6. Phase 2: Canonical Data and Schema Foundation

**Objective**

Build the database and data-model foundation that supports the rewrite without the v1.5 structural defects.

**Entry Gate**

- Phase 1 governance artifacts approved
- Architecture and requirement baselines available

**Sprint Plan**

| Sprint | Focus | Key Build Outcomes | Exit Gate |
| --- | --- | --- | --- |
| `P2-S01` | Domain schema blueprint | Finalize conceptual and logical models for parcels, runs, batches, sources, and scoring | Logical model approved |
| `P2-S02` | Scoring profile normalization | Implement `scoring_profile` and `scoring_profile_factor` to replace invalid JSON budget validation | Factor-budget model approved |
| `P2-S03` | Canonical parcel model | Define `raw_parcels`, lineage columns, county mapping, and parcel identity rules | Parcel schema approved |
| `P2-S04` | Batch and run lifecycle tables | Implement `score_batch` and `score_run` with explicit statuses and failure semantics | Run-state schema validated |
| `P2-S05` | Evaluation and exclusion tables | Implement `parcel_evaluations` and `parcel_exclusion_events` with status and audit rules | Evaluation model validated |
| `P2-S06` | Score detail and provenance tables | Implement `score_factor_detail`, `score_factor_input`, and `score_bonus_detail` with uniqueness rules | Score detail schema validated |
| `P2-S07` | Geospatial persistence and indexing | Implement representative-point storage, spatial indexes, and query-aligned partitioning design | DBA confirms geospatial design |
| `P2-S08` | Migration framework and seed data | Create repeatable migrations, rollback scripts, and reference-data seeds | Migration framework operational |
| `P2-S09` | Migration rehearsal | Execute dry-run migrations, rollback tests, and schema smoke tests across environments | Rehearsal passes without critical defects |
| `P2-S10` | Phase gate and data dictionary | Publish final data dictionary, ERD set, and schema review outputs | Phase 2 exit gate approved |

**Phase Exit Criteria**

- Canonical schema is migration-ready
- Data dictionary and constraints are baselined
- Structural audit defects are removed from the model

## 7. Phase 3: Source Ingestion and Freshness

**Objective**

Build approved source ingestion, quality validation, freshness gating, and source-operability controls.

**Entry Gate**

- Canonical schema deployed to non-production environments
- Source owners and interfaces identified

**Sprint Plan**

| Sprint | Focus | Key Build Outcomes | Exit Gate |
| --- | --- | --- | --- |
| `P3-S01` | Ingestion framework | Build staging patterns, adapter skeletons, load logging, and common validation framework | Shared ingestion framework running |
| `P3-S02` | Parcel source ingestion | Load approved parcel source feeds into staging and canonical parcel storage | Parcel source load validated |
| `P3-S03` | Zoning and land-use ingestion | Load zoning and land-use feeds with county and metro mapping validation | Zoning ingestion validated |
| `P3-S04` | Environmental blocking sources wave 1 | Load first blocking source set such as flood or equivalent approved datasets | Blocking-source wave 1 validated |
| `P3-S05` | Environmental blocking sources wave 2 | Load remaining blocking source sets such as habitat, contamination, or approved equivalents | Blocking-source wave 2 validated |
| `P3-S06` | Utility and hosting-capacity data | Load utility-serving or hosting-capacity evidence used by scoring factors | Utility evidence available in canonical form |
| `P3-S07` | Market and proxy datasets | Load approved market, pricing, or proxy datasets that support factor fallback logic | Proxy evidence available in canonical form |
| `P3-S08` | Data quality and quarantine | Implement malformed-record handling, row-level rejection logging, and source health metrics | Quarantine workflow proven |
| `P3-S09` | Freshness and block-refresh gate | Implement per-metro freshness checks and blocking rules prior to scoring | Freshness gate passes core test cases |
| `P3-S10` | Integration and handoff | Run cross-source integration tests and finalize operational source catalog ownership | Phase 3 exit gate approved |

**Phase Exit Criteria**

- Approved sources are ingestible
- Freshness gating blocks unsafe refreshes
- Operators can see source health and failure context

## 8. Phase 4: Parcel Evaluation and Exclusions

**Objective**

Implement the evaluation state machine that processes every parcel before scoring.

**Entry Gate**

- Freshness-controlled source data available in integration and staging
- Metro and county scope definitions approved

**Sprint Plan**

| Sprint | Focus | Key Build Outcomes | Exit Gate |
| --- | --- | --- | --- |
| `P4-S01` | Metro and county scoping | Implement metro-to-county resolution and run-scoped parcel selection | Scoping service validated |
| `P4-S02` | Evaluation record creation | Write one `parcel_evaluations` row per in-scope parcel and seed lifecycle states | Coverage test passes |
| `P4-S03` | Representative-point band filter | Apply corridor and band rules using persisted `rep_point` logic | Band filter validated |
| `P4-S04` | Acreage and threshold filtering | Implement size and threshold prefilters with status and rationale capture | Prefilter rules validated |
| `P4-S05` | Hard exclusions wave 1 | Implement first set of exclusion rules and log rule-level exclusion events | Exclusion wave 1 validated |
| `P4-S06` | Hard exclusions wave 2 | Implement remaining exclusion rules and refine exclusion taxonomy | Exclusion wave 2 validated |
| `P4-S07` | Candidate-view removal | Remove all remaining dependency on `candidate_parcels_v` and shift to run-scoped evaluation reads | Deprecated view fully removed |
| `P4-S08` | Idempotency and rerun behavior | Add safe reruns, restart checkpoints, and duplicate prevention for evaluation stages | Evaluation reruns proven |
| `P4-S09` | Metrics and reconciliation | Add counts for evaluated, filtered, excluded, and promoted parcels plus reconciliation reports | Reconciliation outputs approved |
| `P4-S10` | Phase acceptance | Execute end-to-end evaluation scenarios for approved pilot metros | Phase 4 exit gate approved |

**Phase Exit Criteria**

- Every parcel in scope is evaluated and categorized
- Exclusion logic is auditable
- Evaluation is rerunnable and run-scoped

## 9. Phase 5: Scoring, Confidence, and Provenance

**Objective**

Implement the scoring engine, confidence model, and evidence traceability required for trustworthy parcel scoring.

**Entry Gate**

- Evaluation pipeline promotes valid survivors to `pending_scoring`
- Factor and bonus catalogs approved for implementation

**Sprint Plan**

| Sprint | Focus | Key Build Outcomes | Exit Gate |
| --- | --- | --- | --- |
| `P5-S01` | Scoring runtime framework | Build scoring job flow, profile resolution, and parcel scoring orchestration | Scoring runtime operational |
| `P5-S02` | Factor engine abstractions | Implement shared factor-calculation contracts, rule hooks, and scoring utilities | Factor framework reviewed |
| `P5-S03` | Factor wave 1 | Implement `F01-F03`, associated evidence resolution, and factor-detail writes | Wave 1 regression passes |
| `P5-S04` | Factor wave 2 | Implement `F04-F06`, evidence fallbacks, and rationale capture | Wave 2 regression passes |
| `P5-S05` | Factor wave 3 | Implement `F07-F10`, edge-case handling, and deterministic point calculations | Wave 3 regression passes |
| `P5-S06` | Bonus engine | Implement `B01-B05`, additive application logic, false-row recording, and 100-point cap | Bonus cardinality test passes |
| `P5-S07` | Confidence model | Implement evidence-quality weights, direct-over-proxy preference, and confidence storage | Confidence rules validated |
| `P5-S08` | Provenance and explanation | Implement `score_factor_input` uniqueness, evidence summaries, and parcel explanation outputs | Provenance review passes |
| `P5-S09` | Regression and oracle datasets | Build seeded reference scenarios and compare scoring outputs against expected results | Scoring oracle tests pass |
| `P5-S10` | Phase certification | Run full factor, bonus, confidence, and cardinality acceptance suite | Phase 5 exit gate approved |

**Phase Exit Criteria**

- Scored parcels have 10 factor rows and 5 bonus rows
- Confidence and evidence traceability are complete
- Scoring is deterministic and retry-safe

## 10. Phase 6: Batch Activation, Read Model, and APIs

**Objective**

Publish only complete batches and expose supported read/query surfaces for users and operators.

**Entry Gate**

- Score runs can complete successfully with validated outputs
- Core scoring invariants pass in staging

**Sprint Plan**

| Sprint | Focus | Key Build Outcomes | Exit Gate |
| --- | --- | --- | --- |
| `P6-S01` | Batch orchestrator core | Build batch creation, metro run scheduling, and run aggregation semantics | Batch orchestration working in integration |
| `P6-S02` | Failure and recovery logic | Implement failed-run handling, retry pathways, and activation-precondition checks | Failure pathways validated |
| `P6-S03` | Active batch read model | Implement active-batch-backed views/materializations and activation pointer logic | Mixed-batch exposure prevented |
| `P6-S04` | Parcel search API | Build active-batch parcel query API with filters for metro, county, score, and acreage | Search API contract approved |
| `P6-S05` | Parcel detail API | Build parcel detail endpoint with factor, bonus, confidence, and provenance summaries | Detail API contract approved |
| `P6-S06` | Export and reporting APIs | Build export interfaces and reporting endpoints for approved business consumers | Export flow validated |
| `P6-S07` | Cross-metro analytics | Build cross-metro comparisons and border-corridor queries using one activated batch context | Cross-metro logic validated |
| `P6-S08` | Activation history and rollback | Implement activation audit trail, previous-batch retention, and controlled rollback model | Batch history review passes |
| `P6-S09` | Performance tuning | Tune indexes, query plans, and read-model refresh behavior for pilot volume | Performance target met in staging |
| `P6-S10` | Phase readiness review | Run integration tests across batch build, activation, API, and export behavior | Phase 6 exit gate approved |

**Phase Exit Criteria**

- Users can only query activated-batch data
- Read APIs are stable and auditable
- Batch failure does not leak partial data

## 11. Phase 7: Operations, Security, and UAT

**Objective**

Prepare the system for real operational ownership, secure access, and business validation.

**Entry Gate**

- Active-batch read APIs available in staging
- Operational telemetry foundations deployed

**Sprint Plan**

| Sprint | Focus | Key Build Outcomes | Exit Gate |
| --- | --- | --- | --- |
| `P7-S01` | Admin monitoring dashboard | Build dashboards for source health, run status, batch progression, and failure counts | Monitoring dashboard usable by operators |
| `P7-S02` | Operator job controls | Build retry, cancel, rerun, and batch-management actions with audit capture | Operator controls validated |
| `P7-S03` | Authentication and RBAC | Integrate enterprise identity and implement `admin`, `operator`, and `reader` roles | Access control tests pass |
| `P7-S04` | Logging, tracing, and alerts | Add structured logs, traces, and alert thresholds for critical failure points | Observability baseline approved |
| `P7-S05` | Audit exports and evidence packages | Build audit-ready exports for source freshness, run history, and parcel evidence | Audit package validated |
| `P7-S06` | Runbooks and support SOPs | Publish runbooks for stale sources, failed batches, retries, and recovery paths | Support documentation approved |
| `P7-S07` | UAT environment and seed data | Prepare UAT environment, masked datasets, and scripted scenarios | UAT environment ready |
| `P7-S08` | UAT wave 1 | Execute first UAT cycle on core workflows and capture severity-ranked defects | Wave 1 sign-off or rework plan agreed |
| `P7-S09` | UAT wave 2 and defect burn | Close priority defects, rerun critical scenarios, and finalize business validation | UAT critical defects closed |
| `P7-S10` | Release candidate gate | Freeze release candidate scope, confirm readiness evidence, and obtain business sign-off | Phase 7 exit gate approved |

**Phase Exit Criteria**

- Operational owners can monitor and support the platform
- Security model is enforced
- UAT is signed off for launch readiness

## 12. Phase 8: Hardening, Cutover, Launch, and Closure

**Objective**

Finish nonfunctional hardening, execute the production launch, stabilize the platform, and transition into Phase 2 planning.

**Entry Gate**

- Release candidate approved
- Production cutover prerequisites available

**Sprint Plan**

| Sprint | Focus | Key Build Outcomes | Exit Gate |
| --- | --- | --- | --- |
| `P8-S01` | Full regression and reconciliation | Execute complete regression suite and reconcile outputs against approved reference data | Regression baseline passes |
| `P8-S02` | Volume and performance testing | Run pilot-scale and stress tests for ingestion, evaluation, scoring, and read APIs | Performance evidence approved |
| `P8-S03` | Backup, restore, and disaster recovery | Validate backup schedules, restore procedures, and recovery runbooks | DR rehearsal succeeds |
| `P8-S04` | Security hardening | Execute security review, secrets rotation, least-privilege checks, and remediate findings | Security sign-off obtained |
| `P8-S05` | Cutover rehearsal 1 | Dry-run the deployment, data load, batch build, and rollback path in pre-production | Rehearsal 1 passed |
| `P8-S06` | Cutover rehearsal 2 | Repeat cutover with timing, operator comms, and defect fixes from rehearsal 1 | Rehearsal 2 passed |
| `P8-S07` | Production deployment preparation | Finalize production checklist, support roster, hypercare metrics, and launch approvals | Go-live approval obtained |
| `P8-S08` | Go-live and first active batch | Deploy to production, execute controlled refresh, and activate the first production batch | Production batch activated |
| `P8-S09` | Hypercare stabilization | Triage launch issues, monitor KPIs, and complete stabilization fixes | Hypercare exit criteria met |
| `P8-S10` | Closure and Phase 2 transition | Publish lessons learned, residual risks, backlog carryover, and Phase 2 recommendations | Program closure approved |

**Phase Exit Criteria**

- Production launch succeeds
- Hypercare stabilizes the first release
- Phase 2 backlog is baselined

## 13. Major Milestones

| Milestone | Sprint | Meaning |
| --- | --- | --- |
| `M1` | `P1-S10` | Program mobilization complete |
| `M2` | `P2-S10` | Canonical schema approved |
| `M3` | `P3-S10` | Source ingestion and freshness controls operational |
| `M4` | `P4-S10` | Parcel evaluation pipeline accepted |
| `M5` | `P5-S10` | Scoring engine certified |
| `M6` | `P6-S10` | Activated-batch read model and APIs ready |
| `M7` | `P7-S10` | Release candidate approved |
| `M8` | `P8-S10` | Launch and closure complete |

## 14. Master Exit Criteria

The master plan is complete when:

- the platform publishes only activated batches,
- critical sources can block unsafe refreshes,
- scored parcels carry complete factor, bonus, confidence, and provenance detail,
- operational and security controls are in place,
- production launch and hypercare are complete,
- the Phase 2 roadmap is agreed.
