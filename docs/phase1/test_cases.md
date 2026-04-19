# KIO Site Finder Phase 1 Test Cases

## 1. Test Approach

This catalog defines the core Phase 1 test cases for functional, integration, data quality, and operational validation. Detailed execution scripts are provided in `test_scripts.md`.

## 2. Functional and Integration Test Cases

| ID | Test Name | Preconditions | Expected Result |
| --- | --- | --- | --- |
| `TC-FRESH-001` | Block stale critical source | Required source snapshot is older than allowed cadence and `block_refresh = TRUE` | Run fails before scoring starts and failure reason is recorded |
| `TC-FRESH-002` | Allow stale non-blocking source | Stale source exists with `block_refresh = FALSE` | Run may proceed with warning and audit record |
| `TC-FRESH-003` | Missing source snapshot | No successful snapshot exists for a required source | Freshness gate fails and run does not enter scoring |
| `TC-FRESH-004` | Source recovery after reload | Previously stale source is reloaded successfully | Freshness gate passes on rerun |
| `TC-EVAL-001` | Create evaluation rows for all parcels | Valid metro run exists with in-scope counties | One evaluation row exists per in-scope parcel |
| `TC-EVAL-002` | Representative-point band filter | Parcel representative point falls outside allowed band | Parcel status becomes `prefiltered_band` |
| `TC-EVAL-003` | Size filter | Parcel acreage is below minimum threshold | Parcel status becomes `prefiltered_size` |
| `TC-EVAL-004` | Exclusion event logging | Parcel violates a hard exclusion rule | Parcel status becomes `excluded` and at least one exclusion event exists |
| `TC-EVAL-005` | Pending-to-scored promotion | Parcel survives all filters and exclusions | Parcel transitions from `pending_scoring` to `scored` |
| `TC-EVAL-006` | No deprecated candidate view dependency | Evaluation pipeline is executed in a clean environment | No query or object requires `candidate_parcels_v` |
| `TC-SCORE-001` | Ten factor rows per scored parcel | Parcel reaches scoring stage | Exactly ten `score_factor_detail` rows exist |
| `TC-SCORE-002` | Five bonus rows per scored parcel | Parcel reaches scoring stage | Exactly five `score_bonus_detail` rows exist |
| `TC-SCORE-003` | Factor budget closure | Active scoring profile exists | Sum of active factor budgets equals 100 |
| `TC-SCORE-004` | Direct evidence precedence | Both direct and proxy evidence are present with conflicting values | Score uses direct evidence and confidence reflects evidence quality |
| `TC-SCORE-005` | Duplicate provenance protection | Scoring retry occurs for same run and parcel | No duplicate `score_factor_input` rows are created |
| `TC-SCORE-006` | Confidence required for scored parcels | Parcel status is `scored` | `confidence_score` is non-null |
| `TC-SCORE-007` | Confidence absent for non-scored parcels | Parcel status is prefiltered or excluded | `confidence_score` is null |
| `TC-BATCH-001` | Batch activates only after all metros complete | One metro run remains incomplete | Batch is not activated |
| `TC-BATCH-002` | Failed metro blocks activation | One required metro run fails | Batch status is failed or non-active |
| `TC-BATCH-003` | Active reads use only active batch | A newer incomplete batch exists | User APIs still return prior active batch data |
| `TC-BATCH-004` | Completed runs contain no pending rows | Run is marked completed | Pending parcel count is zero |
| `TC-API-001` | Parcel search API | Active batch exists | Search returns only active-batch parcels matching filters |
| `TC-API-002` | Parcel detail API | Scored parcel exists in active batch | API returns factor detail, bonus detail, confidence, and provenance summary |
| `TC-API-003` | Admin run status API | Operator is authenticated | API returns run states, timestamps, and failure reasons |
| `TC-OPS-001` | Retry failed run | Run failed due to corrected source issue | Operator retry creates a valid rerun path |
| `TC-OPS-002` | Activation audit trail | Batch is activated | Activation metadata and actor are recorded |
| `TC-PERF-001` | Pilot metro throughput | Production-like pilot dataset available | Run completes within approved batch window |
| `TC-PERF-002` | Search response time | Active batch contains production-like volume | Parcel search meets API latency target |
| `TC-SEC-001` | Role enforcement | Reader user attempts admin action | Request is denied and logged |

## 3. Acceptance Test Coverage Map

| Acceptance ID | Covered By |
| --- | --- |
| `AT-BAT-001` Batch completeness | `TC-BATCH-001`, `TC-BATCH-002` |
| `AT-PEND-001` No pending states | `TC-BATCH-004` |
| `AT-FAC-001` Factor cardinality | `TC-SCORE-001` |
| `AT-BON-001` Bonus cardinality | `TC-SCORE-002` |
| `AT-BO-003` Border corridor batch scoping | `TC-BATCH-003` |
| `AT-SRC-001` Source catalog closure | `TC-FRESH-001`, `TC-FRESH-003` |

## 4. Exit Criteria

Phase 1 testing is complete when:

- all priority functional and integration tests pass,
- all acceptance tests mapped above pass,
- no severity 1 or severity 2 defect is open for launch,
- performance and security checks are signed off.
