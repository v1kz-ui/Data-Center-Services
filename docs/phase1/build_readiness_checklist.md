# Build Readiness Checklist

## 1. Repository Readiness

- [x] Root scaffold exists for `apps`, `workers`, `db`, `tests`, `infra`, `configs`, and `scripts`
- [x] Build stack is explicitly documented
- [x] Environment variable contract exists
- [x] Lint, type-check, and test toolchain is declared

## 2. Delivery Readiness

- [x] `master_delivery_plan.md` is approved
- [x] `delivery_controls_pack.md` is approved
- [x] `agent_execution_playbook.md` is approved
- [x] The owning agent for the next sprint is identified

## 3. Requirement And Design Readiness

- [x] `SRS.md` contains the current approved requirement baseline
- [x] `SAD.md` and `SDD.md` reflect the active implementation boundary
- [x] No reference remains to unsupported metros or deprecated objects
- [x] Initial ADRs exist for build stack and repo structure

## 4. Data Readiness

- [x] Canonical schema plan exists
- [x] Migration path and rollback approach are defined
- [x] Source catalog ownership is identified
- [x] Seed/reference data strategy is defined

## 5. Test Readiness

- [x] Core acceptance tests are defined
- [x] Test scripts cover freshness, evaluation, scoring, batch activation, and security
- [x] The QA agent has an initial certification package format
- [x] Oracle scenarios for scoring are queued for build

## 6. Coding Sprint Entry Criteria

The codebase is build-ready when:

- the next sprint owner can name the exact artifacts to create,
- the repo structure for those artifacts already exists,
- the config contract and stack are clear,
- the tests required for that sprint are known before coding begins.

## 7. Current Gate Decision

- Phase 1 exit recommendation: approved
- Next sprint: `P2-S01`
- Next sprint owner: `Architecture Agent`
