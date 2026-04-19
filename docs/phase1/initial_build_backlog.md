# Initial Build Backlog

## Objective

This backlog defines the first coding-ready implementation slice after planning completes.

## Sprint Slice A: Repository And Runtime Bootstrap

1. Create shared settings module and environment loader.
2. Create shared logging setup.
3. Create shared database engine and session factory.
4. Create API app bootstrap with `/health`.
5. Create worker package skeletons.
6. Add pytest smoke suite.
7. Add Ruff and mypy baseline checks.

## Sprint Slice B: Baseline Database Foundation

1. Create initial Alembic setup.
2. Add migration for `source_catalog`.
3. Add migration for `source_interface`.
4. Add migration for `factor_catalog`.
5. Add migration for `bonus_catalog`.
6. Add migration for `scoring_profile`.
7. Add migration for `scoring_profile_factor`.
8. Add factor-budget validation query test.

## Sprint Slice C: Batch And Run Skeleton

1. Add migration for `score_batch`.
2. Add migration for `score_run`.
3. Add status enum definitions.
4. Add orchestrator service skeleton.
5. Add first batch creation command/service.
6. Add run creation service.
7. Add unit tests for valid status transitions.

## Sprint Slice D: Delivery Evidence

1. Capture ADR for stack choice.
2. Capture ADR for repo structure.
3. Fill first handoff record.
4. Fill first sprint kickoff and closeout templates.
5. Store first QA certification note for smoke coverage.

