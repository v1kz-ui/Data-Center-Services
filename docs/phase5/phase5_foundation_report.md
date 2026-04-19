# Phase 5 Foundation Report

## Status

Phase 5 has been started and the scoring foundation slice is implemented in this
repository.

## Delivered In This Slice

- scoring runtime for `pending_scoring` parcel execution,
- active-profile resolution and budget validation,
- deterministic factor-detail, bonus-detail, and factor-input provenance writes,
- direct-over-proxy evidence precedence,
- parcel-level viability and confidence calculation,
- score-run completion invariants,
- score-run profile tracking through a Phase 5 migration,
- admin scoring APIs for execution and summary retrieval,
- run-scoped parcel explanation output for factor, bonus, and provenance review,
- oracle-backed regression coverage for deterministic scoring expectations,
- unit and API tests for cardinality, precedence, reruns, invalid profile
  handling, and explanation output.

## Validation Evidence

- `.\.venv\Scripts\python.exe -m ruff check ...` passed for the scoring slice
- `.\.venv\Scripts\python.exe -m pytest -q tests/unit/test_scoring_service.py tests/unit/test_scoring_api.py tests/unit/test_phase5_schema_contract.py` passed

## Remaining Phase 5 Work

- replace placeholder evidence-key conventions with final business definitions for
  `F01-F10` and `B01-B05`,
- add richer explanation outputs for downstream parcel-detail APIs, and
- certify the full Phase 5 exit gate across end-to-end scoring scenarios.
