# Phase 6 Batch Activation and API Package

This folder contains the Phase 6 engineering package for batch orchestration,
activation preparation, and user/operator read surfaces.

## Included Artifacts

- `batch_orchestration.md`
- `failure_and_recovery.md`
- `phase6_foundation_report.md`

## Current Foundation Outcome

The repo now includes the Phase 6 foundation slice for orchestration:

- batch creation with one run per metro,
- batch/run operator APIs for creation, listing, lookup, and reconciliation,
- deterministic batch aggregation from metro run states,
- retry controls for failed metro runs,
- activation-precheck reporting before publication is attempted,
- batch readiness signals for future activation controls, and
- service wiring so evaluation/scoring outcomes update parent batch status.

Active-batch publication, search/detail read models, exports, and rollback
history remain later Phase 6 slices.
