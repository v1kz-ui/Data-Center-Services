# Support Runbooks

This document packages the first support-ready SOP set for secured operator
surfaces.

## Stale Source Runbook

1. Open the monitoring overview for the affected metro.
2. Confirm the failing source, freshness code, and latest snapshot status.
3. Validate whether the issue is missing data, stale success, failed load, or
   quarantined load.
4. If the latest load failed or quarantined, coordinate a corrected reload
   through the source load endpoint.
5. Re-run freshness validation and confirm all required sources return `passed`.
6. Export an audit package for the affected run when change evidence is needed.

## Failed Batch Runbook

1. Review the latest batch and failed run counts in monitoring.
2. Inspect the failed run detail and failure reason.
3. If the failure is source-related, remediate the source issue first.
4. Use retry only for a failed, non-active run that is safe to resume in place.
5. Use rerun batch when the full metro pack should be regenerated under a new
   batch identifier.
6. Confirm the failed batch remains non-active until validation passes.

## Retry and Recovery Runbook

1. Verify the target run is not part of the active batch.
2. Record the operator reason before taking action.
3. Retry failed runs when prerequisites are restored.
4. Cancel only actively running, non-active-batch runs that must stop with
   `MANUAL_CANCELLED`.
5. Reconcile the parent batch state after recovery actions.
6. Confirm the operator action appears in audit history.

## Audit Evidence Runbook

1. Export `GET /admin/audit/packages/runs/{run_id}` for run-level evidence.
2. Add `parcel_id` when a parcel-specific dispute or launch check is under
   review.
3. Confirm the package includes freshness status, source snapshot context,
   operator actions, and parcel evidence where applicable.
4. Attach the JSON package to the incident, UAT defect, or release evidence
   record.

## Escalation Guidance

- escalate to data governance when source coverage, lineage, or cadence
  ownership is unclear,
- escalate to the scoring owner when factor or bonus outputs disagree with the
  expected scoring profile, and
- escalate to release leadership when an active-batch safety rule blocks
  cutover or recovery.
