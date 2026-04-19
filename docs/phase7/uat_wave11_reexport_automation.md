# UAT Wave 11 Scheduled Re-Export Automation

## Scope

Wave 11 adds scheduled re-export execution and follow-up notification digests so
archive remediation can move from queued work into an automated operational
loop.

## Delivered Capabilities

- preview or execution of due scheduled re-exports from the admin API,
- retry-material generation as fresh archive exports with traceable retry
  naming,
- source-export completion state updates once a scheduled retry is executed,
- follow-up notification digest exports grouped by support owner, and
- admin API coverage for scheduled re-export execution and digest retrieval.

## Validation Targets

- dry-run re-export execution should preview work without mutating archive
  exports,
- execution should create a fresh retry export and mark the scheduled source
  export complete,
- notification digests should regroup archives by owner and reflect the new
  acknowledgement-pending state after execution, and
- admin-only access control should cover automation and digest routes.
