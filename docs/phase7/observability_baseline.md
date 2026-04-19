# Observability Baseline

This slice adds the first application-level observability baseline for the
secured API.

## Structured Request Logs

- every HTTP request emits a structured completion log entry,
- failed requests emit a structured failure log entry,
- request logs capture:
  - request and trace identifiers,
  - method and path,
  - response status or exception type,
  - duration in milliseconds, and
  - authenticated subject and roles when present.

## Trace Headers

- `X-Request-ID` is generated when the caller does not supply one,
- `X-Trace-ID` is generated or echoed for correlation across operator tooling,
- both headers are returned on API responses so incidents and audit packages can
  be correlated to request traffic.

## Monitoring Thresholds

The monitoring overview now evaluates explicit thresholds for:

- failed runs,
- failed latest source snapshots,
- quarantined latest source snapshots,
- freshness failures, and
- failed latest batches.

Each threshold returns observed count, configured threshold, severity, summary,
and a `triggered` flag.

## Intended Outcome

- operators can correlate API calls to request identifiers,
- support can tie audit packages to request traffic,
- monitoring consumers can distinguish raw alerts from threshold-triggered
  conditions, and
- the Phase 7 platform has a minimal observability baseline before UAT work.
