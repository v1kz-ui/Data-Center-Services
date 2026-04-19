# Ingestion Framework

## Scope

Phase 3 establishes a shared ingestion runtime that can:

- validate source catalog eligibility and metro coverage,
- create immutable `source_snapshot` audit rows for every load attempt,
- load canonical parcel rows into `raw_parcels` and `parcel_rep_point`,
- load zoning rows into `raw_zoning`,
- load generic evidence rows into `source_evidence`,
- quarantine malformed rows into `source_record_rejection`,
- preserve prior successful loads when a new load is quarantined or fails.

## Runtime Pattern

The ingestion worker follows one common execution pattern for all supported sources:

1. Resolve the source catalog entry and verify metro coverage.
2. Compute a checksum for the inbound payload.
3. Create a `source_snapshot` row for the load attempt.
4. Validate each row and separate accepted rows from rejected rows.
5. Write canonical rows for accepted records.
6. Persist `source_record_rejection` rows for rejected records.
7. Mark the snapshot `success`, `quarantined`, or `failed`.

## Canonical Publish Rules

- Parcel loads upsert canonical parcels and representative points.
- Zoning loads mark prior active zoning rows inactive before writing a new active row.
- Generic evidence loads mark prior active evidence rows inactive for the same
  source, record key, and attribute name before writing a new active row.
- Quarantined rows do not delete or mutate the previous successful snapshot.

## Supported Sources

- `PARCEL` -> `raw_parcels`, `parcel_rep_point`
- `ZONING` -> `raw_zoning`
- `FLOOD` -> `source_evidence`
- `UTILITY` -> `source_evidence`
- `MARKET` -> `source_evidence`
