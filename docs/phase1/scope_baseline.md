# Phase 1 Scope Baseline

## Approved Metro Scope

The engineering Phase 1 baseline uses the following metro codes:

- `DFW`
- `AUS`
- `PHX`
- `LAS`

These metro codes are the active implementation baseline for local development, tests, and early orchestration work.

## Explicit Exclusions

- Brownsville / `BRO` is excluded from Phase 1.
- Any metro without approved parcel, zoning, and blocking-source coverage is excluded.
- Public-portal experience work is excluded.
- Machine-learning scoring changes are excluded.

## Approved Source Envelope

| Source ID | Role | Blocking | Target |
| --- | --- | --- | --- |
| `PARCEL` | canonical parcel baseline | yes | `raw_parcels` |
| `ZONING` | zoning and land-use evidence | yes | `raw_zoning` |
| `FLOOD` | environmental blocking evidence | yes | `src_flood` |
| `UTILITY` | supporting utility evidence | no | `src_utility` |
| `MARKET` | supporting market proxy evidence | no | `src_market` |

## Scope Rules

- No source may be treated as Phase 1 approved unless it appears in `source_catalog`.
- No metro may be activated unless all required blocking sources are available and fresh.
- No dependency on `candidate_parcels_v` is permitted in the approved scope.
- Any change to metro scope, blocking-source status, or output surface requires change-control review.

## Deferred Items

- Brownsville enablement
- generalized public user portal
- non-approved source ingestion waves beyond the baseline catalog
- post-launch optimization and automation work

