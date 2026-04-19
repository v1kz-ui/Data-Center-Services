# RAID Log

## Risks

| ID | Type | Description | Impact | Mitigation | Owner |
| --- | --- | --- | --- | --- | --- |
| `R-001` | Risk | Approved source coverage may lag metro ambition | scoring blocked or scope churn | enforce source-catalog gate and scope baseline | `Orchestrator Agent` |
| `R-002` | Risk | Geospatial rule ambiguity may create false exclusions | incorrect parcel outcomes | persist `rep_point`, require GIS review, add oracle tests | `GIS Agent` |
| `R-003` | Risk | Legacy patch-stack assumptions may leak back into implementation | reintroduced contradictions | require ADRs and traceability updates for boundary changes | `Architecture Agent` |
| `R-004` | Risk | Batch publication may expose mixed data if activation logic is bypassed | user trust and audit damage | active-batch-only read contract plus tests | `API Agent` |

## Assumptions

| ID | Type | Description | Validation Point | Owner |
| --- | --- | --- | --- | --- |
| `A-001` | Assumption | Phase 1 pilot engineering metros are `DFW`, `AUS`, `PHX`, `LAS` | Phase 2 scope review | `Requirements Agent` |
| `A-002` | Assumption | Python `3.12+` is acceptable for local, CI, and early deployment work | environment review | `Ops-Release Agent` |
| `A-003` | Assumption | PostgreSQL/PostGIS remains the single system of record | ADR review | `Architecture Agent` |

## Issues

| ID | Type | Description | Current State | Owner |
| --- | --- | --- | --- | --- |
| `I-001` | Issue | Business-approved metro roster is not yet separately documented outside engineering artifacts | controlled by scope baseline assumption | `Requirements Agent` |
| `I-002` | Issue | Sandbox cache directories create local warning noise during test and lint runs | accepted as local-environment limitation | `Ops-Release Agent` |

## Dependencies

| ID | Type | Description | Needed For | Owner |
| --- | --- | --- | --- | --- |
| `D-001` | Dependency | final business confirmation of pilot metros | Phase 2 domain model and integration planning | Product owner |
| `D-002` | Dependency | approved enterprise identity provider integration details | Phase 7 security implementation | `Ops-Release Agent` |
| `D-003` | Dependency | production hosting and secrets platform selection | release automation and environment parity | `Ops-Release Agent` |

