# Release Governance Baseline

## Release Goal

Ensure that every environment promotion is traceable, reversible, and backed by requirement-linked evidence.

## Promotion Path

1. Local developer validation
2. Integration verification
3. Staging release candidate verification
4. Production release

## Required Release Evidence

- linked sprint or change-request identifier
- updated traceability row
- passing lint and automated test evidence
- migration impact note when schema changes exist
- rollback note for production-affecting behavior
- QA certification or explicit waiver

## Change Classes

| Class | Examples | Required Approvals |
| --- | --- | --- |
| `standard` | docs, non-behavioral refactors, low-risk test additions | owner agent |
| `controlled` | API changes, batch logic changes, nontrivial migrations | owner agent plus secondary gate agent |
| `restricted` | scoring logic, geospatial logic, security, publication rules | owner agent, `QA Agent`, and `Orchestrator Agent` |

## Phase Gate Inputs

- rewrite charter and scope baseline
- SRS, SAD, and SDD package
- delivery controls and agent playbook
- traceability matrix
- RAID log
- sprint handoff and closeout artifacts
- QA certification note

## Escalation Path

1. owner agent documents the blocker
2. `Orchestrator Agent` assesses dependency and schedule impact
3. `Requirements Agent` and `Architecture Agent` determine scope or design implications
4. product owner or sponsor resolves business-impacting decisions

