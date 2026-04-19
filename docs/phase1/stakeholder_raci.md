# Stakeholder Map And RACI

## Stakeholder Map

| Stakeholder | Primary Concern | Decision Rights |
| --- | --- | --- |
| Business sponsor | metro scope, scoring policy, launch readiness | approve scope and release readiness |
| Product owner | backlog, acceptance, release sequencing | approve priorities and acceptance |
| Requirements Agent | requirement baseline and traceability | approve requirement wording and acceptance IDs |
| Architecture Agent | architecture boundaries and design integrity | approve design and ADR changes |
| Schema Agent | schema, migrations, seed data | approve database contract changes |
| Ops-Release Agent | environments, CI/CD, support readiness | approve environment and release mechanics |
| QA Agent | evidence quality, regression, gate certification | approve test evidence sufficiency |

## Decision Cadence

| Forum | Cadence | Owner | Output |
| --- | --- | --- | --- |
| Phase standup | weekly | `Orchestrator Agent` | blockers, dependencies, next actions |
| Requirement review | weekly | `Requirements Agent` | requirement delta approval |
| Architecture review | weekly | `Architecture Agent` | ADRs and boundary decisions |
| QA readiness review | weekly | `QA Agent` | regression and traceability status |
| Release control review | biweekly | `Ops-Release Agent` | environment and cutover readiness |

## RACI Matrix

| Work Area | R | A | C | I |
| --- | --- | --- | --- | --- |
| Rewrite charter and backlog baseline | `Orchestrator Agent` | Product owner | `Requirements Agent` | Business sponsor |
| Scope baseline and metro closure | `Requirements Agent` | Business sponsor | `Orchestrator Agent` | `QA Agent` |
| Requirements and acceptance IDs | `Requirements Agent` | Product owner | `QA Agent` | `Architecture Agent` |
| Architecture and ADRs | `Architecture Agent` | Architecture review board | `Schema Agent` | Product owner |
| Repository standards and CI skeleton | `Ops-Release Agent` | `Orchestrator Agent` | `API Agent` | `QA Agent` |
| Environment strategy | `Ops-Release Agent` | `Orchestrator Agent` | `Schema Agent` | Product owner |
| Traceability matrix | `QA Agent` | `Requirements Agent` | `Architecture Agent` | `Orchestrator Agent` |
| RAID and release controls | `Orchestrator Agent` | Product owner | `Ops-Release Agent` | Business sponsor |
| Phase gate certification | `QA Agent` | `Orchestrator Agent` | all owner agents | Business sponsor |

