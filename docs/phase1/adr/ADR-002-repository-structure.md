# ADR-002: Repository Structure

## Status

Accepted

## Decision

Use a single repository with separated service directories for API and workers, plus shared database, test, configuration, and infrastructure folders.

## Rationale

- keeps early build coordination simple,
- makes cross-cutting batch and schema changes easier to trace,
- supports one planning and agent operating model,
- reduces integration overhead during the initial implementation phases.

## Consequences

- service boundaries are explicit within one repo,
- shared conventions must be enforced tightly,
- cross-service imports should be governed rather than improvised.

