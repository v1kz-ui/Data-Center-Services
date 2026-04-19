# Phase 1 Rewrite Charter

## Purpose

This charter converts the v1.6 audit memo into one executable Phase 1 baseline for the clean rewrite.

## Mission

Deliver a controlled, auditable parcel scoring platform that:

- ingests approved sources into canonical storage,
- evaluates and scores parcels by metro-scoped runs,
- blocks unsafe refreshes when critical data is stale,
- publishes only completed activated batches,
- preserves parcel-level provenance, confidence, and audit history.

## Phase 1 Objectives

1. Replace the legacy patch stack with one documented implementation baseline.
2. Lock the initial pilot scope and explicitly exclude unsupported metros.
3. Establish repo standards, CI controls, environment patterns, and approval rules.
4. Publish the baseline requirement, architecture, and design package.
5. Stand up the initial schema, seed data, API scaffold, and orchestration skeleton.
6. Establish traceability, QA evidence format, and phase gate artifacts.

## Engineering Scope Baseline

- Approved engineering pilot metro codes for Phase 1: `DFW`, `AUS`, `PHX`, `LAS`
- Explicitly out of scope: `BRO` / Brownsville until parcel, zoning, and source coverage are formally approved
- Blocking sources in the initial source catalog: `PARCEL`, `ZONING`, `FLOOD`
- Non-blocking supporting sources in the initial source catalog: `UTILITY`, `MARKET`

## Phase 1 Deliverables

- authoritative planning package under `docs/phase1`
- agent operating model and delivery controls
- repository scaffold with linting, tests, and CI skeleton
- canonical foundation schema and seed datasets
- minimal API and orchestration runtime with automated smoke coverage

## Success Measures

- all Phase 1 planning artifacts are linked and internally consistent,
- the repo can install, lint, and run tests without inventing conventions,
- the next sprint owner can start Phase 2 schema work directly from approved artifacts,
- unsupported metros and deprecated objects are explicitly excluded from the active baseline.

## Exit Decision

Phase 1 is complete when the Phase 1 exit report, handoff, QA certification, and readiness checklist all indicate entry criteria are satisfied for `P2-S01`.

