# UAT Environment And Seed Data

This slice prepares the repository for a controlled Phase 7 user-acceptance
environment.

## Seed Bundle

- the controlled reference seed bundle remains in `db/seeds`,
- `scripts/seed_reference_data.py` loads those files into a prepared lower
  environment database,
- the loader is idempotent so repeated execution updates the same reference
  rows instead of duplicating them.

## Scenario Pack

- `infra/uat/phase7_uat_scenarios.json` provides the first scripted UAT pack,
- the pack covers operator monitoring, retry controls, audit export, and
  reader-visible scoring flows,
- expected evidence is listed for each scenario so business testers and support
  staff can validate the same outcomes.

## Manifest Build

- `scripts/build_uat_manifest.py` assembles the seed inventory, row counts,
  scenario pack, observability headers, and monitoring thresholds into one
  machine-readable readiness document,
- this manifest can be attached to a UAT rehearsal ticket or support handoff.

## Environment Contract

- `.env.example` and `configs/app.example.yaml` now declare the UAT-oriented
  seed, scenario, auth, and observability settings,
- the UAT package expects header-based identity, request/trace correlation, and
  the same threshold defaults used by the monitoring API.

## Intended Outcome

- lower environments can be reseeded in a repeatable way,
- UAT operators have a scenario pack tied to real API surfaces,
- support and release owners can prove what data and workflows are present in
  the UAT sandbox before business validation starts.
