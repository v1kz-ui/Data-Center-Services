# Contributing Guide

## Purpose

This repository is operated as a controlled delivery workspace for the KIO Site Finder clean rewrite.
Changes must preserve requirement traceability, design integrity, and test evidence.

## Branching Policy

- `main` is the protected integration branch.
- Feature work should use short-lived branches named `feature/<scope>`, `fix/<scope>`, or `docs/<scope>`.
- One pull request should cover one coherent change set.
- Large cross-cutting work should be decomposed by agent ownership boundary whenever possible.

## Pull Request Expectations

Every pull request should include:

- the requirement IDs or sprint artifact IDs touched by the change,
- a short implementation summary,
- test evidence,
- design notes if schema, scoring, or API behavior changes,
- rollout or rollback notes if production behavior would change.

## Review Rules

- Schema changes require `Schema Agent` ownership and `Architecture Agent` review.
- Requirement or acceptance changes require `Requirements Agent` ownership.
- Scoring changes require `Scoring Agent` ownership and regression evidence.
- Geospatial logic changes require `GIS Agent` validation.
- API publication changes require active-batch isolation verification.

## Quality Gate

Before opening a pull request, run:

```powershell
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m pytest -q
```

If a change introduces new runtime behavior, update the linked docs under `docs/phase1`.

