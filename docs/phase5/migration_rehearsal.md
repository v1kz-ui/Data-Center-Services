# Phase 5 Migration Rehearsal

## Scope

Validated the Phase 5 migration that adds `score_run.profile_name` for scoring
profile auditability.

## Command

```powershell
$env:DATABASE_URL='sqlite+pysqlite:///temp/phase3_rehearsal.sqlite3'
& '.\.venv\Scripts\python.exe' -m alembic upgrade head
& '.\.venv\Scripts\python.exe' -m alembic downgrade base
```

## Result

- Upgrade succeeded through `20260414_0004`
- Downgrade succeeded back to `base`

## Notes

The Phase 5 schema change is limited to adding a nullable `profile_name` column on
`score_run`, so the rehearsal completed without additional backfill requirements in
the local SQLite exercise.
