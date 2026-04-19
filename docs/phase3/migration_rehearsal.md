# Phase 3 Migration Rehearsal

## Result

Phase 3 migration rehearsal passed.

## Command

```powershell
$env:DATABASE_URL='sqlite+pysqlite:///temp/phase3_rehearsal.sqlite3'
& '.\.venv\Scripts\python.exe' -m alembic upgrade head
& '.\.venv\Scripts\python.exe' -m alembic downgrade base
```

## Outcome

- upgrade `-> 20260413_0001` succeeded
- upgrade `20260413_0001 -> 20260413_0002` succeeded
- upgrade `20260413_0002 -> 20260413_0003` succeeded
- downgrade `20260413_0003 -> 20260413_0002` succeeded
- downgrade `20260413_0002 -> 20260413_0001` succeeded
- downgrade `20260413_0001 -> base` succeeded

## Note

The first sandboxed rehearsal attempt hit a SQLite disk I/O restriction. The verified
upgrade/downgrade run succeeded after rerunning the same rehearsal outside the sandbox.
