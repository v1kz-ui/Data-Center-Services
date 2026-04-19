# Phase 2 Migration Rehearsal

## Objective

Verify that the versioned schema can upgrade from base to head and roll back to base.

## Rehearsal Command

```powershell
$env:DATABASE_URL='sqlite+pysqlite:///temp/phase2_rehearsal.sqlite3'
.\.venv\Scripts\python.exe -m alembic upgrade head
.\.venv\Scripts\python.exe -m alembic downgrade base
```

## Result

- upgrade to `20260413_0001` succeeded
- upgrade to `20260413_0002` succeeded
- downgrade from `20260413_0002` to `20260413_0001` succeeded
- downgrade from `20260413_0001` to base succeeded

## Notes

- the first attempt inside the default sandbox hit a SQLite disk I/O restriction
- the successful rehearsal was executed outside the sandbox using the same local workspace and migration code
- automated repo validation also passed after the migration expansion

