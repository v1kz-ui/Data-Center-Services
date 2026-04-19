from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from sqlalchemy.orm import Session

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "apps" / "api" / "src"))

from app.db.session import build_engine  # noqa: E402
from app.services.reference_seeds import load_reference_seed_bundle  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Load the controlled reference seed bundle into a prepared database.",
    )
    parser.add_argument("--database-url", dest="database_url", default=None)
    parser.add_argument("--seed-dir", dest="seed_dir", default=str(ROOT / "db" / "seeds"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    engine = build_engine(args.database_url)
    session = Session(bind=engine, future=True)
    try:
        result = load_reference_seed_bundle(session, seed_dir=args.seed_dir)
        payload = {
            "dry_run": args.dry_run,
            **result.to_dict(),
        }
        if args.dry_run:
            session.rollback()
        else:
            session.commit()
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
