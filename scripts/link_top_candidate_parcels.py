from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "apps" / "api" / "src"))

from app.db.session import SessionLocal  # noqa: E402
from app.services.top_candidate_parcel_linking import (  # noqa: E402
    link_top_live_candidates_to_parcels,
    write_parcel_link_report,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Parcel-link the top-ranked live Texas candidate listings.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Maximum number of ranked live candidates to attempt to parcel-link.",
    )
    parser.add_argument(
        "--output",
        default=str(ROOT / "temp" / "top_candidate_parcel_link_report.json"),
        help="Path for the JSON parcel-link report.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute parcel links and write the report without committing database changes.",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    session = SessionLocal()
    try:
        report = link_top_live_candidates_to_parcels(
            session,
            limit=args.limit,
            write_changes=not args.dry_run,
        )
        output_path = write_parcel_link_report(report, output_path=args.output)
        payload = {
            **report.to_dict(),
            "output_path": str(output_path),
            "dry_run": args.dry_run,
        }
        if args.dry_run:
            session.rollback()
        else:
            session.commit()
        print(json.dumps(payload, indent=2))
        return 0
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
