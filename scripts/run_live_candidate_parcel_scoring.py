# ruff: noqa: E402

from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "apps" / "api" / "src"))
sys.path.insert(0, str(ROOT / "workers" / "orchestrator" / "src"))
sys.path.insert(0, str(ROOT / "workers" / "ingestion" / "src"))
sys.path.insert(0, str(ROOT / "workers" / "evaluation" / "src"))
sys.path.insert(0, str(ROOT / "workers" / "scoring" / "src"))

from app.db.session import SessionLocal  # noqa: E402
from app.services.live_candidate_parcel_scoring import (
    run_live_candidate_parcel_scoring,  # noqa: E402
)
from app.services.reference_seeds import load_reference_seed_bundle  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Derive parcel-scoring evidence from linked live candidates and run scoring.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Maximum number of ranked live candidates to consider before parcel dedupe.",
    )
    parser.add_argument(
        "--profile-name",
        default="texas_live_v1",
        help="Scoring profile name to apply.",
    )
    parser.add_argument(
        "--minimum-acreage",
        type=Decimal,
        default=Decimal("1.0"),
        help="Minimum acreage gate for the parcel evaluation pass.",
    )
    parser.add_argument(
        "--output",
        default=str(ROOT / "temp" / "live_candidate_parcel_scoring_report.json"),
        help="Path for the JSON scoring report.",
    )
    parser.add_argument(
        "--skip-seed-sync",
        action="store_true",
        help="Skip reloading the reference seed bundle before running the scoring pass.",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    session = SessionLocal()
    try:
        seed_result = None
        if not args.skip_seed_sync:
            seed_result = load_reference_seed_bundle(session)
            session.commit()

        report = run_live_candidate_parcel_scoring(
            session,
            limit=args.limit,
            profile_name=args.profile_name,
            minimum_acreage=args.minimum_acreage,
        )
        payload = report.to_dict()
        payload["output_path"] = str(output_path)
        if seed_result is not None:
            payload["seed_sync"] = seed_result.to_dict()
        rendered_payload = json.dumps(payload, indent=2, default=str)
        output_path.write_text(rendered_payload, encoding="utf-8")
        print(rendered_payload)
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
