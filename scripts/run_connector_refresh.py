from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a single source connector refresh and emit a JSON report."
    )
    parser.add_argument(
        "--connector-key",
        required=True,
        help="Connector key from configs/source_connectors.json.",
    )
    parser.add_argument(
        "--config-path",
        default=None,
        help="Optional connector config path. Defaults to configs/source_connectors.json.",
    )
    parser.add_argument(
        "--actor-name",
        default="Codex",
        help="Actor name recorded on the refresh job.",
    )
    parser.add_argument(
        "--requested-at",
        default=None,
        help="Optional ISO timestamp for the requested-at time.",
    )
    return parser


def _parse_requested_at(value: str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root / "workers" / "ingestion" / "src"))
    sys.path.insert(0, str(repo_root / "apps" / "api" / "src"))

    from app.db.session import SessionLocal
    from ingestion.connectors import load_connector_registry
    from ingestion.refresh import refresh_source_connector

    config_path = Path(args.config_path) if args.config_path else repo_root / "configs" / "source_connectors.json"
    registry = load_connector_registry(str(config_path))
    requested_at = _parse_requested_at(args.requested_at)

    session = SessionLocal()
    try:
        report = refresh_source_connector(
            session,
            registry,
            connector_key=args.connector_key,
            actor_name=args.actor_name,
            requested_at=requested_at,
        )
    finally:
        session.close()

    print(json.dumps(asdict(report), default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
