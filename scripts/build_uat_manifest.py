from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "apps" / "api" / "src"))

from app.core.settings import get_settings  # noqa: E402
from app.services.uat_readiness import build_uat_manifest  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build the Phase 7 UAT manifest from the controlled seed bundle "
            "and scenario pack."
        ),
    )
    parser.add_argument("--seed-dir", dest="seed_dir", default=None)
    parser.add_argument("--scenario-pack", dest="scenario_pack", default=None)
    args = parser.parse_args()

    manifest = build_uat_manifest(
        get_settings(),
        seed_dir=args.seed_dir,
        scenario_pack_path=args.scenario_pack,
    )
    print(json.dumps(manifest.to_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
