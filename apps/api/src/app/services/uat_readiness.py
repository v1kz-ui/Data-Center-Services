from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from app.core.settings import Settings

REPO_ROOT = Path(__file__).resolve().parents[5]
DEFAULT_UAT_SCENARIO_PACK = REPO_ROOT / "infra" / "uat" / "phase7_uat_scenarios.json"


@dataclass(slots=True)
class UatSeedFileSummary:
    name: str
    path: str
    row_count: int


@dataclass(slots=True)
class UatScenario:
    scenario_id: str
    title: str
    actor_role: str
    workflow: str
    entrypoint: str
    expected_evidence: list[str]


@dataclass(slots=True)
class UatManifest:
    environment_name: str
    generated_at: str
    request_id_header: str
    trace_id_header: str
    seed_files: list[UatSeedFileSummary]
    scenarios: list[UatScenario]
    monitoring_thresholds: dict[str, int]

    def to_dict(self) -> dict[str, object]:
        return {
            "environment_name": self.environment_name,
            "generated_at": self.generated_at,
            "request_id_header": self.request_id_header,
            "trace_id_header": self.trace_id_header,
            "seed_files": [asdict(seed_file) for seed_file in self.seed_files],
            "scenarios": [asdict(scenario) for scenario in self.scenarios],
            "monitoring_thresholds": self.monitoring_thresholds,
        }


def build_uat_manifest(
    settings: Settings,
    *,
    seed_dir: Path | str | None = None,
    scenario_pack_path: Path | str | None = None,
) -> UatManifest:
    resolved_seed_dir = Path(seed_dir or REPO_ROOT / settings.reference_seed_dir)
    resolved_scenario_pack_path = Path(
        scenario_pack_path or REPO_ROOT / settings.uat_scenario_pack_path
    )

    seed_files = [
        UatSeedFileSummary(
            name=seed_file.name,
            path=str(seed_file),
            row_count=_count_csv_rows(seed_file),
        )
        for seed_file in sorted(resolved_seed_dir.glob("*.csv"))
    ]
    scenarios = load_uat_scenarios(resolved_scenario_pack_path)

    return UatManifest(
        environment_name=settings.uat_environment_name,
        generated_at=datetime.now(UTC).isoformat(),
        request_id_header=settings.request_id_header,
        trace_id_header=settings.trace_id_header,
        seed_files=seed_files,
        scenarios=scenarios,
        monitoring_thresholds={
            "failed_run_threshold": settings.monitoring_failed_run_threshold,
            "failed_snapshot_threshold": settings.monitoring_failed_snapshot_threshold,
            "quarantined_snapshot_threshold": settings.monitoring_quarantined_snapshot_threshold,
            "freshness_failure_threshold": settings.monitoring_freshness_failure_threshold,
            "latest_batch_failed_threshold": settings.monitoring_latest_batch_failed_threshold,
        },
    )


def load_uat_scenarios(path: Path) -> list[UatScenario]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [
        UatScenario(
            scenario_id=item["scenario_id"],
            title=item["title"],
            actor_role=item["actor_role"],
            workflow=item["workflow"],
            entrypoint=item["entrypoint"],
            expected_evidence=item["expected_evidence"],
        )
        for item in payload["scenarios"]
    ]


def _count_csv_rows(path: Path) -> int:
    with path.open(encoding="utf-8", newline="") as csv_file:
        return sum(
            1
            for row in csv.DictReader(csv_file)
            if any(value.strip() for value in row.values())
        )
