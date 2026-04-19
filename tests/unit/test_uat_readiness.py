import json
import subprocess
import sys

from app.core.settings import get_settings
from app.services.uat_readiness import build_uat_manifest


def test_uat_manifest_includes_seed_counts_and_scenarios() -> None:
    manifest = build_uat_manifest(get_settings())
    seed_files = {seed_file.name: seed_file for seed_file in manifest.seed_files}
    scenario_ids = {scenario.scenario_id for scenario in manifest.scenarios}

    assert manifest.environment_name == "uat"
    assert manifest.request_id_header == "X-Request-ID"
    assert manifest.trace_id_header == "X-Trace-ID"
    assert seed_files["bonus_catalog.csv"].row_count == 5
    assert seed_files["metro_catalog.csv"].row_count == 4
    assert seed_files["county_catalog.csv"].row_count == 9
    assert seed_files["factor_catalog.csv"].row_count == 10
    assert seed_files["scoring_profile.csv"].row_count == 1
    assert seed_files["scoring_profile_factor.csv"].row_count == 10
    assert seed_files["source_catalog.csv"].row_count == 5
    assert scenario_ids == {
        "UAT-OPS-001",
        "UAT-OPS-002",
        "UAT-ADM-003",
        "UAT-READ-004",
    }
    assert manifest.monitoring_thresholds["failed_run_threshold"] == 1


def test_build_uat_manifest_script_outputs_json() -> None:
    result = subprocess.run(
        [sys.executable, "scripts/build_uat_manifest.py"],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)

    assert payload["environment_name"] == "uat"
    assert payload["request_id_header"] == "X-Request-ID"
    assert len(payload["seed_files"]) == 8
    assert len(payload["scenarios"]) == 4
