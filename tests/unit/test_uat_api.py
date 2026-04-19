from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

SCENARIO_IDS = [
    "UAT-OPS-001",
    "UAT-OPS-002",
    "UAT-ADM-003",
    "UAT-READ-004",
]


def test_create_list_and_get_uat_cycles_return_seeded_scenarios(
    client: TestClient,
) -> None:
    create_response = client.post(
        "/admin/uat/cycles",
        json={"cycle_name": "phase7-wave1"},
    )

    assert create_response.status_code == 201
    payload = create_response.json()
    assert payload["status"] == "planned"
    assert payload["environment_name"] == "uat"
    assert payload["scenario_count"] == 4
    assert payload["defect_count"] == 0
    assert sorted(item["scenario_id"] for item in payload["scenario_executions"]) == sorted(
        SCENARIO_IDS
    )
    assert _counts(payload["scenario_status_counts"]) == {
        "planned": 4,
        "in_progress": 0,
        "passed": 0,
        "failed": 0,
        "blocked": 0,
    }

    list_response = client.get("/admin/uat/cycles")

    assert list_response.status_code == 200
    list_payload = list_response.json()
    assert len(list_payload) == 1
    assert list_payload[0]["cycle_id"] == payload["cycle_id"]

    get_response = client.get(f"/admin/uat/cycles/{payload['cycle_id']}")

    assert get_response.status_code == 200
    assert get_response.json()["cycle_name"] == "phase7-wave1"


def test_record_uat_execution_result_marks_cycle_in_progress(
    client: TestClient,
) -> None:
    cycle = _create_cycle(client, "phase7-execution")

    response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/scenarios/UAT-OPS-001/results",
        json={
            "status": "passed",
            "execution_notes": "Reviewed monitoring overview and thresholds.",
            "evidence_reference": "sharepoint://uat/phase7/ops-001",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    execution = next(
        item
        for item in payload["scenario_executions"]
        if item["scenario_id"] == "UAT-OPS-001"
    )
    assert payload["status"] == "in_progress"
    assert payload["started_at"] is not None
    assert execution["status"] == "passed"
    assert execution["executed_by"] == "Test Admin"
    assert execution["executed_at"] is not None
    assert _counts(payload["scenario_status_counts"]) == {
        "planned": 3,
        "in_progress": 0,
        "passed": 1,
        "failed": 0,
        "blocked": 0,
    }


def test_finalize_uat_cycle_requires_terminal_scenarios(
    client: TestClient,
) -> None:
    cycle = _create_cycle(client, "phase7-terminal-check")
    client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/scenarios/UAT-OPS-001/results",
        json={"status": "passed"},
    )

    response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/finalize",
        json={"status": "approved"},
    )

    assert response.status_code == 409
    assert "must be terminal" in response.json()["detail"]


def test_finalize_uat_cycle_blocks_open_high_defects_until_resolved(
    client: TestClient,
) -> None:
    cycle = _create_cycle(client, "phase7-defect-burn")
    cycle = _complete_cycle(client, cycle["cycle_id"])

    defect_response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/defects",
        json={
            "scenario_id": "UAT-OPS-002",
            "severity": "high",
            "title": "Retry confirmation remains ambiguous",
            "description": "Operators cannot confirm whether retry succeeded.",
            "owner_name": "Ops QA",
            "external_reference": "JIRA-701",
        },
    )

    assert defect_response.status_code == 201
    defect_payload = defect_response.json()
    defect_id = defect_payload["defects"][0]["defect_id"]

    blocked_response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/finalize",
        json={"status": "approved"},
    )

    assert blocked_response.status_code == 409
    assert "severity defects remain open" in blocked_response.json()["detail"]

    resolve_response = client.patch(
        f"/admin/uat/cycles/{cycle['cycle_id']}/defects/{defect_id}",
        json={
            "status": "resolved",
            "resolution_notes": "Operator toast and audit history were verified.",
        },
    )

    assert resolve_response.status_code == 200
    assert _counts(resolve_response.json()["defect_status_counts"]) == {
        "open": 0,
        "accepted": 0,
        "resolved": 1,
    }

    approved_response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/finalize",
        json={
            "status": "approved",
            "summary_notes": "Wave 1 operator rehearsal approved.",
        },
    )

    assert approved_response.status_code == 200
    approved_payload = approved_response.json()
    assert approved_payload["status"] == "approved"
    assert approved_payload["completed_at"] is not None
    assert approved_payload["summary_notes"] == "Wave 1 operator rehearsal approved."

    locked_response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/defects",
        json={
            "severity": "low",
            "title": "Late observation",
            "description": "This should not be accepted after approval.",
        },
    )

    assert locked_response.status_code == 409
    assert "already approved" in locked_response.json()["detail"]


def test_create_uat_cycle_rejects_duplicate_names(client: TestClient) -> None:
    first_response = client.post(
        "/admin/uat/cycles",
        json={"cycle_name": "phase7-duplicate"},
    )
    second_response = client.post(
        "/admin/uat/cycles",
        json={"cycle_name": "phase7-duplicate"},
    )

    assert first_response.status_code == 201
    assert second_response.status_code == 409
    assert "already exists" in second_response.json()["detail"]


def test_create_uat_defect_rejects_unknown_scenario(client: TestClient) -> None:
    cycle = _create_cycle(client, "phase7-bad-scenario")

    response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/defects",
        json={
            "scenario_id": "UAT-OPS-999",
            "severity": "medium",
            "title": "Unknown scenario reference",
            "description": "Defect references a scenario not in the cycle.",
        },
    )

    assert response.status_code == 404
    assert "is not registered" in response.json()["detail"]


def test_uat_routes_reject_reader_access(
    client: TestClient,
    reader_headers: dict[str, str],
) -> None:
    response = client.get("/admin/uat/cycles", headers=reader_headers)

    assert response.status_code == 403
    assert "requires one of the following roles" in response.json()["detail"]


def test_signoff_report_endpoint_returns_event_history_and_readiness(
    client: TestClient,
) -> None:
    cycle = _create_cycle(client, "phase7-signoff-report")
    cycle = _complete_cycle(client, cycle["cycle_id"])

    defect_response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/defects",
        json={
            "scenario_id": "UAT-OPS-002",
            "severity": "high",
            "title": "Retry messaging needs clarification",
            "description": "Operators need clearer confirmation after retry.",
            "owner_name": "Ops QA",
            "external_reference": "JIRA-702",
        },
    )
    defect_id = defect_response.json()["defects"][0]["defect_id"]

    client.patch(
        f"/admin/uat/cycles/{cycle['cycle_id']}/defects/{defect_id}",
        json={
            "status": "resolved",
            "resolution_notes": "Confirmation copy validated in rehearsal.",
        },
    )
    client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/finalize",
        json={"status": "approved", "summary_notes": "Approved for business review."},
    )

    response = client.get(f"/admin/uat/cycles/{cycle['cycle_id']}/signoff-report")

    assert response.status_code == 200
    payload = response.json()
    event_types = [item["event_type"] for item in payload["event_history"]]
    readiness = payload["approval_readiness"]

    assert payload["report_version"] == "phase7-uat-signoff-v1"
    assert payload["export_scope"] == "cycle_signoff"
    assert payload["exported_by"] == "Test Admin"
    assert payload["cycle"]["status"] == "approved"
    assert payload["open_defects"] == []
    assert readiness["approval_ready"] is True
    assert readiness["blocking_issue_count"] == 0
    assert readiness["terminal_scenario_count"] == 4
    assert readiness["evidence_captured_count"] == 4
    assert readiness["missing_evidence_count"] == 0
    assert readiness["open_high_severity_defect_count"] == 0
    assert event_types[0] == "cycle_created"
    assert event_types.count("scenario_result_recorded") == 4
    assert "defect_logged" in event_types
    assert "defect_updated" in event_types
    assert event_types[-1] == "cycle_finalized"
    assert payload["event_history"][-1]["event_payload"] == {"status": "approved"}


def test_signoff_report_endpoint_surfaces_blockers_and_attention_items(
    client: TestClient,
) -> None:
    cycle = _create_cycle(client, "phase7-signoff-blockers")
    client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/scenarios/UAT-OPS-001/results",
        json={"status": "passed"},
    )

    response = client.get(f"/admin/uat/cycles/{cycle['cycle_id']}/signoff-report")

    assert response.status_code == 200
    payload = response.json()
    readiness = payload["approval_readiness"]

    assert payload["cycle"]["status"] == "in_progress"
    assert readiness["approval_ready"] is False
    assert readiness["blocking_issue_count"] == 1
    assert "remain non-terminal" in readiness["blocking_issues"][0]
    assert readiness["missing_evidence_count"] == 1
    assert readiness["attention_item_count"] >= 1
    assert any(
        "missing evidence references" in item
        for item in readiness["attention_items"]
    )


def test_signoff_report_endpoint_rejects_operator_role(
    client: TestClient,
    operator_headers: dict[str, str],
) -> None:
    cycle = _create_cycle(client, "phase7-signoff-access")

    response = client.get(
        f"/admin/uat/cycles/{cycle['cycle_id']}/signoff-report",
        headers=operator_headers,
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "This operation requires one of the following roles: admin."


def test_create_list_get_and_accept_handoff_snapshot_flow(
    client: TestClient,
) -> None:
    cycle = _create_cycle(client, "phase7-handoff-flow")
    cycle = _complete_cycle(client, cycle["cycle_id"])
    client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/finalize",
        json={"status": "approved", "summary_notes": "Ready for launch board review."},
    )

    create_response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/handoff-snapshots",
        json={"snapshot_name": "launch-board-v1"},
    )

    assert create_response.status_code == 201
    snapshot_payload = create_response.json()
    snapshot_id = snapshot_payload["snapshot_id"]
    assert snapshot_payload["snapshot_name"] == "launch-board-v1"
    assert snapshot_payload["approval_ready"] is True
    assert snapshot_payload["acceptance_artifact_count"] == 0
    assert snapshot_payload["report_payload"]["cycle"]["status"] == "approved"
    assert "Approval ready: yes." in snapshot_payload["distribution_summary"]

    list_response = client.get(
        f"/admin/uat/cycles/{cycle['cycle_id']}/handoff-snapshots"
    )

    assert list_response.status_code == 200
    assert list_response.json()[0]["snapshot_id"] == snapshot_id

    artifact_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot_id}/acceptance-artifacts",
        json={
            "decision": "accepted",
            "stakeholder_name": "Launch Board",
            "stakeholder_role": "Executive Review",
            "stakeholder_organization": "Dense Data Center Locator",
            "decision_notes": "Approved for stakeholder handoff.",
        },
    )

    assert artifact_response.status_code == 201
    artifact_payload = artifact_response.json()
    assert artifact_payload["acceptance_artifact_count"] == 1
    assert artifact_payload["acceptance_artifacts"][0]["decision"] == "accepted"
    assert artifact_payload["acceptance_artifacts"][0]["recorded_by"] == "Test Admin"

    detail_response = client.get(f"/admin/uat/handoff-snapshots/{snapshot_id}")

    assert detail_response.status_code == 200
    detail_payload = detail_response.json()
    assert detail_payload["snapshot_name"] == "launch-board-v1"
    assert detail_payload["acceptance_artifacts"][0]["stakeholder_name"] == "Launch Board"


def test_handoff_snapshot_creation_allows_blocked_cycles_and_rejects_duplicates(
    client: TestClient,
) -> None:
    cycle = _create_cycle(client, "phase7-handoff-blocked")
    client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/scenarios/UAT-OPS-001/results",
        json={"status": "passed"},
    )

    first_response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/handoff-snapshots",
        json={"snapshot_name": "ops-review-v1"},
    )
    second_response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/handoff-snapshots",
        json={"snapshot_name": "ops-review-v1"},
    )

    assert first_response.status_code == 201
    first_payload = first_response.json()
    assert first_payload["approval_ready"] is False
    assert first_payload["blocking_issue_count"] == 1
    assert "Blockers:" in first_payload["distribution_summary"]

    assert second_response.status_code == 409
    assert "already exists" in second_response.json()["detail"]


def test_handoff_snapshot_routes_reject_operator_role(
    client: TestClient,
    operator_headers: dict[str, str],
) -> None:
    cycle = _create_cycle(client, "phase7-handoff-access")

    response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/handoff-snapshots",
        headers=operator_headers,
        json={"snapshot_name": "restricted-snapshot"},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "This operation requires one of the following roles: admin."


def test_create_list_get_and_progress_distribution_packet_flow(
    client: TestClient,
) -> None:
    snapshot = _create_handoff_snapshot(
        client,
        cycle_name="phase7-distribution-flow",
        snapshot_name="launch-board-v1",
    )

    create_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/distribution-packets",
        json={"packet_name": "launch-review-packet"},
    )

    assert create_response.status_code == 201
    packet_payload = create_response.json()
    packet_id = packet_payload["packet_id"]
    assert packet_payload["distribution_status"] == "draft"
    assert packet_payload["ready_to_send"] is False
    assert packet_payload["recipient_count"] == 0
    assert packet_payload["subject_line"].startswith("[UAT] phase7-distribution-flow")
    assert "Executive summary:" in packet_payload["briefing_body"]

    list_response = client.get(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/distribution-packets"
    )

    assert list_response.status_code == 200
    assert list_response.json()[0]["packet_id"] == packet_id

    recipient_response = client.post(
        f"/admin/uat/distribution-packets/{packet_id}/recipients",
        json={
            "recipient_name": "Launch Board",
            "recipient_role": "Executive Review",
            "recipient_organization": "Dense Data Center Locator",
            "recipient_contact": "launch-board@ddcl.test",
            "required_for_ack": True,
        },
    )

    assert recipient_response.status_code == 201
    recipient_payload = recipient_response.json()
    recipient_id = recipient_payload["recipients"][0]["recipient_id"]
    assert recipient_payload["distribution_status"] == "ready"
    assert recipient_payload["ready_to_send"] is True
    assert recipient_payload["recipient_count"] == 1
    assert recipient_payload["pending_recipient_count"] == 1

    sent_response = client.patch(
        f"/admin/uat/distribution-packets/{packet_id}/recipients/{recipient_id}",
        json={
            "delivery_status": "sent",
            "delivery_notes": "Sent to the launch board distribution list.",
        },
    )

    assert sent_response.status_code == 200
    sent_payload = sent_response.json()
    assert sent_payload["distribution_status"] == "distributed"
    assert sent_payload["distributed_at"] is not None
    assert sent_payload["pending_recipient_count"] == 0

    acknowledged_response = client.patch(
        f"/admin/uat/distribution-packets/{packet_id}/recipients/{recipient_id}",
        json={
            "delivery_status": "acknowledged",
            "acknowledged_by": "Launch Board",
            "acknowledgement_notes": "Board acknowledged receipt and readiness review.",
        },
    )

    assert acknowledged_response.status_code == 200
    acknowledged_payload = acknowledged_response.json()
    assert acknowledged_payload["distribution_status"] == "completed"
    assert acknowledged_payload["completed_at"] is not None
    assert acknowledged_payload["acknowledged_recipient_count"] == 1
    assert acknowledged_payload["recipients"][0]["delivery_status"] == "acknowledged"
    assert acknowledged_payload["recipients"][0]["acknowledged_by"] == "Launch Board"

    detail_response = client.get(f"/admin/uat/distribution-packets/{packet_id}")

    assert detail_response.status_code == 200
    detail_payload = detail_response.json()
    assert detail_payload["packet_name"] == "launch-review-packet"
    assert detail_payload["recipients"][0]["acknowledgement_notes"] == (
        "Board acknowledged receipt and readiness review."
    )


def test_distribution_packet_creation_rejects_duplicates_and_seeds_recipients(
    client: TestClient,
) -> None:
    snapshot = _create_handoff_snapshot(
        client,
        cycle_name="phase7-distribution-duplicate",
        snapshot_name="stakeholder-snapshot-v1",
    )

    first_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/distribution-packets",
        json={
            "packet_name": "stakeholder-briefing",
            "channel": "launch_review_packet",
            "recipients": [
                {
                    "recipient_name": "Launch PM",
                    "recipient_contact": "launch-pm@ddcl.test",
                    "required_for_ack": False,
                }
            ],
        },
    )
    second_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/distribution-packets",
        json={"packet_name": "stakeholder-briefing"},
    )

    assert first_response.status_code == 201
    first_payload = first_response.json()
    assert first_payload["channel"] == "launch_review_packet"
    assert first_payload["distribution_status"] == "ready"
    assert first_payload["ready_to_send"] is True
    assert first_payload["recipient_count"] == 1
    assert first_payload["required_recipient_count"] == 0

    assert second_response.status_code == 409
    assert "already exists" in second_response.json()["detail"]


def test_distribution_packet_routes_reject_operator_role(
    client: TestClient,
    operator_headers: dict[str, str],
) -> None:
    snapshot = _create_handoff_snapshot(
        client,
        cycle_name="phase7-distribution-access",
        snapshot_name="access-snapshot-v1",
    )

    response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/distribution-packets",
        headers=operator_headers,
        json={"packet_name": "restricted-packet"},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "This operation requires one of the following roles: admin."


def test_launch_readiness_flow_surfaces_exceptions_and_closeout_report(
    client: TestClient,
) -> None:
    snapshot = _create_handoff_snapshot(
        client,
        cycle_name="phase7-launch-readiness",
        snapshot_name="launch-reconciliation-v1",
    )
    packet = _create_distribution_packet(
        client,
        snapshot["snapshot_id"],
        packet_name="launch-board-packet",
        recipients=[
            {
                "recipient_name": "Launch Board",
                "recipient_role": "Executive Review",
                "recipient_contact": "launch-board@ddcl.test",
                "required_for_ack": True,
            }
        ],
    )
    recipient_id = packet["recipients"][0]["recipient_id"]
    client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/acceptance-artifacts",
        json={
            "decision": "follow_up_required",
            "stakeholder_name": "Operations Council",
            "stakeholder_role": "Operational Review",
            "decision_notes": "Need final confirmation on launch sequencing.",
        },
    )

    readiness_response = client.get(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/launch-readiness"
    )

    assert readiness_response.status_code == 200
    readiness_payload = readiness_response.json()
    assert readiness_payload["recommended_outcome"] == "hold"
    assert readiness_payload["blocking_exception_count"] >= 1
    assert readiness_payload["attention_exception_count"] >= 1
    assert any(
        "has not yet been sent" in item["summary"]
        for item in readiness_payload["exception_queue"]
    )

    recipient_update_response = client.patch(
        f"/admin/uat/distribution-packets/{packet['packet_id']}/recipients/{recipient_id}",
        json={
            "delivery_status": "acknowledged",
            "acknowledged_by": "Launch Board",
            "acknowledgement_notes": "Board acknowledged packet receipt.",
        },
    )

    assert recipient_update_response.status_code == 200

    decision_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/launch-decisions",
        json={
            "decision": "conditional_go",
            "reviewer_name": "Release Board Chair",
            "reviewer_role": "Release Governance",
            "decision_notes": "Conditional on the operations follow-up item.",
        },
    )

    assert decision_response.status_code == 201
    decision_payload = decision_response.json()
    assert decision_payload["recommended_outcome"] == "conditional_go"
    assert decision_payload["blocking_exception_count"] == 0
    assert _counts(decision_payload["launch_decision_counts"]) == {
        "go": 0,
        "conditional_go": 1,
        "hold": 0,
        "no_go": 0,
    }

    closeout_response = client.get(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/launch-closeout-report"
    )

    assert closeout_response.status_code == 200
    closeout_payload = closeout_response.json()
    assert closeout_payload["report_version"] == "phase7-launch-closeout-v1"
    assert closeout_payload["export_scope"] == "launch_closeout"
    assert closeout_payload["exported_by"] == "Test Admin"
    assert closeout_payload["readiness"]["recommended_outcome"] == "conditional_go"
    assert closeout_payload["packet_summaries"][0]["distribution_status"] == "completed"


def test_launch_readiness_recommends_no_go_when_rejection_exists(
    client: TestClient,
) -> None:
    snapshot = _create_handoff_snapshot(
        client,
        cycle_name="phase7-launch-nogo",
        snapshot_name="launch-reconciliation-nogo",
    )
    packet = _create_distribution_packet(
        client,
        snapshot["snapshot_id"],
        packet_name="executive-review-packet",
        recipients=[
            {
                "recipient_name": "Executive Board",
                "recipient_contact": "exec-board@ddcl.test",
                "required_for_ack": True,
            }
        ],
    )
    recipient_id = packet["recipients"][0]["recipient_id"]
    client.patch(
        f"/admin/uat/distribution-packets/{packet['packet_id']}/recipients/{recipient_id}",
        json={
            "delivery_status": "acknowledged",
            "acknowledged_by": "Executive Board",
            "acknowledgement_notes": "Packet received.",
        },
    )
    client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/acceptance-artifacts",
        json={
            "decision": "rejected",
            "stakeholder_name": "Executive Board",
            "stakeholder_role": "Final Approval",
            "decision_notes": "Launch deferred pending rework.",
        },
    )
    client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/launch-decisions",
        json={
            "decision": "go",
            "reviewer_name": "Release PM",
            "reviewer_role": "Program Lead",
            "decision_notes": "Would otherwise be ready.",
        },
    )

    readiness_response = client.get(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/launch-readiness"
    )

    assert readiness_response.status_code == 200
    readiness_payload = readiness_response.json()
    assert readiness_payload["recommended_outcome"] == "no_go"
    assert any(
        "rejected the handoff" in item["summary"]
        for item in readiness_payload["exception_queue"]
    )
    assert _counts(readiness_payload["launch_decision_counts"]) == {
        "go": 1,
        "conditional_go": 0,
        "hold": 0,
        "no_go": 0,
    }


def test_launch_readiness_routes_reject_operator_role(
    client: TestClient,
    operator_headers: dict[str, str],
) -> None:
    snapshot = _create_handoff_snapshot(
        client,
        cycle_name="phase7-launch-access",
        snapshot_name="launch-access-v1",
    )

    response = client.get(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/launch-readiness",
        headers=operator_headers,
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "This operation requires one of the following roles: admin."


def test_create_list_and_get_release_archive_flow(
    client: TestClient,
) -> None:
    snapshot = _create_handoff_snapshot(
        client,
        cycle_name="phase7-release-archive",
        snapshot_name="release-archive-v1",
    )
    packet = _create_distribution_packet(
        client,
        snapshot["snapshot_id"],
        packet_name="support-handoff-packet",
        recipients=[
            {
                "recipient_name": "Support Team",
                "recipient_contact": "support@ddcl.test",
                "required_for_ack": True,
            }
        ],
    )
    recipient_id = packet["recipients"][0]["recipient_id"]
    client.patch(
        f"/admin/uat/distribution-packets/{packet['packet_id']}/recipients/{recipient_id}",
        json={
            "delivery_status": "acknowledged",
            "acknowledged_by": "Support Team",
            "acknowledgement_notes": "Support team has the release packet.",
        },
    )
    client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/acceptance-artifacts",
        json={
            "decision": "accepted",
            "stakeholder_name": "Launch Board",
            "stakeholder_role": "Executive Review",
            "decision_notes": "Approved for release archive sealing.",
        },
    )

    create_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "release-board-v1",
            "support_handoff_owner": "Support Team",
            "operations_runbook_reference": "runbook://phase7/release-handoff",
            "release_manifest_notes": "Seal this package for the release board.",
        },
    )

    assert create_response.status_code == 201
    archive_payload = create_response.json()
    archive_id = archive_payload["archive_id"]
    assert archive_payload["recommended_outcome"] == "go"
    assert archive_payload["retention_status"] == "active"
    assert archive_payload["support_handoff_owner"] == "Support Team"
    assert archive_payload["archive_checksum"] is not None
    assert len(archive_payload["archive_checksum"]) == 64
    assert archive_payload["sealed_at"] is not None
    assert archive_payload["evidence_item_count"] >= 4
    assert archive_payload["manifest_payload"]["report_version"] == "phase7-release-archive-v2"
    assert archive_payload["manifest_payload"]["support_handoff"]["owner_name"] == "Support Team"
    assert archive_payload["manifest_payload"]["closeout_report"]["readiness"][
        "recommended_outcome"
    ] == "go"

    list_response = client.get(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/release-archives"
    )

    assert list_response.status_code == 200
    assert list_response.json()[0]["archive_id"] == archive_id

    detail_response = client.get(f"/admin/uat/release-archives/{archive_id}")

    assert detail_response.status_code == 200
    detail_payload = detail_response.json()
    assert detail_payload["archive_name"] == "release-board-v1"
    assert "Support owner: Support Team." in detail_payload["archive_summary"]
    assert len(detail_payload["evidence_items"]) == detail_payload["evidence_item_count"]


def test_release_archive_creation_rejects_duplicates_and_captures_no_go_state(
    client: TestClient,
) -> None:
    snapshot = _create_handoff_snapshot(
        client,
        cycle_name="phase7-release-archive-nogo",
        snapshot_name="release-archive-nogo",
    )
    packet = _create_distribution_packet(
        client,
        snapshot["snapshot_id"],
        packet_name="exec-handoff-packet",
        recipients=[
            {
                "recipient_name": "Executive Board",
                "recipient_contact": "exec-board@ddcl.test",
                "required_for_ack": True,
            }
        ],
    )
    recipient_id = packet["recipients"][0]["recipient_id"]
    client.patch(
        f"/admin/uat/distribution-packets/{packet['packet_id']}/recipients/{recipient_id}",
        json={
            "delivery_status": "acknowledged",
            "acknowledged_by": "Executive Board",
            "acknowledgement_notes": "Received for review.",
        },
    )
    client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/acceptance-artifacts",
        json={
            "decision": "rejected",
            "stakeholder_name": "Executive Board",
            "stakeholder_role": "Final Approval",
            "decision_notes": "Release is not approved.",
        },
    )

    first_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/release-archives",
        json={"archive_name": "release-board-v1"},
    )
    second_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/release-archives",
        json={"archive_name": "release-board-v1"},
    )

    assert first_response.status_code == 201
    first_payload = first_response.json()
    assert first_payload["recommended_outcome"] == "no_go"
    assert first_payload["blocking_exception_count"] >= 1
    assert first_payload["manifest_payload"]["closeout_report"]["readiness"][
        "recommended_outcome"
    ] == "no_go"

    assert second_response.status_code == 409
    assert "already exists" in second_response.json()["detail"]


def test_release_archive_search_supersession_and_evidence_index_flow(
    client: TestClient,
) -> None:
    cycle = _create_cycle(client, "phase7-release-archive-search")
    _complete_cycle(client, cycle["cycle_id"])

    finalize_response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/finalize",
        json={"status": "approved", "summary_notes": "Approved for archive retrieval tests."},
    )

    assert finalize_response.status_code == 200

    first_snapshot = _create_release_ready_snapshot(
        client,
        cycle_id=cycle["cycle_id"],
        snapshot_name="release-search-v1",
        packet_name="release-search-packet-v1",
    )
    second_snapshot = _create_release_ready_snapshot(
        client,
        cycle_id=cycle["cycle_id"],
        snapshot_name="release-search-v2",
        packet_name="release-search-packet-v2",
    )

    first_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{first_snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "release-board-v1",
            "support_handoff_owner": "Support Team",
            "retention_review_at": "2025-04-01T00:00:00Z",
        },
    )
    second_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{second_snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "release-board-v2",
            "support_handoff_owner": "Support Team",
        },
    )

    assert first_archive_response.status_code == 201
    assert second_archive_response.status_code == 201
    first_archive = first_archive_response.json()
    second_archive = second_archive_response.json()

    supersede_response = client.post(
        f"/admin/uat/release-archives/{first_archive['archive_id']}/supersede",
        json={
            "superseded_by_archive_id": second_archive["archive_id"],
            "supersession_reason": "Superseded by the final board archive.",
        },
    )

    assert supersede_response.status_code == 200
    superseded_payload = supersede_response.json()
    assert superseded_payload["superseded_by_archive_id"] == second_archive["archive_id"]
    assert superseded_payload["retention_status"] == "superseded"
    assert superseded_payload["supersession_reason"] == "Superseded by the final board archive."
    assert superseded_payload["superseded_at"] is not None

    active_search_response = client.get(
        "/admin/uat/release-archives",
        params={
            "cycle_id": cycle["cycle_id"],
            "include_superseded": "false",
            "search": "release-board",
        },
    )

    assert active_search_response.status_code == 200
    active_payload = active_search_response.json()
    assert len(active_payload) == 1
    assert active_payload[0]["archive_id"] == second_archive["archive_id"]
    assert active_payload[0]["retention_status"] == "active"

    superseded_search_response = client.get(
        "/admin/uat/release-archives",
        params={
            "cycle_id": cycle["cycle_id"],
            "retention_status": "superseded",
            "include_superseded": "true",
        },
    )

    assert superseded_search_response.status_code == 200
    superseded_search_payload = superseded_search_response.json()
    assert len(superseded_search_payload) == 1
    assert superseded_search_payload[0]["archive_id"] == first_archive["archive_id"]

    evidence_response = client.get(
        f"/admin/uat/release-archives/{first_archive['archive_id']}/evidence-items"
    )

    assert evidence_response.status_code == 200
    evidence_payload = evidence_response.json()
    assert len(evidence_payload) == superseded_payload["evidence_item_count"]
    assert any(item["evidence_type"] == "acceptance_artifact" for item in evidence_payload)
    assert any(item["retention_label"] == "operations_handoff" for item in evidence_payload)


def test_release_archive_search_rejects_invalid_retention_status(
    client: TestClient,
) -> None:
    response = client.get(
        "/admin/uat/release-archives",
        params={"retention_status": "expired"},
    )

    assert response.status_code == 400
    assert "Retention status" in response.json()["detail"]


def test_release_archive_retention_queue_and_export_flow(
    client: TestClient,
) -> None:
    cycle = _create_cycle(client, "phase7-release-archive-ops")
    _complete_cycle(client, cycle["cycle_id"])

    finalize_response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/finalize",
        json={"status": "approved", "summary_notes": "Approved for retention operations."},
    )

    assert finalize_response.status_code == 200

    overdue_snapshot = _create_release_ready_snapshot(
        client,
        cycle_id=cycle["cycle_id"],
        snapshot_name="release-ops-overdue",
        packet_name="release-ops-overdue-packet",
    )
    due_soon_snapshot = _create_release_ready_snapshot(
        client,
        cycle_id=cycle["cycle_id"],
        snapshot_name="release-ops-due-soon",
        packet_name="release-ops-due-soon-packet",
    )

    overdue_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{overdue_snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "release-ops-overdue-archive",
            "support_handoff_owner": "Support Team",
            "retention_review_at": "2026-04-10T00:00:00Z",
        },
    )
    due_soon_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{due_soon_snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "release-ops-due-soon-archive",
            "support_handoff_owner": "Support Team",
            "retention_review_at": "2026-04-25T00:00:00Z",
        },
    )

    assert overdue_archive_response.status_code == 201
    assert due_soon_archive_response.status_code == 201
    overdue_archive = overdue_archive_response.json()

    queue_response = client.get(
        "/admin/uat/release-archives/retention-queue",
        params={"review_window_days": 15},
    )

    assert queue_response.status_code == 200
    queue_payload = queue_response.json()
    assert queue_payload["item_count"] == 2
    assert queue_payload["overdue_count"] >= 1
    assert queue_payload["due_soon_count"] >= 1
    assert any(item["review_bucket"] == "overdue" for item in queue_payload["items"])
    assert any(item["review_bucket"] == "due_soon" for item in queue_payload["items"])

    export_create_response = client.post(
        f"/admin/uat/release-archives/{overdue_archive['archive_id']}/exports",
        json={
            "export_name": "audit-sync-v1",
            "export_scope": "audit_bundle",
            "destination_system": "audit-vault",
            "destination_reference": "vault://phase7/releases/audit-sync-v1",
            "trigger_reason": "Scheduled retention review handoff.",
            "handoff_notes": "Prepare this package for audit ingestion.",
        },
    )

    assert export_create_response.status_code == 201
    export_payload = export_create_response.json()
    export_id = export_payload["export_id"]
    assert export_payload["handoff_status"] == "prepared"
    assert export_payload["destination_system"] == "audit-vault"
    assert (
        export_payload["export_payload"]["archive"]["archive_id"]
        == overdue_archive["archive_id"]
    )

    export_list_response = client.get(
        f"/admin/uat/release-archives/{overdue_archive['archive_id']}/exports"
    )

    assert export_list_response.status_code == 200
    export_list_payload = export_list_response.json()
    assert len(export_list_payload) == 1
    assert export_list_payload[0]["export_id"] == export_id

    export_detail_response = client.get(f"/admin/uat/release-archive-exports/{export_id}")

    assert export_detail_response.status_code == 200
    export_detail_payload = export_detail_response.json()
    assert export_detail_payload["export_name"] == "audit-sync-v1"
    assert export_detail_payload["export_payload"]["destination"]["system"] == "audit-vault"
    assert (
        export_detail_payload["export_payload"]["archive"]["retention_status"] == "review_due"
    )


def test_release_archive_export_rejects_duplicates_and_retention_queue_hides_superseded(
    client: TestClient,
) -> None:
    cycle = _create_cycle(client, "phase7-release-archive-ops-duplicate")
    _complete_cycle(client, cycle["cycle_id"])

    finalize_response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/finalize",
        json={"status": "approved", "summary_notes": "Approved for archive export conflicts."},
    )

    assert finalize_response.status_code == 200

    first_snapshot = _create_release_ready_snapshot(
        client,
        cycle_id=cycle["cycle_id"],
        snapshot_name="release-ops-superseded",
        packet_name="release-ops-superseded-packet",
    )
    second_snapshot = _create_release_ready_snapshot(
        client,
        cycle_id=cycle["cycle_id"],
        snapshot_name="release-ops-successor",
        packet_name="release-ops-successor-packet",
    )

    first_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{first_snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "release-ops-v1",
            "retention_review_at": "2026-04-15T00:00:00Z",
        },
    )
    second_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{second_snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "release-ops-v2",
            "retention_review_at": "2026-04-18T00:00:00Z",
        },
    )

    assert first_archive_response.status_code == 201
    assert second_archive_response.status_code == 201
    first_archive = first_archive_response.json()
    second_archive = second_archive_response.json()

    supersede_response = client.post(
        f"/admin/uat/release-archives/{first_archive['archive_id']}/supersede",
        json={
            "superseded_by_archive_id": second_archive["archive_id"],
            "supersession_reason": "Replaced by updated release package.",
        },
    )

    assert supersede_response.status_code == 200

    first_export_response = client.post(
        f"/admin/uat/release-archives/{second_archive['archive_id']}/exports",
        json={
            "export_name": "support-sync-v1",
            "export_scope": "support_bundle",
            "destination_system": "support-cabinet",
        },
    )
    duplicate_export_response = client.post(
        f"/admin/uat/release-archives/{second_archive['archive_id']}/exports",
        json={
            "export_name": "support-sync-v1",
            "export_scope": "support_bundle",
            "destination_system": "support-cabinet",
        },
    )

    assert first_export_response.status_code == 201
    assert duplicate_export_response.status_code == 409
    assert "already exists" in duplicate_export_response.json()["detail"]

    hidden_queue_response = client.get(
        "/admin/uat/release-archives/retention-queue",
        params={"review_window_days": 10},
    )

    assert hidden_queue_response.status_code == 200
    hidden_queue_payload = hidden_queue_response.json()
    assert all(
        item["archive_id"] != first_archive["archive_id"]
        for item in hidden_queue_payload["items"]
    )

    visible_queue_response = client.get(
        "/admin/uat/release-archives/retention-queue",
        params={"review_window_days": 10, "include_superseded": "true"},
    )

    assert visible_queue_response.status_code == 200
    visible_queue_payload = visible_queue_response.json()
    assert any(
        item["archive_id"] == first_archive["archive_id"]
        for item in visible_queue_payload["items"]
    )
    assert any(item["review_bucket"] == "superseded" for item in visible_queue_payload["items"])


def test_release_archive_export_handoff_and_retention_action_flow(
    client: TestClient,
) -> None:
    cycle = _create_cycle(client, "phase7-release-archive-followup")
    _complete_cycle(client, cycle["cycle_id"])

    finalize_response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/finalize",
        json={"status": "approved", "summary_notes": "Approved for archive follow-up ops."},
    )

    assert finalize_response.status_code == 200

    snapshot = _create_release_ready_snapshot(
        client,
        cycle_id=cycle["cycle_id"],
        snapshot_name="release-followup-snapshot",
        packet_name="release-followup-packet",
    )
    archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "release-followup-archive",
            "retention_review_at": "2026-04-10T00:00:00Z",
        },
    )

    assert archive_response.status_code == 201
    archive_payload = archive_response.json()

    export_create_response = client.post(
        f"/admin/uat/release-archives/{archive_payload['archive_id']}/exports",
        json={
            "export_name": "audit-followup-v1",
            "export_scope": "audit_bundle",
            "destination_system": "audit-vault",
        },
    )

    assert export_create_response.status_code == 201
    export_payload = export_create_response.json()
    export_id = export_payload["export_id"]
    assert export_payload["retry_count"] == 0
    assert export_payload["last_status_updated_by"] == "Test Admin"

    delivered_response = client.patch(
        f"/admin/uat/release-archive-exports/{export_id}",
        json={
            "handoff_status": "delivered",
            "handoff_notes": "Delivered to the audit vault intake queue.",
        },
    )

    assert delivered_response.status_code == 200
    delivered_payload = delivered_response.json()
    assert delivered_payload["handoff_status"] == "delivered"
    assert delivered_payload["handoff_notes"] == "Delivered to the audit vault intake queue."

    acknowledged_response = client.patch(
        f"/admin/uat/release-archive-exports/{export_id}",
        json={
            "handoff_status": "acknowledged",
            "delivery_confirmed_by": "Audit Vault",
        },
    )

    assert acknowledged_response.status_code == 200
    acknowledged_payload = acknowledged_response.json()
    assert acknowledged_payload["handoff_status"] == "acknowledged"
    assert acknowledged_payload["delivery_confirmed_by"] == "Audit Vault"
    assert acknowledged_payload["delivery_confirmed_at"] is not None

    action_create_response = client.post(
        f"/admin/uat/release-archives/{archive_payload['archive_id']}/retention-actions",
        json={
            "action_type": "re_export_requested",
            "action_notes": "Archive needs a refreshed downstream export.",
            "related_export_id": export_id,
            "scheduled_retry_at": "2026-04-20T00:00:00Z",
            "next_retention_review_at": "2026-06-01T00:00:00Z",
        },
    )

    assert action_create_response.status_code == 201
    action_payload = action_create_response.json()
    assert action_payload["action_type"] == "re_export_requested"
    assert action_payload["related_export_id"] == export_id
    assert action_payload["related_export_status"] == "re_export_scheduled"
    assert action_payload["scheduled_retry_at"] == "2026-04-20T00:00:00Z"
    assert action_payload["next_retention_review_at"] == "2026-06-01T00:00:00Z"

    export_detail_response = client.get(f"/admin/uat/release-archive-exports/{export_id}")

    assert export_detail_response.status_code == 200
    export_detail_payload = export_detail_response.json()
    assert export_detail_payload["handoff_status"] == "re_export_scheduled"
    assert export_detail_payload["next_retry_at"] == "2026-04-20T00:00:00Z"
    assert export_detail_payload["retry_count"] == 1
    assert export_detail_payload["last_status_updated_by"] == "Test Admin"

    action_list_response = client.get(
        f"/admin/uat/release-archives/{archive_payload['archive_id']}/retention-actions"
    )

    assert action_list_response.status_code == 200
    action_list_payload = action_list_response.json()
    assert len(action_list_payload) == 1
    assert action_list_payload[0]["action_id"] == action_payload["action_id"]

    queue_response = client.get(
        "/admin/uat/release-archives/retention-queue",
        params={"review_window_days": 15},
    )

    assert queue_response.status_code == 200
    assert queue_response.json()["item_count"] == 0


def test_release_archive_followup_routes_validate_retry_and_export_membership(
    client: TestClient,
) -> None:
    cycle = _create_cycle(client, "phase7-release-archive-followup-validation")
    _complete_cycle(client, cycle["cycle_id"])

    finalize_response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/finalize",
        json={"status": "approved", "summary_notes": "Approved for archive follow-up validation."},
    )

    assert finalize_response.status_code == 200

    first_snapshot = _create_release_ready_snapshot(
        client,
        cycle_id=cycle["cycle_id"],
        snapshot_name="release-followup-validation-v1",
        packet_name="release-followup-validation-packet-v1",
    )
    second_snapshot = _create_release_ready_snapshot(
        client,
        cycle_id=cycle["cycle_id"],
        snapshot_name="release-followup-validation-v2",
        packet_name="release-followup-validation-packet-v2",
    )

    first_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{first_snapshot['snapshot_id']}/release-archives",
        json={"archive_name": "release-followup-validation-v1"},
    )
    second_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{second_snapshot['snapshot_id']}/release-archives",
        json={"archive_name": "release-followup-validation-v2"},
    )

    assert first_archive_response.status_code == 201
    assert second_archive_response.status_code == 201
    first_archive = first_archive_response.json()
    second_archive = second_archive_response.json()

    export_response = client.post(
        f"/admin/uat/release-archives/{first_archive['archive_id']}/exports",
        json={
            "export_name": "followup-validation-export",
            "export_scope": "support_bundle",
            "destination_system": "support-cabinet",
        },
    )

    assert export_response.status_code == 201
    export_id = export_response.json()["export_id"]

    invalid_export_update_response = client.patch(
        f"/admin/uat/release-archive-exports/{export_id}",
        json={"handoff_status": "re_export_scheduled"},
    )

    assert invalid_export_update_response.status_code == 400
    assert "next_retry_at" in invalid_export_update_response.json()["detail"]

    cross_archive_action_response = client.post(
        f"/admin/uat/release-archives/{second_archive['archive_id']}/retention-actions",
        json={
            "action_type": "re_export_requested",
            "related_export_id": export_id,
            "scheduled_retry_at": "2026-04-22T00:00:00Z",
        },
    )

    assert cross_archive_action_response.status_code == 400
    assert "same release archive" in cross_archive_action_response.json()["detail"]


def test_release_archive_followup_dashboard_and_bulk_review_flow(
    client: TestClient,
) -> None:
    cycle = _create_cycle(client, "phase7-release-archive-dashboard")
    _complete_cycle(client, cycle["cycle_id"])

    finalize_response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/finalize",
        json={"status": "approved", "summary_notes": "Approved for follow-up dashboard flow."},
    )

    assert finalize_response.status_code == 200

    overdue_snapshot = _create_release_ready_snapshot(
        client,
        cycle_id=cycle["cycle_id"],
        snapshot_name="release-dashboard-overdue",
        packet_name="release-dashboard-overdue-packet",
    )
    due_soon_snapshot = _create_release_ready_snapshot(
        client,
        cycle_id=cycle["cycle_id"],
        snapshot_name="release-dashboard-due-soon",
        packet_name="release-dashboard-due-soon-packet",
    )
    followup_snapshot = _create_release_ready_snapshot(
        client,
        cycle_id=cycle["cycle_id"],
        snapshot_name="release-dashboard-followup",
        packet_name="release-dashboard-followup-packet",
    )

    overdue_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{overdue_snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "release-dashboard-overdue-archive",
            "retention_review_at": "2026-04-10T00:00:00Z",
        },
    )
    due_soon_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{due_soon_snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "release-dashboard-due-soon-archive",
            "retention_review_at": "2026-04-25T00:00:00Z",
        },
    )
    followup_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{followup_snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "release-dashboard-followup-archive",
            "retention_review_at": "2026-07-01T00:00:00Z",
        },
    )

    assert overdue_archive_response.status_code == 201
    assert due_soon_archive_response.status_code == 201
    assert followup_archive_response.status_code == 201
    overdue_archive = overdue_archive_response.json()
    due_soon_archive = due_soon_archive_response.json()
    followup_archive = followup_archive_response.json()

    export_response = client.post(
        f"/admin/uat/release-archives/{followup_archive['archive_id']}/exports",
        json={
            "export_name": "release-dashboard-followup-export",
            "export_scope": "support_bundle",
            "destination_system": "support-cabinet",
        },
    )

    assert export_response.status_code == 201
    export_id = export_response.json()["export_id"]

    followup_action_response = client.post(
        f"/admin/uat/release-archives/{followup_archive['archive_id']}/retention-actions",
        json={
            "action_type": "re_export_requested",
            "action_notes": "Schedule downstream refresh.",
            "related_export_id": export_id,
            "scheduled_retry_at": "2026-04-25T00:00:00Z",
            "next_retention_review_at": "2026-07-15T00:00:00Z",
        },
    )

    assert followup_action_response.status_code == 201

    dashboard_response = client.get(
        "/admin/uat/release-archives/followup-dashboard",
        params={"review_window_days": 15},
    )

    assert dashboard_response.status_code == 200
    dashboard_payload = dashboard_response.json()
    assert dashboard_payload["total_archive_count"] == 3
    assert dashboard_payload["action_required_count"] == 3
    assert dashboard_payload["overdue_review_count"] == 1
    assert dashboard_payload["due_soon_review_count"] == 1
    assert dashboard_payload["follow_up_export_count"] == 1
    assert dashboard_payload["re_export_scheduled_count"] == 1
    assert any(
        item["archive_id"] == overdue_archive["archive_id"]
        and "overdue_review" in item["attention_reasons"]
        for item in dashboard_payload["items"]
    )
    assert any(
        item["archive_id"] == due_soon_archive["archive_id"]
        and "due_soon_review" in item["attention_reasons"]
        for item in dashboard_payload["items"]
    )
    assert any(
        item["archive_id"] == followup_archive["archive_id"]
        and "re_export_scheduled" in item["attention_reasons"]
        for item in dashboard_payload["items"]
    )

    bulk_response = client.post(
        "/admin/uat/release-archives/bulk-retention-actions",
        json={
            "archive_ids": [
                overdue_archive["archive_id"],
                due_soon_archive["archive_id"],
            ],
            "action_type": "review_completed",
            "action_notes": "Batch retention review completed.",
            "next_retention_review_at": "2026-06-15T00:00:00Z",
        },
    )

    assert bulk_response.status_code == 200
    bulk_payload = bulk_response.json()
    assert bulk_payload["requested_count"] == 2
    assert bulk_payload["applied_count"] == 2
    assert bulk_payload["failed_count"] == 0
    assert all(result["applied"] is True for result in bulk_payload["results"])

    queue_response = client.get(
        "/admin/uat/release-archives/retention-queue",
        params={"review_window_days": 15},
    )

    assert queue_response.status_code == 200
    assert queue_response.json()["item_count"] == 0

    updated_dashboard_response = client.get(
        "/admin/uat/release-archives/followup-dashboard",
        params={"review_window_days": 15},
    )

    assert updated_dashboard_response.status_code == 200
    updated_dashboard_payload = updated_dashboard_response.json()
    assert updated_dashboard_payload["action_required_count"] == 1
    assert updated_dashboard_payload["overdue_review_count"] == 0
    assert updated_dashboard_payload["due_soon_review_count"] == 0
    assert updated_dashboard_payload["follow_up_export_count"] == 1
    assert len(updated_dashboard_payload["items"]) == 1
    assert updated_dashboard_payload["items"][0]["archive_id"] == followup_archive["archive_id"]


def test_bulk_retention_action_rejects_reexport_requests(
    client: TestClient,
) -> None:
    snapshot = _create_handoff_snapshot(
        client,
        cycle_name="phase7-release-archive-bulk-validation",
        snapshot_name="release-bulk-validation",
    )

    archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/release-archives",
        json={"archive_name": "release-bulk-validation-archive"},
    )

    assert archive_response.status_code == 201
    archive_id = archive_response.json()["archive_id"]

    bulk_response = client.post(
        "/admin/uat/release-archives/bulk-retention-actions",
        json={
            "archive_ids": [archive_id],
            "action_type": "re_export_requested",
            "action_notes": "This is not allowed in bulk mode.",
        },
    )

    assert bulk_response.status_code == 400
    assert "only supports review_completed and retention_extended" in bulk_response.json()[
        "detail"
    ]


def test_execute_due_reexports_and_notification_digest_flow(
    client: TestClient,
) -> None:
    cycle = _create_cycle(client, "phase7-release-archive-automation")
    _complete_cycle(client, cycle["cycle_id"])

    finalize_response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/finalize",
        json={"status": "approved", "summary_notes": "Approved for archive automation flow."},
    )

    assert finalize_response.status_code == 200

    snapshot = _create_release_ready_snapshot(
        client,
        cycle_id=cycle["cycle_id"],
        snapshot_name="release-automation-snapshot",
        packet_name="release-automation-packet",
    )
    archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "release-automation-archive",
            "support_handoff_owner": "Support Team",
            "retention_review_at": _utc_iso(days=45),
        },
    )

    assert archive_response.status_code == 201
    archive_payload = archive_response.json()

    export_response = client.post(
        f"/admin/uat/release-archives/{archive_payload['archive_id']}/exports",
        json={
            "export_name": "release-automation-export",
            "export_scope": "support_bundle",
            "destination_system": "support-cabinet",
        },
    )

    assert export_response.status_code == 201
    source_export = export_response.json()

    action_response = client.post(
        f"/admin/uat/release-archives/{archive_payload['archive_id']}/retention-actions",
        json={
            "action_type": "re_export_requested",
            "action_notes": "Retry this export in the next automation sweep.",
            "related_export_id": source_export["export_id"],
            "scheduled_retry_at": _utc_iso(days=1),
            "next_retention_review_at": _utc_iso(days=60),
        },
    )

    assert action_response.status_code == 201

    preview_response = client.post(
        "/admin/uat/release-archives/execute-due-reexports",
        json={"dry_run": True, "run_at": _utc_iso(days=2)},
    )

    assert preview_response.status_code == 200
    preview_payload = preview_response.json()
    assert preview_payload["dry_run"] is True
    assert preview_payload["due_export_count"] == 1
    assert preview_payload["executed_count"] == 0
    assert preview_payload["items"][0]["action"] == "preview"
    assert preview_payload["items"][0]["generated_export_name"].startswith(
        "release-automation-export-retry-"
    )

    digest_before_response = client.get(
        "/admin/uat/release-archives/followup-notification-digest",
        params={"review_window_days": 30},
    )

    assert digest_before_response.status_code == 200
    digest_before_payload = digest_before_response.json()
    assert digest_before_payload["recipient_count"] == 1
    assert digest_before_payload["recipients"][0]["recipient_name"] == "Support Team"
    assert digest_before_payload["recipients"][0]["re_export_due_count"] == 0
    assert any(
        "re_export_scheduled" in archive_item["attention_reasons"]
        for archive_item in digest_before_payload["recipients"][0]["archives"]
    )

    execute_response = client.post(
        "/admin/uat/release-archives/execute-due-reexports",
        json={"dry_run": False, "run_at": _utc_iso(days=2)},
    )

    assert execute_response.status_code == 200
    execute_payload = execute_response.json()
    assert execute_payload["dry_run"] is False
    assert execute_payload["due_export_count"] == 1
    assert execute_payload["executed_count"] == 1
    assert execute_payload["items"][0]["action"] == "executed"
    assert execute_payload["items"][0]["generated_export_id"] is not None
    assert execute_payload["items"][0]["resulting_source_status"] == "re_export_completed"
    generated_export_id = execute_payload["items"][0]["generated_export_id"]

    source_export_detail_response = client.get(
        f"/admin/uat/release-archive-exports/{source_export['export_id']}"
    )

    assert source_export_detail_response.status_code == 200
    source_export_detail = source_export_detail_response.json()
    assert source_export_detail["handoff_status"] == "re_export_completed"
    assert source_export_detail["next_retry_at"] is None

    generated_export_detail_response = client.get(
        f"/admin/uat/release-archive-exports/{generated_export_id}"
    )

    assert generated_export_detail_response.status_code == 200
    generated_export_detail = generated_export_detail_response.json()
    assert generated_export_detail["handoff_status"] == "prepared"
    assert generated_export_detail["export_name"].startswith("release-automation-export-retry-")

    digest_after_response = client.get(
        "/admin/uat/release-archives/followup-notification-digest",
        params={"review_window_days": 30},
    )

    assert digest_after_response.status_code == 200
    digest_after_payload = digest_after_response.json()
    assert digest_after_payload["recipient_count"] == 1
    assert digest_after_payload["recipients"][0]["acknowledgement_pending_count"] == 1
    assert digest_after_payload["recipients"][0]["re_export_due_count"] == 0
    assert any(
        "acknowledgement_pending" in archive_item["attention_reasons"]
        for archive_item in digest_after_payload["recipients"][0]["archives"]
    )


def test_execute_followup_notification_dispatch_preview_and_execution_flow(
    client: TestClient,
) -> None:
    cycle = _create_cycle(client, "phase7-release-archive-dispatch-run")
    _complete_cycle(client, cycle["cycle_id"])

    finalize_response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/finalize",
        json={"status": "approved", "summary_notes": "Approved for notification dispatch."},
    )

    assert finalize_response.status_code == 200

    snapshot = _create_release_ready_snapshot(
        client,
        cycle_id=cycle["cycle_id"],
        snapshot_name="dispatch-run-snapshot",
        packet_name="dispatch-run-packet",
    )

    ready_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "dispatch-ready-archive",
            "support_handoff_owner": "Support Team",
            "retention_review_at": _utc_iso(days=10),
        },
    )

    assert ready_archive_response.status_code == 201
    ready_archive = ready_archive_response.json()

    ready_export_response = client.post(
        f"/admin/uat/release-archives/{ready_archive['archive_id']}/exports",
        json={
            "export_name": "dispatch-ready-export",
            "export_scope": "support_bundle",
            "destination_system": "support-cabinet",
        },
    )

    assert ready_export_response.status_code == 201
    ready_export = ready_export_response.json()

    missing_export_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "dispatch-missing-export",
            "support_handoff_owner": "Support Team",
            "retention_review_at": _utc_iso(days=10),
        },
    )

    assert missing_export_archive_response.status_code == 201

    digest_before_response = client.get(
        "/admin/uat/release-archives/followup-notification-digest",
        params={"review_window_days": 30},
    )

    assert digest_before_response.status_code == 200
    digest_before_payload = digest_before_response.json()
    assert digest_before_payload["recipient_count"] == 1
    assert digest_before_payload["recipients"][0]["notification_acknowledgement_pending_count"] == 0

    preview_response = client.post(
        "/admin/uat/release-archives/execute-followup-notification-dispatch",
        json={
            "dry_run": True,
            "review_window_days": 30,
            "delivery_channel": "email",
        },
    )

    assert preview_response.status_code == 200
    preview_payload = preview_response.json()
    assert preview_payload["dry_run"] is True
    assert preview_payload["planned_archive_count"] == 1
    assert preview_payload["recorded_archive_count"] == 0
    assert preview_payload["skipped_archive_count"] == 1
    preview_recipient = preview_payload["recipients"][0]
    assert preview_recipient["subject_line"] == (
        "[UAT Follow-up] Support Team - 2 archives need attention"
    )
    preview_actions = {
        item["archive_name"]: item["action"] for item in preview_recipient["archives"]
    }
    assert preview_actions == {
        "dispatch-ready-archive": "preview_send",
        "dispatch-missing-export": "skipped_missing_export",
    }

    execute_response = client.post(
        "/admin/uat/release-archives/execute-followup-notification-dispatch",
        json={
            "dry_run": False,
            "review_window_days": 30,
            "delivery_channel": "email",
        },
    )

    assert execute_response.status_code == 200
    execute_payload = execute_response.json()
    assert execute_payload["dry_run"] is False
    assert execute_payload["planned_archive_count"] == 0
    assert execute_payload["recorded_archive_count"] == 1
    assert execute_payload["skipped_archive_count"] == 1
    assert execute_payload["failed_archive_count"] == 0
    execute_recipient = execute_payload["recipients"][0]
    ready_item = next(
        item
        for item in execute_recipient["archives"]
        if item["archive_name"] == "dispatch-ready-archive"
    )
    assert ready_item["action"] == "recorded"
    assert ready_item["delivery_event_id"] is not None
    assert ready_item["external_reference"].startswith("uat-followup://dispatch/")

    missing_item = next(
        item
        for item in execute_recipient["archives"]
        if item["archive_name"] == "dispatch-missing-export"
    )
    assert missing_item["action"] == "skipped_missing_export"
    assert missing_item["delivery_event_id"] is None

    delivery_events_response = client.get(
        f"/admin/uat/release-archive-exports/{ready_export['export_id']}/delivery-events"
    )

    assert delivery_events_response.status_code == 200
    delivery_events_payload = delivery_events_response.json()
    assert len(delivery_events_payload) == 1
    assert delivery_events_payload[0]["event_type"] == "notification_sent"

    digest_after_response = client.get(
        "/admin/uat/release-archives/followup-notification-digest",
        params={"review_window_days": 30},
    )

    assert digest_after_response.status_code == 200
    digest_after_payload = digest_after_response.json()
    assert digest_after_payload["recipient_count"] == 1
    assert digest_after_payload["recipients"][0]["notification_acknowledgement_pending_count"] == 1

    rerun_response = client.post(
        "/admin/uat/release-archives/execute-followup-notification-dispatch",
        json={
            "dry_run": False,
            "review_window_days": 30,
            "delivery_channel": "email",
        },
    )

    assert rerun_response.status_code == 200
    rerun_payload = rerun_response.json()
    assert rerun_payload["recorded_archive_count"] == 0
    assert rerun_payload["skipped_archive_count"] == 2
    rerun_recipient = rerun_payload["recipients"][0]
    rerun_ready_item = next(
        item
        for item in rerun_recipient["archives"]
        if item["archive_name"] == "dispatch-ready-archive"
    )
    assert rerun_ready_item["action"] == "skipped_pending_acknowledgement"

    delivery_events_after_rerun_response = client.get(
        f"/admin/uat/release-archive-exports/{ready_export['export_id']}/delivery-events"
    )

    assert delivery_events_after_rerun_response.status_code == 200
    assert len(delivery_events_after_rerun_response.json()) == 1


def test_execute_followup_notification_dispatch_rejects_future_live_run(
    client: TestClient,
) -> None:
    response = client.post(
        "/admin/uat/release-archives/execute-followup-notification-dispatch",
        json={
            "dry_run": False,
            "delivery_channel": "email",
            "run_at": _utc_iso(days=1),
        },
    )

    assert response.status_code == 400
    assert "cannot execute in the future" in response.json()["detail"]


def test_release_archive_delivery_ledger_surfaces_escalation_and_retry_status(
    client: TestClient,
) -> None:
    cycle = _create_cycle(client, "phase7-release-archive-delivery-ledger")
    _complete_cycle(client, cycle["cycle_id"])

    finalize_response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/finalize",
        json={"status": "approved", "summary_notes": "Approved for delivery ledger review."},
    )

    assert finalize_response.status_code == 200

    snapshot = _create_release_ready_snapshot(
        client,
        cycle_id=cycle["cycle_id"],
        snapshot_name="delivery-ledger-snapshot",
        packet_name="delivery-ledger-packet",
    )

    escalated_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "ledger-escalated-archive",
            "support_handoff_owner": "Support Team",
            "retention_review_at": _utc_iso(days=10),
        },
    )

    assert escalated_archive_response.status_code == 201
    escalated_archive = escalated_archive_response.json()

    escalated_export_response = client.post(
        f"/admin/uat/release-archives/{escalated_archive['archive_id']}/exports",
        json={
            "export_name": "ledger-escalated-export",
            "export_scope": "support_bundle",
            "destination_system": "support-cabinet",
        },
    )

    assert escalated_export_response.status_code == 201
    escalated_export = escalated_export_response.json()

    notification_sent_response = client.post(
        f"/admin/uat/release-archive-exports/{escalated_export['export_id']}/delivery-events",
        json={
            "event_type": "notification_sent",
            "target_name": "Support Team",
            "delivery_channel": "email",
            "external_reference": "outlook://message/escalated",
            "event_notes": "Sent the first follow-up message.",
            "occurred_at": _utc_iso(days=-3),
        },
    )

    assert notification_sent_response.status_code == 201

    acknowledged_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "ledger-acknowledged-archive",
            "support_handoff_owner": "Support Team",
            "retention_review_at": _utc_iso(days=10),
        },
    )

    assert acknowledged_archive_response.status_code == 201
    acknowledged_archive = acknowledged_archive_response.json()

    acknowledged_export_response = client.post(
        f"/admin/uat/release-archives/{acknowledged_archive['archive_id']}/exports",
        json={
            "export_name": "ledger-acknowledged-export",
            "export_scope": "support_bundle",
            "destination_system": "support-cabinet",
        },
    )

    assert acknowledged_export_response.status_code == 201
    acknowledged_export = acknowledged_export_response.json()

    acknowledged_send_response = client.post(
        f"/admin/uat/release-archive-exports/{acknowledged_export['export_id']}/delivery-events",
        json={
            "event_type": "notification_sent",
            "target_name": "Support Team",
            "delivery_channel": "email",
            "external_reference": "outlook://message/acknowledged",
            "event_notes": "Sent the acknowledged follow-up message.",
            "occurred_at": _utc_iso(days=-2),
        },
    )

    assert acknowledged_send_response.status_code == 201

    acknowledged_ack_response = client.post(
        f"/admin/uat/release-archive-exports/{acknowledged_export['export_id']}/delivery-events",
        json={
            "event_type": "notification_acknowledged",
            "target_name": "Support Team",
            "delivery_channel": "email",
            "event_notes": "Support confirmed receipt.",
            "occurred_at": _utc_iso(days=-1),
        },
    )

    assert acknowledged_ack_response.status_code == 201

    missing_export_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "ledger-missing-export-archive",
            "support_handoff_owner": "Support Team",
            "retention_review_at": _utc_iso(days=10),
        },
    )

    assert missing_export_archive_response.status_code == 201

    retry_due_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "ledger-retry-due-archive",
            "support_handoff_owner": "Support Team",
            "retention_review_at": _utc_iso(days=45),
        },
    )

    assert retry_due_archive_response.status_code == 201
    retry_due_archive = retry_due_archive_response.json()

    retry_due_export_response = client.post(
        f"/admin/uat/release-archives/{retry_due_archive['archive_id']}/exports",
        json={
            "export_name": "ledger-retry-due-export",
            "export_scope": "support_bundle",
            "destination_system": "support-cabinet",
        },
    )

    assert retry_due_export_response.status_code == 201
    retry_due_export = retry_due_export_response.json()

    retry_due_action_response = client.post(
        f"/admin/uat/release-archives/{retry_due_archive['archive_id']}/retention-actions",
        json={
            "action_type": "re_export_requested",
            "action_notes": "Retry export in the next sweep.",
            "related_export_id": retry_due_export["export_id"],
            "scheduled_retry_at": _utc_iso(days=1),
            "next_retention_review_at": _utc_iso(days=60),
        },
    )

    assert retry_due_action_response.status_code == 201

    ledger_response = client.get(
        "/admin/uat/release-archives/delivery-ledger",
        params={
            "review_window_days": 30,
            "stale_reply_after_hours": 24,
            "as_of": _utc_iso(days=2),
        },
    )

    assert ledger_response.status_code == 200
    ledger_payload = ledger_response.json()
    assert ledger_payload["report_version"] == "phase7-release-archive-delivery-ledger-v1"
    assert ledger_payload["total_archive_count"] == 4
    assert ledger_payload["ledger_item_count"] == 4
    assert ledger_payload["escalated_count"] == 1
    assert ledger_payload["notification_pending_count"] == 1
    assert ledger_payload["acknowledged_count"] == 1
    assert ledger_payload["missing_export_count"] == 1
    assert ledger_payload["re_export_due_count"] == 1

    items_by_name = {item["archive_name"]: item for item in ledger_payload["items"]}

    escalated_item = items_by_name["ledger-escalated-archive"]
    assert escalated_item["notification_acknowledgement_status"] == "pending"
    assert escalated_item["escalation_status"] == "escalated"
    assert escalated_item["recommended_action"] == "escalate_notification_reply"
    assert "stale_notification_reply" in escalated_item["attention_reasons"]
    assert escalated_item["latest_notification_reference"] == "outlook://message/escalated"

    acknowledged_item = items_by_name["ledger-acknowledged-archive"]
    assert acknowledged_item["notification_acknowledgement_status"] == "acknowledged"
    assert acknowledged_item["latest_notification_acknowledged_at"] is not None
    assert acknowledged_item["escalation_status"] == "none"

    missing_export_item = items_by_name["ledger-missing-export-archive"]
    assert missing_export_item["latest_export_id"] is None
    assert missing_export_item["escalation_status"] == "missing_export"
    assert missing_export_item["recommended_action"] == "create_archive_export"
    assert "missing_export" in missing_export_item["attention_reasons"]

    retry_due_item = items_by_name["ledger-retry-due-archive"]
    assert retry_due_item["recommended_action"] == "execute_scheduled_reexport"
    assert "re_export_due" in retry_due_item["attention_reasons"]
    assert retry_due_item["next_retry_at"] is not None


def test_release_archive_delivery_ledger_rejects_invalid_escalation_window(
    client: TestClient,
) -> None:
    response = client.get(
        "/admin/uat/release-archives/delivery-ledger",
        params={"stale_reply_after_hours": 0},
    )

    assert response.status_code == 400
    assert "greater than 0 hours" in response.json()["detail"]


def test_release_archive_support_handback_report_groups_ready_and_unresolved_work(
    client: TestClient,
) -> None:
    cycle = _create_cycle(client, "phase7-release-archive-support-handback")
    _complete_cycle(client, cycle["cycle_id"])

    finalize_response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/finalize",
        json={"status": "approved", "summary_notes": "Approved for support handback."},
    )

    assert finalize_response.status_code == 200

    snapshot = _create_release_ready_snapshot(
        client,
        cycle_id=cycle["cycle_id"],
        snapshot_name="support-handback-snapshot",
        packet_name="support-handback-packet",
    )

    ready_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "handback-ready-archive",
            "support_handoff_owner": "Support Team",
            "operations_runbook_reference": "runbook://support/ready",
            "support_handoff_summary_override": "Ready for support handback closure.",
            "retention_review_at": _utc_iso(days=10),
        },
    )

    assert ready_archive_response.status_code == 201
    ready_archive = ready_archive_response.json()

    ready_export_response = client.post(
        f"/admin/uat/release-archives/{ready_archive['archive_id']}/exports",
        json={
            "export_name": "handback-ready-export",
            "export_scope": "support_bundle",
            "destination_system": "support-cabinet",
        },
    )

    assert ready_export_response.status_code == 201
    ready_export = ready_export_response.json()

    ready_handoff_response = client.post(
        f"/admin/uat/release-archive-exports/{ready_export['export_id']}/delivery-events",
        json={
            "event_type": "external_handoff_logged",
            "target_name": "Support Cabinet",
            "delivery_channel": "vendor_portal",
            "external_reference": "support://cabinet/ready",
            "event_notes": "Transferred to the downstream cabinet.",
            "occurred_at": _utc_iso(days=-2),
        },
    )

    assert ready_handoff_response.status_code == 201

    ready_send_response = client.post(
        f"/admin/uat/release-archive-exports/{ready_export['export_id']}/delivery-events",
        json={
            "event_type": "notification_sent",
            "target_name": "Support Team",
            "delivery_channel": "email",
            "external_reference": "outlook://message/ready",
            "event_notes": "Sent the handback summary.",
            "occurred_at": _utc_iso(days=-2),
        },
    )

    assert ready_send_response.status_code == 201

    ready_ack_response = client.post(
        f"/admin/uat/release-archive-exports/{ready_export['export_id']}/delivery-events",
        json={
            "event_type": "notification_acknowledged",
            "target_name": "Support Team",
            "delivery_channel": "email",
            "event_notes": "Support confirmed the handback summary.",
            "occurred_at": _utc_iso(days=-1),
        },
    )

    assert ready_ack_response.status_code == 201

    ready_status_response = client.patch(
        f"/admin/uat/release-archive-exports/{ready_export['export_id']}",
        json={"handoff_status": "acknowledged", "delivery_confirmed_by": "Support Team"},
    )

    assert ready_status_response.status_code == 200

    pending_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "handback-pending-archive",
            "support_handoff_owner": "Support Team",
            "retention_review_at": _utc_iso(days=10),
        },
    )

    assert pending_archive_response.status_code == 201
    pending_archive = pending_archive_response.json()

    pending_export_response = client.post(
        f"/admin/uat/release-archives/{pending_archive['archive_id']}/exports",
        json={
            "export_name": "handback-pending-export",
            "export_scope": "support_bundle",
            "destination_system": "support-cabinet",
        },
    )

    assert pending_export_response.status_code == 201
    pending_export = pending_export_response.json()

    pending_send_response = client.post(
        f"/admin/uat/release-archive-exports/{pending_export['export_id']}/delivery-events",
        json={
            "event_type": "notification_sent",
            "target_name": "Support Team",
            "delivery_channel": "email",
            "external_reference": "outlook://message/pending",
            "event_notes": "Awaiting support confirmation.",
            "occurred_at": _utc_iso(days=-1),
        },
    )

    assert pending_send_response.status_code == 201

    remediation_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "handback-remediation-archive",
            "support_handoff_owner": "Support Team",
            "retention_review_at": _utc_iso(days=45),
        },
    )

    assert remediation_archive_response.status_code == 201
    remediation_archive = remediation_archive_response.json()

    remediation_export_response = client.post(
        f"/admin/uat/release-archives/{remediation_archive['archive_id']}/exports",
        json={
            "export_name": "handback-remediation-export",
            "export_scope": "support_bundle",
            "destination_system": "support-cabinet",
        },
    )

    assert remediation_export_response.status_code == 201
    remediation_export = remediation_export_response.json()

    remediation_action_response = client.post(
        f"/admin/uat/release-archives/{remediation_archive['archive_id']}/retention-actions",
        json={
            "action_type": "re_export_requested",
            "action_notes": "Retry before support closure.",
            "related_export_id": remediation_export["export_id"],
            "scheduled_retry_at": _utc_iso(days=1),
            "next_retention_review_at": _utc_iso(days=60),
        },
    )

    assert remediation_action_response.status_code == 201

    blocked_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "handback-blocked-archive",
            "support_handoff_owner": "Support Team",
            "retention_review_at": _utc_iso(days=10),
        },
    )

    assert blocked_archive_response.status_code == 201

    report_response = client.get(
        "/admin/uat/release-archives/support-handback-report",
        params={
            "review_window_days": 30,
            "stale_reply_after_hours": 96,
            "as_of": _utc_iso(days=2),
            "include_resolved": True,
        },
    )

    assert report_response.status_code == 200
    report_payload = report_response.json()
    assert report_payload["report_version"] == "phase7-release-archive-support-handback-v1"
    assert report_payload["total_archive_count"] == 4
    assert report_payload["included_archive_count"] == 4
    assert report_payload["owner_count"] == 1
    assert report_payload["closure_ready_count"] == 1
    assert report_payload["unresolved_count"] == 3
    assert report_payload["pending_support_confirmation_count"] == 1
    assert report_payload["remediation_in_progress_count"] == 1
    assert report_payload["blocked_count"] == 1

    owner_payload = report_payload["owners"][0]
    assert owner_payload["owner_name"] == "Support Team"
    assert owner_payload["archive_count"] == 4
    assert owner_payload["closure_ready_count"] == 1
    assert owner_payload["unresolved_count"] == 3

    items_by_name = {item["archive_name"]: item for item in owner_payload["archives"]}

    ready_item = items_by_name["handback-ready-archive"]
    assert ready_item["closure_status"] == "ready_for_handback"
    assert ready_item["closure_ready"] is True
    assert ready_item["operations_runbook_reference"] == "runbook://support/ready"
    assert ready_item["support_handoff_summary"] == "Ready for support handback closure."

    pending_item = items_by_name["handback-pending-archive"]
    assert pending_item["closure_status"] == "pending_support_confirmation"
    assert pending_item["closure_ready"] is False
    assert "Notification acknowledgement is still pending." in pending_item["closure_blockers"]

    remediation_item = items_by_name["handback-remediation-archive"]
    assert remediation_item["closure_status"] == "remediation_in_progress"
    assert remediation_item["closure_ready"] is False
    assert "Scheduled re-export is due." in remediation_item["closure_blockers"]

    blocked_item = items_by_name["handback-blocked-archive"]
    assert blocked_item["closure_status"] == "blocked"
    assert blocked_item["closure_ready"] is False
    assert "Archive export has not been created yet." in blocked_item["closure_blockers"]

    unresolved_only_response = client.get(
        "/admin/uat/release-archives/support-handback-report",
        params={
            "review_window_days": 30,
            "stale_reply_after_hours": 96,
            "as_of": _utc_iso(days=2),
            "include_resolved": False,
        },
    )

    assert unresolved_only_response.status_code == 200
    unresolved_only_payload = unresolved_only_response.json()
    assert unresolved_only_payload["included_archive_count"] == 3
    unresolved_names = {
        item["archive_name"]
        for item in unresolved_only_payload["owners"][0]["archives"]
    }
    assert "handback-ready-archive" not in unresolved_names


def test_release_archive_support_handback_report_rejects_invalid_escalation_window(
    client: TestClient,
) -> None:
    response = client.get(
        "/admin/uat/release-archives/support-handback-report",
        params={"stale_reply_after_hours": 0},
    )

    assert response.status_code == 400
    assert "greater than 0 hours" in response.json()["detail"]


def test_release_archive_closure_history_report_tracks_resolution_timeline(
    client: TestClient,
) -> None:
    cycle = _create_cycle(client, "phase7-release-archive-closure-history")
    _complete_cycle(client, cycle["cycle_id"])

    finalize_response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/finalize",
        json={"status": "approved", "summary_notes": "Approved for closure history export."},
    )

    assert finalize_response.status_code == 200

    snapshot = _create_release_ready_snapshot(
        client,
        cycle_id=cycle["cycle_id"],
        snapshot_name="closure-history-snapshot",
        packet_name="closure-history-packet",
    )

    closed_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "closure-closed-archive",
            "support_handoff_owner": "Support Team",
            "retention_review_at": _utc_iso(days=14),
        },
    )

    assert closed_archive_response.status_code == 201
    closed_archive = closed_archive_response.json()

    closed_export_response = client.post(
        f"/admin/uat/release-archives/{closed_archive['archive_id']}/exports",
        json={
            "export_name": "closure-closed-export",
            "export_scope": "support_bundle",
            "destination_system": "support-cabinet",
        },
    )

    assert closed_export_response.status_code == 201
    closed_export = closed_export_response.json()

    for payload in [
        {
            "event_type": "external_handoff_logged",
            "target_name": "Support Cabinet",
            "delivery_channel": "vendor_portal",
            "external_reference": "support://cabinet/closed",
            "event_notes": "Delivered to the downstream support cabinet.",
            "occurred_at": _utc_iso(days=-4),
        },
        {
            "event_type": "notification_sent",
            "target_name": "Support Team",
            "delivery_channel": "email",
            "external_reference": "outlook://message/closed-send",
            "event_notes": "Sent the final support handback packet.",
            "occurred_at": _utc_iso(days=-4),
        },
        {
            "event_type": "notification_acknowledged",
            "target_name": "Support Team",
            "delivery_channel": "email",
            "event_notes": "Support confirmed packet receipt.",
            "occurred_at": _utc_iso(days=-3),
        },
        {
            "event_type": "support_handback_acknowledged",
            "target_name": "Support Team",
            "delivery_channel": "email",
            "event_notes": "Support accepted downstream handback ownership.",
            "occurred_at": _utc_iso(days=-2),
        },
        {
            "event_type": "closure_confirmed",
            "target_name": "Release Manager",
            "delivery_channel": "ops_review",
            "event_notes": "Closure confirmed after support acceptance.",
            "occurred_at": _utc_iso(days=-1),
        },
    ]:
        response = client.post(
            f"/admin/uat/release-archive-exports/{closed_export['export_id']}/delivery-events",
            json=payload,
        )
        assert response.status_code == 201

    awaiting_closure_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "closure-awaiting-confirmation-archive",
            "support_handoff_owner": "Support Team",
            "retention_review_at": _utc_iso(days=14),
        },
    )

    assert awaiting_closure_archive_response.status_code == 201
    awaiting_closure_archive = awaiting_closure_archive_response.json()

    awaiting_closure_export_response = client.post(
        f"/admin/uat/release-archives/{awaiting_closure_archive['archive_id']}/exports",
        json={
            "export_name": "closure-awaiting-confirmation-export",
            "export_scope": "support_bundle",
            "destination_system": "support-cabinet",
        },
    )

    assert awaiting_closure_export_response.status_code == 201
    awaiting_closure_export = awaiting_closure_export_response.json()

    for payload in [
        {
            "event_type": "external_handoff_logged",
            "target_name": "Support Cabinet",
            "delivery_channel": "vendor_portal",
            "external_reference": "support://cabinet/awaiting-confirmation",
            "event_notes": "Delivered for support acknowledgement.",
            "occurred_at": _utc_iso(days=-3),
        },
        {
            "event_type": "support_handback_acknowledged",
            "target_name": "Support Team",
            "delivery_channel": "email",
            "event_notes": "Support acknowledged the handback.",
            "occurred_at": _utc_iso(days=-2),
        },
    ]:
        response = client.post(
            f"/admin/uat/release-archive-exports/{awaiting_closure_export['export_id']}/delivery-events",
            json=payload,
        )
        assert response.status_code == 201

    remediation_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "closure-remediation-archive",
            "support_handoff_owner": "Support Team",
            "retention_review_at": _utc_iso(days=45),
        },
    )

    assert remediation_archive_response.status_code == 201
    remediation_archive = remediation_archive_response.json()

    remediation_export_response = client.post(
        f"/admin/uat/release-archives/{remediation_archive['archive_id']}/exports",
        json={
            "export_name": "closure-remediation-export",
            "export_scope": "support_bundle",
            "destination_system": "support-cabinet",
        },
    )

    assert remediation_export_response.status_code == 201
    remediation_export = remediation_export_response.json()

    for payload in [
        {
            "event_type": "notification_sent",
            "target_name": "Support Team",
            "delivery_channel": "email",
            "external_reference": "outlook://message/remediation",
            "event_notes": "Sent remediation follow-up packet.",
            "occurred_at": _utc_iso(days=-2),
        },
        {
            "event_type": "escalation_outcome_recorded",
            "target_name": "Vendor Escalation Desk",
            "delivery_channel": "ticketing",
            "external_reference": "ticket://remediation/42",
            "event_notes": "Escalated and awaiting vendor remediation confirmation.",
            "occurred_at": _utc_iso(days=-1),
        },
    ]:
        response = client.post(
            f"/admin/uat/release-archive-exports/{remediation_export['export_id']}/delivery-events",
            json=payload,
        )
        assert response.status_code == 201

    support_ack_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "closure-awaiting-support-ack-archive",
            "support_handoff_owner": "Support Team",
            "retention_review_at": _utc_iso(days=14),
        },
    )

    assert support_ack_archive_response.status_code == 201
    support_ack_archive = support_ack_archive_response.json()

    support_ack_export_response = client.post(
        f"/admin/uat/release-archives/{support_ack_archive['archive_id']}/exports",
        json={
            "export_name": "closure-awaiting-support-ack-export",
            "export_scope": "support_bundle",
            "destination_system": "support-cabinet",
        },
    )

    assert support_ack_export_response.status_code == 201
    support_ack_export = support_ack_export_response.json()

    for payload in [
        {
            "event_type": "external_handoff_logged",
            "target_name": "Support Cabinet",
            "delivery_channel": "vendor_portal",
            "external_reference": "support://cabinet/awaiting-ack",
            "event_notes": "Delivered downstream and awaiting handback acknowledgement.",
            "occurred_at": _utc_iso(days=-2),
        },
        {
            "event_type": "notification_sent",
            "target_name": "Support Team",
            "delivery_channel": "email",
            "external_reference": "outlook://message/support-ack",
            "event_notes": "Asked support to confirm ownership.",
            "occurred_at": _utc_iso(days=-2),
        },
        {
            "event_type": "notification_acknowledged",
            "target_name": "Support Team",
            "delivery_channel": "email",
            "event_notes": "Support replied to the delivery digest.",
            "occurred_at": _utc_iso(days=-1),
        },
    ]:
        response = client.post(
            f"/admin/uat/release-archive-exports/{support_ack_export['export_id']}/delivery-events",
            json=payload,
        )
        assert response.status_code == 201

    blocked_archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "closure-blocked-archive",
            "support_handoff_owner": "Support Team",
            "retention_review_at": _utc_iso(days=14),
        },
    )

    assert blocked_archive_response.status_code == 201

    report_response = client.get(
        "/admin/uat/release-archives/closure-history-report",
        params={
            "review_window_days": 30,
            "stale_reply_after_hours": 48,
            "as_of": _utc_iso(days=2),
            "include_closed": True,
        },
    )

    assert report_response.status_code == 200
    report_payload = report_response.json()
    assert report_payload["report_version"] == "phase7-release-archive-closure-history-v1"
    assert report_payload["total_archive_count"] == 5
    assert report_payload["included_archive_count"] == 5
    assert report_payload["owner_count"] == 1
    assert report_payload["closed_count"] == 1
    assert report_payload["awaiting_closure_confirmation_count"] == 1
    assert report_payload["awaiting_support_handback_acknowledgement_count"] == 1
    assert report_payload["remediation_in_progress_count"] == 1
    assert report_payload["blocked_count"] == 1
    assert report_payload["open_followup_count"] == 0

    owner_payload = report_payload["owners"][0]
    items_by_name = {item["archive_name"]: item for item in owner_payload["archives"]}

    closed_item = items_by_name["closure-closed-archive"]
    assert closed_item["closure_status"] == "closed"
    assert closed_item["latest_support_handback_acknowledged_at"] is not None
    assert closed_item["latest_closure_confirmed_at"] is not None
    assert {
        event["event_type"] for event in closed_item["timeline"]
    } >= {
        "external_handoff_logged",
        "notification_sent",
        "notification_acknowledged",
        "support_handback_acknowledged",
        "closure_confirmed",
    }

    awaiting_closure_item = items_by_name["closure-awaiting-confirmation-archive"]
    assert awaiting_closure_item["closure_status"] == "awaiting_closure_confirmation"
    assert "Record final closure confirmation." in awaiting_closure_item["unresolved_actions"]

    remediation_item = items_by_name["closure-remediation-archive"]
    assert remediation_item["closure_status"] == "remediation_in_progress"
    assert remediation_item["latest_escalation_outcome_at"] is not None
    assert "Track the remediation outcome through to closure." in remediation_item[
        "unresolved_actions"
    ]

    support_ack_item = items_by_name["closure-awaiting-support-ack-archive"]
    assert (
        support_ack_item["closure_status"]
        == "awaiting_support_handback_acknowledgement"
    )
    assert "Capture downstream support handback acknowledgement." in support_ack_item[
        "unresolved_actions"
    ]

    blocked_item = items_by_name["closure-blocked-archive"]
    assert blocked_item["closure_status"] == "blocked"
    assert "Create the downstream archive export." in blocked_item["unresolved_actions"]

    unresolved_only_response = client.get(
        "/admin/uat/release-archives/closure-history-report",
        params={
            "review_window_days": 30,
            "stale_reply_after_hours": 48,
            "as_of": _utc_iso(days=2),
            "include_closed": False,
        },
    )

    assert unresolved_only_response.status_code == 200
    unresolved_payload = unresolved_only_response.json()
    assert unresolved_payload["included_archive_count"] == 4
    unresolved_names = {
        item["archive_name"]
        for item in unresolved_payload["owners"][0]["archives"]
    }
    assert "closure-closed-archive" not in unresolved_names


def test_release_archive_delivery_events_capture_closure_milestones(
    client: TestClient,
) -> None:
    cycle = _create_cycle(client, "phase7-release-archive-closure-events")
    _complete_cycle(client, cycle["cycle_id"])

    finalize_response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/finalize",
        json={"status": "approved", "summary_notes": "Approved for closure milestones."},
    )

    assert finalize_response.status_code == 200

    snapshot = _create_release_ready_snapshot(
        client,
        cycle_id=cycle["cycle_id"],
        snapshot_name="closure-events-snapshot",
        packet_name="closure-events-packet",
    )

    archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "closure-events-archive",
            "support_handoff_owner": "Support Team",
            "retention_review_at": _utc_iso(days=14),
        },
    )

    assert archive_response.status_code == 201
    archive = archive_response.json()

    export_response = client.post(
        f"/admin/uat/release-archives/{archive['archive_id']}/exports",
        json={
            "export_name": "closure-events-export",
            "export_scope": "support_bundle",
            "destination_system": "support-cabinet",
        },
    )

    assert export_response.status_code == 201
    export_payload = export_response.json()

    invalid_support_ack_response = client.post(
        f"/admin/uat/release-archive-exports/{export_payload['export_id']}/delivery-events",
        json={
            "event_type": "support_handback_acknowledged",
            "target_name": "Support Team",
            "delivery_channel": "email",
            "event_notes": "Support acknowledged early.",
        },
    )

    assert invalid_support_ack_response.status_code == 400
    assert "prior external_handoff_logged event" in invalid_support_ack_response.json()["detail"]

    invalid_closure_response = client.post(
        f"/admin/uat/release-archive-exports/{export_payload['export_id']}/delivery-events",
        json={
            "event_type": "closure_confirmed",
            "target_name": "Release Manager",
            "delivery_channel": "ops_review",
            "event_notes": "Attempted to close without support handback acknowledgement.",
        },
    )

    assert invalid_closure_response.status_code == 400
    assert "prior support_handback_acknowledged event" in invalid_closure_response.json()[
        "detail"
    ]

    valid_events = [
        {
            "event_type": "external_handoff_logged",
            "target_name": "Support Cabinet",
            "delivery_channel": "vendor_portal",
            "external_reference": "support://cabinet/closure-events",
            "event_notes": "Delivered downstream.",
            "occurred_at": _utc_iso(days=-3),
        },
        {
            "event_type": "notification_sent",
            "target_name": "Support Team",
            "delivery_channel": "email",
            "external_reference": "outlook://message/closure-events",
            "event_notes": "Sent escalation summary.",
            "occurred_at": _utc_iso(days=-2),
        },
        {
            "event_type": "escalation_outcome_recorded",
            "target_name": "Vendor Escalation Desk",
            "delivery_channel": "ticketing",
            "external_reference": "ticket://closure-events/77",
            "event_notes": "Escalation resolved and handed back to support.",
            "occurred_at": _utc_iso(days=-1),
        },
        {
            "event_type": "support_handback_acknowledged",
            "target_name": "Support Team",
            "delivery_channel": "email",
            "event_notes": "Support accepted the handback remediation outcome.",
            "occurred_at": _utc_iso(days=-1),
        },
        {
            "event_type": "closure_confirmed",
            "target_name": "Release Manager",
            "delivery_channel": "ops_review",
            "event_notes": "Release follow-up closed successfully.",
            "occurred_at": _utc_iso(days=-1),
        },
    ]

    for payload in valid_events:
        response = client.post(
            f"/admin/uat/release-archive-exports/{export_payload['export_id']}/delivery-events",
            json=payload,
        )
        assert response.status_code == 201

    delivery_events_response = client.get(
        f"/admin/uat/release-archive-exports/{export_payload['export_id']}/delivery-events"
    )

    assert delivery_events_response.status_code == 200
    listed_event_types = {
        event["event_type"] for event in delivery_events_response.json()
    }
    assert {
        "external_handoff_logged",
        "notification_sent",
        "escalation_outcome_recorded",
        "support_handback_acknowledged",
        "closure_confirmed",
    }.issubset(listed_event_types)

    export_detail_response = client.get(
        f"/admin/uat/release-archive-exports/{export_payload['export_id']}"
    )

    assert export_detail_response.status_code == 200
    export_detail_payload = export_detail_response.json()
    assert export_detail_payload["handoff_status"] == "acknowledged"
    assert "Support handback acknowledged by Support Team" in (
        export_detail_payload["handoff_notes"] or ""
    )
    assert "Closure confirmed by Release Manager" in (
        export_detail_payload["handoff_notes"] or ""
    )


def test_release_archive_export_delivery_events_drive_digest_ack_tracking(
    client: TestClient,
) -> None:
    cycle = _create_cycle(client, "phase7-release-archive-delivery-events")
    _complete_cycle(client, cycle["cycle_id"])

    finalize_response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/finalize",
        json={"status": "approved", "summary_notes": "Approved for delivery event journaling."},
    )

    assert finalize_response.status_code == 200

    snapshot = _create_release_ready_snapshot(
        client,
        cycle_id=cycle["cycle_id"],
        snapshot_name="delivery-event-snapshot",
        packet_name="delivery-event-packet",
    )
    archive_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/release-archives",
        json={
            "archive_name": "delivery-event-archive",
            "support_handoff_owner": "Support Team",
            "retention_review_at": _utc_iso(days=45),
        },
    )

    assert archive_response.status_code == 201
    archive_payload = archive_response.json()

    export_response = client.post(
        f"/admin/uat/release-archives/{archive_payload['archive_id']}/exports",
        json={
            "export_name": "delivery-event-export",
            "export_scope": "support_bundle",
            "destination_system": "support-cabinet",
        },
    )

    assert export_response.status_code == 201
    export_payload = export_response.json()

    invalid_ack_response = client.post(
        f"/admin/uat/release-archive-exports/{export_payload['export_id']}/delivery-events",
        json={
            "event_type": "notification_acknowledged",
            "target_name": "Support Team",
            "delivery_channel": "email",
            "event_notes": "Support confirmed receipt.",
        },
    )

    assert invalid_ack_response.status_code == 400
    assert "requires a prior notification_sent event" in invalid_ack_response.json()["detail"]

    handoff_response = client.post(
        f"/admin/uat/release-archive-exports/{export_payload['export_id']}/delivery-events",
        json={
            "event_type": "external_handoff_logged",
            "target_name": "Support Cabinet",
            "delivery_channel": "vendor_portal",
            "external_reference": "support://cabinet/123",
            "event_notes": "Transferred to the downstream support cabinet.",
        },
    )

    assert handoff_response.status_code == 201
    handoff_payload = handoff_response.json()
    assert handoff_payload["event_type"] == "external_handoff_logged"
    assert handoff_payload["target_name"] == "Support Cabinet"

    export_detail_response = client.get(
        f"/admin/uat/release-archive-exports/{export_payload['export_id']}"
    )

    assert export_detail_response.status_code == 200
    export_detail_payload = export_detail_response.json()
    assert export_detail_payload["handoff_status"] == "delivered"
    assert export_detail_payload["destination_reference"] == "support://cabinet/123"
    assert export_detail_payload["delivery_confirmed_by"] == "Support Cabinet"

    digest_before_send_response = client.get(
        "/admin/uat/release-archives/followup-notification-digest",
        params={"review_window_days": 30},
    )

    assert digest_before_send_response.status_code == 200
    digest_before_send_payload = digest_before_send_response.json()
    recipient_before_send = digest_before_send_payload["recipients"][0]
    archive_before_send = recipient_before_send["archives"][0]
    assert recipient_before_send["notification_acknowledgement_pending_count"] == 0
    assert archive_before_send["notification_acknowledgement_status"] == "not_sent"
    assert archive_before_send["latest_notification_sent_at"] is None

    notification_sent_response = client.post(
        f"/admin/uat/release-archive-exports/{export_payload['export_id']}/delivery-events",
        json={
            "event_type": "notification_sent",
            "target_name": "Support Team",
            "delivery_channel": "email",
            "external_reference": "outlook://message/followup-1",
            "event_notes": "Sent the archive follow-up digest.",
        },
    )

    assert notification_sent_response.status_code == 201

    digest_pending_response = client.get(
        "/admin/uat/release-archives/followup-notification-digest",
        params={"review_window_days": 30},
    )

    assert digest_pending_response.status_code == 200
    digest_pending_payload = digest_pending_response.json()
    recipient_pending = digest_pending_payload["recipients"][0]
    archive_pending = recipient_pending["archives"][0]
    assert recipient_pending["notification_acknowledgement_pending_count"] == 1
    assert archive_pending["notification_acknowledgement_status"] == "pending"
    assert archive_pending["latest_notification_sent_at"] is not None
    assert "notification_acknowledgement_pending" in archive_pending["attention_reasons"]

    notification_ack_response = client.post(
        f"/admin/uat/release-archive-exports/{export_payload['export_id']}/delivery-events",
        json={
            "event_type": "notification_acknowledged",
            "target_name": "Support Team",
            "delivery_channel": "email",
            "event_notes": "Support lead acknowledged the digest.",
        },
    )

    assert notification_ack_response.status_code == 201

    delivery_event_list_response = client.get(
        f"/admin/uat/release-archive-exports/{export_payload['export_id']}/delivery-events"
    )

    assert delivery_event_list_response.status_code == 200
    delivery_event_list_payload = delivery_event_list_response.json()
    assert len(delivery_event_list_payload) == 3
    assert {item["event_type"] for item in delivery_event_list_payload} == {
        "external_handoff_logged",
        "notification_sent",
        "notification_acknowledged",
    }

    digest_acknowledged_response = client.get(
        "/admin/uat/release-archives/followup-notification-digest",
        params={"review_window_days": 30},
    )

    assert digest_acknowledged_response.status_code == 200
    digest_acknowledged_payload = digest_acknowledged_response.json()
    recipient_acknowledged = digest_acknowledged_payload["recipients"][0]
    archive_acknowledged = recipient_acknowledged["archives"][0]
    assert recipient_acknowledged["notification_acknowledgement_pending_count"] == 0
    assert archive_acknowledged["notification_acknowledgement_status"] == "acknowledged"
    assert archive_acknowledged["latest_notification_acknowledged_at"] is not None
    assert "notification_acknowledgement_pending" not in archive_acknowledged["attention_reasons"]


def test_release_archive_routes_reject_operator_role(
    client: TestClient,
    operator_headers: dict[str, str],
) -> None:
    snapshot = _create_handoff_snapshot(
        client,
        cycle_name="phase7-release-archive-access",
        snapshot_name="release-archive-access",
    )

    response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot['snapshot_id']}/release-archives",
        headers=operator_headers,
        json={"archive_name": "restricted-archive"},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "This operation requires one of the following roles: admin."

    search_response = client.get(
        "/admin/uat/release-archives",
        headers=operator_headers,
    )

    assert search_response.status_code == 403
    assert (
        search_response.json()["detail"]
        == "This operation requires one of the following roles: admin."
    )

    queue_response = client.get(
        "/admin/uat/release-archives/retention-queue",
        headers=operator_headers,
    )

    assert queue_response.status_code == 403
    assert queue_response.json()["detail"] == (
        "This operation requires one of the following roles: admin."
    )

    export_response = client.post(
        f"/admin/uat/release-archives/{snapshot['snapshot_id']}/exports",
        headers=operator_headers,
        json={
            "export_name": "restricted-export",
            "export_scope": "audit_bundle",
            "destination_system": "audit-vault",
        },
    )

    assert export_response.status_code == 403
    assert export_response.json()["detail"] == (
        "This operation requires one of the following roles: admin."
    )

    export_update_response = client.patch(
        f"/admin/uat/release-archive-exports/{snapshot['snapshot_id']}",
        headers=operator_headers,
        json={"handoff_status": "delivered"},
    )

    assert export_update_response.status_code == 403
    assert export_update_response.json()["detail"] == (
        "This operation requires one of the following roles: admin."
    )

    delivery_event_create_response = client.post(
        f"/admin/uat/release-archive-exports/{snapshot['snapshot_id']}/delivery-events",
        headers=operator_headers,
        json={
            "event_type": "notification_sent",
            "target_name": "Support Team",
            "delivery_channel": "email",
            "event_notes": "Sent the follow-up digest.",
        },
    )

    assert delivery_event_create_response.status_code == 403
    assert delivery_event_create_response.json()["detail"] == (
        "This operation requires one of the following roles: admin."
    )

    delivery_event_list_response = client.get(
        f"/admin/uat/release-archive-exports/{snapshot['snapshot_id']}/delivery-events",
        headers=operator_headers,
    )

    assert delivery_event_list_response.status_code == 403
    assert delivery_event_list_response.json()["detail"] == (
        "This operation requires one of the following roles: admin."
    )

    retention_action_response = client.post(
        f"/admin/uat/release-archives/{snapshot['snapshot_id']}/retention-actions",
        headers=operator_headers,
        json={
            "action_type": "review_completed",
            "next_retention_review_at": "2026-05-01T00:00:00Z",
        },
    )

    assert retention_action_response.status_code == 403
    assert retention_action_response.json()["detail"] == (
        "This operation requires one of the following roles: admin."
    )

    retention_action_list_response = client.get(
        f"/admin/uat/release-archives/{snapshot['snapshot_id']}/retention-actions",
        headers=operator_headers,
    )

    assert retention_action_list_response.status_code == 403
    assert retention_action_list_response.json()["detail"] == (
        "This operation requires one of the following roles: admin."
    )

    dashboard_response = client.get(
        "/admin/uat/release-archives/followup-dashboard",
        headers=operator_headers,
    )

    assert dashboard_response.status_code == 403
    assert dashboard_response.json()["detail"] == (
        "This operation requires one of the following roles: admin."
    )

    delivery_ledger_response = client.get(
        "/admin/uat/release-archives/delivery-ledger",
        headers=operator_headers,
    )

    assert delivery_ledger_response.status_code == 403
    assert delivery_ledger_response.json()["detail"] == (
        "This operation requires one of the following roles: admin."
    )

    support_handback_response = client.get(
        "/admin/uat/release-archives/support-handback-report",
        headers=operator_headers,
    )

    assert support_handback_response.status_code == 403
    assert support_handback_response.json()["detail"] == (
        "This operation requires one of the following roles: admin."
    )

    closure_history_response = client.get(
        "/admin/uat/release-archives/closure-history-report",
        headers=operator_headers,
    )

    assert closure_history_response.status_code == 403
    assert closure_history_response.json()["detail"] == (
        "This operation requires one of the following roles: admin."
    )

    bulk_response = client.post(
        "/admin/uat/release-archives/bulk-retention-actions",
        headers=operator_headers,
        json={
            "archive_ids": [snapshot["snapshot_id"]],
            "action_type": "review_completed",
            "next_retention_review_at": "2026-05-01T00:00:00Z",
        },
    )

    assert bulk_response.status_code == 403
    assert bulk_response.json()["detail"] == (
        "This operation requires one of the following roles: admin."
    )

    digest_response = client.get(
        "/admin/uat/release-archives/followup-notification-digest",
        headers=operator_headers,
    )

    assert digest_response.status_code == 403
    assert digest_response.json()["detail"] == (
        "This operation requires one of the following roles: admin."
    )

    execute_response = client.post(
        "/admin/uat/release-archives/execute-due-reexports",
        headers=operator_headers,
        json={"dry_run": True},
    )

    assert execute_response.status_code == 403
    assert execute_response.json()["detail"] == (
        "This operation requires one of the following roles: admin."
    )

    notification_dispatch_response = client.post(
        "/admin/uat/release-archives/execute-followup-notification-dispatch",
        headers=operator_headers,
        json={"dry_run": True, "delivery_channel": "email"},
    )

    assert notification_dispatch_response.status_code == 403
    assert notification_dispatch_response.json()["detail"] == (
        "This operation requires one of the following roles: admin."
    )


def _create_cycle(client: TestClient, cycle_name: str) -> dict[str, object]:
    response = client.post("/admin/uat/cycles", json={"cycle_name": cycle_name})

    assert response.status_code == 201
    return response.json()


def _complete_cycle(client: TestClient, cycle_id: str) -> dict[str, object]:
    payload: dict[str, object] | None = None
    for scenario_id in SCENARIO_IDS:
        response = client.post(
            f"/admin/uat/cycles/{cycle_id}/scenarios/{scenario_id}/results",
            json={
                "status": "passed",
                "execution_notes": f"Executed {scenario_id}.",
                "evidence_reference": f"sharepoint://uat/phase7/{scenario_id.lower()}",
            },
        )
        assert response.status_code == 200
        payload = response.json()

    assert payload is not None
    return payload


def _create_handoff_snapshot(
    client: TestClient,
    *,
    cycle_name: str,
    snapshot_name: str,
) -> dict[str, object]:
    cycle = _create_cycle(client, cycle_name)
    _complete_cycle(client, cycle["cycle_id"])

    finalize_response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/finalize",
        json={"status": "approved", "summary_notes": "Approved for distribution prep."},
    )

    assert finalize_response.status_code == 200

    snapshot_response = client.post(
        f"/admin/uat/cycles/{cycle['cycle_id']}/handoff-snapshots",
        json={"snapshot_name": snapshot_name},
    )

    assert snapshot_response.status_code == 201
    return snapshot_response.json()


def _create_release_ready_snapshot(
    client: TestClient,
    *,
    cycle_id: str,
    snapshot_name: str,
    packet_name: str,
) -> dict[str, object]:
    snapshot_response = client.post(
        f"/admin/uat/cycles/{cycle_id}/handoff-snapshots",
        json={"snapshot_name": snapshot_name},
    )

    assert snapshot_response.status_code == 201
    snapshot_payload = snapshot_response.json()
    packet_payload = _create_distribution_packet(
        client,
        snapshot_payload["snapshot_id"],
        packet_name=packet_name,
        recipients=[
            {
                "recipient_name": "Support Team",
                "recipient_contact": "support@ddcl.test",
                "required_for_ack": True,
            }
        ],
    )
    recipient_id = packet_payload["recipients"][0]["recipient_id"]
    recipient_update_response = client.patch(
        f"/admin/uat/distribution-packets/{packet_payload['packet_id']}/recipients/{recipient_id}",
        json={
            "delivery_status": "acknowledged",
            "acknowledged_by": "Support Team",
            "acknowledgement_notes": "Support team acknowledged the packet.",
        },
    )

    assert recipient_update_response.status_code == 200

    acceptance_response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot_payload['snapshot_id']}/acceptance-artifacts",
        json={
            "decision": "accepted",
            "stakeholder_name": "Launch Board",
            "stakeholder_role": "Executive Review",
            "decision_notes": "Approved for release archive retention.",
        },
    )

    assert acceptance_response.status_code == 201
    return snapshot_payload


def _create_distribution_packet(
    client: TestClient,
    snapshot_id: str,
    *,
    packet_name: str,
    recipients: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    response = client.post(
        f"/admin/uat/handoff-snapshots/{snapshot_id}/distribution-packets",
        json={
            "packet_name": packet_name,
            "recipients": recipients or [],
        },
    )

    assert response.status_code == 201
    return response.json()


def _counts(items: list[dict[str, object]]) -> dict[str, int]:
    return {item["category"]: item["count"] for item in items}


def _utc_iso(*, days: int = 0, hours: int = 0) -> str:
    value = datetime.now(UTC) + timedelta(days=days, hours=hours)
    return value.isoformat().replace("+00:00", "Z")
