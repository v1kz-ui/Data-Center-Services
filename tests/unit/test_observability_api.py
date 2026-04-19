from __future__ import annotations

import json
import logging

from fastapi.testclient import TestClient

from app.main import create_app


def test_request_logging_emits_structured_completion_event(
    admin_headers: dict[str, str],
    caplog,
) -> None:
    caplog.set_level(logging.INFO, logger="app.request")
    client = TestClient(create_app())

    response = client.post(
        "/orchestration/plan",
        headers={
            **admin_headers,
            "X-Request-ID": "req-observe-001",
            "X-Trace-ID": "trace-observe-001",
        },
        json={"metro_ids": ["DFW"]},
    )

    assert response.status_code == 200
    event_payload = next(
        json.loads(record.message)
        for record in caplog.records
        if record.name == "app.request"
        and "request_completed" in record.message
        and "/orchestration/plan" in record.message
    )

    assert event_payload["event"] == "request_completed"
    assert event_payload["request_id"] == "req-observe-001"
    assert event_payload["trace_id"] == "trace-observe-001"
    assert event_payload["method"] == "POST"
    assert event_payload["path"] == "/orchestration/plan"
    assert event_payload["principal_subject"] == "test-admin"
    assert event_payload["principal_roles"] == "admin"
    assert event_payload["status_code"] == 200
