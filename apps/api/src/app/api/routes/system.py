# ruff: noqa: E501
from collections.abc import Iterable
from datetime import UTC, datetime
from html import escape
from typing import Annotated, Any

from fastapi import APIRouter, Depends
from fastapi.responses import HTMLResponse
from fastapi.routing import APIRoute
from sqlalchemy.exc import SQLAlchemyError

from app.api.routes import audit as audit_routes
from app.api.routes import evaluation as evaluation_routes
from app.api.routes import ingestion as ingestion_routes
from app.api.routes import monitoring as monitoring_routes
from app.api.routes import orchestration as orchestration_routes
from app.api.routes import scoring as scoring_routes
from app.api.routes import uat as uat_routes
from app.core.security import require_admin_access
from app.core.settings import Settings, get_settings
from app.db.models import MANAGED_TABLES
from app.db.session import SessionLocal
from app.services.monitoring import MonitoringThresholdPolicy, build_monitoring_overview

router = APIRouter()
AdminAccess = Annotated[object, Depends(require_admin_access)]
APP_VERSION = "0.1.0"
PUBLIC_ROUTE_PATHS = frozenset({"/", "/dashboard/summary", "/health", "/version"})


def _api_routes(target_router: APIRouter) -> list[APIRoute]:
    return [route for route in target_router.routes if isinstance(route, APIRoute)]


def _build_domain_inventory() -> list[dict[str, Any]]:
    domain_blueprints = (
        {
            "slug": "foundation",
            "label": "Foundation",
            "router": router,
            "focus": "Public surface, settings, headers, table inventory",
            "accent": "lagoon",
        },
        {
            "slug": "ingestion",
            "label": "Ingestion",
            "router": ingestion_routes.router,
            "focus": "Source loads, freshness, health snapshots",
            "accent": "citrus",
        },
        {
            "slug": "evaluation",
            "label": "Evaluation",
            "router": evaluation_routes.router,
            "focus": "Run scope, exclusion rules, replay control",
            "accent": "ember",
        },
        {
            "slug": "scoring",
            "label": "Scoring",
            "router": scoring_routes.router,
            "focus": "Policies, parcel detail, score summaries",
            "accent": "sky",
        },
        {
            "slug": "orchestration",
            "label": "Orchestration",
            "router": orchestration_routes.router,
            "focus": "Batch planning, retries, activation checks",
            "accent": "marine",
        },
        {
            "slug": "monitoring",
            "label": "Monitoring",
            "router": monitoring_routes.router,
            "focus": "Thresholds, failed runs, alert overview",
            "accent": "signal",
        },
        {
            "slug": "audit",
            "label": "Audit",
            "router": audit_routes.router,
            "focus": "Run audit export and traceable package assembly",
            "accent": "graphite",
        },
        {
            "slug": "uat",
            "label": "UAT + Release",
            "router": uat_routes.router,
            "focus": "Cycles, handoff, signoff, archive operations",
            "accent": "aurora",
        },
    )

    inventory: list[dict[str, Any]] = []

    for blueprint in domain_blueprints:
        routes = _api_routes(blueprint["router"])
        methods = sorted(
            {
                method
                for route in routes
                for method in (route.methods or set())
                if method not in {"HEAD", "OPTIONS"}
            }
        )
        inventory.append(
            {
                "slug": blueprint["slug"],
                "label": blueprint["label"],
                "focus": blueprint["focus"],
                "accent": blueprint["accent"],
                "route_count": len(routes),
                "methods": methods,
                "sample_paths": [route.path for route in routes[:3]],
            }
        )

    return inventory


def _build_tripwires(settings: Settings) -> list[dict[str, Any]]:
    return [
        {
            "label": "Failed runs",
            "value": settings.monitoring_failed_run_threshold,
            "detail": "Recent run failures before alerting",
        },
        {
            "label": "Failed snapshots",
            "value": settings.monitoring_failed_snapshot_threshold,
            "detail": "Broken source loads before escalation",
        },
        {
            "label": "Quarantined snapshots",
            "value": settings.monitoring_quarantined_snapshot_threshold,
            "detail": "Quarantine tolerance for latest loads",
        },
        {
            "label": "Freshness misses",
            "value": settings.monitoring_freshness_failure_threshold,
            "detail": "Out-of-date source checks allowed",
        },
        {
            "label": "Latest batch failures",
            "value": settings.monitoring_latest_batch_failed_threshold,
            "detail": "Batch failure tolerance before paging",
        },
    ]


def _isoformat_or_none(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _status_counts_to_dict(items: Iterable[Any]) -> dict[str, int]:
    return {item.status: item.count for item in items}


def _empty_monitoring_snapshot(
    *,
    evaluated_at: str,
    available: bool,
    error: str | None,
) -> dict[str, Any]:
    return {
        "available": available,
        "error": error,
        "evaluated_at": evaluated_at,
        "alert_count": 0,
        "threshold_trigger_count": 0,
        "failed_run_count": 0,
        "source_issue_count": 0,
        "run_counts": {
            "running": 0,
            "failed": 0,
            "completed": 0,
        },
        "batch_counts": {
            "building": 0,
            "failed": 0,
            "completed": 0,
            "active": 0,
        },
        "latest_batch": None,
        "recent_failed_runs": [],
        "alerts": [],
        "thresholds": [],
        "source_health": {
            "total": 0,
            "healthy": 0,
            "failed": 0,
            "quarantined": 0,
        },
        "freshness": None,
    }


def _read_monitoring_snapshot(settings: Settings) -> dict[str, Any]:
    evaluated_at = datetime.now(UTC).isoformat()

    try:
        with SessionLocal() as session:
            overview = build_monitoring_overview(
                session,
                recent_failed_limit=6,
                threshold_policy=MonitoringThresholdPolicy(
                    failed_run_threshold=settings.monitoring_failed_run_threshold,
                    failed_snapshot_threshold=settings.monitoring_failed_snapshot_threshold,
                    quarantined_snapshot_threshold=settings.monitoring_quarantined_snapshot_threshold,
                    freshness_failure_threshold=settings.monitoring_freshness_failure_threshold,
                    latest_batch_failed_threshold=settings.monitoring_latest_batch_failed_threshold,
                ),
            )
    except SQLAlchemyError as exc:
        return _empty_monitoring_snapshot(
            evaluated_at=evaluated_at,
            available=False,
            error=(
                "Monitoring data is unavailable. "
                f"{exc.__class__.__name__} prevented the dashboard query."
            ),
        )

    failed_snapshots = sum(
        1
        for snapshot in overview.source_health
        if snapshot.latest_snapshot_status == "failed"
    )
    quarantined_snapshots = sum(
        1
        for snapshot in overview.source_health
        if snapshot.latest_snapshot_status == "quarantined"
    )
    healthy_snapshots = max(
        len(overview.source_health) - failed_snapshots - quarantined_snapshots,
        0,
    )
    freshness_payload: dict[str, Any] | None = None
    if overview.freshness is not None:
        freshness_payload = {
            "metro_id": overview.freshness.metro_id,
            "passed": overview.freshness.passed,
            "failed_count": sum(
                1 for status in overview.freshness.statuses if not status.passed
            ),
        }

    latest_batch_payload: dict[str, Any] | None = None
    if overview.latest_batch is not None:
        latest_batch_payload = {
            "batch_id": overview.latest_batch.batch_id,
            "status": overview.latest_batch.status,
            "expected_metros": overview.latest_batch.expected_metros,
            "completed_metros": overview.latest_batch.completed_metros,
            "activation_ready": overview.latest_batch.activation_ready,
            "activated_at": _isoformat_or_none(overview.latest_batch.activated_at),
        }

    return {
        "available": True,
        "error": None,
        "evaluated_at": _isoformat_or_none(overview.evaluated_at) or evaluated_at,
        "alert_count": overview.alert_count,
        "threshold_trigger_count": overview.threshold_trigger_count,
        "failed_run_count": sum(
            1 for run in overview.recent_failed_runs if run.status == "failed"
        ),
        "source_issue_count": failed_snapshots + quarantined_snapshots,
        "run_counts": _status_counts_to_dict(overview.run_status_counts),
        "batch_counts": _status_counts_to_dict(overview.batch_status_counts),
        "latest_batch": latest_batch_payload,
        "recent_failed_runs": [
            {
                "run_id": run.run_id,
                "batch_id": run.batch_id,
                "metro_id": run.metro_id,
                "status": run.status,
                "profile_name": run.profile_name,
                "failure_reason": run.failure_reason,
                "started_at": _isoformat_or_none(run.started_at),
                "completed_at": _isoformat_or_none(run.completed_at),
            }
            for run in overview.recent_failed_runs
        ],
        "alerts": [
            {
                "severity": alert.severity,
                "code": alert.code,
                "summary": alert.summary,
                "metro_id": alert.metro_id,
                "batch_id": alert.batch_id,
                "run_id": alert.run_id,
            }
            for alert in overview.alerts[:6]
        ],
        "thresholds": [
            {
                "code": threshold.code,
                "severity": threshold.severity,
                "observed_value": threshold.observed_value,
                "threshold_value": threshold.threshold_value,
                "triggered": threshold.triggered,
                "summary": threshold.summary,
            }
            for threshold in overview.thresholds
        ],
        "source_health": {
            "total": len(overview.source_health),
            "healthy": healthy_snapshots,
            "failed": failed_snapshots,
            "quarantined": quarantined_snapshots,
        },
        "freshness": freshness_payload,
    }


def _build_dashboard_summary(settings: Settings) -> dict[str, Any]:
    domains = _build_domain_inventory()
    total_route_count = sum(domain["route_count"] for domain in domains)
    protected_route_count = max(total_route_count - len(PUBLIC_ROUTE_PATHS), 0)
    generated_at = datetime.now(UTC)

    return {
        "app_name": settings.app_name,
        "display_name": settings.app_name.replace("-", " ").title(),
        "environment": settings.app_env,
        "version": APP_VERSION,
        "generated_at": generated_at.isoformat(),
        "phase_count": len(domains),
        "public_route_count": len(PUBLIC_ROUTE_PATHS),
        "protected_route_count": protected_route_count,
        "total_route_count": total_route_count,
        "managed_table_count": len(MANAGED_TABLES),
        "managed_tables": MANAGED_TABLES,
        "domains": domains,
        "tripwires": _build_tripwires(settings),
        "monitoring": _read_monitoring_snapshot(settings),
        "auth": {
            "enabled": settings.auth_enabled,
            "subject_header": settings.auth_subject_header,
            "name_header": settings.auth_name_header,
            "roles_header": settings.auth_roles_header,
        },
        "runtime": {
            "request_header": settings.request_id_header,
            "trace_header": settings.trace_id_header,
            "uat_environment": settings.uat_environment_name,
            "scenario_pack_path": settings.uat_scenario_pack_path,
        },
    }


def _render_metric_cards(summary: dict[str, Any]) -> str:
    metrics = (
        (
            "Public APIs",
            summary["public_route_count"],
            "Instant status routes and dashboard surfaces.",
            "lagoon",
        ),
        (
            "Protected APIs",
            summary["protected_route_count"],
            "Operator and admin command lanes behind RBAC.",
            "marine",
        ),
        (
            "Managed Tables",
            summary["managed_table_count"],
            "Structured persistence spanning source, scoring, and UAT.",
            "citrus",
        ),
        (
            "Active Phases",
            summary["phase_count"],
            "Foundation through release archive coverage.",
            "ember",
        ),
    )

    cards: list[str] = []
    for label, value, detail, tone in metrics:
        cards.append(
            f"""
            <article class="metric-card tone-{tone}">
              <span>{escape(label)}</span>
              <strong data-count="{value}">0</strong>
              <p>{escape(detail)}</p>
            </article>
            """
        )
    return "\n".join(cards)


def _render_phase_cards(domains: Iterable[dict[str, Any]]) -> str:
    cards: list[str] = []
    for index, domain in enumerate(domains, start=1):
        methods = ", ".join(domain["methods"][:3]) or "GET"
        cards.append(
            f"""
            <article class="phase-card tone-{escape(domain["accent"])}">
              <span class="phase-index">{index:02d}</span>
              <div class="phase-copy">
                <h3>{escape(domain["label"])}</h3>
                <p>{escape(domain["focus"])}</p>
              </div>
              <div class="phase-meta">
                <strong>{domain["route_count"]} routes</strong>
                <small>{escape(methods)}</small>
              </div>
            </article>
            """
        )
    return "\n".join(cards)


def _render_heat_rows(domains: Iterable[dict[str, Any]]) -> str:
    domain_list = list(domains)
    max_routes = max((domain["route_count"] for domain in domain_list), default=1)

    rows: list[str] = []
    for domain in domain_list:
        width = max(18, round((domain["route_count"] / max_routes) * 100))
        rows.append(
            f"""
            <div class="heat-row">
              <div class="heat-label">
                <span>{escape(domain["label"])}</span>
                <strong>{domain["route_count"]}</strong>
              </div>
              <div class="heat-bar">
                <span class="heat-fill tone-{escape(domain["accent"])}" style="width: {width}%"></span>
              </div>
            </div>
            """
        )
    return "\n".join(rows)


def _render_domain_tiles(domains: Iterable[dict[str, Any]]) -> str:
    tiles: list[str] = []
    for domain in domains:
        path_cluster = "".join(
            f'<code>{escape(path)}</code>' for path in domain["sample_paths"]
        )
        tiles.append(
            f"""
            <article class="domain-tile tone-{escape(domain["accent"])}">
              <header>
                <span>{escape(domain["label"])}</span>
                <strong>{domain["route_count"]} endpoints</strong>
              </header>
              <p>{escape(domain["focus"])}</p>
              <div class="path-cluster">
                {path_cluster}
              </div>
            </article>
            """
        )
    return "\n".join(tiles)


def _render_tripwire_cards(tripwires: Iterable[dict[str, Any]]) -> str:
    cards: list[str] = []
    for tripwire in tripwires:
        cards.append(
            f"""
            <article class="tripwire-card">
              <strong>{escape(tripwire["label"])}</strong>
              <span>Trigger at {tripwire["value"]}</span>
              <p>{escape(tripwire["detail"])}</p>
            </article>
            """
        )
    return "\n".join(cards)


def _render_auth_rows(summary: dict[str, Any]) -> str:
    auth = summary["auth"]
    runtime = summary["runtime"]
    auth_mode = (
        "Header RBAC active"
        if auth["enabled"]
        else "Authentication disabled for local development"
    )
    rows = (
        ("Mode", auth_mode),
        ("Subject header", auth["subject_header"]),
        ("Name header", auth["name_header"]),
        ("Roles header", auth["roles_header"]),
        ("Request correlation", runtime["request_header"]),
        ("Trace correlation", runtime["trace_header"]),
    )

    rendered_rows: list[str] = []
    for label, value in rows:
        rendered_rows.append(
            f"""
            <div class="auth-row">
              <span>{escape(label)}</span>
              <code>{escape(value)}</code>
            </div>
            """
        )
    return "\n".join(rendered_rows)


def _render_table_chips(tables: Iterable[str]) -> str:
    return "".join(f'<span class="table-chip">{escape(table)}</span>' for table in tables)


@router.get("/dashboard/summary")
def dashboard_summary() -> dict[str, Any]:
    settings = get_settings()
    return _build_dashboard_summary(settings)


@router.get("/", response_class=HTMLResponse)
def landing_page() -> str:
    settings = get_settings()
    summary = _build_dashboard_summary(settings)
    monitoring = summary["monitoring"]
    app_name = escape(summary["display_name"])
    environment = escape(summary["environment"].upper())
    version = escape(summary["version"])
    generated_at = escape(summary["generated_at"])
    auth_mode = (
        "Header RBAC active"
        if summary["auth"]["enabled"]
        else "Local development open mode"
    )
    monitoring_status = (
        "Live feed active" if monitoring["available"] else "Monitoring unavailable"
    )
    monitoring_status_class = "is-live" if monitoring["available"] else "is-down"
    latest_batch = monitoring["latest_batch"]
    latest_batch_status = (
        latest_batch["status"].replace("_", " ").title()
        if latest_batch is not None
        else "No batch activity"
    )
    latest_batch_identifier = (
        latest_batch["batch_id"] if latest_batch is not None else "Awaiting first batch"
    )
    latest_batch_progress = (
        f'{latest_batch["completed_metros"]}/{latest_batch["expected_metros"]} metros complete'
        if latest_batch is not None
        else "No orchestration runs have completed yet."
    )
    freshness_label = "Not scoped"
    if monitoring["freshness"] is not None:
        freshness_label = (
            "Freshness passed"
            if monitoring["freshness"]["passed"]
            else f'Freshness failures: {monitoring["freshness"]["failed_count"]}'
        )
    monitoring_note = (
        monitoring["error"]
        or "Polling the live monitoring summary every 15 seconds."
    )
    metric_cards = _render_metric_cards(summary)
    phase_cards = _render_phase_cards(summary["domains"])
    heat_rows = _render_heat_rows(summary["domains"])
    domain_tiles = _render_domain_tiles(summary["domains"])
    tripwire_cards = _render_tripwire_cards(summary["tripwires"])
    auth_rows = _render_auth_rows(summary)
    table_chips = _render_table_chips(summary["managed_tables"])

    return f"""
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>{app_name} Command Center</title>
        <style>
          :root {{
            color-scheme: light;
            --bg: #f4efe4;
            --ink: #0b2239;
            --muted: #4d6378;
            --panel: rgba(255, 251, 244, 0.82);
            --panel-strong: rgba(255, 251, 244, 0.94);
            --line: rgba(11, 34, 57, 0.12);
            --lagoon: #0e7490;
            --marine: #12456b;
            --citrus: #c08412;
            --ember: #d97706;
            --sky: #0284c7;
            --signal: #0f766e;
            --graphite: #475569;
            --aurora: #0891b2;
            --shadow: 0 26px 90px rgba(11, 34, 57, 0.14);
            font-family: "Aptos", "Trebuchet MS", "Segoe UI", sans-serif;
          }}

          * {{
            box-sizing: border-box;
          }}

          html {{
            background:
              radial-gradient(circle at top left, rgba(14, 116, 144, 0.24), transparent 28%),
              radial-gradient(circle at bottom right, rgba(217, 119, 6, 0.18), transparent 26%),
              linear-gradient(180deg, #fbf7ef 0%, #f4efe4 52%, #efe7d8 100%);
          }}

          body {{
            margin: 0;
            min-height: 100vh;
            color: var(--ink);
            background:
              linear-gradient(rgba(11, 34, 57, 0.03) 1px, transparent 1px),
              linear-gradient(90deg, rgba(11, 34, 57, 0.03) 1px, transparent 1px);
            background-size: 28px 28px;
            padding: 28px;
          }}

          body::before,
          body::after {{
            content: "";
            position: fixed;
            width: 42vw;
            height: 42vw;
            border-radius: 50%;
            filter: blur(40px);
            opacity: 0.48;
            pointer-events: none;
            z-index: 0;
          }}

          body::before {{
            top: -14vw;
            left: -8vw;
            background: rgba(8, 145, 178, 0.18);
          }}

          body::after {{
            bottom: -18vw;
            right: -10vw;
            background: rgba(217, 119, 6, 0.16);
          }}

          a {{
            color: inherit;
            text-decoration: none;
          }}

          .dashboard-shell {{
            position: relative;
            z-index: 1;
            width: min(1480px, 100%);
            margin: 0 auto;
            display: grid;
            gap: 20px;
          }}

          .hero {{
            display: grid;
            grid-template-columns: minmax(0, 1.15fr) minmax(320px, 0.85fr);
            gap: 20px;
          }}

          .hero-copy,
          .hero-visual,
          .panel,
          .metric-card {{
            background: var(--panel);
            backdrop-filter: blur(14px);
            border: 1px solid var(--line);
            border-radius: 28px;
            box-shadow: var(--shadow);
          }}

          .hero-copy {{
            padding: 32px;
            position: relative;
            overflow: hidden;
          }}

          .hero-copy::after {{
            content: "";
            position: absolute;
            right: -12%;
            bottom: -35%;
            width: 300px;
            height: 300px;
            border-radius: 50%;
            background:
              radial-gradient(circle, rgba(14, 116, 144, 0.22) 0%, rgba(14, 116, 144, 0) 68%);
          }}

          .eyebrow {{
            display: inline-flex;
            align-items: center;
            gap: 10px;
            text-transform: uppercase;
            letter-spacing: 0.16em;
            font-size: 0.76rem;
            font-weight: 700;
            color: var(--lagoon);
          }}

          .eyebrow::before {{
            content: "";
            width: 34px;
            height: 2px;
            background: linear-gradient(90deg, var(--lagoon), rgba(14, 116, 144, 0.1));
          }}

          h1 {{
            margin: 18px 0 14px;
            font-size: clamp(2.8rem, 5vw, 5.4rem);
            line-height: 0.96;
            letter-spacing: -0.05em;
            max-width: 10ch;
          }}

          .lede {{
            margin: 0 0 24px;
            max-width: 62ch;
            color: var(--muted);
            line-height: 1.65;
            font-size: 1.02rem;
          }}

          .pill-row,
          .hero-actions,
          .metric-strip,
          .lower-grid,
          .secondary-grid {{
            display: grid;
            gap: 14px;
          }}

          .pill-row {{
            grid-template-columns: repeat(auto-fit, minmax(180px, max-content));
            margin-bottom: 26px;
          }}

          .pill {{
            display: inline-flex;
            align-items: center;
            gap: 10px;
            padding: 12px 16px;
            border-radius: 999px;
            border: 1px solid rgba(11, 34, 57, 0.1);
            background: rgba(255, 255, 255, 0.62);
            font-size: 0.92rem;
            font-weight: 600;
            color: var(--ink);
          }}

          .pill::before {{
            content: "";
            width: 10px;
            height: 10px;
            border-radius: 50%;
            background: linear-gradient(180deg, var(--lagoon), var(--sky));
            box-shadow: 0 0 0 6px rgba(14, 116, 144, 0.14);
          }}

          .hero-actions {{
            grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
          }}

          .action-link {{
            padding: 18px;
            border-radius: 22px;
            border: 1px solid rgba(11, 34, 57, 0.08);
            background: linear-gradient(180deg, rgba(255, 255, 255, 0.96), rgba(240, 249, 255, 0.92));
            transition:
              transform 0.18s ease,
              box-shadow 0.18s ease,
              border-color 0.18s ease;
          }}

          .action-link:hover {{
            transform: translateY(-3px);
            border-color: rgba(14, 116, 144, 0.28);
            box-shadow: 0 18px 36px rgba(14, 116, 144, 0.12);
          }}

          .action-link strong,
          .launch-link strong,
          .tripwire-card strong,
          .metric-card strong,
          .domain-tile strong,
          .phase-meta strong,
          .radar-readout strong {{
            display: block;
          }}

          .action-link small,
          .metric-card p,
          .phase-copy p,
          .domain-tile p,
          .tripwire-card p,
          .launch-link small {{
            color: var(--muted);
            line-height: 1.55;
          }}

          .hero-visual {{
            padding: 24px;
            display: grid;
            gap: 18px;
          }}

          .radar-stage {{
            position: relative;
            min-height: 320px;
            border-radius: 28px;
            overflow: hidden;
            background:
              radial-gradient(circle at center, rgba(255, 255, 255, 0.96) 0%, rgba(223, 242, 247, 0.88) 42%, rgba(11, 34, 57, 0.06) 100%),
              linear-gradient(180deg, rgba(8, 145, 178, 0.12), rgba(217, 119, 6, 0.12));
            border: 1px solid rgba(11, 34, 57, 0.08);
          }}

          .radar-stage::before,
          .radar-stage::after {{
            content: "";
            position: absolute;
            inset: 12%;
            border-radius: 50%;
            border: 1px dashed rgba(11, 34, 57, 0.14);
          }}

          .radar-stage::after {{
            inset: 23%;
          }}

          .ring {{
            position: absolute;
            border-radius: 50%;
            border: 1px solid rgba(11, 34, 57, 0.12);
          }}

          .ring-a {{
            inset: 10%;
          }}

          .ring-b {{
            inset: 22%;
          }}

          .ring-c {{
            inset: 34%;
          }}

          .sweep {{
            position: absolute;
            inset: 0;
            background: conic-gradient(from 110deg, transparent 0deg, transparent 248deg, rgba(14, 116, 144, 0.28) 310deg, transparent 360deg);
            animation: sweep 6s linear infinite;
          }}

          .node {{
            position: absolute;
            width: 16px;
            height: 16px;
            border-radius: 50%;
            background: white;
            border: 4px solid var(--lagoon);
            box-shadow: 0 0 0 10px rgba(14, 116, 144, 0.12);
            animation: pulse 2.8s ease-in-out infinite;
          }}

          .node-a {{
            top: 22%;
            left: 18%;
          }}

          .node-b {{
            top: 30%;
            right: 24%;
            border-color: var(--ember);
            box-shadow: 0 0 0 10px rgba(217, 119, 6, 0.14);
          }}

          .node-c {{
            bottom: 24%;
            left: 28%;
            border-color: var(--marine);
            box-shadow: 0 0 0 10px rgba(18, 69, 107, 0.12);
          }}

          .node-d {{
            bottom: 18%;
            right: 18%;
            border-color: var(--signal);
            box-shadow: 0 0 0 10px rgba(15, 118, 110, 0.12);
          }}

          .core-readout {{
            position: absolute;
            left: 50%;
            top: 50%;
            transform: translate(-50%, -50%);
            width: min(240px, 58%);
            padding: 22px;
            border-radius: 26px;
            background: rgba(255, 255, 255, 0.82);
            border: 1px solid rgba(11, 34, 57, 0.08);
            text-align: center;
          }}

          .core-readout span,
          .metric-card span,
          .heat-label span,
          .auth-row span,
          .domain-tile header span,
          .tripwire-card span,
          .launch-link span {{
            display: block;
            font-size: 0.8rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--muted);
          }}

          .core-readout strong {{
            margin-top: 8px;
            font-size: 2rem;
            letter-spacing: -0.04em;
          }}

          .core-readout small {{
            display: block;
            margin-top: 10px;
            color: var(--muted);
            line-height: 1.5;
          }}

          .radar-readout {{
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 12px;
          }}

          .radar-readout article {{
            padding: 14px 16px;
            border-radius: 20px;
            background: rgba(255, 255, 255, 0.7);
            border: 1px solid rgba(11, 34, 57, 0.08);
          }}

          .radar-readout strong {{
            margin-top: 8px;
            font-size: 1rem;
            line-height: 1.35;
          }}

          .metric-strip {{
            grid-template-columns: repeat(4, minmax(0, 1fr));
          }}

          .metric-card {{
            padding: 22px;
            position: relative;
            overflow: hidden;
          }}

          .metric-card::after {{
            content: "";
            position: absolute;
            right: -12%;
            bottom: -35%;
            width: 170px;
            height: 170px;
            border-radius: 50%;
            opacity: 0.32;
          }}

          .metric-card strong {{
            margin: 12px 0 10px;
            font-size: clamp(2rem, 3vw, 3rem);
            line-height: 0.95;
            letter-spacing: -0.05em;
          }}

          .monitoring-grid {{
            display: grid;
            grid-template-columns: minmax(0, 1.2fr) minmax(360px, 0.8fr);
            gap: 20px;
          }}

          .monitoring-head {{
            display: flex;
            justify-content: space-between;
            gap: 18px;
            align-items: start;
            margin-bottom: 18px;
          }}

          .monitoring-badges {{
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            justify-content: end;
          }}

          .monitor-badge {{
            display: inline-flex;
            align-items: center;
            gap: 10px;
            padding: 10px 14px;
            border-radius: 999px;
            border: 1px solid rgba(11, 34, 57, 0.08);
            background: rgba(255, 255, 255, 0.72);
            font-size: 0.82rem;
            font-weight: 700;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: var(--muted);
          }}

          .monitor-badge::before {{
            content: "";
            width: 10px;
            height: 10px;
            border-radius: 50%;
            background: rgba(100, 116, 139, 0.5);
          }}

          .monitor-badge.is-live::before {{
            background: #10b981;
            box-shadow: 0 0 0 6px rgba(16, 185, 129, 0.15);
          }}

          .monitor-badge.is-down::before {{
            background: #dc2626;
            box-shadow: 0 0 0 6px rgba(220, 38, 38, 0.12);
          }}

          .signal-grid {{
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 14px;
            margin-bottom: 18px;
          }}

          .signal-card {{
            padding: 18px;
            border-radius: 22px;
            border: 1px solid rgba(11, 34, 57, 0.08);
            background: linear-gradient(
              180deg,
              rgba(255, 255, 255, 0.88),
              rgba(246, 250, 252, 0.78)
            );
          }}

          .signal-card span,
          .lane-card h3,
          .feed-column h3,
          .latest-batch-card span {{
            display: block;
            font-size: 0.8rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--muted);
          }}

          .signal-card strong {{
            display: block;
            margin: 12px 0 8px;
            font-size: 2rem;
            line-height: 0.95;
            letter-spacing: -0.05em;
          }}

          .signal-card small,
          .latest-batch-card small {{
            display: block;
            color: var(--muted);
            line-height: 1.5;
          }}

          .lane-grid {{
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 14px;
          }}

          .lane-card,
          .latest-batch-card,
          .feed-column {{
            padding: 18px;
            border-radius: 22px;
            border: 1px solid rgba(11, 34, 57, 0.08);
            background: rgba(255, 255, 255, 0.72);
          }}

          .lane-card h3,
          .feed-column h3 {{
            margin: 0 0 14px;
          }}

          .lane-list,
          .feed-list {{
            display: grid;
            gap: 12px;
          }}

          .lane-row {{
            display: grid;
            gap: 8px;
          }}

          .lane-row header {{
            display: flex;
            justify-content: space-between;
            gap: 10px;
            align-items: baseline;
          }}

          .lane-row strong {{
            font-size: 0.95rem;
          }}

          .lane-row small {{
            color: var(--muted);
          }}

          .lane-track {{
            height: 10px;
            border-radius: 999px;
            background: rgba(11, 34, 57, 0.08);
            overflow: hidden;
          }}

          .lane-fill {{
            display: block;
            height: 100%;
            border-radius: inherit;
            background: linear-gradient(90deg, var(--lagoon), var(--sky));
          }}

          .lane-fill.failed,
          .threshold-chip.is-triggered {{
            background: linear-gradient(90deg, #b91c1c, #ef4444);
          }}

          .lane-fill.completed {{
            background: linear-gradient(90deg, #0f766e, #14b8a6);
          }}

          .lane-fill.active {{
            background: linear-gradient(90deg, #0e7490, #06b6d4);
          }}

          .lane-fill.building,
          .lane-fill.running {{
            background: linear-gradient(90deg, #c08412, #f59e0b);
          }}

          .threshold-chip {{
            display: grid;
            gap: 4px;
            padding: 12px 14px;
            border-radius: 16px;
            background: rgba(11, 34, 57, 0.05);
            border: 1px solid rgba(11, 34, 57, 0.08);
          }}

          .threshold-chip strong {{
            font-size: 0.9rem;
          }}

          .threshold-chip small {{
            color: var(--muted);
            line-height: 1.45;
          }}

          .feed-panel {{
            display: grid;
            gap: 14px;
          }}

          .latest-batch-card strong {{
            display: block;
            margin: 10px 0 8px;
            font-size: 1.4rem;
            line-height: 1.1;
          }}

          .feed-columns {{
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 14px;
          }}

          .feed-item {{
            display: grid;
            gap: 6px;
            padding: 14px;
            border-radius: 16px;
            background: rgba(11, 34, 57, 0.05);
            border: 1px solid rgba(11, 34, 57, 0.08);
          }}

          .feed-item header {{
            display: flex;
            justify-content: space-between;
            gap: 10px;
            align-items: start;
          }}

          .feed-item strong {{
            font-size: 0.95rem;
            line-height: 1.35;
          }}

          .feed-item code {{
            width: fit-content;
            padding: 6px 8px;
            border-radius: 10px;
            background: rgba(255, 255, 255, 0.74);
            border: 1px solid rgba(11, 34, 57, 0.08);
            font-family: "Cascadia Mono", "Consolas", monospace;
            font-size: 0.78rem;
          }}

          .feed-item small {{
            color: var(--muted);
            line-height: 1.45;
          }}

          .feed-empty {{
            padding: 14px;
            border-radius: 16px;
            background: rgba(11, 34, 57, 0.05);
            border: 1px dashed rgba(11, 34, 57, 0.12);
            color: var(--muted);
            line-height: 1.5;
          }}

          .tone-lagoon::after {{
            background: radial-gradient(circle, rgba(14, 116, 144, 0.22) 0%, transparent 70%);
          }}

          .tone-marine::after {{
            background: radial-gradient(circle, rgba(18, 69, 107, 0.22) 0%, transparent 70%);
          }}

          .tone-citrus::after {{
            background: radial-gradient(circle, rgba(192, 132, 18, 0.22) 0%, transparent 70%);
          }}

          .tone-ember::after {{
            background: radial-gradient(circle, rgba(217, 119, 6, 0.22) 0%, transparent 70%);
          }}

          .lower-grid {{
            grid-template-columns: minmax(0, 1.1fr) minmax(340px, 0.9fr);
          }}

          .secondary-grid {{
            grid-template-columns: repeat(2, minmax(0, 1fr));
          }}

          .panel {{
            padding: 24px;
          }}

          .panel h2 {{
            margin: 0 0 8px;
            font-size: 1.4rem;
            letter-spacing: -0.03em;
          }}

          .panel-copy {{
            margin: 0 0 18px;
            color: var(--muted);
            line-height: 1.55;
          }}

          .phase-grid {{
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 14px;
          }}

          .phase-card {{
            padding: 18px;
            border-radius: 24px;
            border: 1px solid rgba(11, 34, 57, 0.08);
            background: linear-gradient(180deg, rgba(255, 255, 255, 0.92), rgba(246, 250, 252, 0.82));
            display: grid;
            gap: 12px;
            min-height: 170px;
          }}

          .phase-index {{
            width: fit-content;
            padding: 8px 10px;
            border-radius: 999px;
            background: rgba(11, 34, 57, 0.06);
            font-size: 0.82rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
          }}

          .phase-copy h3 {{
            margin: 0 0 8px;
            font-size: 1.05rem;
          }}

          .phase-copy p,
          .phase-meta small,
          .heat-label strong,
          .auth-note,
          .table-note {{
            margin: 0;
            color: var(--muted);
          }}

          .phase-meta {{
            display: flex;
            justify-content: space-between;
            gap: 14px;
            align-items: end;
            margin-top: auto;
          }}

          .heat-stack {{
            display: grid;
            gap: 14px;
          }}

          .heat-row {{
            display: grid;
            gap: 10px;
          }}

          .heat-label {{
            display: flex;
            justify-content: space-between;
            gap: 16px;
            align-items: baseline;
          }}

          .heat-label strong {{
            font-size: 0.95rem;
          }}

          .heat-bar {{
            height: 12px;
            border-radius: 999px;
            overflow: hidden;
            background: rgba(11, 34, 57, 0.08);
          }}

          .heat-fill {{
            display: block;
            height: 100%;
            border-radius: inherit;
            background: linear-gradient(90deg, var(--marine), var(--lagoon));
            box-shadow: inset 0 0 12px rgba(255, 255, 255, 0.3);
          }}

          .heat-fill.tone-citrus {{
            background: linear-gradient(90deg, #b45309, #d97706);
          }}

          .heat-fill.tone-ember {{
            background: linear-gradient(90deg, #c2410c, #ea580c);
          }}

          .heat-fill.tone-sky,
          .heat-fill.tone-aurora {{
            background: linear-gradient(90deg, #0284c7, #0891b2);
          }}

          .heat-fill.tone-signal {{
            background: linear-gradient(90deg, #0f766e, #14b8a6);
          }}

          .heat-fill.tone-graphite {{
            background: linear-gradient(90deg, #334155, #64748b);
          }}

          .domain-grid {{
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 14px;
          }}

          .domain-tile {{
            padding: 18px;
            border-radius: 24px;
            border: 1px solid rgba(11, 34, 57, 0.08);
            background: rgba(255, 255, 255, 0.76);
          }}

          .domain-tile header {{
            display: flex;
            justify-content: space-between;
            gap: 14px;
            align-items: end;
            margin-bottom: 12px;
          }}

          .path-cluster {{
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
          }}

          .path-cluster code,
          .table-chip,
          .auth-row code {{
            display: inline-flex;
            align-items: center;
            padding: 8px 10px;
            border-radius: 12px;
            background: rgba(11, 34, 57, 0.06);
            border: 1px solid rgba(11, 34, 57, 0.08);
            font-family: "Cascadia Mono", "Consolas", monospace;
            font-size: 0.82rem;
            color: var(--ink);
          }}

          .auth-stack,
          .tripwire-grid,
          .launch-grid {{
            display: grid;
            gap: 12px;
          }}

          .auth-row {{
            display: flex;
            justify-content: space-between;
            gap: 14px;
            align-items: center;
            padding: 14px 16px;
            border-radius: 18px;
            background: rgba(255, 255, 255, 0.68);
            border: 1px solid rgba(11, 34, 57, 0.08);
          }}

          .auth-row span {{
            font-size: 0.75rem;
          }}

          .auth-row code {{
            text-align: right;
          }}

          .auth-note,
          .table-note {{
            line-height: 1.55;
          }}

          .tripwire-grid {{
            grid-template-columns: repeat(2, minmax(0, 1fr));
            margin-top: 18px;
          }}

          .tripwire-card {{
            padding: 16px;
            border-radius: 20px;
            background: linear-gradient(180deg, rgba(255, 255, 255, 0.84), rgba(246, 250, 252, 0.76));
            border: 1px solid rgba(11, 34, 57, 0.08);
          }}

          .tripwire-card span {{
            margin: 8px 0 10px;
            color: var(--ember);
          }}

          .table-cloud {{
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            max-height: 270px;
            overflow: auto;
            padding-right: 4px;
          }}

          .launch-grid {{
            grid-template-columns: repeat(2, minmax(0, 1fr));
          }}

          .launch-link {{
            padding: 18px;
            border-radius: 22px;
            border: 1px solid rgba(11, 34, 57, 0.08);
            background: linear-gradient(180deg, rgba(255, 255, 255, 0.86), rgba(240, 249, 255, 0.78));
            min-height: 136px;
          }}

          .launch-link strong {{
            margin: 10px 0 12px;
            font-size: 1.08rem;
          }}

          .footer-note {{
            display: flex;
            justify-content: space-between;
            gap: 14px;
            align-items: center;
            padding: 18px 22px;
            border-radius: 22px;
            border: 1px solid var(--line);
            background: var(--panel-strong);
            color: var(--muted);
            font-size: 0.92rem;
          }}

          @keyframes pulse {{
            0%, 100% {{
              transform: scale(1);
              opacity: 1;
            }}

            50% {{
              transform: scale(1.18);
              opacity: 0.82;
            }}
          }}

          @keyframes sweep {{
            from {{
              transform: rotate(0deg);
            }}

            to {{
              transform: rotate(360deg);
            }}
          }}

          @media (max-width: 1180px) {{
            .hero,
            .monitoring-grid,
            .lower-grid,
            .secondary-grid,
            .metric-strip {{
              grid-template-columns: 1fr;
            }}

            .signal-grid,
            .lane-grid,
            .feed-columns,
            .launch-grid,
            .tripwire-grid,
            .domain-grid,
            .phase-grid,
            .radar-readout {{
              grid-template-columns: 1fr;
            }}
          }}

          @media (max-width: 760px) {{
            body {{
              padding: 16px;
            }}

            .hero-copy,
            .hero-visual,
            .panel,
            .metric-card {{
              border-radius: 22px;
            }}

            h1 {{
              max-width: none;
            }}
          }}
        </style>
      </head>
      <body>
        <main class="dashboard-shell">
          <section class="hero">
            <section class="hero-copy">
              <span class="eyebrow">Dense Data Center Locator</span>
              <h1>Operations Command Center</h1>
              <p class="lede">
                A single visual surface for the locator platform: source intake,
                evaluation, scoring, orchestration, monitoring, audit, and UAT
                release flow all mapped into one live-ready control plane.
              </p>
              <div class="pill-row">
                <div class="pill">Environment {environment}</div>
                <div class="pill">Version <span id="version-pill">{version}</span></div>
                <div class="pill">{escape(auth_mode)}</div>
              </div>
              <div class="hero-actions">
                <a class="action-link" href="/docs">
                  <strong>Explore API Docs</strong>
                  <small>Jump straight into the FastAPI explorer and inspect every route.</small>
                </a>
                <a class="action-link" href="/dashboard/summary">
                  <strong>Open Dashboard JSON</strong>
                  <small>Inspect the public summary feed that powers this command center.</small>
                </a>
                <a class="action-link" href="/health">
                  <strong>Check Health</strong>
                  <small>Confirm the service is reachable before operator workflows begin.</small>
                </a>
              </div>
            </section>

            <section class="hero-visual">
              <div class="radar-stage" aria-hidden="true">
                <div class="ring ring-a"></div>
                <div class="ring ring-b"></div>
                <div class="ring ring-c"></div>
                <div class="sweep"></div>
                <span class="node node-a"></span>
                <span class="node node-b"></span>
                <span class="node node-c"></span>
                <span class="node node-d"></span>
                <div class="core-readout">
                  <span>Platform Mesh</span>
                  <strong>{summary["phase_count"]} Phases Online</strong>
                  <small>
                    Foundation, ingestion, evaluation, scoring, orchestration,
                    monitoring, audit, and UAT release all surfaced here.
                  </small>
                </div>
              </div>

              <div class="radar-readout">
                <article>
                  <span>Heartbeat</span>
                  <strong id="heartbeat-status">Checking live status</strong>
                </article>
                <article>
                  <span>Generated</span>
                  <strong id="dashboard-clock" data-generated-at="{generated_at}">{generated_at}</strong>
                </article>
                <article>
                  <span>Scenario Pack</span>
                  <strong>{escape(summary["runtime"]["scenario_pack_path"])}</strong>
                </article>
              </div>
            </section>
          </section>

          <section class="metric-strip">
            {metric_cards}
          </section>

          <section class="monitoring-grid">
            <article class="panel">
              <div class="monitoring-head">
                <div>
                  <h2>Live Monitoring Pulse</h2>
                  <p class="panel-copy">
                    Real pipeline telemetry flowing into the dashboard from the
                    monitoring service snapshot.
                  </p>
                </div>
                <div class="monitoring-badges">
                  <span class="monitor-badge {monitoring_status_class}" id="monitoring-status">
                    {escape(monitoring_status)}
                  </span>
                  <span class="monitor-badge" id="monitoring-evaluated">
                    {escape(monitoring["evaluated_at"])}
                  </span>
                </div>
              </div>

              <div class="signal-grid">
                <article class="signal-card">
                  <span>Open Alerts</span>
                  <strong id="monitoring-alert-count">{monitoring["alert_count"]}</strong>
                  <small>Current alert load from the monitoring overview.</small>
                </article>
                <article class="signal-card">
                  <span>Triggered Thresholds</span>
                  <strong id="monitoring-threshold-count">{monitoring["threshold_trigger_count"]}</strong>
                  <small>Tripwires currently breaching configured limits.</small>
                </article>
                <article class="signal-card">
                  <span>Recent Failed Runs</span>
                  <strong id="monitoring-failed-run-count">{monitoring["failed_run_count"]}</strong>
                  <small>Failed runs returned in the recent monitoring window.</small>
                </article>
                <article class="signal-card">
                  <span>Source Issues</span>
                  <strong id="monitoring-source-issue-count">{monitoring["source_issue_count"]}</strong>
                  <small id="monitoring-freshness-note">{escape(freshness_label)}</small>
                </article>
              </div>

              <div class="lane-grid">
                <article class="lane-card">
                  <h3>Run Status</h3>
                  <div class="lane-list" id="monitoring-run-lanes">
                    <div class="feed-empty">Refreshing run status lanes.</div>
                  </div>
                </article>
                <article class="lane-card">
                  <h3>Batch Status</h3>
                  <div class="lane-list" id="monitoring-batch-lanes">
                    <div class="feed-empty">Refreshing batch status lanes.</div>
                  </div>
                </article>
                <article class="lane-card">
                  <h3>Threshold Tripwires</h3>
                  <div class="lane-list" id="monitoring-threshold-list">
                    <div class="feed-empty">Refreshing threshold state.</div>
                  </div>
                </article>
              </div>
            </article>

            <article class="panel feed-panel">
              <article class="latest-batch-card">
                <span>Latest Batch</span>
                <strong id="monitoring-latest-batch-status">{escape(latest_batch_status)}</strong>
                <code id="monitoring-latest-batch-id">{escape(latest_batch_identifier)}</code>
                <small id="monitoring-latest-batch-progress">{escape(latest_batch_progress)}</small>
              </article>

              <div class="feed-columns">
                <article class="feed-column">
                  <h3>Alert Feed</h3>
                  <div class="feed-list" id="monitoring-alert-feed">
                    <div class="feed-empty">Monitoring alerts will appear here.</div>
                  </div>
                </article>
                <article class="feed-column">
                  <h3>Recent Failed Runs</h3>
                  <div class="feed-list" id="monitoring-run-feed">
                    <div class="feed-empty">Recent failed runs will appear here.</div>
                  </div>
                </article>
              </div>

              <p class="panel-copy" id="monitoring-error">{escape(monitoring_note)}</p>
            </article>
          </section>

          <section class="lower-grid">
            <article class="panel">
              <h2>Eight-Phase Delivery Grid</h2>
              <p class="panel-copy">
                The stack is organized into eight operating phases, each with its
                own route surface and control focus.
              </p>
              <div class="phase-grid">
                {phase_cards}
              </div>
            </article>

            <article class="panel">
              <h2>Route Heat</h2>
              <p class="panel-copy">
                Endpoint density by domain. Heavier bands indicate broader
                operator workflow coverage in the current API surface.
              </p>
              <div class="heat-stack">
                {heat_rows}
              </div>
            </article>
          </section>

          <section class="secondary-grid">
            <article class="panel">
              <h2>Domain Inventory</h2>
              <p class="panel-copy">
                Each platform slice includes representative paths so the dashboard
                stays grounded in the actual service surface.
              </p>
              <div class="domain-grid">
                {domain_tiles}
              </div>
            </article>

            <article class="panel">
              <h2>Access + Tripwires</h2>
              <p class="panel-copy">
                Header conventions and monitoring thresholds that shape operator
                access, observability, and release safety.
              </p>
              <div class="auth-stack">
                {auth_rows}
              </div>
              <div class="tripwire-grid">
                {tripwire_cards}
              </div>
            </article>

            <article class="panel">
              <h2>Managed Table Cloud</h2>
              <p class="table-note">
                The persistence layer currently tracks {summary["managed_table_count"]}
                managed tables across raw source data, scoring, operations, and UAT.
              </p>
              <div class="table-cloud">
                {table_chips}
              </div>
            </article>

            <article class="panel">
              <h2>Quick Launch</h2>
              <p class="panel-copy">
                Fast paths into diagnostics, docs, platform metadata, and release
                support endpoints.
              </p>
              <div class="launch-grid">
                <a class="launch-link" href="/docs">
                  <span>Browse</span>
                  <strong>Swagger Explorer</strong>
                  <small>Test routes and review request or response schemas.</small>
                </a>
                <a class="launch-link" href="/health">
                  <span>Probe</span>
                  <strong>Health Endpoint</strong>
                  <small>Simple readiness signal for local startup and uptime checks.</small>
                </a>
                <a class="launch-link" href="/version">
                  <span>Inspect</span>
                  <strong>Version Surface</strong>
                  <small>Expose the running application version for quick validation.</small>
                </a>
                <a class="launch-link" href="/dashboard/summary">
                  <span>Integrate</span>
                  <strong>Dashboard Summary JSON</strong>
                  <small>Public inventory feed for future frontends or status boards.</small>
                </a>
              </div>
            </article>
          </section>

          <section class="footer-note">
            <span>{app_name} is running in {environment} with {summary["protected_route_count"]} protected operator or admin routes.</span>
            <span>UAT environment: {escape(summary["runtime"]["uat_environment"])}</span>
          </section>
        </main>
        <script>
          const counters = document.querySelectorAll("[data-count]");
          counters.forEach((element) => {{
            const target = Number(element.dataset.count || "0");
            let current = 0;
            const step = Math.max(1, Math.ceil(target / 28));
            const tick = () => {{
              current = Math.min(current + step, target);
              element.textContent = String(current);
              if (current < target) {{
                window.requestAnimationFrame(tick);
              }}
            }};
            window.requestAnimationFrame(tick);
          }});

          const heartbeatStatus = document.getElementById("heartbeat-status");
          const versionPill = document.getElementById("version-pill");
          const dashboardClock = document.getElementById("dashboard-clock");
          const monitoringStatus = document.getElementById("monitoring-status");
          const monitoringEvaluated = document.getElementById("monitoring-evaluated");
          const monitoringAlertCount = document.getElementById("monitoring-alert-count");
          const monitoringThresholdCount = document.getElementById("monitoring-threshold-count");
          const monitoringFailedRunCount = document.getElementById("monitoring-failed-run-count");
          const monitoringSourceIssueCount = document.getElementById("monitoring-source-issue-count");
          const monitoringFreshnessNote = document.getElementById("monitoring-freshness-note");
          const monitoringRunLanes = document.getElementById("monitoring-run-lanes");
          const monitoringBatchLanes = document.getElementById("monitoring-batch-lanes");
          const monitoringThresholdList = document.getElementById("monitoring-threshold-list");
          const monitoringAlertFeed = document.getElementById("monitoring-alert-feed");
          const monitoringRunFeed = document.getElementById("monitoring-run-feed");
          const monitoringLatestBatchStatus = document.getElementById("monitoring-latest-batch-status");
          const monitoringLatestBatchId = document.getElementById("monitoring-latest-batch-id");
          const monitoringLatestBatchProgress = document.getElementById("monitoring-latest-batch-progress");
          const monitoringError = document.getElementById("monitoring-error");

          function escapeHtml(value) {{
            return String(value ?? "")
              .replace(/&/g, "&amp;")
              .replace(/</g, "&lt;")
              .replace(/>/g, "&gt;")
              .replace(/"/g, "&quot;")
              .replace(/'/g, "&#39;");
          }}

          function humanizeStatus(value) {{
            return String(value ?? "unknown")
              .replace(/_/g, " ")
              .replace(/\\b\\w/g, (character) => character.toUpperCase());
          }}

          function formatTimestamp(value) {{
            if (!value) {{
              return "No timestamp";
            }}

            const date = new Date(value);
            if (Number.isNaN(date.getTime())) {{
              return String(value);
            }}

            return date.toLocaleString([], {{
              year: "numeric",
              month: "short",
              day: "2-digit",
              hour: "2-digit",
              minute: "2-digit",
              second: "2-digit",
            }});
          }}

          function renderLaneRows(counts) {{
            const entries = Object.entries(counts || {{}});
            if (!entries.length) {{
              return '<div class="feed-empty">No status counts are available yet.</div>';
            }}

            const max = Math.max(...entries.map(([, count]) => Number(count) || 0), 1);
            return entries
              .map(([status, count]) => {{
                const width = Math.max(14, Math.round(((Number(count) || 0) / max) * 100));
                const tone = String(status || "").toLowerCase();
                return `
                  <div class="lane-row">
                    <header>
                      <strong>${{escapeHtml(humanizeStatus(status))}}</strong>
                      <small>${{escapeHtml(count)}}</small>
                    </header>
                    <div class="lane-track">
                      <span class="lane-fill ${{escapeHtml(tone)}}" style="width: ${{width}}%"></span>
                    </div>
                  </div>
                `;
              }})
              .join("");
          }}

          function renderThresholds(thresholds) {{
            if (!thresholds || !thresholds.length) {{
              return '<div class="feed-empty">No monitoring thresholds are configured yet.</div>';
            }}

            return thresholds
              .map((threshold) => `
                <div class="threshold-chip ${{threshold.triggered ? "is-triggered" : ""}}">
                  <strong>${{escapeHtml(humanizeStatus(threshold.code))}}</strong>
                  <small>${{escapeHtml(threshold.summary)}}</small>
                </div>
              `)
              .join("");
          }}

          function renderAlertFeed(alerts) {{
            if (!alerts || !alerts.length) {{
              return '<div class="feed-empty">No active alerts in the current monitoring window.</div>';
            }}

            return alerts
              .map((alert) => `
                <article class="feed-item">
                  <header>
                    <strong>${{escapeHtml(alert.summary)}}</strong>
                    <code>${{escapeHtml(alert.severity)}}</code>
                  </header>
                  <small>${{escapeHtml(alert.code)}}${{alert.metro_id ? ` · Metro ${{escapeHtml(alert.metro_id)}}` : ""}}</small>
                </article>
              `)
              .join("");
          }}

          function renderRunFeed(runs) {{
            if (!runs || !runs.length) {{
              return '<div class="feed-empty">No recent failed runs are present right now.</div>';
            }}

            return runs
              .map((run) => `
                <article class="feed-item">
                  <header>
                    <strong>${{escapeHtml(run.metro_id)}} · ${{escapeHtml(humanizeStatus(run.status))}}</strong>
                    <code>${{escapeHtml(run.run_id)}}</code>
                  </header>
                  <small>${{escapeHtml(run.failure_reason || "No failure reason recorded")}}</small>
                  <small>${{escapeHtml(formatTimestamp(run.completed_at || run.started_at))}}</small>
                </article>
              `)
              .join("");
          }}

          function applyMonitoring(monitoring) {{
            const isAvailable = Boolean(monitoring && monitoring.available);
            monitoringStatus.textContent = isAvailable ? "Live feed active" : "Monitoring unavailable";
            monitoringStatus.classList.toggle("is-live", isAvailable);
            monitoringStatus.classList.toggle("is-down", !isAvailable);
            monitoringEvaluated.textContent = formatTimestamp(monitoring.evaluated_at);
            monitoringAlertCount.textContent = String(monitoring.alert_count || 0);
            monitoringThresholdCount.textContent = String(monitoring.threshold_trigger_count || 0);
            monitoringFailedRunCount.textContent = String(monitoring.failed_run_count || 0);
            monitoringSourceIssueCount.textContent = String(monitoring.source_issue_count || 0);
            monitoringFreshnessNote.textContent = monitoring.freshness
              ? (monitoring.freshness.passed
                  ? "Freshness passed"
                  : `Freshness failures: ${{monitoring.freshness.failed_count}}`)
              : "Not scoped";

            const latestBatch = monitoring.latest_batch;
            if (latestBatch) {{
              monitoringLatestBatchStatus.textContent = humanizeStatus(latestBatch.status);
              monitoringLatestBatchId.textContent = latestBatch.batch_id;
              monitoringLatestBatchProgress.textContent =
                `${{latestBatch.completed_metros}}/${{latestBatch.expected_metros}} metros complete`;
            }} else {{
              monitoringLatestBatchStatus.textContent = "No batch activity";
              monitoringLatestBatchId.textContent = "Awaiting first batch";
              monitoringLatestBatchProgress.textContent = "No orchestration runs have completed yet.";
            }}

            monitoringRunLanes.innerHTML = renderLaneRows(monitoring.run_counts);
            monitoringBatchLanes.innerHTML = renderLaneRows(monitoring.batch_counts);
            monitoringThresholdList.innerHTML = renderThresholds(monitoring.thresholds);
            monitoringAlertFeed.innerHTML = renderAlertFeed(monitoring.alerts);
            monitoringRunFeed.innerHTML = renderRunFeed(monitoring.recent_failed_runs);
            monitoringError.textContent =
              monitoring.error || "Polling the live monitoring summary every 15 seconds.";
          }}

          async function refreshHeartbeat() {{
            try {{
              const response = await fetch("/health", {{ headers: {{ "Accept": "application/json" }} }});
              const payload = await response.json();
              heartbeatStatus.textContent = payload.status === "ok" ? "Healthy and reachable" : "Health check returned warnings";
            }} catch (_error) {{
              heartbeatStatus.textContent = "Unable to reach /health";
            }}
          }}

          async function refreshVersion() {{
            try {{
              const response = await fetch("/version", {{ headers: {{ "Accept": "application/json" }} }});
              const payload = await response.json();
              versionPill.textContent = payload.version || "{version}";
            }} catch (_error) {{
              versionPill.textContent = "{version}";
            }}
          }}

          async function refreshDashboardSummary() {{
            try {{
              const response = await fetch("/dashboard/summary", {{
                headers: {{ "Accept": "application/json" }},
              }});
              const payload = await response.json();
              if (payload.generated_at) {{
                dashboardClock.dataset.generatedAt = payload.generated_at;
              }}
              applyMonitoring(payload.monitoring || {{}});
            }} catch (_error) {{
              applyMonitoring({{
                available: false,
                evaluated_at: null,
                alert_count: 0,
                threshold_trigger_count: 0,
                failed_run_count: 0,
                source_issue_count: 0,
                run_counts: {{}},
                batch_counts: {{}},
                thresholds: [],
                alerts: [],
                recent_failed_runs: [],
                latest_batch: null,
                freshness: null,
                error: "Dashboard summary refresh failed. Verify the monitoring backend.",
              }});
            }}
          }}

          function refreshClock() {{
            const generatedAt = dashboardClock.dataset.generatedAt;
            if (!generatedAt) {{
              return;
            }}

            const date = new Date(generatedAt);
            dashboardClock.textContent = date.toLocaleString([], {{
              year: "numeric",
              month: "short",
              day: "2-digit",
              hour: "2-digit",
              minute: "2-digit",
              second: "2-digit",
            }});
          }}

          refreshClock();
          refreshHeartbeat();
          refreshVersion();
          refreshDashboardSummary();
          window.setInterval(refreshClock, 1000);
          window.setInterval(refreshDashboardSummary, 15000);
        </script>
      </body>
    </html>
    """


@router.get("/health")
def health() -> dict[str, str]:
    settings = get_settings()
    return {
        "status": "ok",
        "app": settings.app_name,
        "env": settings.app_env,
    }


@router.get("/version")
def version() -> dict[str, str]:
    return {"version": APP_VERSION}


@router.get("/foundation/tables")
def foundation_tables(_: AdminAccess) -> dict[str, list[str]]:
    return {"tables": MANAGED_TABLES}
