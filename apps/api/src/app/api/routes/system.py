# ruff: noqa: E501
from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, datetime
from html import escape
from typing import Annotated, Any

from fastapi import APIRouter, Depends
from fastapi.responses import HTMLResponse
from sqlalchemy.exc import SQLAlchemyError

from app.core.security import require_admin_access
from app.core.settings import Settings, get_settings
from app.db.models import MANAGED_TABLES
from app.db.session import SessionLocal
from app.schemas.dashboard import TexasDashboardSummaryResponse
from app.services.customer_dashboard import build_customer_dashboard_summary
from app.services.monitoring import MonitoringThresholdPolicy, build_monitoring_overview
from app.services.source_inventory import (
    SourceInventoryConfigurationError,
    load_authoritative_source_inventory,
)

router = APIRouter()
AdminAccess = Annotated[object, Depends(require_admin_access)]
APP_VERSION = "0.1.0"


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


def _read_source_inventory_snapshot(settings: Settings) -> dict[str, Any]:
    try:
        inventory = load_authoritative_source_inventory(
            settings.authoritative_source_inventory_path
        )
    except SourceInventoryConfigurationError as exc:
        return {
            "available": False,
            "error": str(exc),
            "version": None,
            "captured_at": None,
            "total_sources": 0,
            "free_sources": 0,
            "config_flag_count": 0,
            "phase_totals": [],
        }

    return {
        "available": True,
        "error": None,
        "version": inventory.version,
        "captured_at": inventory.captured_at,
        "total_sources": len(inventory.sources),
        "free_sources": sum(1 for item in inventory.sources if item.free),
        "config_flag_count": len(inventory.config_flags),
        "phase_totals": [
            {
                "phase": item.phase,
                "scope": item.scope,
                "source_count": item.source_count,
                "config_flag_count": item.config_flag_count,
            }
            for item in inventory.phase_totals
        ],
    }


def _build_dashboard_summary(settings: Settings) -> dict[str, Any]:
    return build_customer_dashboard_summary(
        settings,
        monitoring_snapshot=_read_monitoring_snapshot(settings),
        source_inventory_snapshot=_read_source_inventory_snapshot(settings),
    )


def _json_for_html(payload: Any) -> str:
    return json.dumps(payload, separators=(",", ":")).replace("</", "<\\/")


def _tokenize(value: str) -> str:
    return "-".join(
        chunk
        for chunk in "".join(
            character.lower() if character.isalnum() else " "
            for character in value
        ).split()
        if chunk
    )


def _map_position(lat: float, lon: float) -> tuple[float, float]:
    min_lat, max_lat = 25.7, 36.6
    min_lon, max_lon = -106.7, -93.5
    x = ((lon - min_lon) / (max_lon - min_lon)) * 100
    y = 100 - ((lat - min_lat) / (max_lat - min_lat)) * 100
    return max(4.0, min(96.0, x)), max(6.0, min(94.0, y))


def _render_metric_cards(metrics: Iterable[dict[str, Any]]) -> str:
    cards: list[str] = []
    for metric in metrics:
        cards.append(
            f"""
            <article class="metric-card tone-{escape(metric["tone"])}">
              <span>{escape(metric["label"])}</span>
              <strong>{metric["value"]}</strong>
              <p>{escape(metric["detail"])}</p>
            </article>
            """
        )
    return "\n".join(cards)


def _render_strength_chips(strengths: Iterable[str]) -> str:
    return "".join(
        f'<span class="strength-chip">{escape(strength)}</span>'
        for strength in strengths
    )


def _render_featured_cards(opportunities: Iterable[dict[str, Any]]) -> str:
    cards: list[str] = []
    for item in opportunities:
        cards.append(
            f"""
            <article class="featured-card">
              <div class="featured-score">
                <span>Score</span>
                <strong>{item["viability_score"]}</strong>
              </div>
              <div class="featured-copy">
                <header>
                  <small>{escape(item["corridor_name"])}</small>
                  <h3>{escape(item["site_name"])}</h3>
                </header>
                <p>{escape(item["summary"])}</p>
                <div class="featured-meta">
                  <span>{escape(item["city"])} · {escape(item["county"])}</span>
                  <span>{escape(item["readiness_stage"])}</span>
                </div>
                <div class="strength-list">
                  {_render_strength_chips(item["strengths"])}
                </div>
              </div>
            </article>
            """
        )
    return "\n".join(cards)


def _render_corridor_rows(corridors: Iterable[dict[str, Any]]) -> str:
    rows: list[str] = []
    for item in corridors:
        width = max(18, min(100, round((item["average_viability_score"] / 100) * 100)))
        rows.append(
            f"""
            <article class="corridor-row">
              <div class="corridor-copy">
                <header>
                  <strong>{escape(item["name"])}</strong>
                  <span>{item["site_count"]} sites · {item["priority_now_count"]} priority now</span>
                </header>
                <small>{escape(item["lead_metro"])} · {escape(item["lead_university"])}</small>
              </div>
              <div class="corridor-meter">
                <div class="corridor-track">
                  <span class="corridor-fill" style="width: {width}%"></span>
                </div>
                <strong>{item["average_viability_score"]}</strong>
              </div>
            </article>
            """
        )
    return "\n".join(rows)


def _render_opportunity_rows(opportunities: Iterable[dict[str, Any]]) -> str:
    rows: list[str] = []
    for item in opportunities:
        search_blob = " ".join(
            [
                item["site_name"],
                item["corridor_name"],
                item["metro"],
                item["city"],
                item["county"],
                item["university_anchor"],
                item["readiness_stage"],
            ]
        )
        rows.append(
            f"""
            <tr
              class="opportunity-row"
              data-metro="{escape(_tokenize(item["metro"]))}"
              data-stage="{escape(_tokenize(item["readiness_stage"]))}"
              data-university="{escape(_tokenize(item["university_anchor"]))}"
              data-search="{escape(search_blob.lower())}"
            >
              <td><span class="rank-pill">{item["rank"]:02d}</span></td>
              <td>
                <div class="site-cell">
                  <strong>{escape(item["site_name"])}</strong>
                  <span>{escape(item["city"])} · {escape(item["county"])}</span>
                  <small>{escape(item["corridor_name"])}</small>
                </div>
              </td>
              <td>{escape(item["metro"])}</td>
              <td>{escape(item["university_anchor"])}</td>
              <td>{escape(item["acreage_band"])}</td>
              <td><span class="stage-chip">{escape(item["readiness_stage"])}</span></td>
              <td>
                <div class="score-cell">
                  <strong>{item["viability_score"]}</strong>
                  <small>{escape(item["score_band"])}</small>
                </div>
              </td>
              <td>
                <div class="distance-cell">
                  <strong>{item["distance_to_city_miles"]} mi</strong>
                  <small>{item["distance_to_university_miles"]} mi to campus</small>
                </div>
              </td>
            </tr>
            """
        )
    return "\n".join(rows)


def _render_map_points(opportunities: Iterable[dict[str, Any]]) -> str:
    points: list[str] = []
    for item in opportunities:
        x, y = _map_position(item["lat"], item["lon"])
        if item["score_band"] == "Tier 1":
            radius = 4.8
            tone = "tier-1"
        elif item["score_band"] == "Tier 2":
            radius = 4.0
            tone = "tier-2"
        else:
            radius = 3.2
            tone = "tier-3"
        points.append(
            f"""
            <circle class="map-point {tone}" cx="{x:.2f}" cy="{y:.2f}" r="{radius:.2f}">
              <title>{escape(item["site_name"])} · {escape(item["city"])} · score {item["viability_score"]}</title>
            </circle>
            """
        )
    return "\n".join(points)


def _render_map_labels(opportunities: Iterable[dict[str, Any]]) -> str:
    labels: list[str] = []
    for item in list(opportunities)[:6]:
        x, y = _map_position(item["lat"], item["lon"])
        labels.append(
            f"""
            <g class="map-label">
              <text x="{x + 1.6:.2f}" y="{y - 1.4:.2f}">{escape(item["city"])}</text>
            </g>
            """
        )
    return "\n".join(labels)


def _render_filter_options(values: Iterable[str]) -> str:
    return "".join(
        f'<option value="{escape(_tokenize(value))}">{escape(value)}</option>'
        for value in values
    )


def _render_phase_totals(phase_totals: Iterable[dict[str, Any]]) -> str:
    cards: list[str] = []
    for item in phase_totals:
        cards.append(
            f"""
            <article class="phase-pill">
              <strong>Phase {item["phase"]}</strong>
              <span>{item["source_count"]} sources</span>
              <small>{escape(item["scope"])}</small>
            </article>
            """
        )
    return "\n".join(cards)


@router.get("/dashboard/summary", response_model=TexasDashboardSummaryResponse)
def dashboard_summary() -> TexasDashboardSummaryResponse:
    settings = get_settings()
    summary = _build_dashboard_summary(settings)
    return TexasDashboardSummaryResponse.model_validate(summary)


@router.get("/", response_class=HTMLResponse)
def landing_page() -> str:
    settings = get_settings()
    summary = _build_dashboard_summary(settings)
    monitoring = summary["monitoring"]
    coverage = summary["data_coverage"]

    metrics_html = _render_metric_cards(summary["metrics"])
    featured_html = _render_featured_cards(summary["featured_opportunities"])
    corridor_html = _render_corridor_rows(summary["corridors"])
    opportunity_rows_html = _render_opportunity_rows(summary["opportunities"])
    map_points_html = _render_map_points(summary["opportunities"])
    map_labels_html = _render_map_labels(summary["featured_opportunities"])
    metro_options_html = _render_filter_options(summary["filters"]["metros"])
    stage_options_html = _render_filter_options(summary["filters"]["readiness_stages"])
    university_options_html = _render_filter_options(summary["filters"]["university_anchors"])
    phase_totals_html = _render_phase_totals(coverage.get("phase_totals", []))
    payload_json = _json_for_html(summary)

    monitoring_status = (
        "Live pipeline feed active" if monitoring["available"] else "Monitoring unavailable"
    )
    monitoring_class = "live" if monitoring["available"] else "down"
    latest_batch = monitoring.get("latest_batch")
    latest_batch_label = (
        latest_batch["status"].replace("_", " ").title()
        if latest_batch is not None
        else "No live batch"
    )
    latest_batch_detail = (
        f'{latest_batch["completed_metros"]}/{latest_batch["expected_metros"]} metros complete'
        if latest_batch is not None
        else "Parcel scoring has not activated a batch yet."
    )
    freshness_label = "Freshness not scoped"
    if monitoring.get("freshness") is not None:
        freshness_label = (
            "Freshness passed"
            if monitoring["freshness"]["passed"]
            else f'Freshness failures: {monitoring["freshness"]["failed_count"]}'
        )

    coverage_status = "Authoritative source inventory loaded" if coverage["available"] else "Source inventory unavailable"
    coverage_note = coverage["error"] or (
        f'{coverage["free_sources"]} free sources across 3 build phases.'
        if coverage["available"]
        else "Dashboard is running without source inventory metadata."
    )

    return f"""
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>{escape(summary["display_name"])}</title>
        <style>
          :root {{
            color-scheme: light;
            --bg: #f5efe1;
            --ink: #102438;
            --muted: #5f6f7f;
            --paper: rgba(255, 250, 240, 0.9);
            --paper-strong: rgba(255, 252, 247, 0.96);
            --line: rgba(16, 36, 56, 0.12);
            --shadow: 0 30px 90px rgba(16, 36, 56, 0.14);
            --teal: #0f766e;
            --navy: #153b5c;
            --gold: #b7791f;
            --rust: #b45309;
            --sage: #5b7553;
            --cream: #fffaf2;
            --danger: #b91c1c;
            --tier1: #0f766e;
            --tier2: #c08124;
            --tier3: #64748b;
            font-family: "Aptos", "Segoe UI", sans-serif;
          }}

          * {{
            box-sizing: border-box;
          }}

          html {{
            background:
              radial-gradient(circle at top left, rgba(15, 118, 110, 0.2), transparent 28%),
              radial-gradient(circle at top right, rgba(180, 83, 9, 0.16), transparent 24%),
              linear-gradient(180deg, #fbf7ee 0%, #f5efe1 54%, #efe6d3 100%);
          }}

          body {{
            margin: 0;
            min-height: 100vh;
            color: var(--ink);
            background:
              linear-gradient(rgba(16, 36, 56, 0.03) 1px, transparent 1px),
              linear-gradient(90deg, rgba(16, 36, 56, 0.03) 1px, transparent 1px);
            background-size: 30px 30px;
            padding: 28px;
          }}

          body::before,
          body::after {{
            content: "";
            position: fixed;
            inset: auto;
            width: 36vw;
            height: 36vw;
            border-radius: 50%;
            filter: blur(48px);
            opacity: 0.4;
            pointer-events: none;
            z-index: 0;
          }}

          body::before {{
            top: -10vw;
            left: -8vw;
            background: rgba(15, 118, 110, 0.18);
          }}

          body::after {{
            right: -8vw;
            bottom: -12vw;
            background: rgba(180, 83, 9, 0.15);
          }}

          a {{
            color: inherit;
            text-decoration: none;
          }}

          .page-shell {{
            position: relative;
            z-index: 1;
            width: min(1500px, 100%);
            margin: 0 auto;
            display: grid;
            gap: 20px;
          }}

          .panel,
          .hero-copy,
          .hero-map,
          .metric-card {{
            background: var(--paper);
            border: 1px solid var(--line);
            border-radius: 28px;
            box-shadow: var(--shadow);
            backdrop-filter: blur(16px);
          }}

          .hero {{
            display: grid;
            grid-template-columns: minmax(0, 1.08fr) minmax(320px, 0.92fr);
            gap: 20px;
          }}

          .hero-copy {{
            padding: 34px;
            overflow: hidden;
            position: relative;
          }}

          .hero-copy::before {{
            content: "";
            position: absolute;
            inset: 0;
            background:
              radial-gradient(circle at top left, rgba(15, 118, 110, 0.1), transparent 32%),
              radial-gradient(circle at bottom right, rgba(180, 83, 9, 0.1), transparent 28%);
            pointer-events: none;
          }}

          .eyebrow {{
            display: inline-flex;
            align-items: center;
            gap: 10px;
            text-transform: uppercase;
            letter-spacing: 0.16em;
            font-size: 0.74rem;
            color: var(--teal);
            font-weight: 700;
          }}

          .eyebrow::before {{
            content: "";
            width: 40px;
            height: 1px;
            background: currentColor;
          }}

          h1,
          h2,
          h3 {{
            margin: 0;
            font-family: "Georgia", "Times New Roman", serif;
            letter-spacing: -0.03em;
          }}

          .hero-copy h1 {{
            margin-top: 18px;
            max-width: 13ch;
            font-size: clamp(2.8rem, 5vw, 5rem);
            line-height: 0.95;
          }}

          .hero-copy p {{
            max-width: 60ch;
            margin: 20px 0 0;
            color: var(--muted);
            font-size: 1.05rem;
            line-height: 1.7;
          }}

          .hero-actions {{
            margin-top: 26px;
            display: flex;
            flex-wrap: wrap;
            gap: 12px;
          }}

          .hero-link {{
            padding: 12px 18px;
            border-radius: 999px;
            border: 1px solid rgba(16, 36, 56, 0.12);
            font-weight: 700;
            background: rgba(255, 255, 255, 0.66);
          }}

          .hero-link.primary {{
            background: linear-gradient(135deg, #124a61, #0f766e);
            color: white;
            border-color: transparent;
          }}

          .hero-kpis {{
            margin-top: 28px;
            display: flex;
            flex-wrap: wrap;
            gap: 18px;
          }}

          .hero-kpis article {{
            min-width: 140px;
          }}

          .hero-kpis strong {{
            display: block;
            font-size: 1.9rem;
            line-height: 1;
          }}

          .hero-kpis span {{
            color: var(--muted);
            font-size: 0.92rem;
          }}

          .hero-meta {{
            margin-top: 20px;
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            color: var(--muted);
            font-size: 0.9rem;
          }}

          .meta-pill {{
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 8px 12px;
            border-radius: 999px;
            background: rgba(255, 255, 255, 0.76);
            border: 1px solid rgba(16, 36, 56, 0.1);
          }}

          .hero-map {{
            padding: 24px;
            display: grid;
            gap: 16px;
          }}

          .hero-map header {{
            display: flex;
            justify-content: space-between;
            align-items: baseline;
            gap: 12px;
          }}

          .hero-map header h2 {{
            font-size: 1.6rem;
          }}

          .hero-map header span {{
            color: var(--muted);
            font-size: 0.92rem;
          }}

          .map-stage {{
            width: 100%;
            min-height: 420px;
            border-radius: 24px;
            overflow: hidden;
            background:
              radial-gradient(circle at 22% 24%, rgba(15, 118, 110, 0.14), transparent 18%),
              radial-gradient(circle at 78% 70%, rgba(180, 83, 9, 0.12), transparent 20%),
              linear-gradient(180deg, rgba(21, 59, 92, 0.08), rgba(255, 255, 255, 0.12)),
              #f6efdf;
            border: 1px solid rgba(16, 36, 56, 0.08);
            position: relative;
          }}

          .map-stage svg {{
            width: 100%;
            height: 100%;
            display: block;
          }}

          .map-outline {{
            fill: rgba(21, 59, 92, 0.08);
            stroke: rgba(21, 59, 92, 0.35);
            stroke-width: 1.8;
          }}

          .map-grid {{
            stroke: rgba(21, 59, 92, 0.08);
            stroke-dasharray: 3 6;
          }}

          .map-point {{
            stroke: rgba(255, 255, 255, 0.85);
            stroke-width: 1.2;
            opacity: 0.96;
          }}

          .map-point.tier-1 {{
            fill: var(--tier1);
          }}

          .map-point.tier-2 {{
            fill: var(--tier2);
          }}

          .map-point.tier-3 {{
            fill: var(--tier3);
          }}

          .map-label text {{
            fill: var(--navy);
            font-size: 3.2px;
            font-weight: 700;
            paint-order: stroke;
            stroke: rgba(255, 250, 240, 0.92);
            stroke-width: 1;
            stroke-linecap: round;
          }}

          .map-legend {{
            display: flex;
            flex-wrap: wrap;
            gap: 14px;
            color: var(--muted);
            font-size: 0.88rem;
          }}

          .map-legend span {{
            display: inline-flex;
            align-items: center;
            gap: 8px;
          }}

          .map-legend i {{
            width: 10px;
            height: 10px;
            border-radius: 50%;
            display: inline-block;
          }}

          .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(5, minmax(0, 1fr));
            gap: 16px;
          }}

          .metric-card {{
            padding: 22px;
            display: grid;
            gap: 10px;
          }}

          .metric-card span {{
            color: var(--muted);
            font-size: 0.9rem;
          }}

          .metric-card strong {{
            font-size: 2.3rem;
            line-height: 1;
          }}

          .metric-card p {{
            margin: 0;
            color: var(--muted);
            line-height: 1.6;
            font-size: 0.95rem;
          }}

          .tone-teal strong {{ color: var(--teal); }}
          .tone-gold strong {{ color: var(--gold); }}
          .tone-navy strong {{ color: var(--navy); }}
          .tone-rust strong {{ color: var(--rust); }}
          .tone-sage strong {{ color: var(--sage); }}

          .insight-grid {{
            display: grid;
            grid-template-columns: minmax(0, 1.15fr) minmax(320px, 0.85fr);
            gap: 20px;
          }}

          .panel {{
            padding: 24px;
          }}

          .panel-header {{
            display: flex;
            justify-content: space-between;
            align-items: end;
            gap: 14px;
            margin-bottom: 18px;
          }}

          .panel-header p {{
            margin: 8px 0 0;
            color: var(--muted);
            line-height: 1.6;
          }}

          .featured-grid {{
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 16px;
          }}

          .featured-card {{
            display: grid;
            grid-template-columns: 88px minmax(0, 1fr);
            gap: 16px;
            padding: 18px;
            border: 1px solid rgba(16, 36, 56, 0.08);
            border-radius: 22px;
            background: rgba(255, 255, 255, 0.7);
          }}

          .featured-score {{
            width: 88px;
            height: 88px;
            border-radius: 50%;
            display: grid;
            place-items: center;
            align-content: center;
            background: conic-gradient(var(--teal), rgba(15, 118, 110, 0.18));
            color: white;
            text-align: center;
          }}

          .featured-score span {{
            font-size: 0.74rem;
            letter-spacing: 0.06em;
            text-transform: uppercase;
          }}

          .featured-score strong {{
            font-size: 2rem;
            line-height: 1;
          }}

          .featured-copy header {{
            display: grid;
            gap: 4px;
          }}

          .featured-copy small,
          .featured-meta span {{
            color: var(--muted);
          }}

          .featured-copy p {{
            margin: 12px 0;
            color: var(--muted);
            line-height: 1.6;
          }}

          .featured-meta {{
            display: flex;
            flex-wrap: wrap;
            gap: 12px;
            font-size: 0.9rem;
          }}

          .strength-list {{
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            margin-top: 14px;
          }}

          .strength-chip {{
            padding: 7px 10px;
            border-radius: 999px;
            font-size: 0.8rem;
            background: rgba(15, 118, 110, 0.09);
            color: var(--navy);
          }}

          .telemetry-grid {{
            display: grid;
            gap: 16px;
          }}

          .telemetry-card {{
            padding: 18px;
            border-radius: 22px;
            background: rgba(255, 255, 255, 0.72);
            border: 1px solid rgba(16, 36, 56, 0.08);
            display: grid;
            gap: 14px;
          }}

          .status-pill {{
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 8px 12px;
            border-radius: 999px;
            font-weight: 700;
            width: fit-content;
          }}

          .status-pill.live {{
            background: rgba(15, 118, 110, 0.12);
            color: var(--teal);
          }}

          .status-pill.down {{
            background: rgba(185, 28, 28, 0.1);
            color: var(--danger);
          }}

          .mini-metrics {{
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 12px;
          }}

          .mini-metrics article {{
            padding: 12px 14px;
            border-radius: 18px;
            background: rgba(16, 36, 56, 0.04);
          }}

          .mini-metrics span,
          .note {{
            color: var(--muted);
          }}

          .mini-metrics strong {{
            display: block;
            margin-top: 6px;
            font-size: 1.5rem;
          }}

          .corridor-stack {{
            display: grid;
            gap: 12px;
          }}

          .corridor-row {{
            display: grid;
            grid-template-columns: minmax(0, 1fr) 120px;
            gap: 16px;
            padding: 14px 16px;
            border-radius: 18px;
            background: rgba(255, 255, 255, 0.72);
            border: 1px solid rgba(16, 36, 56, 0.08);
          }}

          .corridor-copy header {{
            display: flex;
            justify-content: space-between;
            gap: 16px;
            margin-bottom: 6px;
          }}

          .corridor-copy span,
          .corridor-copy small {{
            color: var(--muted);
          }}

          .corridor-meter {{
            display: grid;
            gap: 8px;
            align-content: center;
          }}

          .corridor-track {{
            height: 12px;
            border-radius: 999px;
            background: rgba(16, 36, 56, 0.08);
            overflow: hidden;
          }}

          .corridor-fill {{
            display: block;
            height: 100%;
            border-radius: inherit;
            background: linear-gradient(90deg, var(--gold), var(--teal));
          }}

          .coverage-shell {{
            display: grid;
            gap: 16px;
          }}

          .phase-grid {{
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 12px;
          }}

          .phase-pill {{
            padding: 14px;
            border-radius: 18px;
            background: rgba(255, 255, 255, 0.72);
            border: 1px solid rgba(16, 36, 56, 0.08);
            display: grid;
            gap: 4px;
          }}

          .phase-pill span,
          .phase-pill small {{
            color: var(--muted);
          }}

          .catalog-shell {{
            display: grid;
            gap: 18px;
          }}

          .catalog-header {{
            display: flex;
            justify-content: space-between;
            align-items: end;
            gap: 18px;
          }}

          .catalog-header p {{
            margin: 8px 0 0;
            color: var(--muted);
          }}

          .filters {{
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
          }}

          .filters input,
          .filters select {{
            min-width: 180px;
            padding: 12px 14px;
            border-radius: 999px;
            border: 1px solid rgba(16, 36, 56, 0.12);
            background: rgba(255, 255, 255, 0.88);
            color: var(--ink);
          }}

          .table-shell {{
            overflow: auto;
            border-radius: 22px;
            border: 1px solid rgba(16, 36, 56, 0.08);
            background: rgba(255, 255, 255, 0.72);
          }}

          table {{
            width: 100%;
            border-collapse: collapse;
            min-width: 960px;
          }}

          th,
          td {{
            padding: 16px 18px;
            text-align: left;
            border-bottom: 1px solid rgba(16, 36, 56, 0.08);
            vertical-align: top;
          }}

          th {{
            position: sticky;
            top: 0;
            background: rgba(255, 250, 240, 0.96);
            font-size: 0.78rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--muted);
            z-index: 1;
          }}

          tbody tr:last-child td {{
            border-bottom: 0;
          }}

          .opportunity-row.is-hidden {{
            display: none;
          }}

          .rank-pill,
          .stage-chip {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            padding: 8px 12px;
            border-radius: 999px;
            background: rgba(16, 36, 56, 0.06);
            font-weight: 700;
          }}

          .site-cell,
          .score-cell,
          .distance-cell {{
            display: grid;
            gap: 4px;
          }}

          .site-cell span,
          .site-cell small,
          .score-cell small,
          .distance-cell small {{
            color: var(--muted);
          }}

          .footer-bar {{
            display: flex;
            flex-wrap: wrap;
            justify-content: space-between;
            gap: 14px;
            color: var(--muted);
            font-size: 0.92rem;
          }}

          .footer-links {{
            display: flex;
            flex-wrap: wrap;
            gap: 12px;
          }}

          .footer-links a {{
            padding: 10px 14px;
            border-radius: 999px;
            background: rgba(255, 255, 255, 0.72);
            border: 1px solid rgba(16, 36, 56, 0.1);
          }}

          @media (max-width: 1180px) {{
            .hero,
            .insight-grid {{
              grid-template-columns: 1fr;
            }}

            .metrics-grid,
            .phase-grid {{
              grid-template-columns: repeat(2, minmax(0, 1fr));
            }}
          }}

          @media (max-width: 860px) {{
            body {{
              padding: 16px;
            }}

            .metrics-grid,
            .featured-grid,
            .mini-metrics,
            .phase-grid {{
              grid-template-columns: 1fr;
            }}

            .catalog-header {{
              align-items: start;
            }}

            .filters {{
              width: 100%;
            }}

            .filters input,
            .filters select {{
              width: 100%;
            }}
          }}
        </style>
      </head>
      <body>
        <div class="page-shell">
          <section class="hero">
            <div class="hero-copy">
              <span class="eyebrow">Texas Data Center Siting Dashboard</span>
              <h1>{escape(summary["hero_title"])}</h1>
              <p>{escape(summary["hero_subtitle"])}</p>
              <div class="hero-actions">
                <a class="hero-link primary" href="#opportunity-catalogue">Explore the 50-site watchlist</a>
                <a class="hero-link" href="/dashboard/summary">Open live JSON</a>
                <a class="hero-link" href="/health">Health check</a>
              </div>
              <div class="hero-kpis">
                <article>
                  <strong>{summary["opportunity_count"]}</strong>
                  <span>Ranked Texas possibilities</span>
                </article>
                <article>
                  <strong>{summary["priority_now_count"]}</strong>
                  <span>Priority-now corridors</span>
                </article>
                <article>
                  <strong>{summary["top_tier_count"]}</strong>
                  <span>Tier 1 opportunities</span>
                </article>
                <article>
                  <strong>{summary["corridor_count"]}</strong>
                  <span>Major state corridors</span>
                </article>
              </div>
              <div class="hero-meta">
                <span class="meta-pill">Mode: {escape(summary["data_mode"])}</span>
                <span class="meta-pill">Market: {escape(summary["market"])}</span>
                <span class="meta-pill">Generated: <span id="dashboard-generated">{escape(summary["generated_at"])}</span></span>
              </div>
            </div>
            <div class="hero-map">
              <header>
                <div>
                  <h2>Texas Opportunity Field</h2>
                  <span>Geographic spread of the current customer watchlist.</span>
                </div>
                <strong>{summary["opportunity_count"]} sites</strong>
              </header>
              <div class="map-stage">
                <svg viewBox="0 0 100 100" role="img" aria-label="Texas opportunity field">
                  <path class="map-outline" d="M26 8 L43 11 L49 21 L63 21 L72 31 L80 31 L88 38 L84 49 L91 59 L86 83 L71 86 L56 94 L42 82 L37 67 L26 61 L22 46 L11 40 L15 24 L24 18 Z"></path>
                  <path class="map-grid" d="M12 28 H88 M16 46 H84 M22 64 H80 M30 20 V82 M48 16 V90 M66 22 V88"></path>
                  {map_points_html}
                  {map_labels_html}
                </svg>
              </div>
              <div class="map-legend">
                <span><i style="background: var(--tier1)"></i> Tier 1</span>
                <span><i style="background: var(--tier2)"></i> Tier 2</span>
                <span><i style="background: var(--tier3)"></i> Strategic reserve</span>
              </div>
            </div>
          </section>

          <section class="metrics-grid">
            {metrics_html}
          </section>

          <section class="insight-grid">
            <section class="panel">
              <div class="panel-header">
                <div>
                  <h2>Featured Opportunities</h2>
                  <p>Highest-scoring sites balancing utility access, fiber reach, land scale, and university talent.</p>
                </div>
                <strong>Top 6</strong>
              </div>
              <div class="featured-grid">
                {featured_html}
              </div>
            </section>

            <section class="panel">
              <div class="telemetry-grid">
                <article class="telemetry-card">
                  <div class="panel-header" style="margin-bottom: 0;">
                    <div>
                      <h2>Live Build Telemetry</h2>
                      <p>Operational read-through from the ingestion and scoring pipeline.</p>
                    </div>
                  </div>
                  <span id="monitoring-status" class="status-pill {monitoring_class}">{escape(monitoring_status)}</span>
                  <div class="mini-metrics">
                    <article>
                      <span>Alerts</span>
                      <strong id="monitoring-alert-count">{monitoring["alert_count"]}</strong>
                    </article>
                    <article>
                      <span>Source issues</span>
                      <strong id="monitoring-source-issues">{monitoring["source_issue_count"]}</strong>
                    </article>
                    <article>
                      <span>Failed runs</span>
                      <strong id="monitoring-failed-runs">{monitoring["failed_run_count"]}</strong>
                    </article>
                    <article>
                      <span>Freshness</span>
                      <strong id="monitoring-freshness">{escape(freshness_label)}</strong>
                    </article>
                  </div>
                  <div class="note">
                    <strong id="monitoring-latest-batch">{escape(latest_batch_label)}</strong><br />
                    <span id="monitoring-latest-batch-detail">{escape(latest_batch_detail)}</span>
                  </div>
                  <div class="note" id="monitoring-note">{escape(monitoring["error"] or "Polling live monitoring detail for customer confidence and operator visibility.")}</div>
                </article>

                <article class="telemetry-card">
                  <div class="panel-header" style="margin-bottom: 0;">
                    <div>
                      <h2>Data Coverage</h2>
                      <p>Authoritative public-source inventory staged behind the parcel-scoring roadmap.</p>
                    </div>
                  </div>
                  <span id="coverage-status" class="status-pill {'live' if coverage['available'] else 'down'}">{escape(coverage_status)}</span>
                  <div class="mini-metrics">
                    <article>
                      <span>Total sources</span>
                      <strong id="coverage-total-sources">{coverage["total_sources"]}</strong>
                    </article>
                    <article>
                      <span>Free sources</span>
                      <strong id="coverage-free-sources">{coverage["free_sources"]}</strong>
                    </article>
                    <article>
                      <span>Config flags</span>
                      <strong id="coverage-config-flags">{coverage["config_flag_count"]}</strong>
                    </article>
                    <article>
                      <span>Inventory version</span>
                      <strong id="coverage-version">{escape(coverage["version"] or "unavailable")}</strong>
                    </article>
                  </div>
                  <div class="note" id="coverage-note">{escape(coverage_note)}</div>
                  <div class="phase-grid" id="coverage-phase-grid">
                    {phase_totals_html}
                  </div>
                </article>
              </div>
            </section>
          </section>

          <section class="panel">
            <div class="panel-header">
              <div>
                <h2>Corridor Momentum</h2>
                <p>Corridors ranked by average site viability and immediate build readiness.</p>
              </div>
              <strong>{summary["corridor_count"]} corridors</strong>
            </div>
            <div class="corridor-stack">
              {corridor_html}
            </div>
          </section>

          <section class="panel" id="opportunity-catalogue">
            <div class="catalog-shell">
              <div class="catalog-header">
                <div>
                  <h2>Texas Opportunity Catalogue</h2>
                  <p id="catalogue-count">Showing {summary["opportunity_count"]} of {summary["opportunity_count"]} sites.</p>
                </div>
                <div class="filters">
                  <input id="search-filter" type="search" placeholder="Search city, metro, university, or corridor" />
                  <select id="metro-filter">
                    <option value="">All metros</option>
                    {metro_options_html}
                  </select>
                  <select id="stage-filter">
                    <option value="">All readiness stages</option>
                    {stage_options_html}
                  </select>
                  <select id="university-filter">
                    <option value="">All university anchors</option>
                    {university_options_html}
                  </select>
                </div>
              </div>
              <div class="table-shell">
                <table>
                  <thead>
                    <tr>
                      <th>Rank</th>
                      <th>Site</th>
                      <th>Metro</th>
                      <th>University anchor</th>
                      <th>Land scale</th>
                      <th>Readiness</th>
                      <th>Score</th>
                      <th>Access</th>
                    </tr>
                  </thead>
                  <tbody id="opportunity-table-body">
                    {opportunity_rows_html}
                  </tbody>
                </table>
              </div>
            </div>
          </section>

          <footer class="footer-bar">
            <span>{escape(settings.app_name)} · version <span id="version-pill">{escape(APP_VERSION)}</span></span>
            <div class="footer-links">
              <a href="/dashboard/summary">Live JSON</a>
              <a href="/health">Health</a>
              <a href="/version">Version</a>
            </div>
          </footer>
        </div>

        <script id="dashboard-payload" type="application/json">{payload_json}</script>
        <script>
          const payloadNode = document.getElementById("dashboard-payload");
          let dashboardPayload = payloadNode ? JSON.parse(payloadNode.textContent) : null;

          const searchFilter = document.getElementById("search-filter");
          const metroFilter = document.getElementById("metro-filter");
          const stageFilter = document.getElementById("stage-filter");
          const universityFilter = document.getElementById("university-filter");
          const catalogueCount = document.getElementById("catalogue-count");
          const rows = Array.from(document.querySelectorAll(".opportunity-row"));

          const monitoringStatus = document.getElementById("monitoring-status");
          const monitoringAlertCount = document.getElementById("monitoring-alert-count");
          const monitoringSourceIssues = document.getElementById("monitoring-source-issues");
          const monitoringFailedRuns = document.getElementById("monitoring-failed-runs");
          const monitoringFreshness = document.getElementById("monitoring-freshness");
          const monitoringLatestBatch = document.getElementById("monitoring-latest-batch");
          const monitoringLatestBatchDetail = document.getElementById("monitoring-latest-batch-detail");
          const monitoringNote = document.getElementById("monitoring-note");
          const coverageStatus = document.getElementById("coverage-status");
          const coverageTotalSources = document.getElementById("coverage-total-sources");
          const coverageFreeSources = document.getElementById("coverage-free-sources");
          const coverageConfigFlags = document.getElementById("coverage-config-flags");
          const coverageVersion = document.getElementById("coverage-version");
          const coverageNote = document.getElementById("coverage-note");
          const dashboardGenerated = document.getElementById("dashboard-generated");
          const versionPill = document.getElementById("version-pill");

          function humanizeStatus(value) {{
            return String(value || "")
              .replace(/_/g, " ")
              .replace(/\\b\\w/g, (character) => character.toUpperCase());
          }}

          function applyFilters() {{
            const searchValue = String(searchFilter.value || "").trim().toLowerCase();
            const metroValue = metroFilter.value;
            const stageValue = stageFilter.value;
            const universityValue = universityFilter.value;

            let visibleCount = 0;
            for (const row of rows) {{
              const matchesSearch = !searchValue || row.dataset.search.includes(searchValue);
              const matchesMetro = !metroValue || row.dataset.metro === metroValue;
              const matchesStage = !stageValue || row.dataset.stage === stageValue;
              const matchesUniversity = !universityValue || row.dataset.university === universityValue;
              const isVisible = matchesSearch && matchesMetro && matchesStage && matchesUniversity;
              row.classList.toggle("is-hidden", !isVisible);
              if (isVisible) {{
                visibleCount += 1;
              }}
            }}

            catalogueCount.textContent = `Showing ${{visibleCount}} of ${{rows.length}} sites.`;
          }}

          function updateMonitoring(monitoring) {{
            if (!monitoringStatus || !monitoring) {{
              return;
            }}

            const isAvailable = Boolean(monitoring.available);
            monitoringStatus.textContent = isAvailable ? "Live pipeline feed active" : "Monitoring unavailable";
            monitoringStatus.classList.toggle("live", isAvailable);
            monitoringStatus.classList.toggle("down", !isAvailable);
            monitoringAlertCount.textContent = String(monitoring.alert_count || 0);
            monitoringSourceIssues.textContent = String(monitoring.source_issue_count || 0);
            monitoringFailedRuns.textContent = String(monitoring.failed_run_count || 0);
            monitoringFreshness.textContent = monitoring.freshness
              ? (monitoring.freshness.passed
                  ? "Freshness passed"
                  : `Freshness failures: ${{monitoring.freshness.failed_count}}`)
              : "Freshness not scoped";

            if (monitoring.latest_batch) {{
              monitoringLatestBatch.textContent = humanizeStatus(monitoring.latest_batch.status);
              monitoringLatestBatchDetail.textContent =
                `${{monitoring.latest_batch.completed_metros}}/${{monitoring.latest_batch.expected_metros}} metros complete`;
            }} else {{
              monitoringLatestBatch.textContent = "No live batch";
              monitoringLatestBatchDetail.textContent = "Parcel scoring has not activated a batch yet.";
            }}

            monitoringNote.textContent =
              monitoring.error || "Polling live monitoring detail for customer confidence and operator visibility.";
          }}

          function updateCoverage(coverage) {{
            if (!coverageStatus || !coverage) {{
              return;
            }}

            const isAvailable = Boolean(coverage.available);
            coverageStatus.textContent = isAvailable
              ? "Authoritative source inventory loaded"
              : "Source inventory unavailable";
            coverageStatus.classList.toggle("live", isAvailable);
            coverageStatus.classList.toggle("down", !isAvailable);
            coverageTotalSources.textContent = String(coverage.total_sources || 0);
            coverageFreeSources.textContent = String(coverage.free_sources || 0);
            coverageConfigFlags.textContent = String(coverage.config_flag_count || 0);
            coverageVersion.textContent = coverage.version || "unavailable";
            coverageNote.textContent = coverage.error || `${{coverage.free_sources || 0}} free sources across 3 build phases.`;
          }}

          async function refreshSummary() {{
            try {{
              const response = await fetch("/dashboard/summary", {{
                headers: {{ Accept: "application/json" }},
              }});
              if (!response.ok) {{
                return;
              }}
              dashboardPayload = await response.json();
              if (dashboardPayload.generated_at && dashboardGenerated) {{
                dashboardGenerated.textContent = dashboardPayload.generated_at;
              }}
              if (dashboardPayload.version && versionPill) {{
                versionPill.textContent = dashboardPayload.version;
              }}
              updateMonitoring(dashboardPayload.monitoring);
              updateCoverage(dashboardPayload.data_coverage);
            }} catch (_error) {{
            }}
          }}

          if (searchFilter) {{
            searchFilter.addEventListener("input", applyFilters);
          }}
          if (metroFilter) {{
            metroFilter.addEventListener("change", applyFilters);
          }}
          if (stageFilter) {{
            stageFilter.addEventListener("change", applyFilters);
          }}
          if (universityFilter) {{
            universityFilter.addEventListener("change", applyFilters);
          }}

          applyFilters();
          if (dashboardPayload) {{
            updateMonitoring(dashboardPayload.monitoring);
            updateCoverage(dashboardPayload.data_coverage);
          }}
          window.setInterval(refreshSummary, 60000);
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
