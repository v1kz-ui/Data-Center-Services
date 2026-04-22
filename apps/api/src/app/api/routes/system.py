# ruff: noqa: E501
from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, datetime
from html import escape
from statistics import mean
from typing import Annotated, Any
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import HTMLResponse
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.core.security import AppRole, require_admin_access, require_roles
from app.core.settings import Settings, get_settings
from app.db.models import MANAGED_TABLES
from app.db.session import SessionLocal, get_db
from app.schemas.dashboard import TexasDashboardSummaryResponse
from app.services.customer_dashboard import build_customer_dashboard_summary
from app.services.monitoring import MonitoringThresholdPolicy, build_monitoring_overview
from app.services.source_inventory import (
    SourceInventoryConfigurationError,
    load_authoritative_source_inventory,
)

router = APIRouter()
AdminAccess = Annotated[object, Depends(require_admin_access)]
DbSession = Annotated[Session, Depends(get_db)]
DashboardAccess = Annotated[
    object,
    Depends(require_roles(AppRole.ADMIN, AppRole.OPERATOR, AppRole.READER)),
]
APP_VERSION = "0.1.0"
_CLIENT_DOSSIER_LIMIT = 136
_METRO_SHORT_LABELS = {
    "Dallas-Fort Worth": "DFW",
    "Houston": "HOU",
    "Austin": "AUS",
    "San Antonio": "SAT",
    "Brazos Valley": "BCS",
    "El Paso": "ELP",
    "Rio Grande Valley": "RGV",
}
_FIELD_LINKS: tuple[tuple[str, str], ...] = (
    ("Dallas-Fort Worth", "Austin"),
    ("Austin", "San Antonio"),
    ("Austin", "Houston"),
    ("Austin", "Brazos Valley"),
    ("San Antonio", "Houston"),
    ("San Antonio", "Rio Grande Valley"),
    ("San Antonio", "El Paso"),
)
_PREMIUM_FONT_LINKS = """
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
        <link href="https://fonts.googleapis.com/css2?family=Cormorant+Garamond:wght@500;600;700&family=IBM+Plex+Mono:wght@500;600;700&family=Source+Sans+3:wght@400;500;600;700;800&display=swap" rel="stylesheet" />
"""
_PREMIUM_SHARED_CSS = """
          :root {
            --font-sans: "Source Sans 3", "Avenir Next", "Segoe UI", system-ui, sans-serif;
            --font-display: "Cormorant Garamond", "Bodoni 72", "Didot", "Georgia", serif;
            --font-mono: "IBM Plex Mono", "Cascadia Mono", "SFMono-Regular", Consolas, monospace;
            --bg: #f6f7f2;
            --ink: #15211f;
            --muted: #64716d;
            --line: rgba(21, 33, 31, 0.12);
            --panel: rgba(255, 255, 252, 0.88);
            --panel-strong: rgba(255, 255, 255, 0.96);
            --paper: rgba(255, 255, 252, 0.88);
            --paper-strong: rgba(255, 255, 255, 0.96);
            --teal: #08766d;
            --navy: #19364c;
            --gold: #ad7f28;
            --rust: #96482e;
            --sage: #697e5a;
            --danger: #a61b1b;
            --tier1: #08766d;
            --tier2: #ad7f28;
            --tier3: #65707b;
            --shadow: 0 22px 58px rgba(21, 33, 31, 0.12);
            --shadow-soft: 0 12px 30px rgba(21, 33, 31, 0.08);
            font-family: var(--font-sans);
          }

          html {
            text-rendering: optimizeLegibility;
            -webkit-font-smoothing: antialiased;
            -moz-osx-font-smoothing: grayscale;
            background: #f6f7f2 !important;
          }

          body {
            font-family: var(--font-sans) !important;
            background:
              linear-gradient(180deg, rgba(8, 118, 109, 0.08), transparent 34%),
              linear-gradient(90deg, rgba(21, 33, 31, 0.035) 1px, transparent 1px),
              linear-gradient(rgba(21, 33, 31, 0.035) 1px, transparent 1px),
              linear-gradient(180deg, #fbfcf8 0%, #f6f7f2 48%, #edf0e7 100%) !important;
            background-size: auto, 34px 34px, 34px 34px, auto !important;
            font-feature-settings: "kern" 1, "liga" 1;
          }

          body::before,
          body::after,
          .hero-copy::before,
          .hero-map::before {
            display: none !important;
          }

          h1,
          h2,
          h3 {
            font-family: var(--font-display) !important;
            font-weight: 600 !important;
            letter-spacing: 0 !important;
            color: var(--ink);
          }

          h1,
          .hero-copy h1 {
            font-size: 4.65rem !important;
            line-height: 0.95 !important;
          }

          h2 {
            font-size: 2.08rem;
            line-height: 1.05;
          }

          h3 {
            font-size: 1.42rem;
            line-height: 1.12;
          }

          p,
          li,
          td,
          th,
          small,
          span {
            letter-spacing: 0;
          }

          .eyebrow,
          thead th,
          th,
          .metric-card span,
          .stat-card span,
          .decision-card span,
          .dossier-card span,
          .risk-card span,
          .timeline-step span,
          .brief-card span,
          .hero-kicker span,
          .map-badge,
          .map-stat span {
            font-family: var(--font-mono) !important;
            letter-spacing: 0 !important;
          }

          .top-nav,
          .quick-nav,
          .detail-hero,
          .hero,
          .hero-copy,
          .hero-map,
          .panel,
          .metric-card,
          .stat-card,
          .brief-card,
          .featured-card,
          .decision-card,
          .dossier-card,
          .risk-card,
          .timeline-step,
          .related-card,
          .telemetry-card,
          .corridor-row,
          .phase-pill,
          .evidence-row,
          .detail-score-row,
          .table-shell,
          .map-stage,
          .mini-map,
          .map-focus-card {
            border-radius: 8px !important;
            border-color: var(--line) !important;
            background: linear-gradient(180deg, rgba(255, 255, 255, 0.92), rgba(250, 251, 246, 0.84)) !important;
            box-shadow: var(--shadow-soft) !important;
          }

          .detail-hero,
          .hero,
          .panel {
            box-shadow: var(--shadow) !important;
          }

          .dossier-panel,
          .score-orbit {
            background:
              linear-gradient(135deg, rgba(25, 54, 76, 0.98), rgba(8, 118, 109, 0.86)),
              #19364c !important;
            color: white;
          }

          .map-stage,
          .mini-map {
            background:
              linear-gradient(135deg, rgba(17, 34, 45, 0.96), rgba(18, 70, 72, 0.88) 45%, rgba(237, 240, 231, 0.92) 100%) !important;
            border-color: rgba(255, 255, 255, 0.18) !important;
            box-shadow:
              inset 0 1px 0 rgba(255, 255, 255, 0.22),
              0 18px 46px rgba(21, 33, 31, 0.14) !important;
          }

          .hero-link,
          .action-link,
          .detail-button,
          .evidence-link,
          .top-nav a,
          .footer-links a,
          .filters input,
          .filters select {
            border-radius: 999px !important;
            box-shadow: 0 10px 24px rgba(21, 33, 31, 0.07);
            transition:
              transform 160ms ease,
              box-shadow 160ms ease,
              border-color 160ms ease,
              background 160ms ease;
          }

          .hero-link:hover,
          .hero-link:focus-visible,
          .action-link:hover,
          .action-link:focus-visible,
          .detail-button:hover,
          .detail-button:focus-visible,
          .evidence-link:hover,
          .evidence-link:focus-visible,
          .top-nav a:hover,
          .top-nav a:focus-visible,
          .footer-links a:hover,
          .footer-links a:focus-visible {
            transform: translateY(-1px);
            box-shadow: 0 16px 34px rgba(21, 33, 31, 0.12);
            outline: none;
          }

          .hero-link.primary,
          .action-link.primary,
          .hero-primary-action,
          .detail-button {
            background: linear-gradient(135deg, #19364c, #08766d) !important;
            color: white !important;
          }

          .meta-pill,
          .rank-pill,
          .stage-chip,
          .strength-chip,
          .headwind-chip,
          .quick-nav a {
            box-shadow: none !important;
          }

          .table-shell {
            background: rgba(255, 255, 255, 0.94) !important;
          }

          table {
            font-variant-numeric: tabular-nums;
          }

          thead th,
          th {
            background: rgba(246, 247, 242, 0.96) !important;
            color: #52605c !important;
            font-size: 0.76rem !important;
            font-weight: 700 !important;
          }

          td {
            color: #1f2b28;
          }

          tbody tr,
          .opportunity-row,
          .featured-card,
          .related-card {
            transition:
              background 160ms ease,
              transform 160ms ease,
              box-shadow 160ms ease,
              border-color 160ms ease;
          }

          .opportunity-row:hover,
          .opportunity-row:focus-visible {
            background: rgba(8, 118, 109, 0.075) !important;
          }

          .featured-card:hover,
          .featured-card:focus-visible,
          .related-card:hover,
          .related-card:focus-visible {
            border-color: rgba(8, 118, 109, 0.34) !important;
          }

          .score-ring,
          .featured-score {
            box-shadow:
              inset 0 0 0 1px rgba(255, 255, 255, 0.4),
              0 18px 46px rgba(8, 118, 109, 0.16) !important;
          }

          .score-meter,
          .corridor-track {
            background: rgba(21, 33, 31, 0.08) !important;
          }

          .score-meter i,
          .corridor-fill {
            background: linear-gradient(90deg, #08766d, #ad7f28) !important;
          }

          .filters input,
          .filters select {
            font: 600 0.95rem var(--font-sans) !important;
          }

          .footer-bar {
            font-family: var(--font-mono);
            letter-spacing: 0;
          }

          @media (max-width: 860px) {
            h1,
            .hero-copy h1 {
              font-size: 2.85rem !important;
              line-height: 0.98 !important;
            }

            h2 {
              font-size: 1.72rem;
            }
          }

          @media (max-width: 520px) {
            h1,
            .hero-copy h1 {
              font-size: 2.45rem !important;
            }
          }
"""


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


def _build_dashboard_summary(settings: Settings, db_session: Session) -> dict[str, Any]:
    return build_customer_dashboard_summary(
        settings,
        db_session=db_session,
        monitoring_snapshot=_read_monitoring_snapshot(settings),
        source_inventory_snapshot=_read_source_inventory_snapshot(settings),
    )


def _json_for_html(payload: Any) -> str:
    return json.dumps(payload, separators=(",", ":")).replace("</", "<\\/")


def _contender_detail_href(item: dict[str, Any]) -> str:
    return f"/dashboard/contenders/{quote(str(item['site_id']), safe='')}"


def _find_contender(contenders: Iterable[dict[str, Any]], site_id: str) -> dict[str, Any] | None:
    for item in contenders:
        if str(item.get("site_id")) == site_id:
            return item
    return None


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


def _format_count(value: int | float | None) -> str:
    if value is None:
        return "0"
    return f"{int(value):,}"


def _format_money(value: Any) -> str:
    if value is None:
        return "Not disclosed"
    try:
        amount = float(value)
    except (TypeError, ValueError):
        return "Not disclosed"
    if amount >= 1_000_000:
        return f"${amount / 1_000_000:.1f}M"
    return f"${amount:,.0f}"


def _format_status_label(value: Any) -> str:
    if value is None or str(value).strip() == "":
        return "Not attached"
    return str(value).replace("_", " ").strip().title()


def _score_value(item: dict[str, Any], key: str) -> int:
    try:
        return int(round(float(item.get(key) or 0)))
    except (TypeError, ValueError):
        return 0


def _score_tone(score: int) -> str:
    if score >= 88:
        return "elite"
    if score >= 74:
        return "strong"
    if score >= 60:
        return "managed"
    return "watch"


def _score_width(score: int) -> int:
    return max(6, min(100, score))


def _render_detail_score_rows(item: dict[str, Any]) -> str:
    score_rows = (
        (
            "Viability",
            "viability_score",
            "Composite siting fit across power, fiber, land, water, talent, market, and resilience signals.",
        ),
        (
            "Power",
            "power_score",
            "Utility and high-voltage adjacency signal used to judge whether the site can support serious load conversations.",
        ),
        (
            "Fiber",
            "fiber_score",
            "Carrier, peering, and network-route proximity signal for latency-sensitive data-center use.",
        ),
        (
            "Water",
            "water_score",
            "Municipal, reuse, and surface-water optionality proxy for cooling and entitlement diligence.",
        ),
        (
            "Talent",
            "talent_score",
            "Access to major population centers and university anchors for construction, operations, and vendor depth.",
        ),
        (
            "Approval",
            "approval_score",
            "Social and political pathway estimate after metro profile, land scale, water, and proximity adjustments.",
        ),
    )
    rows: list[str] = []
    for label, key, detail in score_rows:
        score = _score_value(item, key)
        rows.append(
            f"""
            <article class="detail-score-row tone-{_score_tone(score)}">
              <div>
                <strong>{escape(label)}</strong>
                <span>{escape(detail)}</span>
              </div>
              <div class="score-meter" aria-label="{escape(label)} score {score}">
                <i style="width: {_score_width(score)}%"></i>
              </div>
              <b>{score}</b>
            </article>
            """
        )
    return "\n".join(rows)


def _render_headwind_chips(item: dict[str, Any]) -> str:
    headwinds = [str(value) for value in item.get("approval_headwinds", []) if value]
    if not headwinds:
        return '<span class="headwind-chip is-clear">No named headwinds flagged</span>'
    return "".join(
        f'<span class="headwind-chip">{escape(headwind)}</span>'
        for headwind in headwinds
    )


def _build_client_positioning(item: dict[str, Any]) -> list[str]:
    strengths = [str(value) for value in item.get("strengths", []) if value]
    while len(strengths) < 3:
        strengths.append("balanced infrastructure fit")
    listing_source = item.get("listing_source_id")
    source_sentence = (
        f"The attached {listing_source} listing gives the team a traceable first call path."
        if listing_source
        else "This seeded contender needs a market-listing attachment before outreach."
    )
    confidence_sentence = (
        f"Confidence is C{_score_value(item, 'confidence_score'):02d}, so it is suitable for client review once parcel facts are checked."
        if item.get("confidence_score") is not None
        else "Confidence is catalogue-based, so this should be presented as a strategic watchlist item until live parcel evidence is attached."
    )
    return [
        (
            f"Position {item['site_name']} as a {item['score_band']} "
            f"{item['metro']} contender with a {item['viability_score']} viability score "
            f"and a {item['readiness_stage'].lower()} action posture."
        ),
        (
            f"The strongest client-facing case is {strengths[0]}, {strengths[1]}, "
            f"and {strengths[2]}, with {item['university_anchor']} anchoring the talent story."
        ),
        (
            f"Approval currently reads as {str(item['approval_stage']).lower()} "
            f"with social and political scores of {item['social_score']} and {item['political_score']}."
        ),
        f"{source_sentence} {confidence_sentence}",
    ]


def _strength_list(item: dict[str, Any]) -> list[str]:
    strengths = [str(value) for value in item.get("strengths", []) if value]
    while len(strengths) < 3:
        strengths.append("balanced infrastructure fit")
    return strengths


def _score_read(score: int) -> str:
    if score >= 88:
        return "lead-strength"
    if score >= 74:
        return "credible"
    if score >= 60:
        return "workable with diligence"
    return "watch item"


def _price_read(item: dict[str, Any]) -> str:
    price = item.get("asking_price")
    if price is None:
        return "Pricing is not disclosed in the client view, so the first commercial question is whether the seller will support a disciplined valuation conversation."
    return (
        f"The current asking-price signal is {_format_money(price)}, which gives the team "
        "an early anchor for price-per-acre, option structure, and whether control should be pursued quickly or staged."
    )


def _build_top_ten_executive_memo(item: dict[str, Any]) -> list[str]:
    strengths = _strength_list(item)
    return [
        (
            f"{item['site_name']} is a top-{item['rank']} Texas contender because it combines "
            f"{_score_read(_score_value(item, 'power_score'))} power positioning, "
            f"{_score_read(_score_value(item, 'fiber_score'))} fiber reach, and "
            f"{_score_read(_score_value(item, 'talent_score'))} workforce access inside the "
            f"{item['metro']} market. The page should be read as a pursuit memo, not just a scorecard."
        ),
        (
            f"The client-facing story is straightforward: the asset sits near {item['city']} in "
            f"{item['county']}, is tied to the {item['corridor_name']} corridor, and has "
            f"{item['university_anchor']} as the named talent anchor. The strongest message is "
            f"{strengths[0]}, followed by {strengths[1]} and {strengths[2]}."
        ),
        (
            f"Readiness is currently marked {item['readiness_stage'].lower()} with a "
            f"{item['viability_score']} viability score and {item['approval_score']} approval score. "
            f"The approval read is {str(item['approval_stage']).lower()}, meaning this can be discussed "
            "with clients as a credible opportunity while still being explicit about the diligence gates."
        ),
        _price_read(item),
    ]


def _build_top_ten_thesis_cards(item: dict[str, Any]) -> list[dict[str, str]]:
    strengths = _strength_list(item)
    return [
        {
            "label": "Why it ranks",
            "title": f"{item['score_band']} contender with board-leading balance",
            "body": (
                f"Rank {item['rank']} reflects a strong combined read across infrastructure, "
                f"location, approval path, and market evidence. The site is not carried by one metric; "
                f"it stays competitive because {strengths[0]}, {strengths[1]}, and {strengths[2]} "
                "all support the same pursuit narrative."
            ),
        },
        {
            "label": "Client angle",
            "title": f"{item['metro']} access without losing tract focus",
            "body": (
                f"The client can understand this as a {item['metro']} option with concrete "
                f"{item['city']} / {item['county']} geography, {item['acreage_band']} land scale, "
                f"and a university anchor at {item['university_anchor']}."
            ),
        },
        {
            "label": "Decision use",
            "title": "Good enough for a principal-level first pass",
            "body": (
                f"The page supports a first client conversation because it states the upside, the "
                f"approval posture, the market-listing evidence, and the next diligence gates in one place."
            ),
        },
        {
            "label": "Red-team note",
            "title": "Do not oversell before utility confirmation",
            "body": (
                "The ranking earns attention, but capacity, interconnection timing, water strategy, "
                "title, and local entitlement conditions still need proof before a site-control recommendation."
            ),
        },
    ]


def _build_top_ten_risk_cards(item: dict[str, Any]) -> list[dict[str, str]]:
    risks = [
        {
            "label": "Power capacity",
            "risk": "Grid fit may not match the headline score until utility load, substation, and delivery-timeline facts are confirmed.",
            "mitigation": "Request utility service territory context, nearest substation details, available capacity, and expected interconnection study path.",
        },
        {
            "label": "Water and cooling",
            "risk": "Water score is a proxy, not a final cooling answer, and local scarcity or discharge constraints can change the pursuit posture.",
            "mitigation": "Validate municipal supply, reuse options, wastewater capacity, and whether an air-cooled or hybrid design is more realistic.",
        },
        {
            "label": "Entitlement path",
            "risk": f"Approval is {str(item['approval_stage']).lower()}, so the public-process burden may vary by jurisdiction and neighborhood context.",
            "mitigation": "Map zoning, ETJ exposure, hearings, incentive appetite, and likely stakeholder concerns before naming this as ready-to-control.",
        },
        {
            "label": "Commercial control",
            "risk": "The marketed listing does not prove clean title, seller flexibility, utility easements, or contiguous developable acreage.",
            "mitigation": "Run ownership, parcel assembly, survey, title, easement, floodplain, and access checks before LOI structure is drafted.",
        },
    ]
    if _score_value(item, "approval_score") < 68:
        risks.append(
            {
                "label": "Community reception",
                "risk": "Approval score is not yet in the low-friction range, so the public narrative may need active shaping.",
                "mitigation": "Prepare jobs, tax base, water, noise, visual-screening, and grid-resilience messaging before outreach.",
            }
        )
    return risks


def _build_top_ten_pursuit_steps(item: dict[str, Any]) -> list[dict[str, str]]:
    return [
        {
            "phase": "First 24 hours",
            "title": "Confirm the asset is real and available",
            "body": "Validate listing freshness, seller/broker contact, current price posture, acreage, access, and whether the asset can be placed under control without a public process leak.",
        },
        {
            "phase": "Days 2-5",
            "title": "Build the infrastructure proof pack",
            "body": "Collect utility territory, substation distance, likely voltage class, fiber route optionality, water/wastewater facts, and highway/heavy-haul access assumptions.",
        },
        {
            "phase": "Week 1",
            "title": "Run entitlement and community screens",
            "body": f"Check {item['city']} and {item['county']} rules, ETJ exposure, floodplain, wetlands, protected uses, adjacent neighborhoods, and incentive appetite.",
        },
        {
            "phase": "Week 2",
            "title": "Prepare go / no-go recommendation",
            "body": "Present a client-ready memo with control path, estimated diligence cost, top risks, mitigation plan, and whether the site deserves an LOI, watchlist hold, or removal.",
        },
    ]


def _build_top_ten_client_questions(item: dict[str, Any]) -> list[str]:
    return [
        f"Is the client prioritizing speed-to-power in {item['metro']}, or is land-control optionality more important than schedule?",
        f"Would the client accept a {item['readiness_stage'].lower()} site if utility confirmation requires a staged diligence spend?",
        "What is the minimum campus load, phase-one MW target, and acceptable interconnection timeline?",
        "Does the client require a single-owner tract, or can parcel assembly be considered?",
        "Should water intensity be treated as a gating constraint, or can design flexibility reduce that risk?",
        "Is the client comfortable with a confidential broker/seller inquiry before the local approval read is fully mapped?",
    ]


def _render_top_ten_thesis_cards(cards: Iterable[dict[str, str]]) -> str:
    return "\n".join(
        f"""
        <article class="dossier-card">
          <span>{escape(card["label"])}</span>
          <strong>{escape(card["title"])}</strong>
          <p>{escape(card["body"])}</p>
        </article>
        """
        for card in cards
    )


def _render_top_ten_risk_cards(cards: Iterable[dict[str, str]]) -> str:
    return "\n".join(
        f"""
        <article class="risk-card">
          <span>{escape(card["label"])}</span>
          <strong>{escape(card["risk"])}</strong>
          <p>{escape(card["mitigation"])}</p>
        </article>
        """
        for card in cards
    )


def _render_top_ten_pursuit_steps(steps: Iterable[dict[str, str]]) -> str:
    return "\n".join(
        f"""
        <article class="timeline-step">
          <span>{escape(step["phase"])}</span>
          <strong>{escape(step["title"])}</strong>
          <p>{escape(step["body"])}</p>
        </article>
        """
        for step in steps
    )


def _render_top_ten_client_questions(questions: Iterable[str]) -> str:
    return "\n".join(
        f"<li>{escape(question)}</li>"
        for question in questions
    )


def _build_decision_lens(item: dict[str, Any]) -> list[dict[str, str]]:
    viability = _score_value(item, "viability_score")
    approval = _score_value(item, "approval_score")
    water = _score_value(item, "water_score")
    power = _score_value(item, "power_score")
    confidence = _score_value(item, "confidence_score")
    recommendation = (
        "Advance to controlled diligence"
        if viability >= 74 and approval >= 60
        else "Hold for targeted diligence"
    )
    throttle = (
        "Utility capacity"
        if power < 74
        else "Approval path"
        if approval < 68
        else "Water strategy"
        if water < 72
        else "Commercial control"
    )
    return [
        {
            "label": "Recommendation",
            "title": recommendation,
            "body": (
                f"Use this as a client-ready shortlist item, but keep the ask disciplined: "
                f"confirm site control, utility facts, and entitlement exposure before any pursuit spend scales."
            ),
        },
        {
            "label": "Primary throttle",
            "title": throttle,
            "body": (
                f"The scorecard reads well, but the next decision should hinge on {throttle.lower()} "
                "because that is where attractive land most often becomes non-actionable."
            ),
        },
        {
            "label": "Confidence",
            "title": f"C{confidence:02d}" if item.get("confidence_score") is not None else "Catalogue-based",
            "body": (
                "Enough signal exists for a client conversation; the next layer is evidence collection, "
                "not more ranking."
            ),
        },
        {
            "label": "Client next move",
            "title": "Approve a diligence sprint",
            "body": (
                "The cleanest UX path for the client is a yes/no on a short diligence sprint, "
                "with findings returned as a pursuit memo."
            ),
        },
    ]


def _render_decision_lens(item: dict[str, Any]) -> str:
    return "\n".join(
        f"""
        <article class="decision-card">
          <span>{escape(card["label"])}</span>
          <strong>{escape(card["title"])}</strong>
          <p>{escape(card["body"])}</p>
        </article>
        """
        for card in _build_decision_lens(item)
    )


def _render_top_ten_client_dossier(item: dict[str, Any]) -> str:
    rank = int(item.get("rank") or 0)
    if rank < 1 or rank > _CLIENT_DOSSIER_LIMIT:
        return ""

    executive_memo_html = _render_paragraph_list(_build_top_ten_executive_memo(item))
    thesis_cards_html = _render_top_ten_thesis_cards(_build_top_ten_thesis_cards(item))
    risk_cards_html = _render_top_ten_risk_cards(_build_top_ten_risk_cards(item))
    pursuit_steps_html = _render_top_ten_pursuit_steps(_build_top_ten_pursuit_steps(item))
    client_questions_html = _render_top_ten_client_questions(
        _build_top_ten_client_questions(item)
    )
    return f"""
      <section class="panel dossier-panel" id="dossier">
        <div class="panel-header">
          <div>
            <span class="eyebrow">Top {_CLIENT_DOSSIER_LIMIT} client dossier</span>
            <h2>Executive Briefing Memo</h2>
            <p>Expanded client-ready narrative for the highest-priority contenders.</p>
          </div>
          <strong>Rank {rank:02d}</strong>
        </div>
        <div class="memo-body">
          {executive_memo_html}
        </div>
      </section>

      <section class="panel" id="thesis">
        <div class="panel-header">
          <div>
            <h2>Client Thesis</h2>
            <p>The concise argument for why this site deserves attention, how to frame it, and where to stay cautious.</p>
          </div>
        </div>
        <div class="dossier-grid">
          {thesis_cards_html}
        </div>
      </section>

      <section class="content-grid" id="risk">
        <section class="panel">
          <div class="panel-header">
            <div>
              <h2>Risk And Mitigation Read</h2>
              <p>What could weaken the opportunity, paired with the practical diligence action that keeps the pursuit grounded.</p>
            </div>
          </div>
          <div class="risk-grid">
            {risk_cards_html}
          </div>
        </section>

        <aside class="panel">
          <div class="panel-header">
            <div>
              <h2>Client Call Questions</h2>
              <p>Questions to ask before recommending capital, site control, or a deeper broker/seller process.</p>
            </div>
          </div>
          <ol class="question-list">
            {client_questions_html}
          </ol>
        </aside>
      </section>

      <section class="panel" id="timeline">
        <div class="panel-header">
          <div>
            <h2>Pursuit Workplan</h2>
            <p>A practical sequence for turning the contender into a defendable go / no-go recommendation.</p>
          </div>
        </div>
        <div class="timeline-grid">
          {pursuit_steps_html}
        </div>
      </section>
    """


def _build_next_diligence_steps(item: dict[str, Any]) -> list[str]:
    steps = [
        "Confirm ownership, site control path, title constraints, and whether the marketed acreage is contiguous.",
        "Request utility capacity, substation, and interconnection context before client capital is committed.",
        "Validate diverse fiber paths, nearest carrier routes, and realistic lateral construction cost.",
        "Check zoning, extra-territorial jurisdiction exposure, permitting calendar, and incentive posture.",
        "Run floodplain, wetlands, environmental, water-supply, and wastewater/reuse diligence before LOI terms.",
    ]
    if _score_value(item, "water_score") < 72:
        steps.append("Prioritize water rights, reuse agreements, and cooling strategy because the water score is not yet a lead strength.")
    if _score_value(item, "approval_score") < 64:
        steps.append("Add a local political read before external outreach because the approval pathway is likely to need active management.")
    if item.get("source_url"):
        steps.append("Open the source listing, preserve the evidence link, and confirm asking price, broker contact, and current availability.")
    return steps


def _render_paragraph_list(items: Iterable[str]) -> str:
    return "\n".join(
        f"<p>{escape(item)}</p>"
        for item in items
    )


def _render_action_list(items: Iterable[str]) -> str:
    return "\n".join(
        f"<li>{escape(item)}</li>"
        for item in items
    )


def _render_related_contenders(items: Iterable[dict[str, Any]]) -> str:
    cards: list[str] = []
    for item in items:
        cards.append(
            f"""
            <a class="related-card" href="{escape(_contender_detail_href(item))}">
              <span>Rank {item["rank"]:02d} &middot; {escape(item["metro"])}</span>
              <strong>{escape(item["site_name"])}</strong>
              <small>{escape(item["readiness_stage"])} &middot; score {item["viability_score"]}</small>
            </a>
            """
        )
    if not cards:
        return '<p class="empty-note">No nearby contenders are currently attached to this board view.</p>'
    return "\n".join(cards)


def _related_contenders(
    contenders: list[dict[str, Any]],
    contender: dict[str, Any],
    *,
    limit: int = 4,
) -> list[dict[str, Any]]:
    site_id = contender["site_id"]
    related = [
        item for item in contenders
        if item["site_id"] != site_id and (
            item["metro"] == contender["metro"]
            or item["corridor_name"] == contender["corridor_name"]
        )
    ]
    if len(related) < limit:
        related.extend(
            item for item in contenders
            if item["site_id"] != site_id and item not in related
        )
    return sorted(related, key=lambda item: item["rank"])[:limit]


def _read_client_readiness_brief(db_session: Session) -> dict[str, Any]:
    try:
        listing_rows = db_session.execute(
            text(
                """
                select listing_source_id, coalesce(listing_status, 'unknown') as listing_status, count(*) as count
                from market_listing
                where is_active
                group by listing_source_id, coalesce(listing_status, 'unknown')
                """
            )
        ).mappings().all()
        refresh_rows = db_session.execute(
            text(
                """
                with ranked as (
                  select connector_key, source_id, status, row_count, accepted_count, completed_at, error_message,
                         row_number() over (partition by connector_key order by started_at desc) as rn
                  from source_refresh_job
                )
                select connector_key, source_id, status, row_count, accepted_count, completed_at, error_message
                from ranked
                where rn = 1
                """
            )
        ).mappings().all()
    except SQLAlchemyError:
        return {
            "sale_listing_count": 0,
            "lease_listing_count": 0,
            "total_listing_count": 0,
            "successful_refresh_count": 0,
            "failed_refresh_count": 0,
            "quarantined_refresh_count": 0,
            "latest_refresh_label": "Live status unavailable",
            "caveat_label": "Source health check unavailable",
        }

    sale_listing_count = sum(
        int(row["count"])
        for row in listing_rows
        if str(row["listing_status"]).lower() == "sale"
    )
    lease_listing_count = sum(
        int(row["count"])
        for row in listing_rows
        if str(row["listing_status"]).lower() == "lease"
    )
    successful_refreshes = [row for row in refresh_rows if row["status"] == "success"]
    failed_refreshes = [row for row in refresh_rows if row["status"] == "failed"]
    quarantined_refreshes = [row for row in refresh_rows if row["status"] == "quarantined"]
    latest_success = max(
        successful_refreshes,
        key=lambda row: row["completed_at"],
        default=None,
    )
    latest_refresh_label = "No successful pulls recorded"
    if latest_success is not None:
        latest_refresh_label = (
            f"{latest_success['connector_key']} refreshed "
            f"{_format_count(latest_success['accepted_count'])} records"
        )

    caveat_label = "All tracked live pulls are currently healthy"
    if failed_refreshes or quarantined_refreshes:
        caveat_label = (
            f"{len(failed_refreshes)} failed and {len(quarantined_refreshes)} quarantined "
            "tracked pull remain outside the ready set"
        )

    return {
        "sale_listing_count": sale_listing_count,
        "lease_listing_count": lease_listing_count,
        "total_listing_count": sale_listing_count + lease_listing_count,
        "successful_refresh_count": len(successful_refreshes),
        "failed_refresh_count": len(failed_refreshes),
        "quarantined_refresh_count": len(quarantined_refreshes),
        "latest_refresh_label": latest_refresh_label,
        "caveat_label": caveat_label,
    }


def _render_client_brief(brief: dict[str, Any], *, data_mode: str) -> str:
    mode_label = (
        "Live candidate scoring"
        if data_mode == "live_candidate_scoring"
        else "Seeded opportunity catalogue"
    )
    return f"""
      <article class="brief-card">
        <span>Scoring mode</span>
        <strong>{escape(mode_label)}</strong>
        <p>Client board is generated from live marketed sites plus public infrastructure and approval overlays.</p>
      </article>
      <article class="brief-card">
        <span>Market feed</span>
        <strong>{_format_count(brief["total_listing_count"])} listings</strong>
        <p>{_format_count(brief["sale_listing_count"])} sale and {_format_count(brief["lease_listing_count"])} lease records are active in the review universe.</p>
      </article>
      <article class="brief-card">
        <span>Latest successful pull</span>
        <strong>{escape(brief["latest_refresh_label"])}</strong>
        <p>{_format_count(brief["successful_refresh_count"])} tracked connector refreshes currently have successful latest runs.</p>
      </article>
      <article class="brief-card warning">
        <span>Review caveat</span>
        <strong>{escape(brief["caveat_label"])}</strong>
        <p>Known caveats stay visible here so the board reads as diligence-grade, not a black box.</p>
      </article>
    """


def _render_featured_cards(opportunities: Iterable[dict[str, Any]]) -> str:
    cards: list[str] = []
    for item in opportunities:
        detail_href = _contender_detail_href(item)
        cards.append(
            f"""
            <a class="featured-card" href="{escape(detail_href)}">
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
                  <span>Approval {item["approval_score"]} · {escape(item["approval_stage"])}</span>
                </div>
                <p>{escape(item["approval_summary"])}</p>
                <div class="strength-list">
                  {_render_strength_chips(item["strengths"])}
                </div>
              </div>
            </a>
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
                item["approval_stage"],
                item["social_category"],
                item["political_category"],
                " ".join(item.get("approval_headwinds", [])),
            ]
        )
        confidence_label = (
            f'{escape(item["score_band"])} · C{int(item["confidence_score"]):02d}'
            if item.get("confidence_score") is not None
            else escape(item["score_band"])
        )
        detail_href = _contender_detail_href(item)
        rows.append(
            f"""
            <tr
              class="opportunity-row"
              data-href="{escape(detail_href)}"
              data-metro="{escape(_tokenize(item["metro"]))}"
              data-stage="{escape(_tokenize(item["readiness_stage"]))}"
              data-university="{escape(_tokenize(item["university_anchor"]))}"
              data-search="{escape(search_blob.lower())}"
              role="link"
              tabindex="0"
              aria-label="Open contender detail for {escape(item["site_name"])}"
              onclick="if (!event.target.closest('a, button, input, select')) window.location.assign(this.dataset.href)"
              onkeydown="if ((event.key === 'Enter' || event.key === ' ') && !event.target.closest('a, button, input, select')) {{ event.preventDefault(); window.location.assign(this.dataset.href); }}"
            >
              <td><span class="rank-pill">{item["rank"]:02d}</span></td>
              <td>
                <div class="site-cell">
                  <a class="site-title-link" href="{escape(detail_href)}"><strong>{escape(item["site_name"])}</strong></a>
                  <span>{escape(item["city"])} · {escape(item["county"])}</span>
                  <small>{escape(item["corridor_name"])}</small>
                  <small><a href="{escape(detail_href)}">Open contender detail</a></small>
                </div>
              </td>
              <td>{escape(item["metro"])}</td>
              <td>{escape(item["university_anchor"])}</td>
              <td>{escape(item["acreage_band"])}</td>
              <td><span class="stage-chip">{escape(item["readiness_stage"])}</span></td>
              <td>
                <div class="score-cell">
                  <strong>{item["approval_score"]}</strong>
                  <small>{escape(item["approval_stage"])}</small>
                  <small>{escape(item["social_category"])} · {escape(item["political_category"])}</small>
                </div>
              </td>
              <td>
                <div class="score-cell">
                  <strong>{item["viability_score"]}</strong>
                  <small>{confidence_label}</small>
                </div>
              </td>
              <td>
                <div class="distance-cell">
                  <strong>{item["distance_to_city_miles"]} mi</strong>
                  <small>{item["distance_to_university_miles"]} mi to campus</small>
                  <a class="detail-button" href="{escape(detail_href)}">Open page</a>
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


def _build_map_metro_snapshots(
    opportunities: Iterable[dict[str, Any]],
    *,
    limit: int = 6,
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, float | int | str]] = {}
    for item in opportunities:
        metro = str(item["metro"])
        entry = grouped.setdefault(
            metro,
            {
                "metro": metro,
                "site_count": 0,
                "score_total": 0.0,
                "top_score": 0,
                "lat_total": 0.0,
                "lon_total": 0.0,
            },
        )
        entry["site_count"] = int(entry["site_count"]) + 1
        entry["score_total"] = float(entry["score_total"]) + float(item["viability_score"])
        entry["top_score"] = max(int(entry["top_score"]), int(item["viability_score"]))
        entry["lat_total"] = float(entry["lat_total"]) + float(item["lat"])
        entry["lon_total"] = float(entry["lon_total"]) + float(item["lon"])

    snapshots: list[dict[str, Any]] = []
    for metro, entry in grouped.items():
        site_count = int(entry["site_count"])
        average_score = round(float(entry["score_total"]) / max(site_count, 1), 1)
        tone = "tier-1" if average_score >= 72 else "tier-2" if average_score >= 68 else "tier-3"
        snapshots.append(
            {
                "metro": metro,
                "short_label": _METRO_SHORT_LABELS.get(metro, metro),
                "site_count": site_count,
                "average_score": average_score,
                "top_score": int(entry["top_score"]),
                "lat": float(entry["lat_total"]) / max(site_count, 1),
                "lon": float(entry["lon_total"]) / max(site_count, 1),
                "tone": tone,
            }
        )

    return sorted(
        snapshots,
        key=lambda item: (-item["site_count"], -item["average_score"], item["metro"]),
    )[:limit]


def _render_map_corridors(metro_snapshots: Iterable[dict[str, Any]]) -> str:
    by_metro = {item["metro"]: item for item in metro_snapshots}
    paths: list[str] = []
    for left_metro, right_metro in _FIELD_LINKS:
        left = by_metro.get(left_metro)
        right = by_metro.get(right_metro)
        if left is None or right is None:
            continue
        x1, y1 = _map_position(left["lat"], left["lon"])
        x2, y2 = _map_position(right["lat"], right["lon"])
        control_x = (x1 + x2) / 2
        control_y = max(8.0, min(y1, y2) - 7.5)
        paths.append(
            f"""
            <path
              class="map-beam"
              d="M{x1:.2f} {y1:.2f} Q {control_x:.2f} {control_y:.2f} {x2:.2f} {y2:.2f}"
            ></path>
            """
        )
    return "\n".join(paths)


def _render_map_hotspots(metro_snapshots: Iterable[dict[str, Any]]) -> str:
    hotspots: list[str] = []
    for index, item in enumerate(metro_snapshots):
        x, y = _map_position(item["lat"], item["lon"])
        radius = max(5.6, min(10.6, 5.0 + (item["site_count"] * 0.13)))
        pulse_radius = radius + 2.1
        label_anchor = "start"
        label_x = min(92.0, x + radius + 1.8)
        if x >= 66:
            label_anchor = "end"
            label_x = max(8.0, x - radius - 1.8)
        label_y = max(8.5, min(90.0, y - radius - 0.8))
        hotspots.append(
            f"""
            <g class="map-hotspot {escape(item["tone"])}" style="animation-delay: {index * 0.35:.2f}s;">
              <circle class="hotspot-pulse" cx="{x:.2f}" cy="{y:.2f}" r="{pulse_radius:.2f}"></circle>
              <circle class="hotspot-halo" cx="{x:.2f}" cy="{y:.2f}" r="{radius + 0.9:.2f}"></circle>
              <circle class="hotspot-core" cx="{x:.2f}" cy="{y:.2f}" r="{radius:.2f}"></circle>
              <text class="hotspot-count" x="{x:.2f}" y="{y + 1.1:.2f}">{item["site_count"]}</text>
              <path class="hotspot-link" d="M{x:.2f} {y - radius:.2f} L{label_x:.2f} {label_y + 1.1:.2f}"></path>
              <text class="hotspot-name" x="{label_x:.2f}" y="{label_y:.2f}" text-anchor="{label_anchor}">{escape(item["short_label"])}</text>
              <text class="hotspot-meta" x="{label_x:.2f}" y="{label_y + 4.2:.2f}" text-anchor="{label_anchor}">{item["average_score"]} avg</text>
            </g>
            """
        )
    return "\n".join(hotspots)


def _render_map_focus_cards(metro_snapshots: Iterable[dict[str, Any]]) -> str:
    cards: list[str] = []
    for item in metro_snapshots:
        cards.append(
            f"""
            <article class="map-focus-card tone-{escape(item["tone"])}">
              <span>{escape(item["metro"])}</span>
              <strong>{item["site_count"]} sites</strong>
              <small>{item["average_score"]} avg score · top {item["top_score"]}</small>
            </article>
            """
        )
    return "\n".join(cards)


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


def _render_contender_detail_page(
    *,
    settings: Settings,
    summary: dict[str, Any],
    contender: dict[str, Any],
    related: list[dict[str, Any]],
) -> str:
    x, y = _map_position(float(contender["lat"]), float(contender["lon"]))
    score_rows_html = _render_detail_score_rows(contender)
    headwind_chips_html = _render_headwind_chips(contender)
    positioning_html = _render_paragraph_list(_build_client_positioning(contender))
    top_ten_dossier_html = _render_top_ten_client_dossier(contender)
    decision_lens_html = _render_decision_lens(contender)
    next_steps_html = _render_action_list(_build_next_diligence_steps(contender))
    related_html = _render_related_contenders(related)
    source_link_html = (
        f'<a class="evidence-link" href="{escape(contender["source_url"])}" '
        'target="_blank" rel="noreferrer">Open source evidence</a>'
        if contender.get("source_url")
        else '<span class="evidence-link muted">No live listing link attached</span>'
    )
    confidence_label = (
        f'C{_score_value(contender, "confidence_score"):02d}'
        if contender.get("confidence_score") is not None
        else "Catalogue confidence"
    )
    listing_source = contender.get("listing_source_id") or "seeded-catalogue"
    source_key = contender.get("source_listing_key") or "Not attached"
    market_listing_id = contender.get("market_listing_id") or "Not attached"

    return f"""
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>{escape(contender["site_name"])} | Contender Detail</title>
        {_PREMIUM_FONT_LINKS}
        <style>
          :root {{
            color-scheme: light;
            --bg: #f6f1e8;
            --ink: #102438;
            --muted: #5f6f7f;
            --line: rgba(16, 36, 56, 0.12);
            --panel: rgba(255, 252, 246, 0.94);
            --panel-strong: rgba(255, 255, 255, 0.98);
            --teal: #0f766e;
            --navy: #153b5c;
            --gold: #c08124;
            --rust: #b45309;
            --danger: #b91c1c;
            --shadow: 0 28px 72px rgba(16, 36, 56, 0.15);
            font-family: "Aptos", "Segoe UI", sans-serif;
          }}

          * {{
            box-sizing: border-box;
          }}

          html {{
            scroll-behavior: smooth;
          }}

          body {{
            margin: 0;
            min-height: 100vh;
            color: var(--ink);
            background:
              linear-gradient(135deg, rgba(15, 118, 110, 0.14), transparent 34%),
              linear-gradient(225deg, rgba(180, 83, 9, 0.12), transparent 30%),
              linear-gradient(180deg, #fbf8f0 0%, var(--bg) 55%, #eadccb 100%);
            padding: 24px;
          }}

          a {{
            color: inherit;
          }}

          .page-shell {{
            width: min(1480px, 100%);
            margin: 0 auto;
            display: grid;
            gap: 18px;
          }}

          .top-nav,
          .detail-hero,
          .panel,
          .metric-card,
          .related-card {{
            background: var(--panel);
            border: 1px solid var(--line);
            box-shadow: var(--shadow);
            backdrop-filter: blur(14px);
          }}

          .top-nav {{
            position: sticky;
            top: 14px;
            z-index: 20;
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 12px;
            padding: 12px 14px;
            border-radius: 18px;
          }}

          .quick-nav {{
            position: sticky;
            top: 86px;
            z-index: 19;
            display: flex;
            gap: 8px;
            overflow-x: auto;
            padding: 8px;
            border-radius: 18px;
            background: rgba(255, 252, 246, 0.88);
            border: 1px solid var(--line);
            box-shadow: 0 16px 42px rgba(16, 36, 56, 0.1);
            backdrop-filter: blur(14px);
          }}

          .quick-nav a {{
            flex: 0 0 auto;
            padding: 9px 12px;
            border-radius: 999px;
            color: var(--muted);
            font-size: 0.86rem;
            font-weight: 800;
            text-decoration: none;
          }}

          .quick-nav a:hover,
          .quick-nav a:focus-visible {{
            color: white;
            background: linear-gradient(135deg, #124a61, #0f766e);
            outline: none;
          }}

          .top-nav a,
          .action-link {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-height: 42px;
            padding: 10px 14px;
            border-radius: 999px;
            border: 1px solid rgba(16, 36, 56, 0.12);
            background: rgba(255, 255, 255, 0.72);
            color: var(--ink);
            font-weight: 800;
            text-decoration: none;
          }}

          .action-link.primary {{
            background: linear-gradient(135deg, #124a61, #0f766e);
            color: white;
            border-color: transparent;
          }}

          .action-link.muted {{
            color: var(--muted);
          }}

          .hero-primary-action {{
            background: linear-gradient(135deg, #124a61, #0f766e);
            color: white;
            border-color: transparent;
          }}

          .evidence-link {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: fit-content;
            min-height: 38px;
            padding: 9px 12px;
            border-radius: 999px;
            border: 1px solid rgba(16, 36, 56, 0.12);
            background: rgba(255, 255, 255, 0.78);
            color: var(--teal);
            font-weight: 800;
            text-decoration: none;
          }}

          .evidence-link.muted {{
            color: var(--muted);
          }}

          .detail-hero {{
            display: grid;
            grid-template-columns: minmax(0, 1.05fr) minmax(340px, 0.95fr);
            gap: 22px;
            padding: 26px;
            border-radius: 28px;
            overflow: hidden;
          }}

          .hero-kicker-grid {{
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 10px;
            margin-top: 22px;
          }}

          .hero-kicker {{
            min-height: 94px;
            padding: 13px;
            border-radius: 16px;
            background: rgba(255, 255, 255, 0.74);
            border: 1px solid rgba(16, 36, 56, 0.08);
          }}

          .hero-kicker span {{
            display: block;
            color: var(--muted);
            font-size: 0.74rem;
            font-weight: 800;
            letter-spacing: 0.09em;
            text-transform: uppercase;
          }}

          .hero-kicker strong {{
            display: block;
            margin-top: 7px;
            font-size: 1.1rem;
            line-height: 1.25;
          }}

          .eyebrow {{
            display: inline-flex;
            align-items: center;
            gap: 10px;
            color: var(--teal);
            font-size: 0.74rem;
            font-weight: 800;
            letter-spacing: 0.14em;
            text-transform: uppercase;
          }}

          .eyebrow::before {{
            content: "";
            width: 34px;
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

          h1 {{
            margin-top: 16px;
            max-width: 13ch;
            font-size: clamp(2.55rem, 5vw, 5.2rem);
            line-height: 0.95;
          }}

          .hero-copy p,
          .panel p,
          .evidence-row span,
          .score-copy,
          .empty-note {{
            color: var(--muted);
            line-height: 1.65;
          }}

          .hero-copy p {{
            max-width: 66ch;
            margin: 18px 0 0;
            font-size: 1.04rem;
          }}

          .hero-actions,
          .hero-meta,
          .headwind-list,
          .footer-links {{
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
          }}

          .hero-actions {{
            margin-top: 24px;
          }}

          .hero-meta {{
            margin-top: 18px;
          }}

          .meta-pill,
          .headwind-chip {{
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 8px 12px;
            border-radius: 999px;
            background: rgba(255, 255, 255, 0.74);
            border: 1px solid rgba(16, 36, 56, 0.1);
            color: var(--muted);
            font-size: 0.9rem;
            font-weight: 700;
          }}

          .headwind-chip {{
            color: var(--rust);
            background: rgba(180, 83, 9, 0.09);
          }}

          .headwind-chip.is-clear {{
            color: var(--teal);
            background: rgba(15, 118, 110, 0.1);
          }}

          .visual-panel {{
            display: grid;
            gap: 14px;
          }}

          .score-orbit {{
            min-height: 236px;
            border-radius: 24px;
            padding: 22px;
            color: white;
            background:
              linear-gradient(135deg, rgba(16, 36, 56, 0.95), rgba(15, 118, 110, 0.78)),
              #102438;
            display: grid;
            grid-template-columns: 180px minmax(0, 1fr);
            gap: 20px;
            align-items: center;
          }}

          .score-ring {{
            width: 170px;
            height: 170px;
            border-radius: 50%;
            display: grid;
            place-items: center;
            background:
              conic-gradient(var(--gold) 0 {max(1, min(100, int(contender["viability_score"])))}%, rgba(255, 255, 255, 0.18) 0),
              rgba(255, 255, 255, 0.08);
            box-shadow: inset 0 0 0 16px rgba(16, 36, 56, 0.45);
          }}

          .score-ring strong {{
            font-size: 3.8rem;
            line-height: 1;
          }}

          .score-ring span {{
            display: block;
            font-size: 0.75rem;
            letter-spacing: 0.1em;
            text-transform: uppercase;
            text-align: center;
          }}

          .score-copy h2 {{
            color: white;
          }}

          .score-copy p {{
            margin: 10px 0 0;
            color: rgba(255, 255, 255, 0.78);
          }}

          .mini-map {{
            min-height: 250px;
            border-radius: 24px;
            overflow: hidden;
            border: 1px solid rgba(16, 36, 56, 0.08);
            background:
              linear-gradient(180deg, rgba(21, 59, 92, 0.08), rgba(255, 255, 255, 0.14)),
              #f7efdf;
          }}

          .mini-map svg {{
            width: 100%;
            height: 100%;
            min-height: 250px;
            display: block;
          }}

          .map-outline {{
            fill: rgba(21, 59, 92, 0.08);
            stroke: rgba(21, 59, 92, 0.32);
            stroke-width: 1.8;
          }}

          .map-grid {{
            stroke: rgba(21, 59, 92, 0.1);
            stroke-dasharray: 3 6;
          }}

          .map-point {{
            fill: var(--teal);
            stroke: white;
            stroke-width: 1.4;
          }}

          .map-halo {{
            fill: rgba(15, 118, 110, 0.16);
            stroke: rgba(15, 118, 110, 0.24);
          }}

          .metric-grid {{
            display: grid;
            grid-template-columns: repeat(5, minmax(0, 1fr));
            gap: 14px;
          }}

          .decision-strip {{
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 14px;
          }}

          .decision-card {{
            min-height: 168px;
            padding: 18px;
            border-radius: 20px;
            background: var(--panel);
            border: 1px solid var(--line);
            box-shadow: 0 18px 48px rgba(16, 36, 56, 0.11);
            display: grid;
            align-content: start;
            gap: 9px;
          }}

          .decision-card span {{
            color: var(--teal);
            font-size: 0.74rem;
            font-weight: 800;
            letter-spacing: 0.1em;
            text-transform: uppercase;
          }}

          .decision-card strong {{
            font-size: 1.18rem;
            line-height: 1.28;
          }}

          .decision-card p {{
            margin: 0;
            color: var(--muted);
            line-height: 1.55;
            font-size: 0.94rem;
          }}

          .metric-card {{
            min-height: 132px;
            padding: 18px;
            border-radius: 22px;
            display: grid;
            align-content: start;
            gap: 8px;
          }}

          .metric-card span {{
            color: var(--muted);
            font-size: 0.82rem;
            font-weight: 800;
            letter-spacing: 0.08em;
            text-transform: uppercase;
          }}

          .metric-card strong {{
            font-size: 2rem;
            line-height: 1.05;
            color: var(--navy);
          }}

          .metric-card small {{
            color: var(--muted);
            line-height: 1.45;
          }}

          .content-grid {{
            display: grid;
            grid-template-columns: minmax(0, 1.08fr) minmax(320px, 0.92fr);
            gap: 18px;
          }}

          .panel {{
            padding: 24px;
            border-radius: 24px;
          }}

          .panel-header {{
            display: flex;
            justify-content: space-between;
            align-items: end;
            gap: 14px;
            margin-bottom: 16px;
          }}

          .panel-header p {{
            margin: 8px 0 0;
          }}

          .explanation-body {{
            display: grid;
            gap: 10px;
          }}

          .explanation-body p {{
            margin: 0;
          }}

          .dossier-panel {{
            background:
              linear-gradient(135deg, rgba(21, 59, 92, 0.95), rgba(15, 118, 110, 0.78)),
              var(--navy);
            color: white;
          }}

          .dossier-panel .eyebrow,
          .dossier-panel p {{
            color: rgba(255, 255, 255, 0.78);
          }}

          .dossier-panel h2,
          .dossier-panel strong {{
            color: white;
          }}

          .memo-body {{
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 14px;
          }}

          .memo-body p {{
            margin: 0;
            padding: 16px;
            border-radius: 18px;
            background: rgba(255, 255, 255, 0.1);
            border: 1px solid rgba(255, 255, 255, 0.14);
            color: rgba(255, 255, 255, 0.84);
          }}

          .dossier-grid,
          .risk-grid,
          .timeline-grid {{
            display: grid;
            gap: 14px;
          }}

          .dossier-grid {{
            grid-template-columns: repeat(4, minmax(0, 1fr));
          }}

          .risk-grid {{
            grid-template-columns: repeat(2, minmax(0, 1fr));
          }}

          .timeline-grid {{
            grid-template-columns: repeat(4, minmax(0, 1fr));
          }}

          .dossier-card,
          .risk-card,
          .timeline-step {{
            min-height: 178px;
            padding: 18px;
            border-radius: 18px;
            background: rgba(255, 255, 255, 0.74);
            border: 1px solid rgba(16, 36, 56, 0.08);
            display: grid;
            align-content: start;
            gap: 10px;
          }}

          .dossier-card span,
          .risk-card span,
          .timeline-step span {{
            color: var(--teal);
            font-size: 0.74rem;
            font-weight: 800;
            letter-spacing: 0.1em;
            text-transform: uppercase;
          }}

          .dossier-card strong,
          .risk-card strong,
          .timeline-step strong {{
            font-size: 1.05rem;
            line-height: 1.35;
          }}

          .dossier-card p,
          .risk-card p,
          .timeline-step p {{
            margin: 0;
            color: var(--muted);
            line-height: 1.6;
          }}

          .risk-card span {{
            color: var(--rust);
          }}

          .timeline-step span {{
            color: var(--navy);
          }}

          .question-list {{
            margin: 0;
            padding-left: 22px;
            display: grid;
            gap: 12px;
            color: var(--muted);
            line-height: 1.6;
          }}

          .section-anchor {{
            scroll-margin-top: 160px;
          }}

          .score-stack {{
            display: grid;
            gap: 12px;
          }}

          .detail-score-row {{
            display: grid;
            grid-template-columns: minmax(220px, 1fr) minmax(160px, 0.48fr) 46px;
            gap: 14px;
            align-items: center;
            padding: 14px;
            border-radius: 18px;
            background: rgba(255, 255, 255, 0.72);
            border: 1px solid rgba(16, 36, 56, 0.08);
          }}

          .detail-score-row strong {{
            display: block;
            margin-bottom: 4px;
          }}

          .detail-score-row span {{
            color: var(--muted);
            line-height: 1.5;
            font-size: 0.92rem;
          }}

          .detail-score-row b {{
            font-size: 1.45rem;
            color: var(--navy);
            text-align: right;
          }}

          .score-meter {{
            height: 11px;
            border-radius: 999px;
            background: rgba(16, 36, 56, 0.08);
            overflow: hidden;
          }}

          .score-meter i {{
            display: block;
            height: 100%;
            border-radius: inherit;
            background: linear-gradient(90deg, var(--teal), var(--gold));
          }}

          .evidence-grid {{
            display: grid;
            gap: 12px;
          }}

          .evidence-row {{
            display: grid;
            gap: 4px;
            padding: 14px;
            border-radius: 18px;
            background: rgba(255, 255, 255, 0.72);
            border: 1px solid rgba(16, 36, 56, 0.08);
          }}

          .evidence-row strong {{
            font-size: 1rem;
          }}

          .diligence-list {{
            margin: 0;
            padding-left: 22px;
            display: grid;
            gap: 10px;
            color: var(--muted);
            line-height: 1.6;
          }}

          .related-grid {{
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 12px;
          }}

          .related-card {{
            display: grid;
            gap: 8px;
            min-height: 136px;
            padding: 16px;
            border-radius: 18px;
            color: inherit;
            text-decoration: none;
            transition: transform 160ms ease, border-color 160ms ease, box-shadow 160ms ease;
          }}

          .related-card:hover,
          .related-card:focus-visible {{
            transform: translateY(-2px);
            border-color: rgba(15, 118, 110, 0.32);
            box-shadow: 0 18px 42px rgba(16, 36, 56, 0.14);
          }}

          .related-card span,
          .related-card small {{
            color: var(--muted);
            line-height: 1.45;
          }}

          .footer-bar {{
            display: flex;
            justify-content: space-between;
            flex-wrap: wrap;
            gap: 12px;
            padding: 0 4px 8px;
            color: var(--muted);
            font-size: 0.92rem;
          }}

          .footer-links a {{
            color: inherit;
            text-decoration: none;
          }}

          @media (max-width: 1120px) {{
            .detail-hero,
            .content-grid,
            .memo-body,
            .metric-grid,
            .decision-strip,
            .dossier-grid,
            .risk-grid,
            .timeline-grid,
            .hero-kicker-grid,
            .related-grid {{
              grid-template-columns: 1fr;
            }}

            .quick-nav {{
              top: 74px;
            }}

            .score-orbit {{
              grid-template-columns: 1fr;
            }}

            .detail-score-row {{
              grid-template-columns: 1fr;
            }}

            .detail-score-row b {{
              text-align: left;
            }}
          }}

          @media (max-width: 680px) {{
            body {{
              padding: 14px;
            }}

            .detail-hero,
            .panel {{
              padding: 18px;
              border-radius: 20px;
            }}

            h1 {{
              font-size: 2.4rem;
            }}

            .top-nav {{
              position: static;
              align-items: start;
              flex-direction: column;
            }}

            .quick-nav {{
              position: static;
            }}

            .score-ring {{
              width: 148px;
              height: 148px;
            }}
          }}
{_PREMIUM_SHARED_CSS}
        </style>
      </head>
      <body>
        <div class="page-shell">
          <nav class="top-nav" aria-label="Contender detail navigation">
            <a href="/dashboard/contenders">Back to contender board</a>
            <div class="footer-links">
              <a href="/">Main portal</a>
              <a href="/dashboard/summary">Live JSON</a>
            </div>
          </nav>

          <nav class="quick-nav" aria-label="Contender detail sections">
            <a href="#snapshot">Snapshot</a>
            <a href="#decision">Decision Lens</a>
            <a href="#dossier">Dossier</a>
            <a href="#thesis">Thesis</a>
            <a href="#risk">Risks</a>
            <a href="#evidence">Evidence</a>
            <a href="#scoring">Scoring</a>
            <a href="#timeline">Workplan</a>
            <a href="#comparisons">Comparisons</a>
          </nav>

          <section class="detail-hero section-anchor" id="snapshot">
            <div class="hero-copy">
              <span class="eyebrow">Contender detail &middot; rank {contender["rank"]:02d}</span>
              <h1>{escape(contender["site_name"])}</h1>
              <p>{escape(contender["summary"])}</p>
              <div class="hero-actions">
                <a class="action-link hero-primary-action" href="#decision">Review decision lens</a>
                <a class="action-link" href="/dashboard/contenders">Compare board</a>
              </div>
              <div class="hero-meta">
                <span class="meta-pill">{escape(contender["metro"])}</span>
                <span class="meta-pill">{escape(contender["city"])} &middot; {escape(contender["county"])}</span>
                <span class="meta-pill">{escape(contender["readiness_stage"])}</span>
                <span class="meta-pill">{escape(confidence_label)}</span>
              </div>
              <div class="hero-kicker-grid" aria-label="Client summary">
                <article class="hero-kicker">
                  <span>Client Read</span>
                  <strong>{escape(contender["score_band"])} &middot; {escape(contender["readiness_stage"])}</strong>
                </article>
                <article class="hero-kicker">
                  <span>Primary Market</span>
                  <strong>{escape(contender["metro"])}</strong>
                </article>
                <article class="hero-kicker">
                  <span>Anchor</span>
                  <strong>{escape(contender["university_anchor"])}</strong>
                </article>
              </div>
            </div>

            <div class="visual-panel">
              <div class="score-orbit">
                <div class="score-ring" aria-label="Viability score {contender["viability_score"]}">
                  <div>
                    <span>Score</span>
                    <strong>{contender["viability_score"]}</strong>
                  </div>
                </div>
                <div class="score-copy">
                  <h2>{escape(contender["score_band"])} candidate</h2>
                  <p>{escape(contender["approval_stage"])} with {contender["approval_score"]} approval score and {escape(contender["university_anchor"])} as the talent anchor.</p>
                </div>
              </div>
              <div class="mini-map">
                <svg viewBox="0 0 100 100" role="img" aria-label="Texas map location for {escape(contender["site_name"])}">
                  <path class="map-outline" d="M26 8 L43 11 L49 21 L63 21 L72 31 L80 31 L88 38 L84 49 L91 59 L86 83 L71 86 L56 94 L42 82 L37 67 L26 61 L22 46 L11 40 L15 24 L24 18 Z"></path>
                  <path class="map-grid" d="M12 28 H88 M16 46 H84 M22 64 H80 M30 20 V82 M48 16 V90 M66 22 V88"></path>
                  <circle class="map-halo" cx="{x:.2f}" cy="{y:.2f}" r="9"></circle>
                  <circle class="map-point" cx="{x:.2f}" cy="{y:.2f}" r="4.8"></circle>
                  <text x="{min(88, x + 2.6):.2f}" y="{max(10, y - 2.0):.2f}" fill="#153b5c" font-size="3.2" font-weight="800">{escape(contender["city"])}</text>
                </svg>
              </div>
            </div>
          </section>

          <section class="metric-grid section-anchor" aria-label="Contender snapshot">
            <article class="metric-card">
              <span>Viability</span>
              <strong>{contender["viability_score"]}</strong>
              <small>{escape(contender["score_band"])} across the current Texas board.</small>
            </article>
            <article class="metric-card">
              <span>Approval</span>
              <strong>{contender["approval_score"]}</strong>
              <small>{escape(contender["approval_stage"])}</small>
            </article>
            <article class="metric-card">
              <span>Land Scale</span>
              <strong>{escape(contender["acreage_band"])}</strong>
              <small>Current marketed or modelled tract signal.</small>
            </article>
            <article class="metric-card">
              <span>City Access</span>
              <strong>{contender["distance_to_city_miles"]} mi</strong>
              <small>{contender["distance_to_university_miles"]} mi to university anchor.</small>
            </article>
            <article class="metric-card">
              <span>Asking Price</span>
              <strong>{escape(_format_money(contender.get("asking_price")))}</strong>
              <small>{escape(_format_status_label(contender.get("listing_status")))} status.</small>
            </article>
          </section>

          <section class="decision-strip section-anchor" id="decision" aria-label="Decision lens">
            {decision_lens_html}
          </section>

          {top_ten_dossier_html}

          <section class="content-grid section-anchor" id="explanation">
            <section class="panel">
              <div class="panel-header">
                <div>
                  <span class="eyebrow">Comprehensive explanation</span>
                  <h2>Why this contender belongs in the client conversation</h2>
                  <p>Plain-language positioning for principals, site selectors, and diligence teams.</p>
                </div>
              </div>
              <div class="explanation-body">
                {positioning_html}
              </div>
            </section>

            <aside class="panel section-anchor" id="evidence">
              <div class="panel-header">
                <div>
                  <h2>Market Evidence</h2>
                  <p>Traceable listing and board context attached to this contender.</p>
                </div>
              </div>
              <div class="evidence-grid">
                <div class="evidence-row">
                  <strong>Listing source</strong>
                  <span>{escape(str(listing_source))}</span>
                </div>
                <div class="evidence-row">
                  <strong>Source listing key</strong>
                  <span>{escape(str(source_key))}</span>
                </div>
                <div class="evidence-row">
                  <strong>Market listing id</strong>
                  <span>{escape(str(market_listing_id))}</span>
                </div>
                <div class="evidence-row">
                  <strong>Corridor</strong>
                  <span>{escape(contender["corridor_name"])}</span>
                </div>
                <div class="evidence-row">
                  <strong>Source evidence</strong>
                  {source_link_html}
                </div>
              </div>
            </aside>
          </section>

          <section class="content-grid section-anchor" id="scoring">
            <section class="panel">
              <div class="panel-header">
                <div>
                  <h2>Scoring Anatomy</h2>
                  <p>The factor stack behind the ranking, shown as client-readable diligence signals.</p>
                </div>
              </div>
              <div class="score-stack">
                {score_rows_html}
              </div>
            </section>

            <section class="panel section-anchor" id="approval">
              <div class="panel-header">
                <div>
                  <h2>Approval Path</h2>
                  <p>{escape(contender["approval_summary"])}</p>
                </div>
              </div>
              <div class="metric-grid" style="grid-template-columns: repeat(2, minmax(0, 1fr)); margin-bottom: 16px;">
                <article class="metric-card">
                  <span>Social</span>
                  <strong>{contender["social_score"]}</strong>
                  <small>{escape(contender["social_category"])}</small>
                </article>
                <article class="metric-card">
                  <span>Political</span>
                  <strong>{contender["political_score"]}</strong>
                  <small>{escape(contender["political_category"])}</small>
                </article>
              </div>
              <div class="headwind-list">
                {headwind_chips_html}
              </div>
            </section>
          </section>

          <section class="panel section-anchor" id="workplan">
            <div class="panel-header">
              <div>
                <h2>Recommended Diligence Path</h2>
                <p>The immediate work plan before this moves from attractive contender to client-ready pursuit.</p>
              </div>
              <strong>{escape(contender["readiness_stage"])}</strong>
            </div>
            <ol class="diligence-list">
              {next_steps_html}
            </ol>
          </section>

          <section class="panel section-anchor" id="comparisons">
            <div class="panel-header">
              <div>
                <h2>Nearby Board Comparisons</h2>
                <p>Related contenders in the same metro or corridor for fast client alternatives.</p>
              </div>
              <strong>{escape(summary["market"])}</strong>
            </div>
            <div class="related-grid">
              {related_html}
            </div>
          </section>

          <footer class="footer-bar">
            <span>{escape(settings.app_name)} &middot; version {escape(APP_VERSION)} &middot; generated {escape(summary["generated_at"])}</span>
            <div class="footer-links">
              <a href="/dashboard/contenders">Contenders</a>
              <a href="/dashboard/summary">Live JSON</a>
              <a href="/health">Health</a>
            </div>
          </footer>
        </div>
      </body>
    </html>
    """


@router.get("/dashboard/summary", response_model=TexasDashboardSummaryResponse)
def dashboard_summary(_: DashboardAccess, db: DbSession) -> TexasDashboardSummaryResponse:
    settings = get_settings()
    summary = _build_dashboard_summary(settings, db)
    return TexasDashboardSummaryResponse.model_validate(summary)


@router.get("/dashboard/contenders/{site_id:path}", response_class=HTMLResponse)
def contender_detail_page(site_id: str, _: DashboardAccess, db: DbSession) -> str:
    settings = get_settings()
    summary = _build_dashboard_summary(settings, db)
    contenders = list(summary["opportunities"])
    contender = _find_contender(contenders, site_id)
    if contender is None:
        raise HTTPException(status_code=404, detail="Contender not found")
    return _render_contender_detail_page(
        settings=settings,
        summary=summary,
        contender=contender,
        related=_related_contenders(contenders, contender),
    )


@router.get("/dashboard/contenders", response_class=HTMLResponse)
def contenders_page(_: DashboardAccess, db: DbSession) -> str:
    settings = get_settings()
    summary = _build_dashboard_summary(settings, db)
    contenders = list(summary["opportunities"])
    featured = contenders[:8]

    contender_count = len(contenders)
    average_score = round(
        mean(item["viability_score"] for item in contenders),
        1,
    ) if contenders else 0.0
    top_score = max((item["viability_score"] for item in contenders), default=0)
    priority_now_count = sum(
        1 for item in contenders if item["readiness_stage"] == "Priority now"
    )
    near_term_count = sum(
        1 for item in contenders if item["readiness_stage"] == "Near-term build"
    )
    metro_count = len({item["metro"] for item in contenders})
    client_brief = _read_client_readiness_brief(db)

    featured_html = _render_featured_cards(featured)
    client_brief_html = _render_client_brief(
        client_brief,
        data_mode=str(summary["data_mode"]),
    )
    opportunity_rows_html = _render_opportunity_rows(contenders)
    map_points_html = _render_map_points(contenders)
    map_labels_html = _render_map_labels(featured)
    metro_options_html = _render_filter_options(summary["filters"]["metros"])
    stage_options_html = _render_filter_options(summary["filters"]["readiness_stages"])
    university_options_html = _render_filter_options(summary["filters"]["university_anchors"])

    return f"""
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Texas Top {contender_count} Contenders</title>
        {_PREMIUM_FONT_LINKS}
        <style>
          :root {{
            color-scheme: light;
            --bg: #f4efe4;
            --panel: rgba(255, 251, 244, 0.92);
            --panel-strong: rgba(255, 253, 249, 0.98);
            --ink: #102438;
            --muted: #5f6f7f;
            --line: rgba(16, 36, 56, 0.12);
            --shadow: 0 28px 80px rgba(16, 36, 56, 0.14);
            --teal: #0f766e;
            --navy: #153b5c;
            --gold: #c08124;
            --rust: #b45309;
            --tier1: #0f766e;
            --tier2: #c08124;
            --tier3: #64748b;
            font-family: "Aptos", "Segoe UI", sans-serif;
          }}

          * {{
            box-sizing: border-box;
          }}

          body {{
            margin: 0;
            min-height: 100vh;
            color: var(--ink);
            background:
              radial-gradient(circle at top left, rgba(15, 118, 110, 0.18), transparent 24%),
              radial-gradient(circle at top right, rgba(180, 83, 9, 0.14), transparent 20%),
              linear-gradient(180deg, #fbf8f0 0%, #f4efe4 52%, #ece0cf 100%);
            padding: 26px;
          }}

          .page-shell {{
            width: min(1500px, 100%);
            margin: 0 auto;
            display: grid;
            gap: 20px;
          }}

          .hero,
          .panel,
          .stat-card {{
            background: var(--panel);
            border: 1px solid var(--line);
            border-radius: 28px;
            box-shadow: var(--shadow);
            backdrop-filter: blur(14px);
          }}

          .hero {{
            display: grid;
            grid-template-columns: minmax(0, 1.02fr) minmax(320px, 0.98fr);
            gap: 20px;
            padding: 28px;
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
            width: 34px;
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
            max-width: 11ch;
            font-size: clamp(2.8rem, 4.8vw, 4.8rem);
            line-height: 0.95;
          }}

          .hero-copy p {{
            margin: 18px 0 0;
            max-width: 60ch;
            color: var(--muted);
            font-size: 1.04rem;
            line-height: 1.7;
          }}

          .hero-actions,
          .hero-meta,
          .map-legend,
          .filters,
          .footer-links,
          .featured-meta,
          .catalog-tools {{
            display: flex;
            flex-wrap: wrap;
            gap: 12px;
          }}

          .hero-actions {{
            margin-top: 24px;
          }}

          .hero-link {{
            padding: 12px 18px;
            border-radius: 999px;
            border: 1px solid rgba(16, 36, 56, 0.12);
            font-weight: 700;
            background: rgba(255, 255, 255, 0.68);
            color: inherit;
            text-decoration: none;
          }}

          .hero-link.primary {{
            background: linear-gradient(135deg, #124a61, #0f766e);
            color: white;
            border-color: transparent;
          }}

          .hero-meta {{
            margin-top: 20px;
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

          .map-panel {{
            display: grid;
            gap: 14px;
          }}

          .map-panel header {{
            display: flex;
            justify-content: space-between;
            align-items: baseline;
            gap: 12px;
          }}

          .map-panel header span {{
            color: var(--muted);
            font-size: 0.92rem;
          }}

          .map-stage {{
            width: 100%;
            min-height: 410px;
            border-radius: 24px;
            overflow: hidden;
            background:
              radial-gradient(circle at 18% 24%, rgba(15, 118, 110, 0.14), transparent 18%),
              radial-gradient(circle at 78% 70%, rgba(180, 83, 9, 0.12), transparent 20%),
              linear-gradient(180deg, rgba(21, 59, 92, 0.08), rgba(255, 255, 255, 0.12)),
              #f6efdf;
            border: 1px solid rgba(16, 36, 56, 0.08);
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

          .stat-grid {{
            display: grid;
            grid-template-columns: repeat(5, minmax(0, 1fr));
            gap: 16px;
          }}

          .stat-card {{
            padding: 22px;
            display: grid;
            gap: 10px;
          }}

          .stat-card span {{
            color: var(--muted);
            font-size: 0.9rem;
          }}

          .stat-card strong {{
            font-size: 2.25rem;
            line-height: 1;
            color: var(--navy);
          }}

          .stat-card p {{
            margin: 0;
            color: var(--muted);
            line-height: 1.6;
            font-size: 0.94rem;
          }}

          .client-brief {{
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 16px;
          }}

          .brief-card {{
            min-height: 168px;
            padding: 20px;
            border-radius: 22px;
            border: 1px solid rgba(16, 36, 56, 0.1);
            background: rgba(255, 255, 255, 0.72);
            display: grid;
            align-content: start;
            gap: 10px;
          }}

          .brief-card span {{
            color: var(--teal);
            font-size: 0.76rem;
            font-weight: 800;
            letter-spacing: 0.08em;
            text-transform: uppercase;
          }}

          .brief-card strong {{
            font-size: 1.08rem;
            line-height: 1.35;
          }}

          .brief-card p {{
            margin: 0;
            color: var(--muted);
            font-size: 0.94rem;
            line-height: 1.6;
          }}

          .brief-card.warning span {{
            color: var(--rust);
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
            background: rgba(255, 255, 255, 0.72);
            color: inherit;
            text-decoration: none;
            transition: transform 160ms ease, border-color 160ms ease, box-shadow 160ms ease;
          }}

          .featured-card:hover,
          .featured-card:focus-visible {{
            transform: translateY(-2px);
            border-color: rgba(15, 118, 110, 0.32);
            box-shadow: 0 18px 42px rgba(16, 36, 56, 0.14);
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

          .strength-list {{
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            margin-top: 14px;
          }}

          .strength-chip,
          .stage-chip,
          .rank-pill {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            border-radius: 999px;
            font-weight: 700;
          }}

          .strength-chip {{
            padding: 7px 10px;
            background: rgba(15, 118, 110, 0.1);
            color: var(--teal);
            font-size: 0.78rem;
          }}

          .catalog-header {{
            display: flex;
            justify-content: space-between;
            align-items: end;
            gap: 16px;
            margin-bottom: 18px;
          }}

          .catalog-tools {{
            align-items: center;
            justify-content: space-between;
            margin-bottom: 16px;
          }}

          .catalog-tools strong {{
            font-size: 0.95rem;
          }}

          .filters {{
            justify-content: end;
          }}

          .filters input,
          .filters select {{
            min-width: 180px;
            padding: 11px 14px;
            border-radius: 14px;
            border: 1px solid rgba(16, 36, 56, 0.12);
            background: rgba(255, 255, 255, 0.84);
            color: var(--ink);
            font: inherit;
          }}

          .table-shell {{
            overflow: auto;
            border-radius: 22px;
            border: 1px solid rgba(16, 36, 56, 0.08);
            background: var(--panel-strong);
          }}

          table {{
            width: 100%;
            border-collapse: collapse;
            min-width: 980px;
          }}

          th,
          td {{
            padding: 16px 14px;
            text-align: left;
            border-bottom: 1px solid rgba(16, 36, 56, 0.08);
            vertical-align: top;
          }}

          thead th {{
            position: sticky;
            top: 0;
            background: rgba(248, 242, 232, 0.96);
            z-index: 1;
            font-size: 0.82rem;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: var(--muted);
          }}

          .rank-pill {{
            width: 40px;
            height: 40px;
            background: rgba(21, 59, 92, 0.1);
            color: var(--navy);
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

          .site-cell a {{
            color: var(--teal);
            text-decoration: none;
          }}

          .site-title-link {{
            color: var(--ink) !important;
          }}

          .opportunity-row {{
            cursor: pointer;
            transition: background 140ms ease;
          }}

          .opportunity-row:hover,
          .opportunity-row:focus-visible {{
            background: rgba(15, 118, 110, 0.07);
            outline: none;
          }}

          .stage-chip {{
            padding: 8px 11px;
            background: rgba(15, 118, 110, 0.1);
            color: var(--teal);
            font-size: 0.8rem;
          }}

          .detail-button {{
            width: fit-content;
            margin-top: 4px;
            padding: 8px 11px;
            border-radius: 999px;
            background: linear-gradient(135deg, #124a61, #0f766e);
            color: white;
            font-size: 0.8rem;
            font-weight: 800;
            text-decoration: none;
          }}

          .score-cell strong {{
            font-size: 1.2rem;
          }}

          .footer-bar {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 12px;
            padding: 0 4px 8px;
            color: var(--muted);
            font-size: 0.92rem;
          }}

          .footer-links a {{
            color: inherit;
            text-decoration: none;
          }}

          @media (max-width: 1100px) {{
            .hero,
            .client-brief,
            .featured-grid,
            .stat-grid {{
              grid-template-columns: 1fr;
            }}

            .catalog-header,
            .catalog-tools {{
              align-items: start;
              flex-direction: column;
            }}

            .filters {{
              width: 100%;
              justify-content: start;
            }}

            .filters input,
            .filters select {{
              width: 100%;
            }}
          }}
{_PREMIUM_SHARED_CSS}
        </style>
      </head>
      <body>
        <div class="page-shell">
          <section class="hero">
            <div class="hero-copy">
              <span class="eyebrow">Private contender board</span>
              <h1>Texas Top {contender_count} Contenders</h1>
              <p>The dedicated contender view isolates the strongest current Texas data-center prospects into one board for faster client review, with deeper major-metro coverage instead of a narrow top-50 clip.</p>
              <div class="hero-actions">
                <a class="hero-link primary" href="#contender-catalogue">Open contender board</a>
                <a class="hero-link" href="/">Main portal</a>
                <a class="hero-link" href="/dashboard/summary">Live JSON</a>
              </div>
              <div class="hero-meta">
                <span class="meta-pill">Mode: {escape(summary["data_mode"])}</span>
                <span class="meta-pill">Generated: {escape(summary["generated_at"])}</span>
                <span class="meta-pill">{contender_count} contenders</span>
              </div>
            </div>
            <div class="map-panel">
              <header>
                <div>
                  <h2>Contender Field</h2>
                  <span>The balanced live board with minimum coverage across the major Texas metros.</span>
                </div>
                <strong>{contender_count} sites</strong>
              </header>
              <div class="map-stage">
                <svg viewBox="0 0 100 100" role="img" aria-label="Texas contender field">
                  <path class="map-outline" d="M26 8 L43 11 L49 21 L63 21 L72 31 L80 31 L88 38 L84 49 L91 59 L86 83 L71 86 L56 94 L42 82 L37 67 L26 61 L22 46 L11 40 L15 24 L24 18 Z"></path>
                  <path class="map-grid" d="M12 28 H88 M16 46 H84 M22 64 H80 M30 20 V82 M48 16 V90 M66 22 V88"></path>
                  {map_points_html}
                  {map_labels_html}
                </svg>
              </div>
              <div class="map-legend">
                <span><i style="background: var(--tier1)"></i> Tier 1</span>
                <span><i style="background: var(--tier2)"></i> Tier 2</span>
                <span><i style="background: var(--tier3)"></i> Tier 3</span>
              </div>
            </div>
          </section>

          <section class="stat-grid">
            <article class="stat-card">
              <span>Contenders</span>
              <strong>{contender_count}</strong>
              <p>The current board size presented to clients.</p>
            </article>
            <article class="stat-card">
              <span>Top score</span>
              <strong>{top_score}</strong>
              <p>Highest current viability score on the board.</p>
            </article>
            <article class="stat-card">
              <span>Average score</span>
              <strong>{average_score}</strong>
              <p>Average viability across the current contender set.</p>
            </article>
            <article class="stat-card">
              <span>Priority now</span>
              <strong>{priority_now_count}</strong>
              <p>Sites already sitting in the immediate action band.</p>
            </article>
            <article class="stat-card">
              <span>Metros covered</span>
              <strong>{metro_count}</strong>
              <p>Distinct Texas metros represented in the balanced contender board.</p>
            </article>
          </section>

          <section class="panel">
            <div class="panel-header">
              <div>
                <h2>Client Readiness Brief</h2>
                <p>Fast context for what is live, what changed, and what still needs diligence before a client conversation.</p>
              </div>
              <strong>Live data</strong>
            </div>
            <div class="client-brief">
              {client_brief_html}
            </div>
          </section>

          <section class="panel">
            <div class="panel-header">
              <div>
                <h2>Fast Read Spotlight</h2>
                <p>The first scan list for client conversations and internal triage.</p>
              </div>
              <strong>Top {len(featured)}</strong>
            </div>
            <div class="featured-grid">
              {featured_html}
            </div>
          </section>

          <section class="panel" id="contender-catalogue">
            <div class="catalog-header">
              <div>
                <h2>Major Metro Contender Board</h2>
                <p id="catalogue-count">Showing {contender_count} of {contender_count} contenders.</p>
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
            <div class="catalog-tools">
              <strong>{priority_now_count} priority-now sites · {near_term_count} near-term builds</strong>
              <span>{escape(summary["market"])} private client shortlist with major-metro balance</span>
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
                    <th>Approval</th>
                    <th>Score</th>
                    <th>Access</th>
                  </tr>
                </thead>
                <tbody id="opportunity-table-body">
                  {opportunity_rows_html}
                </tbody>
              </table>
            </div>
          </section>

          <footer class="footer-bar">
            <span>{escape(settings.app_name)} · version {escape(APP_VERSION)}</span>
            <div class="footer-links">
              <a href="/">Main portal</a>
              <a href="/dashboard/summary">Live JSON</a>
              <a href="/health">Health</a>
            </div>
          </footer>
        </div>

        <script>
          const searchFilter = document.getElementById("search-filter");
          const metroFilter = document.getElementById("metro-filter");
          const stageFilter = document.getElementById("stage-filter");
          const universityFilter = document.getElementById("university-filter");
          const catalogueCount = document.getElementById("catalogue-count");
          const rows = Array.from(document.querySelectorAll(".opportunity-row"));

          const openRowDetail = (row) => {{
            const href = row.dataset.href;
            if (href) {{
              window.location.href = href;
            }}
          }};

          const applyFilters = () => {{
            const searchToken = (searchFilter.value || "").trim().toLowerCase();
            const metroToken = metroFilter.value;
            const stageToken = stageFilter.value;
            const universityToken = universityFilter.value;
            let visibleCount = 0;

            rows.forEach((row) => {{
              const matchesSearch = !searchToken || row.dataset.search.includes(searchToken);
              const matchesMetro = !metroToken || row.dataset.metro === metroToken;
              const matchesStage = !stageToken || row.dataset.stage === stageToken;
              const matchesUniversity = !universityToken || row.dataset.university === universityToken;
              const visible = matchesSearch && matchesMetro && matchesStage && matchesUniversity;
              row.hidden = !visible;
              if (visible) {{
                visibleCount += 1;
              }}
            }});

            catalogueCount.textContent = `Showing ${{visibleCount}} of {contender_count} contenders.`;
          }};

          [searchFilter, metroFilter, stageFilter, universityFilter].forEach((node) => {{
            node.addEventListener("input", applyFilters);
            node.addEventListener("change", applyFilters);
          }});

          rows.forEach((row) => {{
            row.addEventListener("click", (event) => {{
              if (event.target.closest("a, button, input, select")) {{
                return;
              }}
              openRowDetail(row);
            }});
            row.addEventListener("keydown", (event) => {{
              if (event.key === "Enter" || event.key === " ") {{
                event.preventDefault();
                openRowDetail(row);
              }}
            }});
          }});
        </script>
      </body>
    </html>
    """


@router.get("/", response_class=HTMLResponse)
def landing_page(_: DashboardAccess, db: DbSession) -> str:
    settings = get_settings()
    summary = _build_dashboard_summary(settings, db)
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
    metro_snapshots = _build_map_metro_snapshots(summary["opportunities"])
    map_hotspots_html = _render_map_hotspots(metro_snapshots)
    map_corridors_html = _render_map_corridors(metro_snapshots)
    map_focus_cards_html = _render_map_focus_cards(metro_snapshots[:4])
    field_average_score = round(
        mean(item["viability_score"] for item in summary["opportunities"]),
        1,
    ) if summary["opportunities"] else 0.0
    top_metro_label = metro_snapshots[0]["short_label"] if metro_snapshots else "TX"
    top_metro_count = metro_snapshots[0]["site_count"] if metro_snapshots else 0
    priority_share = round(
        (summary["priority_now_count"] / max(summary["opportunity_count"], 1)) * 100
    )
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
        {_PREMIUM_FONT_LINKS}
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
            position: relative;
            overflow: hidden;
          }}

          .hero-map::before {{
            content: "";
            position: absolute;
            inset: -16% -10% auto auto;
            width: 280px;
            height: 280px;
            border-radius: 50%;
            background: radial-gradient(circle, rgba(15, 118, 110, 0.16), transparent 68%);
            filter: blur(12px);
            pointer-events: none;
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
            min-height: 470px;
            border-radius: 24px;
            overflow: hidden;
            background:
              radial-gradient(circle at 18% 20%, rgba(15, 118, 110, 0.22), transparent 22%),
              radial-gradient(circle at 78% 24%, rgba(36, 99, 160, 0.14), transparent 20%),
              radial-gradient(circle at 72% 78%, rgba(180, 83, 9, 0.16), transparent 24%),
              linear-gradient(180deg, rgba(6, 22, 39, 0.18), rgba(255, 250, 240, 0.04)),
              linear-gradient(180deg, #0f2438 0%, #183956 32%, #f4ead8 100%);
            border: 1px solid rgba(16, 36, 56, 0.08);
            position: relative;
            isolation: isolate;
            box-shadow:
              inset 0 1px 0 rgba(255, 255, 255, 0.18),
              inset 0 -30px 60px rgba(16, 36, 56, 0.18);
          }}

          .map-stage::before {{
            content: "";
            position: absolute;
            inset: 0;
            background:
              linear-gradient(transparent 0%, rgba(255, 255, 255, 0.03) 8%, transparent 18%),
              linear-gradient(90deg, rgba(255, 255, 255, 0.04) 1px, transparent 1px),
              linear-gradient(rgba(255, 255, 255, 0.04) 1px, transparent 1px);
            background-size: auto, 34px 34px, 34px 34px;
            opacity: 0.22;
            mix-blend-mode: screen;
            pointer-events: none;
            z-index: 0;
          }}

          .map-stage::after {{
            content: "";
            position: absolute;
            inset: 0;
            background: linear-gradient(
              180deg,
              transparent 0%,
              rgba(15, 118, 110, 0.16) 48%,
              transparent 100%
            );
            transform: translateY(-120%);
            opacity: 0.55;
            pointer-events: none;
            z-index: 2;
            animation: mapScan 7.5s linear infinite;
          }}

          .map-stage-overlay {{
            position: absolute;
            left: 18px;
            right: 18px;
            z-index: 3;
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            justify-content: space-between;
            gap: 12px;
            pointer-events: none;
          }}

          .map-stage-overlay.top {{
            top: 18px;
          }}

          .map-badge {{
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 9px 12px;
            border-radius: 999px;
            background: rgba(7, 19, 31, 0.72);
            border: 1px solid rgba(148, 163, 184, 0.24);
            color: rgba(236, 253, 245, 0.9);
            font-size: 0.78rem;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            font-weight: 700;
            backdrop-filter: blur(12px);
          }}

          .map-badge::before {{
            content: "";
            width: 9px;
            height: 9px;
            border-radius: 50%;
            background: linear-gradient(135deg, #34d399, #0f766e);
            box-shadow: 0 0 16px rgba(52, 211, 153, 0.7);
          }}

          .map-stat-grid {{
            display: flex;
            flex-wrap: wrap;
            justify-content: end;
            gap: 10px;
          }}

          .map-stat {{
            min-width: 108px;
            padding: 10px 12px;
            border-radius: 16px;
            background: rgba(7, 19, 31, 0.68);
            border: 1px solid rgba(148, 163, 184, 0.22);
            backdrop-filter: blur(12px);
            color: white;
          }}

          .map-stat span {{
            display: block;
            font-size: 0.72rem;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            color: rgba(226, 232, 240, 0.72);
          }}

          .map-stat strong {{
            display: block;
            margin-top: 4px;
            font-size: 1.38rem;
            line-height: 1;
          }}

          .map-stage svg {{
            width: 100%;
            height: 100%;
            display: block;
            position: relative;
            z-index: 1;
          }}

          .map-shell {{
            fill: rgba(240, 249, 255, 0.11);
            stroke: rgba(125, 211, 252, 0.14);
            stroke-width: 0.8;
            filter: drop-shadow(0 0 16px rgba(15, 118, 110, 0.26));
          }}

          .map-shell-outline {{
            fill: none;
            stroke: rgba(224, 242, 254, 0.45);
            stroke-width: 1.8;
          }}

          .map-grid {{
            stroke: rgba(226, 232, 240, 0.1);
            stroke-dasharray: 3 6;
          }}

          .map-beam {{
            fill: none;
            stroke: rgba(125, 211, 252, 0.28);
            stroke-width: 1.3;
            stroke-linecap: round;
            stroke-dasharray: 4 7;
            animation: beamFlow 10s linear infinite;
          }}

          .map-point {{
            stroke: rgba(248, 250, 252, 0.9);
            stroke-width: 1.2;
            opacity: 0.98;
            filter: drop-shadow(0 0 10px rgba(255, 255, 255, 0.18));
          }}

          .map-point.tier-1 {{
            fill: var(--tier1);
            filter: drop-shadow(0 0 12px rgba(15, 118, 110, 0.48));
          }}

          .map-point.tier-2 {{
            fill: var(--tier2);
            filter: drop-shadow(0 0 10px rgba(192, 129, 36, 0.4));
          }}

          .map-point.tier-3 {{
            fill: var(--tier3);
          }}

          .map-hotspot {{
            transform-origin: center;
          }}

          .map-hotspot.tier-1 .hotspot-core {{
            fill: rgba(15, 118, 110, 0.82);
          }}

          .map-hotspot.tier-2 .hotspot-core {{
            fill: rgba(192, 129, 36, 0.78);
          }}

          .map-hotspot.tier-3 .hotspot-core {{
            fill: rgba(100, 116, 139, 0.78);
          }}

          .hotspot-pulse {{
            fill: rgba(148, 163, 184, 0.06);
            stroke: rgba(226, 232, 240, 0.16);
            stroke-width: 0.8;
            animation: hotspotPulse 4.2s ease-out infinite;
          }}

          .hotspot-halo {{
            fill: rgba(255, 255, 255, 0.06);
            stroke: rgba(226, 232, 240, 0.18);
            stroke-width: 0.9;
          }}

          .hotspot-core {{
            stroke: rgba(248, 250, 252, 0.75);
            stroke-width: 1.2;
            filter: drop-shadow(0 0 18px rgba(148, 163, 184, 0.22));
          }}

          .hotspot-count {{
            fill: white;
            font-size: 3.8px;
            font-weight: 800;
            text-anchor: middle;
          }}

          .hotspot-link {{
            fill: none;
            stroke: rgba(226, 232, 240, 0.28);
            stroke-width: 0.7;
            stroke-dasharray: 2 3;
          }}

          .hotspot-name {{
            fill: rgba(248, 250, 252, 0.94);
            font-size: 3.2px;
            font-weight: 800;
            letter-spacing: 0.08em;
            paint-order: stroke;
            stroke: rgba(7, 19, 31, 0.7);
            stroke-width: 0.9;
          }}

          .hotspot-meta {{
            fill: rgba(191, 219, 254, 0.88);
            font-size: 2.5px;
            font-weight: 700;
            paint-order: stroke;
            stroke: rgba(7, 19, 31, 0.6);
            stroke-width: 0.8;
          }}

          .map-label text {{
            fill: rgba(241, 245, 249, 0.92);
            font-size: 3px;
            font-weight: 700;
            paint-order: stroke;
            stroke: rgba(7, 19, 31, 0.72);
            stroke-width: 1.1;
            stroke-linecap: round;
          }}

          .map-legend {{
            display: flex;
            flex-wrap: wrap;
            gap: 14px;
            color: var(--muted);
            font-size: 0.88rem;
          }}

          .map-focus-grid {{
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 10px;
          }}

          .map-focus-card {{
            padding: 14px 14px 13px;
            border-radius: 18px;
            border: 1px solid rgba(16, 36, 56, 0.08);
            background: linear-gradient(180deg, rgba(255, 255, 255, 0.92), rgba(255, 250, 242, 0.72));
          }}

          .map-focus-card span {{
            display: block;
            color: var(--muted);
            font-size: 0.8rem;
          }}

          .map-focus-card strong {{
            display: block;
            margin-top: 6px;
            font-size: 1.18rem;
            line-height: 1;
            color: var(--ink);
          }}

          .map-focus-card small {{
            display: block;
            margin-top: 6px;
            color: var(--muted);
            line-height: 1.45;
          }}

          .tone-tier-1 strong {{
            color: var(--teal);
          }}

          .tone-tier-2 strong {{
            color: var(--gold);
          }}

          .tone-tier-3 strong {{
            color: var(--navy);
          }}

          @keyframes hotspotPulse {{
            0% {{
              opacity: 0.2;
              transform: scale(0.92);
            }}
            50% {{
              opacity: 0.45;
              transform: scale(1.04);
            }}
            100% {{
              opacity: 0.12;
              transform: scale(1.14);
            }}
          }}

          @keyframes beamFlow {{
            from {{
              stroke-dashoffset: 0;
            }}
            to {{
              stroke-dashoffset: -88;
            }}
          }}

          @keyframes mapScan {{
            0% {{
              transform: translateY(-120%);
            }}
            100% {{
              transform: translateY(120%);
            }}
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
            color: inherit;
            text-decoration: none;
            transition: transform 160ms ease, border-color 160ms ease, box-shadow 160ms ease;
          }}

          .featured-card:hover,
          .featured-card:focus-visible {{
            transform: translateY(-2px);
            border-color: rgba(15, 118, 110, 0.32);
            box-shadow: 0 18px 42px rgba(16, 36, 56, 0.14);
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

          .site-cell a {{
            color: var(--teal);
            text-decoration: none;
          }}

          .site-title-link {{
            color: var(--ink) !important;
          }}

          .opportunity-row {{
            cursor: pointer;
            transition: background 140ms ease;
          }}

          .opportunity-row:hover,
          .opportunity-row:focus-visible {{
            background: rgba(15, 118, 110, 0.07);
            outline: none;
          }}

          .detail-button {{
            width: fit-content;
            margin-top: 4px;
            padding: 8px 11px;
            border-radius: 999px;
            background: linear-gradient(135deg, #124a61, #0f766e);
            color: white;
            font-size: 0.8rem;
            font-weight: 800;
            text-decoration: none;
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
            .phase-grid,
            .map-focus-grid {{
              grid-template-columns: repeat(2, minmax(0, 1fr));
            }}

            .map-stat-grid {{
              justify-content: start;
            }}
          }}

          @media (max-width: 860px) {{
            body {{
              padding: 16px;
            }}

            .metrics-grid,
            .featured-grid,
            .mini-metrics,
            .phase-grid,
            .map-focus-grid {{
              grid-template-columns: 1fr;
            }}

            .map-stage {{
              min-height: 420px;
            }}

            .map-stage-overlay {{
              left: 14px;
              right: 14px;
            }}

            .map-stage-overlay.top {{
              top: 14px;
            }}

            .map-stat-grid {{
              justify-content: start;
            }}

            .map-stat {{
              min-width: 96px;
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
{_PREMIUM_SHARED_CSS}
        </style>
      </head>
      <body>
        <div class="page-shell">
          <section class="hero">
            <div class="hero-copy">
              <span class="eyebrow">Private Texas Client Intelligence Portal</span>
              <h1>{escape(summary["hero_title"])}</h1>
              <p>{escape(summary["hero_subtitle"])}</p>
              <div class="hero-actions">
                <a class="hero-link primary" href="#opportunity-catalogue">Explore the {summary["opportunity_count"]}-site watchlist</a>
                <a class="hero-link" href="/dashboard/contenders">Open contender board</a>
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
                  <span>Live siting field with metro hotspots, corridor beams, and ranking density across the active client board.</span>
                </div>
                <strong>{summary["opportunity_count"]} sites</strong>
              </header>
              <div class="map-stage">
                <div class="map-stage-overlay top">
                  <span class="map-badge">Live field telemetry</span>
                  <div class="map-stat-grid">
                    <article class="map-stat">
                      <span>Avg score</span>
                      <strong>{field_average_score}</strong>
                    </article>
                    <article class="map-stat">
                      <span>Priority now</span>
                      <strong>{priority_share}%</strong>
                    </article>
                    <article class="map-stat">
                      <span>Lead metro</span>
                      <strong>{escape(top_metro_label)}</strong>
                    </article>
                    <article class="map-stat">
                      <span>Lead count</span>
                      <strong>{top_metro_count}</strong>
                    </article>
                  </div>
                </div>
                <svg viewBox="0 0 100 100" role="img" aria-label="Texas opportunity field">
                  <defs>
                    <pattern id="mapMesh" width="8" height="8" patternUnits="userSpaceOnUse">
                      <path d="M0 0 H8 M0 0 V8" fill="none" stroke="white" stroke-opacity="0.08" stroke-width="0.35"></path>
                    </pattern>
                  </defs>
                  <rect x="0" y="0" width="100" height="100" fill="url(#mapMesh)"></rect>
                  <path class="map-shell" d="M26 8 L43 11 L49 21 L63 21 L72 31 L80 31 L88 38 L84 49 L91 59 L86 83 L71 86 L56 94 L42 82 L37 67 L26 61 L22 46 L11 40 L15 24 L24 18 Z"></path>
                  {map_corridors_html}
                  <path class="map-shell-outline" d="M26 8 L43 11 L49 21 L63 21 L72 31 L80 31 L88 38 L84 49 L91 59 L86 83 L71 86 L56 94 L42 82 L37 67 L26 61 L22 46 L11 40 L15 24 L24 18 Z"></path>
                  <path class="map-grid" d="M12 28 H88 M16 46 H84 M22 64 H80 M30 20 V82 M48 16 V90 M66 22 V88"></path>
                  {map_hotspots_html}
                  {map_points_html}
                  {map_labels_html}
                </svg>
              </div>
              <div class="map-focus-grid">
                {map_focus_cards_html}
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
                  <div class="note" id="monitoring-note">{escape(monitoring["error"] or "Polling live monitoring detail for private client briefings and operator visibility.")}</div>
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
                      <th>Approval</th>
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
              <a href="/dashboard/contenders">Contenders</a>
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

          const openRowDetail = (row) => {{
            const href = row.dataset.href;
            if (href) {{
              window.location.href = href;
            }}
          }};

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
              monitoring.error || "Polling live monitoring detail for private client briefings and operator visibility.";
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

          rows.forEach((row) => {{
            row.addEventListener("click", (event) => {{
              if (event.target.closest("a, button, input, select")) {{
                return;
              }}
              openRowDetail(row);
            }});
            row.addEventListener("keydown", (event) => {{
              if (event.key === "Enter" || event.key === " ") {{
                event.preventDefault();
                openRowDetail(row);
              }}
            }});
          }});

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
