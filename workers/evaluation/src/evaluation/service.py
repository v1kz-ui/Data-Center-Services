from __future__ import annotations

from collections import Counter, defaultdict
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from shapely import wkt
from shapely.errors import ShapelyError
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from app.db.models.batching import ScoreRun
from app.db.models.enums import ParcelEvaluationStatus, ScoreRunStatus
from app.db.models.evaluation import ParcelEvaluation, ParcelExclusionEvent
from app.db.models.scoring import ScoreBonusDetail, ScoreFactorDetail
from app.db.models.source_data import RawZoning, SourceEvidence
from app.db.models.territory import CountyCatalog, RawParcel
from evaluation.models import (
    EvaluationPolicy,
    EvaluationStatusCount,
    EvaluationSummary,
    EvidenceExclusionRule,
)
from ingestion.service import evaluate_freshness
from orchestrator.service import reconcile_batch_for_run


class EvaluationRunNotFoundError(LookupError):
    """Raised when a requested score run does not exist."""


class EvaluationReplayBlockedError(RuntimeError):
    """Raised when evaluation cannot be safely replayed for a run."""


def describe_service() -> dict[str, str]:
    return {
        "service": "evaluation",
        "purpose": (
            "Scope parcels by metro, apply prefilters and exclusions, "
            "and promote survivors to pending_scoring."
        ),
    }


def evaluate_run(
    session: Session,
    run_id: str | UUID,
    policy: EvaluationPolicy,
) -> EvaluationSummary:
    run = _get_run(session, run_id)
    now = datetime.now(UTC)

    if run.status is ScoreRunStatus.FAILED and not policy.restart_failed_run:
        raise EvaluationReplayBlockedError(
            f"Run `{run.run_id}` is failed and restart_failed_run is disabled."
        )

    _ensure_no_scoring_outputs(session, run.run_id)
    _reset_run_for_replay(run)

    freshness_report = evaluate_freshness(session, run.metro_id, evaluated_at=now)
    if not freshness_report.passed:
        first_failure = next(status for status in freshness_report.statuses if not status.passed)
        run.status = ScoreRunStatus.FAILED
        run.failure_reason = first_failure.freshness_code
        run.completed_at = now
        _clear_run_evaluation_state(session, run.run_id)
        reconcile_batch_for_run(session, run.run_id)
        session.commit()
        return get_evaluation_summary(session, run.run_id, rule_version=policy.rule_version)

    _clear_run_evaluation_state(session, run.run_id)

    county_fips_values = session.scalars(
        select(CountyCatalog.county_fips).where(CountyCatalog.metro_id == run.metro_id)
    ).all()
    parcel_rows = session.scalars(
        select(RawParcel).where(
            RawParcel.metro_id == run.metro_id,
            RawParcel.county_fips.in_(county_fips_values),
            RawParcel.is_active.is_(True),
        )
    ).all()
    parcel_ids = [parcel.parcel_id for parcel in parcel_rows]

    zoning_rows = session.scalars(
        select(RawZoning).where(
            RawZoning.metro_id == run.metro_id,
            RawZoning.parcel_id.in_(parcel_ids),
            RawZoning.is_active.is_(True),
        )
    ).all()
    evidence_rows = session.scalars(
        select(SourceEvidence).where(
            SourceEvidence.metro_id == run.metro_id,
            SourceEvidence.parcel_id.in_(parcel_ids),
            SourceEvidence.is_active.is_(True),
        )
    ).all()

    zoning_by_parcel = {row.parcel_id: row for row in zoning_rows}
    evidence_by_parcel: dict[str, list[SourceEvidence]] = defaultdict(list)
    for row in evidence_rows:
        if row.parcel_id:
            evidence_by_parcel[row.parcel_id].append(row)

    allowed_band = _load_allowed_band(policy.allowed_band_wkt)
    blocked_zoning_codes = {code.upper() for code in policy.blocked_zoning_codes}
    blocked_land_use_codes = {code.upper() for code in policy.blocked_land_use_codes}
    evidence_rules = _normalize_evidence_rules(policy.evidence_exclusion_rules)

    for parcel in parcel_rows:
        status, reason, exclusion_events = _evaluate_parcel(
            parcel=parcel,
            zoning=zoning_by_parcel.get(parcel.parcel_id),
            evidence_rows=evidence_by_parcel.get(parcel.parcel_id, []),
            allowed_band=allowed_band,
            minimum_acreage=policy.minimum_acreage,
            blocked_zoning_codes=blocked_zoning_codes,
            blocked_land_use_codes=blocked_land_use_codes,
            evidence_rules=evidence_rules,
            rule_version=policy.rule_version,
        )

        evaluation = ParcelEvaluation(
            run_id=run.run_id,
            parcel_id=parcel.parcel_id,
            status=status,
            status_reason=reason,
        )
        session.add(evaluation)
        session.flush()

        for event in exclusion_events:
            session.add(
                ParcelExclusionEvent(
                    run_id=run.run_id,
                    parcel_id=parcel.parcel_id,
                    evaluation_id=evaluation.evaluation_id,
                    exclusion_code=event["exclusion_code"],
                    exclusion_reason=event["exclusion_reason"],
                    rule_version=policy.rule_version,
                )
            )

    run.status = ScoreRunStatus.RUNNING
    run.failure_reason = None
    run.completed_at = None
    reconcile_batch_for_run(session, run.run_id)
    session.commit()

    return get_evaluation_summary(session, run.run_id, rule_version=policy.rule_version)


def get_evaluation_summary(
    session: Session,
    run_id: str | UUID,
    rule_version: str | None = None,
) -> EvaluationSummary:
    run = _get_run(session, run_id)
    evaluations = session.scalars(
        select(ParcelEvaluation).where(ParcelEvaluation.run_id == run.run_id)
    ).all()
    counts = Counter(evaluation.status.value for evaluation in evaluations)

    return EvaluationSummary(
        run_id=str(run.run_id),
        batch_id=str(run.batch_id),
        metro_id=run.metro_id,
        run_status=run.status.value,
        rule_version=rule_version or "phase4-default",
        evaluated_count=len(evaluations),
        band_filtered_count=counts.get(ParcelEvaluationStatus.PREFILTERED_BAND.value, 0),
        size_filtered_count=counts.get(ParcelEvaluationStatus.PREFILTERED_SIZE.value, 0),
        excluded_count=counts.get(ParcelEvaluationStatus.EXCLUDED.value, 0),
        pending_scoring_count=counts.get(ParcelEvaluationStatus.PENDING_SCORING.value, 0),
        pending_exclusion_check_count=counts.get(
            ParcelEvaluationStatus.PENDING_EXCLUSION_CHECK.value,
            0,
        ),
        status_counts=[
            EvaluationStatusCount(status=status, count=count)
            for status, count in sorted(counts.items())
        ],
        failure_reason=run.failure_reason,
    )


def describe_run_scope(session: Session, run_id: str | UUID) -> dict[str, object]:
    run = _get_run(session, run_id)
    county_fips_values = session.scalars(
        select(CountyCatalog.county_fips).where(CountyCatalog.metro_id == run.metro_id)
    ).all()
    parcel_count = session.scalar(
        select(func.count())
        .select_from(RawParcel)
        .where(
            RawParcel.metro_id == run.metro_id,
            RawParcel.county_fips.in_(county_fips_values),
            RawParcel.is_active.is_(True),
        )
    )
    return {
        "run_id": str(run.run_id),
        "batch_id": str(run.batch_id),
        "metro_id": run.metro_id,
        "county_fips": county_fips_values,
        "parcel_count": int(parcel_count or 0),
    }


def _get_run(session: Session, run_id: str | UUID) -> ScoreRun:
    typed_run_id = run_id if isinstance(run_id, UUID) else UUID(str(run_id))
    run = session.get(ScoreRun, typed_run_id)
    if run is None:
        raise EvaluationRunNotFoundError(f"Run `{typed_run_id}` was not found.")
    return run


def _ensure_no_scoring_outputs(session: Session, run_id: UUID) -> None:
    factor_detail_exists = session.scalar(
        select(ScoreFactorDetail.factor_detail_id)
        .where(ScoreFactorDetail.run_id == run_id)
        .limit(1)
    )
    bonus_detail_exists = session.scalar(
        select(ScoreBonusDetail.bonus_detail_id).where(ScoreBonusDetail.run_id == run_id).limit(1)
    )
    if factor_detail_exists or bonus_detail_exists:
        raise EvaluationReplayBlockedError(
            "Evaluation replay is blocked because scoring outputs already exist for the run."
        )


def _reset_run_for_replay(run: ScoreRun) -> None:
    if run.status is ScoreRunStatus.FAILED:
        run.status = ScoreRunStatus.RUNNING
        run.failure_reason = None
        run.completed_at = None


def _clear_run_evaluation_state(session: Session, run_id: UUID) -> None:
    session.execute(delete(ParcelExclusionEvent).where(ParcelExclusionEvent.run_id == run_id))
    session.execute(delete(ParcelEvaluation).where(ParcelEvaluation.run_id == run_id))
    session.flush()


def _evaluate_parcel(
    parcel: RawParcel,
    zoning: RawZoning | None,
    evidence_rows: list[SourceEvidence],
    allowed_band: object | None,
    minimum_acreage: Decimal,
    blocked_zoning_codes: set[str],
    blocked_land_use_codes: set[str],
    evidence_rules: tuple[EvidenceExclusionRule, ...],
    rule_version: str,
) -> tuple[ParcelEvaluationStatus, str, list[dict[str, str]]]:
    if allowed_band is not None:
        if parcel.rep_point is None:
            return (
                ParcelEvaluationStatus.PREFILTERED_BAND,
                "Representative point is missing for band evaluation.",
                [],
            )

        try:
            rep_point = wkt.loads(parcel.rep_point.rep_point_wkt)
        except (TypeError, ShapelyError) as exc:
            raise ValueError("Stored parcel representative point is not valid WKT.") from exc

        if not allowed_band.covers(rep_point):
            return (
                ParcelEvaluationStatus.PREFILTERED_BAND,
                "Representative point falls outside the allowed band.",
                [],
            )

    if parcel.acreage < minimum_acreage:
        return (
            ParcelEvaluationStatus.PREFILTERED_SIZE,
            f"Parcel acreage is below the minimum threshold of {minimum_acreage}.",
            [],
        )

    exclusion_events: list[dict[str, str]] = []
    zoning_code = zoning.zoning_code.upper() if zoning and zoning.zoning_code else None
    land_use_code = zoning.land_use_code.upper() if zoning and zoning.land_use_code else None

    if zoning_code and zoning_code in blocked_zoning_codes:
        exclusion_events.append(
            {
                "exclusion_code": "ZONING_BLOCKED",
                "exclusion_reason": f"Zoning code `{zoning_code}` is excluded by {rule_version}.",
            }
        )

    if land_use_code and land_use_code in blocked_land_use_codes:
        exclusion_events.append(
            {
                "exclusion_code": "LAND_USE_BLOCKED",
                "exclusion_reason": (
                    f"Land use code `{land_use_code}` is excluded by {rule_version}."
                ),
            }
        )

    for rule in evidence_rules:
        blocked_values = {value.upper() for value in rule.blocked_values}
        for evidence in evidence_rows:
            if evidence.source_id != rule.source_id:
                continue
            if evidence.attribute_name.lower() != rule.attribute_name.lower():
                continue
            if evidence.attribute_value.upper() not in blocked_values:
                continue
            exclusion_events.append(
                {
                    "exclusion_code": rule.exclusion_code,
                    "exclusion_reason": rule.exclusion_reason,
                }
            )

    if exclusion_events:
        unique_reasons = sorted({event["exclusion_reason"] for event in exclusion_events})
        return (
            ParcelEvaluationStatus.EXCLUDED,
            "; ".join(unique_reasons),
            exclusion_events,
        )

    return (
        ParcelEvaluationStatus.PENDING_SCORING,
        "Parcel passed prefilters and exclusion checks.",
        [],
    )


def _load_allowed_band(allowed_band_wkt: str | None):
    if allowed_band_wkt is None:
        return None
    try:
        geometry = wkt.loads(allowed_band_wkt)
    except (TypeError, ShapelyError) as exc:
        raise ValueError("allowed_band_wkt is not valid WKT.") from exc

    if geometry.is_empty:
        raise ValueError("allowed_band_wkt cannot be empty.")
    return geometry


def _normalize_evidence_rules(
    rules: tuple[EvidenceExclusionRule, ...],
) -> tuple[EvidenceExclusionRule, ...]:
    normalized_rules: list[EvidenceExclusionRule] = []
    for rule in rules:
        normalized_rules.append(
            EvidenceExclusionRule(
                source_id=rule.source_id.upper(),
                attribute_name=rule.attribute_name,
                blocked_values=tuple(value.upper() for value in rule.blocked_values),
                exclusion_code=rule.exclusion_code,
                exclusion_reason=rule.exclusion_reason,
            )
        )
    return tuple(normalized_rules)
