from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from uuid import UUID

from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from app.db.models.batching import ScoreRun
from app.db.models.catalogs import BonusCatalog, ScoringProfile, ScoringProfileFactor
from app.db.models.enums import (
    ParcelEvaluationStatus,
    ScoreRunStatus,
    ScoringProfileStatus,
)
from app.db.models.evaluation import ParcelEvaluation
from app.db.models.scoring import ScoreBonusDetail, ScoreFactorDetail, ScoreFactorInput
from app.db.models.source_data import SourceEvidence
from ingestion.service import evaluate_freshness
from orchestrator.service import reconcile_batch_for_run
from scoring.models import (
    BonusDetailBreakdown,
    EvidenceQualityWeights,
    FactorDetailBreakdown,
    ParcelScoringDetail,
    ProvenanceInputDetail,
    ScoringPolicy,
    ScoringStatusCount,
    ScoringSummary,
)

_FACTOR_COUNT = 10
_BONUS_COUNT = 5
_MAX_TOTAL_SCORE = Decimal("100")
_QUALITY_PRECEDENCE = ("measured", "manual", "proxy", "heuristic")


@dataclass(slots=True)
class _EvidenceCandidate:
    attribute_name: str
    raw_value: str
    quality: str
    parsed_value: Decimal | bool | None


@dataclass(slots=True)
class _ProvenanceInput:
    input_name: str
    input_value: str
    evidence_quality: str


@dataclass(slots=True)
class _FactorScoreResult:
    factor_id: str
    points_awarded: Decimal
    rationale: str
    selected_quality: str
    provenance_inputs: tuple[_ProvenanceInput, ...]


@dataclass(slots=True)
class _BonusScoreResult:
    bonus_id: str
    applied: bool
    points_awarded: Decimal
    rationale: str


class ScoringRunNotFoundError(LookupError):
    """Raised when a requested score run does not exist."""


class ScoringParcelNotFoundError(LookupError):
    """Raised when a requested parcel evaluation does not exist for the run."""


class ScoringReplayBlockedError(RuntimeError):
    """Raised when a scoring rerun is not allowed."""


class ScoringProfileValidationError(ValueError):
    """Raised when the requested scoring profile is invalid."""


class ScoringInvariantError(RuntimeError):
    """Raised when run state prevents safe scoring completion."""


def describe_service() -> dict[str, str]:
    return {
        "service": "scoring",
        "purpose": "Calculate factor, bonus, confidence, and provenance outputs.",
    }


def score_run(
    session: Session,
    run_id: str | UUID,
    policy: ScoringPolicy,
) -> ScoringSummary:
    run = _get_run(session, run_id)
    now = datetime.now(UTC)
    _prepare_run_for_scoring(run, policy)

    freshness_report = evaluate_freshness(session, run.metro_id, evaluated_at=now)
    if not freshness_report.passed:
        first_failure = next(status for status in freshness_report.statuses if not status.passed)
        run.status = ScoreRunStatus.FAILED
        run.failure_reason = first_failure.freshness_code
        run.completed_at = now
        reconcile_batch_for_run(session, run.run_id)
        session.commit()
        return get_scoring_summary(session, run.run_id)

    profile, profile_factors = _resolve_profile(session, policy, now)
    bonuses = _load_bonus_catalog(session)
    _validate_profile(profile, profile_factors, bonuses)

    run.profile_name = profile.profile_name

    evaluations = session.scalars(
        select(ParcelEvaluation).where(ParcelEvaluation.run_id == run.run_id)
    ).all()
    _clear_run_scoring_state(session, run.run_id, evaluations)

    if any(
        evaluation.status is ParcelEvaluationStatus.PENDING_EXCLUSION_CHECK
        for evaluation in evaluations
    ):
        raise ScoringInvariantError(
            "Run contains parcels still pending exclusion review and cannot be scored."
        )

    score_candidates = [
        evaluation
        for evaluation in evaluations
        if evaluation.status is ParcelEvaluationStatus.PENDING_SCORING
    ]
    parcel_ids = [evaluation.parcel_id for evaluation in score_candidates]
    evidence_rows = session.scalars(
        select(SourceEvidence).where(
            SourceEvidence.metro_id == run.metro_id,
            SourceEvidence.parcel_id.in_(parcel_ids),
            SourceEvidence.is_active.is_(True),
        )
    ).all()
    evidence_by_parcel: dict[str, list[SourceEvidence]] = defaultdict(list)
    for evidence in evidence_rows:
        if evidence.parcel_id:
            evidence_by_parcel[evidence.parcel_id].append(evidence)

    for evaluation in score_candidates:
        parcel_evidence = sorted(
            evidence_by_parcel.get(evaluation.parcel_id, []),
            key=lambda item: (
                item.attribute_name.lower(),
                item.source_id,
                item.record_key,
                item.attribute_value,
            ),
        )
        factor_results = [
            _score_factor(factor, parcel_evidence, policy.evidence_quality_weights)
            for factor in profile_factors
        ]
        for factor_result in factor_results:
            session.add(
                ScoreFactorDetail(
                    run_id=run.run_id,
                    parcel_id=evaluation.parcel_id,
                    factor_id=factor_result.factor_id,
                    points_awarded=factor_result.points_awarded,
                    rationale=factor_result.rationale,
                )
            )
            for provenance_input in factor_result.provenance_inputs:
                session.add(
                    ScoreFactorInput(
                        run_id=run.run_id,
                        parcel_id=evaluation.parcel_id,
                        factor_id=factor_result.factor_id,
                        input_name=provenance_input.input_name,
                        input_value=provenance_input.input_value,
                        evidence_quality=provenance_input.evidence_quality,
                    )
                )

        bonus_results = [_score_bonus(bonus, parcel_evidence) for bonus in bonuses]
        for bonus_result in bonus_results:
            session.add(
                ScoreBonusDetail(
                    run_id=run.run_id,
                    parcel_id=evaluation.parcel_id,
                    bonus_id=bonus_result.bonus_id,
                    applied=bonus_result.applied,
                    points_awarded=bonus_result.points_awarded,
                    rationale=bonus_result.rationale,
                )
            )

        base_score = _quantize_score(sum(result.points_awarded for result in factor_results))
        bonus_score = _quantize_score(sum(result.points_awarded for result in bonus_results))
        evaluation.status = ParcelEvaluationStatus.SCORED
        evaluation.viability_score = _quantize_score(
            min(base_score + bonus_score, _MAX_TOTAL_SCORE)
        )
        evaluation.confidence_score = _calculate_confidence(
            factor_results,
            profile_factors,
            policy.evidence_quality_weights,
        )
        evaluation.status_reason = f"Parcel scored with profile `{profile.profile_name}`."

    session.flush()
    _validate_run_cardinality(
        session,
        run.run_id,
        parcel_ids,
        factor_count=len(profile_factors),
        bonus_count=len(bonuses),
    )
    _ensure_no_pending_states(session, run.run_id)
    run.status = ScoreRunStatus.COMPLETED
    run.failure_reason = None
    run.completed_at = now
    reconcile_batch_for_run(session, run.run_id)
    session.commit()
    return get_scoring_summary(session, run.run_id)


def get_scoring_summary(session: Session, run_id: str | UUID) -> ScoringSummary:
    run = _get_run(session, run_id)
    evaluations = session.scalars(
        select(ParcelEvaluation).where(ParcelEvaluation.run_id == run.run_id)
    ).all()
    status_counts = Counter(evaluation.status.value for evaluation in evaluations)
    factor_detail_count = session.scalar(
        select(func.count())
        .select_from(ScoreFactorDetail)
        .where(ScoreFactorDetail.run_id == run.run_id)
    )
    bonus_detail_count = session.scalar(
        select(func.count())
        .select_from(ScoreBonusDetail)
        .where(ScoreBonusDetail.run_id == run.run_id)
    )
    provenance_count = session.scalar(
        select(func.count())
        .select_from(ScoreFactorInput)
        .where(ScoreFactorInput.run_id == run.run_id)
    )
    scored_evaluations = [
        evaluation
        for evaluation in evaluations
        if evaluation.status is ParcelEvaluationStatus.SCORED
    ]

    return ScoringSummary(
        run_id=str(run.run_id),
        batch_id=str(run.batch_id),
        metro_id=run.metro_id,
        profile_name=run.profile_name,
        run_status=run.status.value,
        scored_count=len(scored_evaluations),
        pending_scoring_count=status_counts.get(ParcelEvaluationStatus.PENDING_SCORING.value, 0),
        factor_detail_count=int(factor_detail_count or 0),
        bonus_detail_count=int(bonus_detail_count or 0),
        provenance_count=int(provenance_count or 0),
        average_viability_score=_average_score(
            [evaluation.viability_score for evaluation in scored_evaluations]
        ),
        average_confidence_score=_average_score(
            [evaluation.confidence_score for evaluation in scored_evaluations]
        ),
        status_counts=[
            ScoringStatusCount(status=status, count=count)
            for status, count in sorted(status_counts.items())
        ],
        failure_reason=run.failure_reason,
    )


def get_parcel_scoring_detail(
    session: Session,
    run_id: str | UUID,
    parcel_id: str,
) -> ParcelScoringDetail:
    run = _get_run(session, run_id)
    evaluation = session.scalar(
        select(ParcelEvaluation).where(
            ParcelEvaluation.run_id == run.run_id,
            ParcelEvaluation.parcel_id == parcel_id,
        )
    )
    if evaluation is None:
        raise ScoringParcelNotFoundError(
            f"Parcel `{parcel_id}` was not found for run `{run.run_id}`."
        )

    factor_rows = session.scalars(
        select(ScoreFactorDetail)
        .where(
            ScoreFactorDetail.run_id == run.run_id,
            ScoreFactorDetail.parcel_id == parcel_id,
        )
        .order_by(ScoreFactorDetail.factor_id)
    ).all()
    bonus_rows = session.scalars(
        select(ScoreBonusDetail)
        .where(
            ScoreBonusDetail.run_id == run.run_id,
            ScoreBonusDetail.parcel_id == parcel_id,
        )
        .order_by(ScoreBonusDetail.bonus_id)
    ).all()
    provenance_rows = session.scalars(
        select(ScoreFactorInput)
        .where(
            ScoreFactorInput.run_id == run.run_id,
            ScoreFactorInput.parcel_id == parcel_id,
        )
        .order_by(ScoreFactorInput.factor_id, ScoreFactorInput.input_name)
    ).all()

    inputs_by_factor: dict[str, list[ProvenanceInputDetail]] = defaultdict(list)
    quality_counts = Counter(input_row.evidence_quality for input_row in provenance_rows)
    for input_row in provenance_rows:
        inputs_by_factor[input_row.factor_id].append(
            ProvenanceInputDetail(
                input_name=input_row.input_name,
                input_value=input_row.input_value,
                evidence_quality=input_row.evidence_quality,
            )
        )

    return ParcelScoringDetail(
        run_id=str(run.run_id),
        batch_id=str(run.batch_id),
        metro_id=run.metro_id,
        profile_name=run.profile_name,
        parcel_id=parcel_id,
        status=evaluation.status.value,
        status_reason=evaluation.status_reason,
        viability_score=evaluation.viability_score,
        confidence_score=evaluation.confidence_score,
        factor_details=[
            FactorDetailBreakdown(
                factor_id=row.factor_id,
                points_awarded=row.points_awarded,
                rationale=row.rationale,
                inputs=inputs_by_factor.get(row.factor_id, []),
            )
            for row in factor_rows
        ],
        bonus_details=[
            BonusDetailBreakdown(
                bonus_id=row.bonus_id,
                applied=row.applied,
                points_awarded=row.points_awarded,
                rationale=row.rationale,
            )
            for row in bonus_rows
        ],
        evidence_quality_counts=dict(sorted(quality_counts.items())),
    )


def _get_run(session: Session, run_id: str | UUID) -> ScoreRun:
    typed_run_id = run_id if isinstance(run_id, UUID) else UUID(str(run_id))
    run = session.get(ScoreRun, typed_run_id)
    if run is None:
        raise ScoringRunNotFoundError(f"Run `{typed_run_id}` was not found.")
    return run


def _prepare_run_for_scoring(run: ScoreRun, policy: ScoringPolicy) -> None:
    if run.status is ScoreRunStatus.FAILED and not policy.restart_failed_run:
        raise ScoringReplayBlockedError(
            f"Run `{run.run_id}` is failed and restart_failed_run is disabled."
        )
    if run.status is ScoreRunStatus.COMPLETED and not policy.allow_completed_run_rerun:
        raise ScoringReplayBlockedError(
            f"Run `{run.run_id}` is completed and allow_completed_run_rerun is disabled."
        )

    run.status = ScoreRunStatus.RUNNING
    run.failure_reason = None
    run.completed_at = None


def _resolve_profile(
    session: Session,
    policy: ScoringPolicy,
    evaluated_at: datetime,
) -> tuple[ScoringProfile, list[ScoringProfileFactor]]:
    if policy.profile_name:
        profile = session.scalar(
            select(ScoringProfile).where(ScoringProfile.profile_name == policy.profile_name)
        )
        if profile is None:
            raise ScoringProfileValidationError(
                f"Scoring profile `{policy.profile_name}` was not found."
            )
    else:
        profiles = session.scalars(
            select(ScoringProfile).where(
                ScoringProfile.status == ScoringProfileStatus.ACTIVE,
                (ScoringProfile.effective_from.is_(None))
                | (ScoringProfile.effective_from <= evaluated_at),
                (ScoringProfile.effective_to.is_(None))
                | (ScoringProfile.effective_to > evaluated_at),
            )
        ).all()
        if not profiles:
            raise ScoringProfileValidationError("No active scoring profile is available.")
        if len(profiles) > 1:
            raise ScoringProfileValidationError(
                "Multiple active scoring profiles are available; profile_name is required."
            )
        profile = profiles[0]

    profile_factors = session.scalars(
        select(ScoringProfileFactor)
        .where(ScoringProfileFactor.profile_id == profile.profile_id)
        .order_by(ScoringProfileFactor.ordinal)
    ).all()
    return profile, profile_factors


def _load_bonus_catalog(session: Session) -> list[BonusCatalog]:
    return session.scalars(
        select(BonusCatalog)
        .where(BonusCatalog.is_active.is_(True))
        .order_by(BonusCatalog.bonus_id)
    ).all()


def _validate_profile(
    profile: ScoringProfile,
    profile_factors: list[ScoringProfileFactor],
    bonuses: list[BonusCatalog],
) -> None:
    if profile.status is not ScoringProfileStatus.ACTIVE:
        raise ScoringProfileValidationError(
            f"Scoring profile `{profile.profile_name}` is not active."
        )
    if len(profile_factors) != _FACTOR_COUNT:
        raise ScoringProfileValidationError(
            f"Scoring profile `{profile.profile_name}` must contain exactly 10 factors."
        )
    factor_budget = sum(factor.max_points for factor in profile_factors)
    if factor_budget != 100:
        raise ScoringProfileValidationError(
            f"Scoring profile `{profile.profile_name}` budget must sum to 100."
        )
    if len(bonuses) != _BONUS_COUNT:
        raise ScoringProfileValidationError("Exactly 5 active bonus definitions are required.")


def _clear_run_scoring_state(
    session: Session,
    run_id: UUID,
    evaluations: list[ParcelEvaluation],
) -> None:
    session.execute(delete(ScoreFactorInput).where(ScoreFactorInput.run_id == run_id))
    session.execute(delete(ScoreFactorDetail).where(ScoreFactorDetail.run_id == run_id))
    session.execute(delete(ScoreBonusDetail).where(ScoreBonusDetail.run_id == run_id))
    for evaluation in evaluations:
        if evaluation.status is ParcelEvaluationStatus.SCORED:
            evaluation.status = ParcelEvaluationStatus.PENDING_SCORING
            evaluation.viability_score = None
            evaluation.confidence_score = None
            evaluation.status_reason = "Parcel passed prefilters and exclusion checks."
    session.flush()


def _score_factor(
    factor: ScoringProfileFactor,
    parcel_evidence: list[SourceEvidence],
    weights: EvidenceQualityWeights,
) -> _FactorScoreResult:
    factor_code = factor.factor_id.lower()
    candidates = _collect_candidates(
        parcel_evidence,
        {
            "measured": (f"{factor_code}_measured", f"{factor_code}_score"),
            "manual": (f"{factor_code}_manual",),
            "proxy": (f"{factor_code}_proxy",),
            "heuristic": (f"{factor_code}_heuristic",),
        },
        parser=_parse_numeric_value,
    )
    selected = _select_candidate(candidates)
    provenance_inputs = tuple(
        _ProvenanceInput(
            input_name=candidate.attribute_name.lower(),
            input_value=candidate.raw_value,
            evidence_quality=candidate.quality,
        )
        for candidate in candidates
    )
    if selected is None or not isinstance(selected.parsed_value, Decimal):
        return _FactorScoreResult(
            factor_id=factor.factor_id,
            points_awarded=Decimal("0.00"),
            rationale=f"{factor.factor_id} has no usable evidence; points default to 0.",
            selected_quality="missing",
            provenance_inputs=provenance_inputs
            or (
                _ProvenanceInput(
                    input_name=f"{factor_code}_missing",
                    input_value="missing",
                    evidence_quality="missing",
                ),
            ),
        )

    normalized_value = _normalize_score(selected.parsed_value)
    points_awarded = _quantize_score(Decimal(factor.max_points) * normalized_value)
    weight = getattr(weights, selected.quality)
    rationale = (
        f"{factor.factor_id} selected `{selected.attribute_name}` "
        f"({selected.quality}, weight={weight}) and awarded {points_awarded} points."
    )
    return _FactorScoreResult(
        factor_id=factor.factor_id,
        points_awarded=points_awarded,
        rationale=rationale,
        selected_quality=selected.quality,
        provenance_inputs=provenance_inputs,
    )


def _score_bonus(
    bonus: BonusCatalog,
    parcel_evidence: list[SourceEvidence],
) -> _BonusScoreResult:
    bonus_code = bonus.bonus_id.lower()
    candidates = _collect_candidates(
        parcel_evidence,
        {
            "measured": (f"{bonus_code}_measured", f"{bonus_code}_applies"),
            "manual": (f"{bonus_code}_manual",),
            "proxy": (f"{bonus_code}_proxy",),
            "heuristic": (f"{bonus_code}_heuristic",),
        },
        parser=_parse_boolean_value,
    )
    selected = _select_candidate(candidates)
    applied = bool(selected.parsed_value) if selected is not None else False
    points_awarded = Decimal(bonus.max_points if applied else 0).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP,
    )
    if selected is None:
        rationale = f"{bonus.bonus_id} has no qualifying evidence and was not applied."
    elif applied:
        rationale = f"{bonus.bonus_id} applied from `{selected.attribute_name}`."
    else:
        rationale = f"{bonus.bonus_id} evaluated `{selected.attribute_name}` and did not apply."
    return _BonusScoreResult(
        bonus_id=bonus.bonus_id,
        applied=applied,
        points_awarded=points_awarded,
        rationale=rationale,
    )


def _collect_candidates(
    parcel_evidence: list[SourceEvidence],
    attribute_map: dict[str, tuple[str, ...]],
    parser,
) -> tuple[_EvidenceCandidate, ...]:
    evidence_by_attribute: dict[str, SourceEvidence] = {}
    for evidence in parcel_evidence:
        attribute_name = evidence.attribute_name.lower()
        evidence_by_attribute.setdefault(attribute_name, evidence)

    candidates: list[_EvidenceCandidate] = []
    for quality in _QUALITY_PRECEDENCE:
        for attribute_name in attribute_map.get(quality, ()):
            evidence = evidence_by_attribute.get(attribute_name)
            if evidence is None:
                continue
            candidates.append(
                _EvidenceCandidate(
                    attribute_name=evidence.attribute_name,
                    raw_value=evidence.attribute_value,
                    quality=quality,
                    parsed_value=parser(evidence.attribute_value),
                )
            )
            break
    return tuple(candidates)


def _select_candidate(
    candidates: tuple[_EvidenceCandidate, ...],
) -> _EvidenceCandidate | None:
    for quality in _QUALITY_PRECEDENCE:
        for candidate in candidates:
            if candidate.quality == quality and candidate.parsed_value is not None:
                return candidate
    return None


def _parse_numeric_value(raw_value: str) -> Decimal | None:
    try:
        value = Decimal(str(raw_value).strip())
    except (InvalidOperation, ValueError):
        return None
    if value < 0:
        return Decimal("0")
    if value > 1:
        if value <= 100:
            return value / Decimal("100")
        return Decimal("1")
    return value


def _parse_boolean_value(raw_value: str) -> bool | None:
    normalized = str(raw_value).strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n"}:
        return False
    return None


def _normalize_score(value: Decimal) -> Decimal:
    if value < 0:
        return Decimal("0")
    if value > 1:
        return Decimal("1")
    return value


def _calculate_confidence(
    factor_results: list[_FactorScoreResult],
    profile_factors: list[ScoringProfileFactor],
    weights: EvidenceQualityWeights,
) -> Decimal:
    profile_factor_by_id = {factor.factor_id: factor for factor in profile_factors}
    weighted_sum = Decimal("0")
    for result in factor_results:
        profile_factor = profile_factor_by_id[result.factor_id]
        weighted_sum += Decimal(profile_factor.max_points) * getattr(
            weights,
            result.selected_quality,
        )
    return _quantize_score(weighted_sum)


def _validate_run_cardinality(
    session: Session,
    run_id: UUID,
    parcel_ids: list[str],
    *,
    factor_count: int,
    bonus_count: int,
) -> None:
    if not parcel_ids:
        return

    factor_rows = session.execute(
        select(ScoreFactorDetail.parcel_id, func.count(ScoreFactorDetail.factor_detail_id))
        .where(
            ScoreFactorDetail.run_id == run_id,
            ScoreFactorDetail.parcel_id.in_(parcel_ids),
        )
        .group_by(ScoreFactorDetail.parcel_id)
    ).all()
    factor_counts = {parcel_id: count for parcel_id, count in factor_rows}
    bonus_rows = session.execute(
        select(ScoreBonusDetail.parcel_id, func.count(ScoreBonusDetail.bonus_detail_id))
        .where(
            ScoreBonusDetail.run_id == run_id,
            ScoreBonusDetail.parcel_id.in_(parcel_ids),
        )
        .group_by(ScoreBonusDetail.parcel_id)
    ).all()
    bonus_counts = {parcel_id: count for parcel_id, count in bonus_rows}

    for parcel_id in parcel_ids:
        if factor_counts.get(parcel_id) != factor_count:
            raise ScoringInvariantError(
                f"Parcel `{parcel_id}` does not have the required {factor_count} factor rows."
            )
        if bonus_counts.get(parcel_id) != bonus_count:
            raise ScoringInvariantError(
                f"Parcel `{parcel_id}` does not have the required {bonus_count} bonus rows."
            )


def _ensure_no_pending_states(session: Session, run_id: UUID) -> None:
    pending_count = session.scalar(
        select(func.count())
        .select_from(ParcelEvaluation)
        .where(
            ParcelEvaluation.run_id == run_id,
            ParcelEvaluation.status.in_(
                (
                    ParcelEvaluationStatus.PENDING_EXCLUSION_CHECK,
                    ParcelEvaluationStatus.PENDING_SCORING,
                )
            ),
        )
    )
    if pending_count:
        raise ScoringInvariantError("Run cannot complete while pending parcel states remain.")


def _average_score(values: list[Decimal | None]) -> Decimal | None:
    present_values = [value for value in values if value is not None]
    if not present_values:
        return None
    return _quantize_score(sum(present_values) / Decimal(len(present_values)))


def _quantize_score(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
