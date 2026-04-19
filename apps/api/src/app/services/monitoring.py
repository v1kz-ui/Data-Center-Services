from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models.batching import ScoreRun
from app.db.models.enums import ScoreBatchStatus, ScoreRunStatus, SourceSnapshotStatus
from ingestion.models import MetroFreshnessReport, SourceHealthSnapshot
from ingestion.service import evaluate_freshness, summarize_source_health
from orchestrator.models import PlannedBatch, PlannedRun
from orchestrator.service import list_batches


@dataclass(slots=True)
class MonitoringStatusCount:
    status: str
    count: int


@dataclass(slots=True)
class MonitoringAlert:
    severity: str
    code: str
    summary: str
    metro_id: str | None = None
    batch_id: str | None = None
    run_id: str | None = None


@dataclass(slots=True)
class MonitoringThresholdPolicy:
    failed_run_threshold: int = 1
    failed_snapshot_threshold: int = 1
    quarantined_snapshot_threshold: int = 1
    freshness_failure_threshold: int = 1
    latest_batch_failed_threshold: int = 1


@dataclass(slots=True)
class MonitoringThresholdEvaluation:
    code: str
    severity: str
    observed_value: int
    threshold_value: int
    triggered: bool
    summary: str


@dataclass(slots=True)
class MonitoringOverview:
    evaluated_at: datetime
    metro_id: str | None
    source_health: list[SourceHealthSnapshot] = field(default_factory=list)
    freshness: MetroFreshnessReport | None = None
    batch_status_counts: list[MonitoringStatusCount] = field(default_factory=list)
    run_status_counts: list[MonitoringStatusCount] = field(default_factory=list)
    latest_batch: PlannedBatch | None = None
    recent_failed_runs: list[PlannedRun] = field(default_factory=list)
    alert_count: int = 0
    alerts: list[MonitoringAlert] = field(default_factory=list)
    threshold_trigger_count: int = 0
    thresholds: list[MonitoringThresholdEvaluation] = field(default_factory=list)


def build_monitoring_overview(
    session: Session,
    *,
    metro_id: str | None = None,
    recent_failed_limit: int = 5,
    threshold_policy: MonitoringThresholdPolicy | None = None,
) -> MonitoringOverview:
    threshold_policy = threshold_policy or MonitoringThresholdPolicy()
    evaluated_at = datetime.now(UTC)
    source_health = summarize_source_health(session, metro_id=metro_id)
    freshness = evaluate_freshness(session, metro_id) if metro_id else None

    batches = list_batches(session)
    batch_status_counts = _build_batch_status_counts(batches)
    latest_batch = batches[0] if batches else None

    run_models = session.scalars(select(ScoreRun)).all()
    if metro_id:
        normalized_metro_id = metro_id.strip().upper()
        run_models = [run for run in run_models if run.metro_id == normalized_metro_id]

    run_status_counts = _build_run_status_counts(run_models)
    recent_failed_runs = _build_recent_failed_runs(run_models, recent_failed_limit)
    alerts = _build_alerts(
        source_health=source_health,
        freshness=freshness,
        latest_batch=latest_batch,
        recent_failed_runs=recent_failed_runs,
    )
    thresholds = _build_threshold_evaluations(
        source_health=source_health,
        freshness=freshness,
        latest_batch=latest_batch,
        recent_failed_runs=recent_failed_runs,
        threshold_policy=threshold_policy,
    )

    return MonitoringOverview(
        evaluated_at=evaluated_at,
        metro_id=metro_id.strip().upper() if metro_id else None,
        source_health=source_health,
        freshness=freshness,
        batch_status_counts=batch_status_counts,
        run_status_counts=run_status_counts,
        latest_batch=latest_batch,
        recent_failed_runs=recent_failed_runs,
        alert_count=len(alerts),
        alerts=alerts,
        threshold_trigger_count=sum(1 for threshold in thresholds if threshold.triggered),
        thresholds=thresholds,
    )


def _build_batch_status_counts(batches: list[PlannedBatch]) -> list[MonitoringStatusCount]:
    counts = Counter(batch.status for batch in batches)
    return [
        MonitoringStatusCount(status=status.value, count=counts.get(status.value, 0))
        for status in (
            ScoreBatchStatus.BUILDING,
            ScoreBatchStatus.FAILED,
            ScoreBatchStatus.COMPLETED,
            ScoreBatchStatus.ACTIVE,
        )
    ]


def _build_run_status_counts(run_models: list[ScoreRun]) -> list[MonitoringStatusCount]:
    counts = Counter(run.status.value for run in run_models)
    return [
        MonitoringStatusCount(status=status.value, count=counts.get(status.value, 0))
        for status in (
            ScoreRunStatus.RUNNING,
            ScoreRunStatus.FAILED,
            ScoreRunStatus.COMPLETED,
        )
    ]


def _build_recent_failed_runs(
    run_models: list[ScoreRun],
    recent_failed_limit: int,
) -> list[PlannedRun]:
    failed_runs = [run for run in run_models if run.status is ScoreRunStatus.FAILED]
    failed_runs.sort(
        key=lambda run: (
            run.completed_at or datetime.min.replace(tzinfo=UTC),
            run.updated_at,
            run.run_id,
        ),
        reverse=True,
    )
    return [_to_planned_run(run) for run in failed_runs[:recent_failed_limit]]


def _build_alerts(
    *,
    source_health: list[SourceHealthSnapshot],
    freshness: MetroFreshnessReport | None,
    latest_batch: PlannedBatch | None,
    recent_failed_runs: list[PlannedRun],
) -> list[MonitoringAlert]:
    alerts: list[MonitoringAlert] = []

    for snapshot in source_health:
        if snapshot.latest_snapshot_status == SourceSnapshotStatus.FAILED.value:
            alerts.append(
                MonitoringAlert(
                    severity="error",
                    code="SOURCE_SNAPSHOT_FAILED",
                    summary=(
                        f"Source `{snapshot.source_id}` in metro `{snapshot.metro_id}` "
                        "has a failed latest snapshot."
                    ),
                    metro_id=snapshot.metro_id,
                )
            )
        elif snapshot.latest_snapshot_status == SourceSnapshotStatus.QUARANTINED.value:
            alerts.append(
                MonitoringAlert(
                    severity="warning",
                    code="SOURCE_SNAPSHOT_QUARANTINED",
                    summary=(
                        f"Source `{snapshot.source_id}` in metro `{snapshot.metro_id}` "
                        "has quarantined rows in the latest snapshot."
                    ),
                    metro_id=snapshot.metro_id,
                )
            )

    if freshness is not None and not freshness.passed:
        for status in freshness.statuses:
            if status.passed:
                continue
            alerts.append(
                MonitoringAlert(
                    severity="error" if status.required else "warning",
                    code="FRESHNESS_FAILURE",
                    summary=(
                        f"Freshness check failed for source `{status.source_id}` "
                        f"with code `{status.freshness_code}`."
                    ),
                    metro_id=status.metro_id,
                )
            )

    if latest_batch is not None and latest_batch.status == ScoreBatchStatus.FAILED.value:
        alerts.append(
            MonitoringAlert(
                severity="error",
                code="LATEST_BATCH_FAILED",
                summary=f"Latest batch `{latest_batch.batch_id}` is failed.",
                batch_id=latest_batch.batch_id,
            )
        )

    for run in recent_failed_runs:
        alerts.append(
            MonitoringAlert(
                severity="error",
                code="FAILED_RUN",
                summary=(
                    f"Run `{run.run_id}` for metro `{run.metro_id}` failed with "
                    f"`{run.failure_reason or 'UNKNOWN'}`."
                ),
                metro_id=run.metro_id,
                batch_id=run.batch_id,
                run_id=run.run_id,
            )
        )

    return alerts


def _build_threshold_evaluations(
    *,
    source_health: list[SourceHealthSnapshot],
    freshness: MetroFreshnessReport | None,
    latest_batch: PlannedBatch | None,
    recent_failed_runs: list[PlannedRun],
    threshold_policy: MonitoringThresholdPolicy,
) -> list[MonitoringThresholdEvaluation]:
    failed_snapshot_count = sum(
        1
        for snapshot in source_health
        if snapshot.latest_snapshot_status == SourceSnapshotStatus.FAILED.value
    )
    quarantined_snapshot_count = sum(
        1
        for snapshot in source_health
        if snapshot.latest_snapshot_status == SourceSnapshotStatus.QUARANTINED.value
    )
    freshness_failure_count = (
        sum(1 for status in freshness.statuses if not status.passed)
        if freshness is not None
        else 0
    )
    latest_batch_failed_count = (
        1
        if latest_batch is not None
        and latest_batch.status == ScoreBatchStatus.FAILED.value
        else 0
    )

    return [
        _evaluate_threshold(
            code="FAILED_RUN_COUNT",
            severity="error",
            observed_value=len(recent_failed_runs),
            threshold_value=threshold_policy.failed_run_threshold,
            summary_template=(
                "{observed} failed run(s) observed in the recent-failure window; "
                "threshold is {threshold}."
            ),
        ),
        _evaluate_threshold(
            code="FAILED_SNAPSHOT_COUNT",
            severity="error",
            observed_value=failed_snapshot_count,
            threshold_value=threshold_policy.failed_snapshot_threshold,
            summary_template=(
                "{observed} failed latest source snapshot(s) observed; threshold is {threshold}."
            ),
        ),
        _evaluate_threshold(
            code="QUARANTINED_SNAPSHOT_COUNT",
            severity="warning",
            observed_value=quarantined_snapshot_count,
            threshold_value=threshold_policy.quarantined_snapshot_threshold,
            summary_template=(
                "{observed} quarantined latest snapshot(s) observed; threshold is {threshold}."
            ),
        ),
        _evaluate_threshold(
            code="FRESHNESS_FAILURE_COUNT",
            severity="error",
            observed_value=freshness_failure_count,
            threshold_value=threshold_policy.freshness_failure_threshold,
            summary_template=(
                "{observed} freshness failure(s) observed in the scoped metro check; "
                "threshold is {threshold}."
            ),
        ),
        _evaluate_threshold(
            code="LATEST_BATCH_FAILED_COUNT",
            severity="error",
            observed_value=latest_batch_failed_count,
            threshold_value=threshold_policy.latest_batch_failed_threshold,
            summary_template=(
                "{observed} failed latest batch condition(s) observed; threshold is {threshold}."
            ),
        ),
    ]


def _evaluate_threshold(
    *,
    code: str,
    severity: str,
    observed_value: int,
    threshold_value: int,
    summary_template: str,
) -> MonitoringThresholdEvaluation:
    return MonitoringThresholdEvaluation(
        code=code,
        severity=severity,
        observed_value=observed_value,
        threshold_value=threshold_value,
        triggered=observed_value >= threshold_value,
        summary=summary_template.format(observed=observed_value, threshold=threshold_value),
    )


def _to_planned_run(run: ScoreRun) -> PlannedRun:
    return PlannedRun(
        run_id=str(run.run_id),
        batch_id=str(run.batch_id),
        metro_id=run.metro_id,
        profile_name=run.profile_name,
        status=run.status.value,
        failure_reason=run.failure_reason,
        started_at=run.started_at,
        completed_at=run.completed_at,
    )
