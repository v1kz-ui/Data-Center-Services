from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.core.security import CurrentPrincipal, require_admin_access, require_operator_access
from app.core.settings import Settings, get_settings
from app.db.session import get_db
from app.schemas.uat import (
    BulkUatReleaseArchiveRetentionActionRequest,
    BulkUatReleaseArchiveRetentionActionResponse,
    CreateUatAcceptanceArtifactRequest,
    CreateUatCycleRequest,
    CreateUatDefectRequest,
    CreateUatDistributionPacketRequest,
    CreateUatDistributionRecipientRequest,
    CreateUatHandoffSnapshotRequest,
    CreateUatLaunchDecisionRequest,
    CreateUatReleaseArchiveExportDeliveryEventRequest,
    CreateUatReleaseArchiveExportRequest,
    CreateUatReleaseArchiveRequest,
    CreateUatReleaseArchiveRetentionActionRequest,
    ExecuteUatReleaseArchiveNotificationDispatchRequest,
    ExecuteUatReleaseArchiveReexportsRequest,
    FinalizeUatCycleRequest,
    RecordUatExecutionResultRequest,
    SupersedeUatReleaseArchiveRequest,
    UatCycleDetailResponse,
    UatCycleSummaryResponse,
    UatDistributionPacketDetailResponse,
    UatDistributionPacketSummaryResponse,
    UatHandoffSnapshotDetailResponse,
    UatHandoffSnapshotSummaryResponse,
    UatLaunchCloseoutReportResponse,
    UatLaunchReadinessResponse,
    UatReleaseArchiveClosureHistoryReportResponse,
    UatReleaseArchiveDeliveryLedgerResponse,
    UatReleaseArchiveDetailResponse,
    UatReleaseArchiveEvidenceItemResponse,
    UatReleaseArchiveExportDeliveryEventResponse,
    UatReleaseArchiveExportDetailResponse,
    UatReleaseArchiveExportSummaryResponse,
    UatReleaseArchiveFollowupDashboardResponse,
    UatReleaseArchiveNotificationDigestResponse,
    UatReleaseArchiveNotificationDispatchRunResponse,
    UatReleaseArchiveReexportExecutionRunResponse,
    UatReleaseArchiveRetentionActionResponse,
    UatReleaseArchiveRetentionQueueResponse,
    UatReleaseArchiveSummaryResponse,
    UatReleaseArchiveSupportHandbackReportResponse,
    UatSignoffReportResponse,
    UpdateUatDefectRequest,
    UpdateUatDistributionRecipientRequest,
    UpdateUatReleaseArchiveExportRequest,
)
from app.services.uat import (
    UatCycleConflictError,
    UatCycleFinalizeError,
    UatCycleNotFoundError,
    UatDefectNotFoundError,
    UatScenarioExecutionNotFoundError,
    create_uat_cycle,
    create_uat_defect,
    finalize_uat_cycle,
    get_uat_cycle,
    list_uat_cycles,
    record_uat_execution_result,
    update_uat_defect,
)
from app.services.uat_distribution import (
    UatDistributionPacketConflictError,
    UatDistributionPacketNotFoundError,
    UatDistributionRecipientNotFoundError,
    UatDistributionRecipientSeed,
    create_uat_distribution_packet,
    create_uat_distribution_recipient,
    get_uat_distribution_packet,
    list_uat_distribution_packets,
    update_uat_distribution_recipient,
)
from app.services.uat_handoff import (
    UatHandoffSnapshotConflictError,
    UatHandoffSnapshotNotFoundError,
    create_uat_acceptance_artifact,
    create_uat_handoff_snapshot,
    get_uat_handoff_snapshot,
    list_uat_handoff_snapshots,
)
from app.services.uat_launch import (
    build_uat_launch_closeout_report,
    create_uat_launch_decision_record,
    get_uat_launch_readiness,
)
from app.services.uat_release_archive import (
    UatReleaseArchiveConflictError,
    UatReleaseArchiveNotFoundError,
    UatReleaseArchiveValidationError,
    create_uat_release_archive,
    get_uat_release_archive,
    list_uat_release_archive_evidence_items,
    list_uat_release_archives,
    supersede_uat_release_archive,
)
from app.services.uat_release_archive_automation import (
    UatReleaseArchiveAutomationValidationError,
    build_uat_release_archive_followup_notification_digest,
    execute_due_uat_release_archive_reexports,
    execute_uat_release_archive_followup_notification_dispatch,
)
from app.services.uat_release_archive_closure_history import (
    build_uat_release_archive_closure_history_report,
)
from app.services.uat_release_archive_delivery_ledger import (
    UatReleaseArchiveDeliveryLedgerValidationError,
    build_uat_release_archive_delivery_ledger,
)
from app.services.uat_release_archive_followup import (
    UatReleaseArchiveFollowupValidationError,
    apply_bulk_uat_release_archive_retention_action,
    build_uat_release_archive_followup_dashboard,
)
from app.services.uat_release_archive_handback import (
    build_uat_release_archive_support_handback_report,
)
from app.services.uat_release_archive_operations import (
    UatReleaseArchiveExportConflictError,
    UatReleaseArchiveExportDeliveryEventValidationError,
    UatReleaseArchiveExportNotFoundError,
    UatReleaseArchiveExportValidationError,
    UatReleaseArchiveRetentionActionValidationError,
    build_uat_release_archive_retention_queue,
    create_uat_release_archive_export,
    create_uat_release_archive_retention_action,
    get_uat_release_archive_export,
    list_uat_release_archive_export_delivery_events,
    list_uat_release_archive_exports,
    list_uat_release_archive_retention_actions,
    record_uat_release_archive_export_delivery_event,
    update_uat_release_archive_export,
)
from app.services.uat_reporting import build_uat_signoff_report

router = APIRouter(dependencies=[Depends(require_operator_access)])
DbSession = Annotated[Session, Depends(get_db)]
AppSettings = Annotated[Settings, Depends(get_settings)]


@router.post(
    "/admin/uat/cycles",
    response_model=UatCycleDetailResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_admin_uat_cycle(
    request: CreateUatCycleRequest,
    db: DbSession,
    principal: CurrentPrincipal,
    settings: AppSettings,
) -> UatCycleDetailResponse:
    try:
        cycle = create_uat_cycle(
            db,
            cycle_name=request.cycle_name,
            created_by=principal.display_name or principal.subject,
            environment_name=request.environment_name or settings.uat_environment_name,
            scenario_pack_path=request.scenario_pack_path or settings.uat_scenario_pack_path,
            summary_notes=request.summary_notes,
        )
    except (FileNotFoundError, UatCycleConflictError) as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    return UatCycleDetailResponse.model_validate(cycle)


@router.get("/admin/uat/cycles", response_model=list[UatCycleSummaryResponse])
def list_admin_uat_cycles(db: DbSession) -> list[UatCycleSummaryResponse]:
    return [UatCycleSummaryResponse.model_validate(cycle) for cycle in list_uat_cycles(db)]


@router.get("/admin/uat/cycles/{cycle_id}", response_model=UatCycleDetailResponse)
def get_admin_uat_cycle(
    cycle_id: UUID,
    db: DbSession,
) -> UatCycleDetailResponse:
    try:
        cycle = get_uat_cycle(db, str(cycle_id))
    except UatCycleNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return UatCycleDetailResponse.model_validate(cycle)


@router.post(
    "/admin/uat/cycles/{cycle_id}/scenarios/{scenario_id}/results",
    response_model=UatCycleDetailResponse,
)
def record_admin_uat_execution_result(
    cycle_id: UUID,
    scenario_id: str,
    request: RecordUatExecutionResultRequest,
    db: DbSession,
    principal: CurrentPrincipal,
) -> UatCycleDetailResponse:
    try:
        cycle = record_uat_execution_result(
            db,
            cycle_id=str(cycle_id),
            scenario_id=scenario_id,
            status=request.status,
            executed_by=request.executed_by or principal.display_name or principal.subject,
            execution_notes=request.execution_notes,
            evidence_reference=request.evidence_reference,
        )
    except UatCycleNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except UatScenarioExecutionNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except UatCycleConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    return UatCycleDetailResponse.model_validate(cycle)


@router.post(
    "/admin/uat/cycles/{cycle_id}/defects",
    response_model=UatCycleDetailResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_admin_uat_defect(
    cycle_id: UUID,
    request: CreateUatDefectRequest,
    db: DbSession,
    principal: CurrentPrincipal,
) -> UatCycleDetailResponse:
    try:
        cycle = create_uat_defect(
            db,
            cycle_id=str(cycle_id),
            reported_by=principal.display_name or principal.subject,
            scenario_id=request.scenario_id,
            severity=request.severity,
            title=request.title,
            description=request.description,
            owner_name=request.owner_name,
            external_reference=request.external_reference,
        )
    except UatCycleNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except UatScenarioExecutionNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except UatCycleConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    return UatCycleDetailResponse.model_validate(cycle)


@router.patch(
    "/admin/uat/cycles/{cycle_id}/defects/{defect_id}",
    response_model=UatCycleDetailResponse,
)
def update_admin_uat_defect(
    cycle_id: UUID,
    defect_id: UUID,
    request: UpdateUatDefectRequest,
    db: DbSession,
    principal: CurrentPrincipal,
) -> UatCycleDetailResponse:
    try:
        cycle = update_uat_defect(
            db,
            cycle_id=str(cycle_id),
            defect_id=str(defect_id),
            updated_by=principal.display_name or principal.subject,
            status=request.status,
            owner_name=request.owner_name,
            external_reference=request.external_reference,
            resolution_notes=request.resolution_notes,
        )
    except UatCycleNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except UatDefectNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except UatCycleConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    return UatCycleDetailResponse.model_validate(cycle)


@router.post(
    "/admin/uat/cycles/{cycle_id}/finalize",
    response_model=UatCycleDetailResponse,
)
def finalize_admin_uat_cycle(
    cycle_id: UUID,
    request: FinalizeUatCycleRequest,
    db: DbSession,
    principal: CurrentPrincipal,
) -> UatCycleDetailResponse:
    try:
        cycle = finalize_uat_cycle(
            db,
            cycle_id=str(cycle_id),
            finalized_by=principal.display_name or principal.subject,
            status=request.status,
            summary_notes=request.summary_notes,
        )
    except UatCycleNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except (UatCycleConflictError, UatCycleFinalizeError) as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    return UatCycleDetailResponse.model_validate(cycle)


@router.get(
    "/admin/uat/cycles/{cycle_id}/signoff-report",
    response_model=UatSignoffReportResponse,
    dependencies=[Depends(require_admin_access)],
)
def export_admin_uat_signoff_report(
    cycle_id: UUID,
    db: DbSession,
    principal: CurrentPrincipal,
) -> UatSignoffReportResponse:
    try:
        report = build_uat_signoff_report(
            db,
            str(cycle_id),
            exported_by=principal.display_name or principal.subject,
        )
    except UatCycleNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return UatSignoffReportResponse.model_validate(report)


@router.post(
    "/admin/uat/cycles/{cycle_id}/handoff-snapshots",
    response_model=UatHandoffSnapshotDetailResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_admin_access)],
)
def create_admin_uat_handoff_snapshot(
    cycle_id: UUID,
    request: CreateUatHandoffSnapshotRequest,
    db: DbSession,
    principal: CurrentPrincipal,
) -> UatHandoffSnapshotDetailResponse:
    try:
        snapshot = create_uat_handoff_snapshot(
            db,
            cycle_id=str(cycle_id),
            snapshot_name=request.snapshot_name,
            created_by=principal.display_name or principal.subject,
            distribution_summary_override=request.distribution_summary_override,
        )
    except UatCycleNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except UatHandoffSnapshotConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    return UatHandoffSnapshotDetailResponse.model_validate(snapshot)


@router.get(
    "/admin/uat/cycles/{cycle_id}/handoff-snapshots",
    response_model=list[UatHandoffSnapshotSummaryResponse],
    dependencies=[Depends(require_admin_access)],
)
def list_admin_uat_handoff_snapshots(
    cycle_id: UUID,
    db: DbSession,
) -> list[UatHandoffSnapshotSummaryResponse]:
    try:
        snapshots = list_uat_handoff_snapshots(db, cycle_id=str(cycle_id))
    except UatCycleNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return [
        UatHandoffSnapshotSummaryResponse.model_validate(snapshot)
        for snapshot in snapshots
    ]


@router.get(
    "/admin/uat/handoff-snapshots/{snapshot_id}",
    response_model=UatHandoffSnapshotDetailResponse,
    dependencies=[Depends(require_admin_access)],
)
def get_admin_uat_handoff_snapshot(
    snapshot_id: UUID,
    db: DbSession,
) -> UatHandoffSnapshotDetailResponse:
    try:
        snapshot = get_uat_handoff_snapshot(db, str(snapshot_id))
    except UatHandoffSnapshotNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return UatHandoffSnapshotDetailResponse.model_validate(snapshot)


@router.post(
    "/admin/uat/handoff-snapshots/{snapshot_id}/distribution-packets",
    response_model=UatDistributionPacketDetailResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_admin_access)],
)
def create_admin_uat_distribution_packet(
    snapshot_id: UUID,
    request: CreateUatDistributionPacketRequest,
    db: DbSession,
    principal: CurrentPrincipal,
) -> UatDistributionPacketDetailResponse:
    try:
        packet = create_uat_distribution_packet(
            db,
            snapshot_id=str(snapshot_id),
            packet_name=request.packet_name,
            channel=request.channel,
            created_by=principal.display_name or principal.subject,
            subject_line_override=request.subject_line_override,
            summary_excerpt_override=request.summary_excerpt_override,
            briefing_body_override=request.briefing_body_override,
            distribution_notes=request.distribution_notes,
            recipients=[
                UatDistributionRecipientSeed(
                    recipient_name=recipient.recipient_name,
                    recipient_role=recipient.recipient_role,
                    recipient_organization=recipient.recipient_organization,
                    recipient_contact=recipient.recipient_contact,
                    required_for_ack=recipient.required_for_ack,
                )
                for recipient in request.recipients
            ],
        )
    except UatHandoffSnapshotNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except UatDistributionPacketConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    return UatDistributionPacketDetailResponse.model_validate(packet)


@router.get(
    "/admin/uat/handoff-snapshots/{snapshot_id}/distribution-packets",
    response_model=list[UatDistributionPacketSummaryResponse],
    dependencies=[Depends(require_admin_access)],
)
def list_admin_uat_distribution_packets(
    snapshot_id: UUID,
    db: DbSession,
) -> list[UatDistributionPacketSummaryResponse]:
    try:
        packets = list_uat_distribution_packets(db, snapshot_id=str(snapshot_id))
    except UatHandoffSnapshotNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return [UatDistributionPacketSummaryResponse.model_validate(packet) for packet in packets]


@router.get(
    "/admin/uat/distribution-packets/{packet_id}",
    response_model=UatDistributionPacketDetailResponse,
    dependencies=[Depends(require_admin_access)],
)
def get_admin_uat_distribution_packet(
    packet_id: UUID,
    db: DbSession,
) -> UatDistributionPacketDetailResponse:
    try:
        packet = get_uat_distribution_packet(db, str(packet_id))
    except UatDistributionPacketNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return UatDistributionPacketDetailResponse.model_validate(packet)


@router.post(
    "/admin/uat/distribution-packets/{packet_id}/recipients",
    response_model=UatDistributionPacketDetailResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_admin_access)],
)
def create_admin_uat_distribution_recipient(
    packet_id: UUID,
    request: CreateUatDistributionRecipientRequest,
    db: DbSession,
    principal: CurrentPrincipal,
) -> UatDistributionPacketDetailResponse:
    try:
        packet = create_uat_distribution_recipient(
            db,
            packet_id=str(packet_id),
            recipient_name=request.recipient_name,
            recipient_role=request.recipient_role,
            recipient_organization=request.recipient_organization,
            recipient_contact=request.recipient_contact,
            required_for_ack=request.required_for_ack,
            recorded_by=principal.display_name or principal.subject,
        )
    except UatDistributionPacketNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return UatDistributionPacketDetailResponse.model_validate(packet)


@router.patch(
    "/admin/uat/distribution-packets/{packet_id}/recipients/{recipient_id}",
    response_model=UatDistributionPacketDetailResponse,
    dependencies=[Depends(require_admin_access)],
)
def update_admin_uat_distribution_recipient(
    packet_id: UUID,
    recipient_id: UUID,
    request: UpdateUatDistributionRecipientRequest,
    db: DbSession,
    principal: CurrentPrincipal,
) -> UatDistributionPacketDetailResponse:
    try:
        packet = update_uat_distribution_recipient(
            db,
            packet_id=str(packet_id),
            recipient_id=str(recipient_id),
            updated_by=principal.display_name or principal.subject,
            delivery_status=request.delivery_status,
            delivery_notes=request.delivery_notes,
            acknowledgement_notes=request.acknowledgement_notes,
            acknowledged_by=request.acknowledged_by,
        )
    except UatDistributionPacketNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except UatDistributionRecipientNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return UatDistributionPacketDetailResponse.model_validate(packet)


@router.get(
    "/admin/uat/handoff-snapshots/{snapshot_id}/launch-readiness",
    response_model=UatLaunchReadinessResponse,
    dependencies=[Depends(require_admin_access)],
)
def get_admin_uat_launch_readiness(
    snapshot_id: UUID,
    db: DbSession,
) -> UatLaunchReadinessResponse:
    try:
        readiness = get_uat_launch_readiness(db, str(snapshot_id))
    except UatHandoffSnapshotNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return UatLaunchReadinessResponse.model_validate(readiness)


@router.post(
    "/admin/uat/handoff-snapshots/{snapshot_id}/launch-decisions",
    response_model=UatLaunchReadinessResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_admin_access)],
)
def create_admin_uat_launch_decision(
    snapshot_id: UUID,
    request: CreateUatLaunchDecisionRequest,
    db: DbSession,
    principal: CurrentPrincipal,
) -> UatLaunchReadinessResponse:
    try:
        readiness = create_uat_launch_decision_record(
            db,
            snapshot_id=str(snapshot_id),
            decision=request.decision,
            reviewer_name=request.reviewer_name,
            reviewer_role=request.reviewer_role,
            reviewer_organization=request.reviewer_organization,
            decision_notes=request.decision_notes,
            recorded_by=principal.display_name or principal.subject,
        )
    except UatHandoffSnapshotNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return UatLaunchReadinessResponse.model_validate(readiness)


@router.get(
    "/admin/uat/handoff-snapshots/{snapshot_id}/launch-closeout-report",
    response_model=UatLaunchCloseoutReportResponse,
    dependencies=[Depends(require_admin_access)],
)
def export_admin_uat_launch_closeout_report(
    snapshot_id: UUID,
    db: DbSession,
    principal: CurrentPrincipal,
) -> UatLaunchCloseoutReportResponse:
    try:
        report = build_uat_launch_closeout_report(
            db,
            str(snapshot_id),
            exported_by=principal.display_name or principal.subject,
        )
    except UatHandoffSnapshotNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return UatLaunchCloseoutReportResponse.model_validate(report)


@router.post(
    "/admin/uat/handoff-snapshots/{snapshot_id}/release-archives",
    response_model=UatReleaseArchiveDetailResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_admin_access)],
)
def create_admin_uat_release_archive(
    snapshot_id: UUID,
    request: CreateUatReleaseArchiveRequest,
    db: DbSession,
    principal: CurrentPrincipal,
) -> UatReleaseArchiveDetailResponse:
    try:
        archive = create_uat_release_archive(
            db,
            snapshot_id=str(snapshot_id),
            archive_name=request.archive_name,
            created_by=principal.display_name or principal.subject,
            support_handoff_owner=request.support_handoff_owner,
            operations_runbook_reference=request.operations_runbook_reference,
            support_handoff_summary_override=request.support_handoff_summary_override,
            release_manifest_notes=request.release_manifest_notes,
            retention_review_at=request.retention_review_at,
        )
    except UatHandoffSnapshotNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except UatReleaseArchiveConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    return UatReleaseArchiveDetailResponse.model_validate(archive)


@router.get(
    "/admin/uat/handoff-snapshots/{snapshot_id}/release-archives",
    response_model=list[UatReleaseArchiveSummaryResponse],
    dependencies=[Depends(require_admin_access)],
)
def list_admin_uat_release_archives(
    snapshot_id: UUID,
    db: DbSession,
) -> list[UatReleaseArchiveSummaryResponse]:
    try:
        archives = list_uat_release_archives(db, snapshot_id=str(snapshot_id))
    except UatHandoffSnapshotNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return [UatReleaseArchiveSummaryResponse.model_validate(archive) for archive in archives]


@router.get(
    "/admin/uat/release-archives",
    response_model=list[UatReleaseArchiveSummaryResponse],
    dependencies=[Depends(require_admin_access)],
)
def search_admin_uat_release_archives(
    db: DbSession,
    cycle_id: UUID | None = None,
    recommended_outcome: str | None = None,
    retention_status: str | None = None,
    include_superseded: bool = False,
    search: str | None = None,
) -> list[UatReleaseArchiveSummaryResponse]:
    try:
        archives = list_uat_release_archives(
            db,
            cycle_id=str(cycle_id) if cycle_id is not None else None,
            recommended_outcome=recommended_outcome,
            retention_status=retention_status,
            include_superseded=include_superseded,
            search=search,
        )
    except UatReleaseArchiveValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return [UatReleaseArchiveSummaryResponse.model_validate(archive) for archive in archives]


@router.get(
    "/admin/uat/release-archives/retention-queue",
    response_model=UatReleaseArchiveRetentionQueueResponse,
    dependencies=[Depends(require_admin_access)],
)
def get_admin_uat_release_archive_retention_queue(
    db: DbSession,
    review_window_days: int = 30,
    include_superseded: bool = False,
    include_active: bool = False,
) -> UatReleaseArchiveRetentionQueueResponse:
    queue = build_uat_release_archive_retention_queue(
        db,
        review_window_days=review_window_days,
        include_superseded=include_superseded,
        include_active=include_active,
    )
    return UatReleaseArchiveRetentionQueueResponse.model_validate(queue)


@router.get(
    "/admin/uat/release-archives/followup-dashboard",
    response_model=UatReleaseArchiveFollowupDashboardResponse,
    dependencies=[Depends(require_admin_access)],
)
def get_admin_uat_release_archive_followup_dashboard(
    db: DbSession,
    review_window_days: int = 30,
    include_resolved: bool = False,
) -> UatReleaseArchiveFollowupDashboardResponse:
    dashboard = build_uat_release_archive_followup_dashboard(
        db,
        review_window_days=review_window_days,
        include_resolved=include_resolved,
    )
    return UatReleaseArchiveFollowupDashboardResponse.model_validate(dashboard)


@router.get(
    "/admin/uat/release-archives/delivery-ledger",
    response_model=UatReleaseArchiveDeliveryLedgerResponse,
    dependencies=[Depends(require_admin_access)],
)
def get_admin_uat_release_archive_delivery_ledger(
    db: DbSession,
    principal: CurrentPrincipal,
    review_window_days: int = 30,
    stale_reply_after_hours: int = 48,
    include_resolved: bool = False,
    as_of: datetime | None = None,
) -> UatReleaseArchiveDeliveryLedgerResponse:
    try:
        ledger = build_uat_release_archive_delivery_ledger(
            db,
            generated_by=principal.display_name or principal.subject,
            review_window_days=review_window_days,
            stale_reply_after_hours=stale_reply_after_hours,
            include_resolved=include_resolved,
            as_of=as_of,
        )
    except UatReleaseArchiveDeliveryLedgerValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return UatReleaseArchiveDeliveryLedgerResponse.model_validate(ledger)


@router.get(
    "/admin/uat/release-archives/closure-history-report",
    response_model=UatReleaseArchiveClosureHistoryReportResponse,
    dependencies=[Depends(require_admin_access)],
)
def get_admin_uat_release_archive_closure_history_report(
    db: DbSession,
    principal: CurrentPrincipal,
    review_window_days: int = 30,
    stale_reply_after_hours: int = 48,
    include_closed: bool = True,
    as_of: datetime | None = None,
) -> UatReleaseArchiveClosureHistoryReportResponse:
    try:
        report = build_uat_release_archive_closure_history_report(
            db,
            generated_by=principal.display_name or principal.subject,
            review_window_days=review_window_days,
            stale_reply_after_hours=stale_reply_after_hours,
            include_closed=include_closed,
            as_of=as_of,
        )
    except UatReleaseArchiveDeliveryLedgerValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return UatReleaseArchiveClosureHistoryReportResponse.model_validate(report)


@router.get(
    "/admin/uat/release-archives/support-handback-report",
    response_model=UatReleaseArchiveSupportHandbackReportResponse,
    dependencies=[Depends(require_admin_access)],
)
def get_admin_uat_release_archive_support_handback_report(
    db: DbSession,
    principal: CurrentPrincipal,
    review_window_days: int = 30,
    stale_reply_after_hours: int = 48,
    include_resolved: bool = True,
    as_of: datetime | None = None,
) -> UatReleaseArchiveSupportHandbackReportResponse:
    try:
        report = build_uat_release_archive_support_handback_report(
            db,
            generated_by=principal.display_name or principal.subject,
            review_window_days=review_window_days,
            stale_reply_after_hours=stale_reply_after_hours,
            include_resolved=include_resolved,
            as_of=as_of,
        )
    except UatReleaseArchiveDeliveryLedgerValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return UatReleaseArchiveSupportHandbackReportResponse.model_validate(report)


@router.post(
    "/admin/uat/release-archives/execute-due-reexports",
    response_model=UatReleaseArchiveReexportExecutionRunResponse,
    dependencies=[Depends(require_admin_access)],
)
def execute_admin_uat_release_archive_due_reexports(
    request: ExecuteUatReleaseArchiveReexportsRequest,
    db: DbSession,
    principal: CurrentPrincipal,
) -> UatReleaseArchiveReexportExecutionRunResponse:
    try:
        run = execute_due_uat_release_archive_reexports(
            db,
            executed_by=principal.display_name or principal.subject,
            dry_run=request.dry_run,
            limit=request.limit,
            as_of=request.run_at,
        )
    except UatReleaseArchiveAutomationValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return UatReleaseArchiveReexportExecutionRunResponse.model_validate(run)


@router.post(
    "/admin/uat/release-archives/execute-followup-notification-dispatch",
    response_model=UatReleaseArchiveNotificationDispatchRunResponse,
    dependencies=[Depends(require_admin_access)],
)
def execute_admin_uat_release_archive_followup_notification_dispatch(
    request: ExecuteUatReleaseArchiveNotificationDispatchRequest,
    db: DbSession,
    principal: CurrentPrincipal,
) -> UatReleaseArchiveNotificationDispatchRunResponse:
    try:
        run = execute_uat_release_archive_followup_notification_dispatch(
            db,
            executed_by=principal.display_name or principal.subject,
            delivery_channel=request.delivery_channel,
            dry_run=request.dry_run,
            review_window_days=request.review_window_days,
            recipient_limit=request.recipient_limit,
            as_of=request.run_at,
        )
    except UatReleaseArchiveAutomationValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return UatReleaseArchiveNotificationDispatchRunResponse.model_validate(run)


@router.get(
    "/admin/uat/release-archives/followup-notification-digest",
    response_model=UatReleaseArchiveNotificationDigestResponse,
    dependencies=[Depends(require_admin_access)],
)
def get_admin_uat_release_archive_followup_notification_digest(
    db: DbSession,
    principal: CurrentPrincipal,
    review_window_days: int = 30,
) -> UatReleaseArchiveNotificationDigestResponse:
    digest = build_uat_release_archive_followup_notification_digest(
        db,
        generated_by=principal.display_name or principal.subject,
        review_window_days=review_window_days,
    )
    return UatReleaseArchiveNotificationDigestResponse.model_validate(digest)


@router.post(
    "/admin/uat/release-archives/bulk-retention-actions",
    response_model=BulkUatReleaseArchiveRetentionActionResponse,
    dependencies=[Depends(require_admin_access)],
)
def bulk_admin_uat_release_archive_retention_action(
    request: BulkUatReleaseArchiveRetentionActionRequest,
    db: DbSession,
    principal: CurrentPrincipal,
) -> BulkUatReleaseArchiveRetentionActionResponse:
    try:
        outcome = apply_bulk_uat_release_archive_retention_action(
            db,
            archive_ids=request.archive_ids,
            action_type=request.action_type,
            recorded_by=principal.display_name or principal.subject,
            action_notes=request.action_notes,
            next_retention_review_at=request.next_retention_review_at,
        )
    except UatReleaseArchiveFollowupValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return BulkUatReleaseArchiveRetentionActionResponse.model_validate(outcome)


@router.post(
    "/admin/uat/release-archives/{archive_id}/exports",
    response_model=UatReleaseArchiveExportDetailResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_admin_access)],
)
def create_admin_uat_release_archive_export(
    archive_id: UUID,
    request: CreateUatReleaseArchiveExportRequest,
    db: DbSession,
    principal: CurrentPrincipal,
) -> UatReleaseArchiveExportDetailResponse:
    try:
        export_record = create_uat_release_archive_export(
            db,
            archive_id=str(archive_id),
            export_name=request.export_name,
            export_scope=request.export_scope,
            destination_system=request.destination_system,
            destination_reference=request.destination_reference,
            trigger_reason=request.trigger_reason,
            handoff_notes=request.handoff_notes,
            exported_by=principal.display_name or principal.subject,
        )
    except UatReleaseArchiveNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except UatReleaseArchiveExportConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    return UatReleaseArchiveExportDetailResponse.model_validate(export_record)


@router.get(
    "/admin/uat/release-archives/{archive_id}/exports",
    response_model=list[UatReleaseArchiveExportSummaryResponse],
    dependencies=[Depends(require_admin_access)],
)
def list_admin_uat_release_archive_exports(
    archive_id: UUID,
    db: DbSession,
) -> list[UatReleaseArchiveExportSummaryResponse]:
    try:
        export_records = list_uat_release_archive_exports(db, archive_id=str(archive_id))
    except UatReleaseArchiveNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return [
        UatReleaseArchiveExportSummaryResponse.model_validate(export_record)
        for export_record in export_records
    ]


@router.get(
    "/admin/uat/release-archive-exports/{export_id}",
    response_model=UatReleaseArchiveExportDetailResponse,
    dependencies=[Depends(require_admin_access)],
)
def get_admin_uat_release_archive_export(
    export_id: UUID,
    db: DbSession,
) -> UatReleaseArchiveExportDetailResponse:
    try:
        export_record = get_uat_release_archive_export(db, str(export_id))
    except UatReleaseArchiveExportNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return UatReleaseArchiveExportDetailResponse.model_validate(export_record)


@router.patch(
    "/admin/uat/release-archive-exports/{export_id}",
    response_model=UatReleaseArchiveExportDetailResponse,
    dependencies=[Depends(require_admin_access)],
)
def update_admin_uat_release_archive_export(
    export_id: UUID,
    request: UpdateUatReleaseArchiveExportRequest,
    db: DbSession,
    principal: CurrentPrincipal,
) -> UatReleaseArchiveExportDetailResponse:
    try:
        export_record = update_uat_release_archive_export(
            db,
            export_id=str(export_id),
            updated_by=principal.display_name or principal.subject,
            handoff_status=request.handoff_status,
            destination_reference=request.destination_reference,
            handoff_notes=request.handoff_notes,
            delivery_confirmed_by=request.delivery_confirmed_by,
            next_retry_at=request.next_retry_at,
        )
    except UatReleaseArchiveExportNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except UatReleaseArchiveExportValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return UatReleaseArchiveExportDetailResponse.model_validate(export_record)


@router.post(
    "/admin/uat/release-archive-exports/{export_id}/delivery-events",
    response_model=UatReleaseArchiveExportDeliveryEventResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_admin_access)],
)
def create_admin_uat_release_archive_export_delivery_event(
    export_id: UUID,
    request: CreateUatReleaseArchiveExportDeliveryEventRequest,
    db: DbSession,
    principal: CurrentPrincipal,
) -> UatReleaseArchiveExportDeliveryEventResponse:
    try:
        event_record = record_uat_release_archive_export_delivery_event(
            db,
            export_id=str(export_id),
            event_type=request.event_type,
            recorded_by=principal.display_name or principal.subject,
            target_name=request.target_name,
            delivery_channel=request.delivery_channel,
            external_reference=request.external_reference,
            event_notes=request.event_notes,
            occurred_at=request.occurred_at,
        )
    except UatReleaseArchiveExportNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except UatReleaseArchiveExportDeliveryEventValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return UatReleaseArchiveExportDeliveryEventResponse.model_validate(event_record)


@router.get(
    "/admin/uat/release-archive-exports/{export_id}/delivery-events",
    response_model=list[UatReleaseArchiveExportDeliveryEventResponse],
    dependencies=[Depends(require_admin_access)],
)
def list_admin_uat_release_archive_export_delivery_events(
    export_id: UUID,
    db: DbSession,
) -> list[UatReleaseArchiveExportDeliveryEventResponse]:
    try:
        event_records = list_uat_release_archive_export_delivery_events(
            db,
            export_id=str(export_id),
        )
    except UatReleaseArchiveExportNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return [
        UatReleaseArchiveExportDeliveryEventResponse.model_validate(event_record)
        for event_record in event_records
    ]


@router.post(
    "/admin/uat/release-archives/{archive_id}/retention-actions",
    response_model=UatReleaseArchiveRetentionActionResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_admin_access)],
)
def create_admin_uat_release_archive_retention_action(
    archive_id: UUID,
    request: CreateUatReleaseArchiveRetentionActionRequest,
    db: DbSession,
    principal: CurrentPrincipal,
) -> UatReleaseArchiveRetentionActionResponse:
    try:
        action_record = create_uat_release_archive_retention_action(
            db,
            archive_id=str(archive_id),
            action_type=request.action_type,
            recorded_by=principal.display_name or principal.subject,
            action_notes=request.action_notes,
            next_retention_review_at=request.next_retention_review_at,
            related_export_id=request.related_export_id,
            scheduled_retry_at=request.scheduled_retry_at,
        )
    except UatReleaseArchiveNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except UatReleaseArchiveRetentionActionValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return UatReleaseArchiveRetentionActionResponse.model_validate(action_record)


@router.get(
    "/admin/uat/release-archives/{archive_id}/retention-actions",
    response_model=list[UatReleaseArchiveRetentionActionResponse],
    dependencies=[Depends(require_admin_access)],
)
def list_admin_uat_release_archive_retention_actions(
    archive_id: UUID,
    db: DbSession,
) -> list[UatReleaseArchiveRetentionActionResponse]:
    try:
        action_records = list_uat_release_archive_retention_actions(
            db,
            archive_id=str(archive_id),
        )
    except UatReleaseArchiveNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return [
        UatReleaseArchiveRetentionActionResponse.model_validate(action_record)
        for action_record in action_records
    ]


@router.get(
    "/admin/uat/release-archives/{archive_id}",
    response_model=UatReleaseArchiveDetailResponse,
    dependencies=[Depends(require_admin_access)],
)
def get_admin_uat_release_archive(
    archive_id: UUID,
    db: DbSession,
) -> UatReleaseArchiveDetailResponse:
    try:
        archive = get_uat_release_archive(db, str(archive_id))
    except UatReleaseArchiveNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return UatReleaseArchiveDetailResponse.model_validate(archive)


@router.get(
    "/admin/uat/release-archives/{archive_id}/evidence-items",
    response_model=list[UatReleaseArchiveEvidenceItemResponse],
    dependencies=[Depends(require_admin_access)],
)
def list_admin_uat_release_archive_evidence_items(
    archive_id: UUID,
    db: DbSession,
) -> list[UatReleaseArchiveEvidenceItemResponse]:
    try:
        evidence_items = list_uat_release_archive_evidence_items(db, archive_id=str(archive_id))
    except UatReleaseArchiveNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return [
        UatReleaseArchiveEvidenceItemResponse.model_validate(item)
        for item in evidence_items
    ]


@router.post(
    "/admin/uat/release-archives/{archive_id}/supersede",
    response_model=UatReleaseArchiveDetailResponse,
    dependencies=[Depends(require_admin_access)],
)
def supersede_admin_uat_release_archive(
    archive_id: UUID,
    request: SupersedeUatReleaseArchiveRequest,
    db: DbSession,
) -> UatReleaseArchiveDetailResponse:
    try:
        archive = supersede_uat_release_archive(
            db,
            archive_id=str(archive_id),
            superseded_by_archive_id=request.superseded_by_archive_id,
            supersession_reason=request.supersession_reason,
        )
    except UatReleaseArchiveNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except UatReleaseArchiveValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except UatReleaseArchiveConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    return UatReleaseArchiveDetailResponse.model_validate(archive)


@router.post(
    "/admin/uat/handoff-snapshots/{snapshot_id}/acceptance-artifacts",
    response_model=UatHandoffSnapshotDetailResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_admin_access)],
)
def create_admin_uat_acceptance_artifact(
    snapshot_id: UUID,
    request: CreateUatAcceptanceArtifactRequest,
    db: DbSession,
    principal: CurrentPrincipal,
) -> UatHandoffSnapshotDetailResponse:
    try:
        snapshot = create_uat_acceptance_artifact(
            db,
            snapshot_id=str(snapshot_id),
            decision=request.decision,
            stakeholder_name=request.stakeholder_name,
            stakeholder_role=request.stakeholder_role,
            stakeholder_organization=request.stakeholder_organization,
            decision_notes=request.decision_notes,
            recorded_by=principal.display_name or principal.subject,
        )
    except UatHandoffSnapshotNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return UatHandoffSnapshotDetailResponse.model_validate(snapshot)
