from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.db.models.enums import (
    UatAcceptanceDecision,
    UatCycleStatus,
    UatDefectSeverity,
    UatDefectStatus,
    UatDistributionChannel,
    UatDistributionRecipientStatus,
    UatExecutionStatus,
    UatLaunchDecisionOutcome,
    UatReleaseArchiveExportDeliveryEventType,
    UatReleaseArchiveExportHandoffStatus,
    UatReleaseArchiveRetentionActionType,
)


class UatCountResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    category: str
    count: int


class UatScenarioExecutionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    execution_id: str
    scenario_id: str
    title: str
    actor_role: str
    workflow: str
    entrypoint: str
    status: str
    execution_notes: str | None
    evidence_reference: str | None
    executed_by: str | None
    executed_at: datetime | None


class UatDefectResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    defect_id: str
    scenario_id: str | None
    severity: str
    status: str
    title: str
    description: str
    reported_by: str
    owner_name: str | None
    external_reference: str | None
    resolution_notes: str | None
    created_at: datetime
    updated_at: datetime


class UatCycleSummaryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    cycle_id: str
    cycle_name: str
    environment_name: str
    scenario_pack_path: str
    status: str
    created_by: str
    summary_notes: str | None
    started_at: datetime | None
    completed_at: datetime | None
    created_at: datetime
    updated_at: datetime
    scenario_count: int
    defect_count: int
    scenario_status_counts: list[UatCountResponse]
    defect_severity_counts: list[UatCountResponse]
    defect_status_counts: list[UatCountResponse]


class UatCycleDetailResponse(UatCycleSummaryResponse):
    scenario_executions: list[UatScenarioExecutionResponse]
    defects: list[UatDefectResponse]


class UatApprovalReadinessResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    approval_ready: bool
    blocking_issue_count: int
    blocking_issues: list[str]
    attention_item_count: int
    attention_items: list[str]
    terminal_scenario_count: int
    non_terminal_scenario_count: int
    evidence_captured_count: int
    missing_evidence_count: int
    open_defect_count: int
    open_high_severity_defect_count: int


class UatCycleEventRecordResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    event_id: str
    event_type: str
    actor_name: str
    scenario_id: str | None
    defect_id: str | None
    event_notes: str | None
    event_payload: dict[str, Any] | None
    created_at: datetime


class UatSignoffReportResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    report_version: str
    export_scope: str
    exported_at: datetime
    exported_by: str
    cycle: UatCycleDetailResponse
    approval_readiness: UatApprovalReadinessResponse
    open_defects: list[UatDefectResponse]
    event_history: list[UatCycleEventRecordResponse]


class UatAcceptanceArtifactResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    artifact_id: str
    snapshot_id: str
    decision: str
    stakeholder_name: str
    stakeholder_role: str | None
    stakeholder_organization: str | None
    decision_notes: str | None
    recorded_by: str
    created_at: datetime
    updated_at: datetime


class UatHandoffSnapshotSummaryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    snapshot_id: str
    cycle_id: str
    snapshot_name: str
    report_version: str
    export_scope: str
    cycle_status: str
    approval_ready: bool
    blocking_issue_count: int
    open_defect_count: int
    open_high_severity_defect_count: int
    distribution_summary: str
    created_by: str
    created_at: datetime
    updated_at: datetime
    acceptance_artifact_count: int


class UatHandoffSnapshotDetailResponse(UatHandoffSnapshotSummaryResponse):
    report_payload: dict[str, Any]
    acceptance_artifacts: list[UatAcceptanceArtifactResponse]


class UatDistributionRecipientResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    recipient_id: str
    packet_id: str
    recipient_name: str
    recipient_role: str | None
    recipient_organization: str | None
    recipient_contact: str | None
    required_for_ack: bool
    delivery_status: str
    delivery_notes: str | None
    acknowledgement_notes: str | None
    acknowledged_by: str | None
    recorded_by: str
    last_status_updated_by: str
    delivered_at: datetime | None
    acknowledged_at: datetime | None
    created_at: datetime
    updated_at: datetime


class UatDistributionPacketSummaryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    packet_id: str
    snapshot_id: str
    packet_name: str
    channel: str
    distribution_status: str
    ready_to_send: bool
    subject_line: str
    summary_excerpt: str
    distribution_notes: str | None
    created_by: str
    created_at: datetime
    updated_at: datetime
    distributed_at: datetime | None
    completed_at: datetime | None
    recipient_count: int
    required_recipient_count: int
    acknowledged_recipient_count: int
    pending_recipient_count: int


class UatDistributionPacketDetailResponse(UatDistributionPacketSummaryResponse):
    briefing_body: str
    recipients: list[UatDistributionRecipientResponse]


class UatLaunchDecisionRecordResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    decision_id: str
    snapshot_id: str
    decision: str
    reviewer_name: str
    reviewer_role: str | None
    reviewer_organization: str | None
    decision_notes: str | None
    recorded_by: str
    created_at: datetime
    updated_at: datetime


class UatLaunchExceptionItemResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    source_type: str
    source_id: str
    severity: str
    status: str
    summary: str
    owner_name: str | None


class UatLaunchPacketSummaryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    packet_id: str
    snapshot_id: str
    packet_name: str
    channel: str
    distribution_status: str
    ready_to_send: bool
    recipient_count: int
    required_recipient_count: int
    acknowledged_recipient_count: int
    pending_recipient_count: int
    distributed_at: datetime | None
    completed_at: datetime | None


class UatLaunchReadinessResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    snapshot_id: str
    cycle_id: str
    snapshot_name: str
    cycle_status: str
    approval_ready: bool
    distribution_summary: str
    recommended_outcome: str
    blocking_exception_count: int
    attention_exception_count: int
    distribution_packet_count: int
    completed_packet_count: int
    required_recipient_count: int
    acknowledged_required_recipient_count: int
    acceptance_decision_counts: list[UatCountResponse]
    launch_decision_counts: list[UatCountResponse]
    exception_queue: list[UatLaunchExceptionItemResponse]
    decision_records: list[UatLaunchDecisionRecordResponse]


class UatLaunchCloseoutReportResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    report_version: str
    export_scope: str
    exported_at: datetime
    exported_by: str
    readiness: UatLaunchReadinessResponse
    packet_summaries: list[UatLaunchPacketSummaryResponse]


class UatReleaseArchiveSummaryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    archive_id: str
    snapshot_id: str
    cycle_id: str
    snapshot_name: str
    archive_name: str
    recommended_outcome: str
    blocking_exception_count: int
    attention_exception_count: int
    support_handoff_owner: str | None
    support_handoff_summary: str
    operations_runbook_reference: str | None
    archive_summary: str
    archive_checksum: str
    retention_review_at: datetime
    retention_status: str
    superseded_by_archive_id: str | None
    supersession_reason: str | None
    superseded_at: datetime | None
    created_by: str
    created_at: datetime
    updated_at: datetime
    sealed_at: datetime
    evidence_item_count: int


class UatReleaseArchiveEvidenceItemResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    evidence_item_id: str
    archive_id: str
    evidence_type: str
    evidence_status: str
    reference_id: str
    reference_name: str
    retention_label: str
    evidence_summary: str
    source_recorded_at: datetime | None
    source_location: str | None
    created_at: datetime
    updated_at: datetime


class UatReleaseArchiveDetailResponse(UatReleaseArchiveSummaryResponse):
    manifest_payload: dict[str, Any]
    evidence_items: list[UatReleaseArchiveEvidenceItemResponse]


class UatReleaseArchiveRetentionQueueItemResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    archive_id: str
    snapshot_id: str
    cycle_id: str
    snapshot_name: str
    archive_name: str
    recommended_outcome: str
    retention_review_at: datetime
    retention_status: str
    support_handoff_owner: str | None
    superseded_by_archive_id: str | None
    latest_export_name: str | None
    latest_exported_at: datetime | None
    export_count: int
    days_until_review: int
    review_bucket: str


class UatReleaseArchiveRetentionQueueResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    generated_at: datetime
    review_window_days: int
    item_count: int
    overdue_count: int
    due_soon_count: int
    active_count: int
    superseded_count: int
    items: list[UatReleaseArchiveRetentionQueueItemResponse]


class UatReleaseArchiveExportSummaryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    export_id: str
    archive_id: str
    export_name: str
    export_scope: str
    destination_system: str
    destination_reference: str | None
    handoff_status: str
    trigger_reason: str | None
    handoff_notes: str | None
    export_checksum: str
    exported_by: str
    exported_at: datetime
    delivery_confirmed_by: str | None
    delivery_confirmed_at: datetime | None
    next_retry_at: datetime | None
    retry_count: int
    last_status_updated_by: str | None
    created_at: datetime
    updated_at: datetime


class UatReleaseArchiveExportDetailResponse(UatReleaseArchiveExportSummaryResponse):
    export_payload: dict[str, Any]


class UatReleaseArchiveExportDeliveryEventResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    event_id: str
    export_id: str
    archive_id: str
    export_name: str
    archive_name: str
    event_type: str
    target_name: str
    delivery_channel: str | None
    external_reference: str | None
    event_notes: str | None
    occurred_at: datetime
    recorded_by: str
    created_at: datetime
    updated_at: datetime


class UatReleaseArchiveRetentionActionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    action_id: str
    archive_id: str
    related_export_id: str | None
    related_export_name: str | None
    related_export_status: str | None
    action_type: str
    previous_retention_review_at: datetime
    next_retention_review_at: datetime | None
    scheduled_retry_at: datetime | None
    action_notes: str | None
    recorded_by: str
    recorded_at: datetime
    created_at: datetime
    updated_at: datetime


class UatReleaseArchiveFollowupDashboardItemResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    archive_id: str
    snapshot_id: str
    cycle_id: str
    snapshot_name: str
    archive_name: str
    retention_review_at: datetime
    retention_status: str
    days_until_review: int
    latest_export_name: str | None
    latest_handoff_status: str | None
    next_retry_at: datetime | None
    latest_retention_action_type: str | None
    latest_retention_action_at: datetime | None
    follow_up_export_count: int
    attention_reasons: list[str]


class UatReleaseArchiveFollowupDashboardResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    generated_at: datetime
    review_window_days: int
    total_archive_count: int
    action_required_count: int
    overdue_review_count: int
    due_soon_review_count: int
    acknowledgement_pending_count: int
    follow_up_export_count: int
    re_export_scheduled_count: int
    re_export_due_count: int
    items: list[UatReleaseArchiveFollowupDashboardItemResponse]


class BulkUatReleaseArchiveRetentionActionResultResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    archive_id: str
    archive_name: str | None
    applied: bool
    action_id: str | None
    action_type: str
    retention_review_at: datetime | None
    error: str | None


class BulkUatReleaseArchiveRetentionActionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    requested_count: int
    applied_count: int
    failed_count: int
    results: list[BulkUatReleaseArchiveRetentionActionResultResponse]


class UatReleaseArchiveReexportExecutionItemResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    archive_id: str
    archive_name: str
    source_export_id: str
    source_export_name: str
    destination_system: str
    scheduled_retry_at: datetime
    action: str
    generated_export_id: str | None
    generated_export_name: str | None
    resulting_source_status: str
    note: str


class UatReleaseArchiveReexportExecutionRunResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    report_version: str
    run_at: datetime
    executed_by: str
    dry_run: bool
    due_export_count: int
    executed_count: int
    skipped_count: int
    items: list[UatReleaseArchiveReexportExecutionItemResponse]


class UatReleaseArchiveNotificationDigestArchiveItemResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    archive_id: str
    archive_name: str
    snapshot_name: str
    retention_review_at: datetime
    next_retry_at: datetime | None
    latest_export_name: str | None
    latest_notification_sent_at: datetime | None
    latest_notification_acknowledged_at: datetime | None
    notification_acknowledgement_status: str
    attention_reasons: list[str]


class UatReleaseArchiveNotificationDigestRecipientResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    recipient_name: str
    archive_count: int
    overdue_review_count: int
    due_soon_review_count: int
    acknowledgement_pending_count: int
    re_export_due_count: int
    notification_acknowledgement_pending_count: int
    archives: list[UatReleaseArchiveNotificationDigestArchiveItemResponse]
    digest_message: str


class UatReleaseArchiveNotificationDigestResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    report_version: str
    generated_at: datetime
    generated_by: str
    review_window_days: int
    action_required_count: int
    recipient_count: int
    recipients: list[UatReleaseArchiveNotificationDigestRecipientResponse]


class UatReleaseArchiveNotificationDispatchArchiveItemResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    archive_id: str
    archive_name: str
    snapshot_name: str
    export_id: str | None
    export_name: str | None
    next_retry_at: datetime | None
    latest_notification_sent_at: datetime | None
    latest_notification_acknowledged_at: datetime | None
    notification_acknowledgement_status: str
    attention_reasons: list[str]
    action: str
    delivery_event_id: str | None
    external_reference: str | None
    note: str


class UatReleaseArchiveNotificationDispatchRecipientResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    recipient_name: str
    archive_count: int
    planned_archive_count: int
    recorded_archive_count: int
    skipped_archive_count: int
    failed_archive_count: int
    delivery_channel: str
    external_reference: str | None
    subject_line: str
    digest_message: str
    archives: list[UatReleaseArchiveNotificationDispatchArchiveItemResponse]


class UatReleaseArchiveNotificationDispatchRunResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    report_version: str
    run_at: datetime
    executed_by: str
    dry_run: bool
    review_window_days: int
    delivery_channel: str
    recipient_count: int
    planned_archive_count: int
    recorded_archive_count: int
    skipped_archive_count: int
    failed_archive_count: int
    recipients: list[UatReleaseArchiveNotificationDispatchRecipientResponse]


class UatReleaseArchiveDeliveryLedgerItemResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    archive_id: str
    snapshot_id: str
    cycle_id: str
    snapshot_name: str
    archive_name: str
    support_handoff_owner: str | None
    latest_export_id: str | None
    latest_export_name: str | None
    latest_export_status: str | None
    destination_system: str | None
    destination_reference: str | None
    latest_external_handoff_at: datetime | None
    latest_external_handoff_reference: str | None
    latest_notification_sent_at: datetime | None
    latest_notification_acknowledged_at: datetime | None
    latest_notification_channel: str | None
    latest_notification_reference: str | None
    notification_acknowledgement_status: str
    escalation_status: str
    escalation_due_at: datetime | None
    next_retry_at: datetime | None
    attention_reasons: list[str]
    recommended_action: str


class UatReleaseArchiveDeliveryLedgerResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    report_version: str
    generated_at: datetime
    generated_by: str
    as_of: datetime
    review_window_days: int
    stale_reply_after_hours: int
    total_archive_count: int
    ledger_item_count: int
    action_required_count: int
    escalated_count: int
    notification_pending_count: int
    acknowledged_count: int
    missing_export_count: int
    re_export_due_count: int
    items: list[UatReleaseArchiveDeliveryLedgerItemResponse]


class UatReleaseArchiveSupportHandbackArchiveItemResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    archive_id: str
    snapshot_id: str
    cycle_id: str
    snapshot_name: str
    archive_name: str
    owner_name: str
    support_handoff_summary: str
    operations_runbook_reference: str | None
    latest_export_id: str | None
    latest_export_name: str | None
    latest_export_status: str | None
    destination_system: str | None
    destination_reference: str | None
    notification_acknowledgement_status: str
    escalation_status: str
    closure_status: str
    closure_ready: bool
    closure_blockers: list[str]
    attention_reasons: list[str]
    recommended_action: str
    handback_summary: str


class UatReleaseArchiveSupportHandbackOwnerResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    owner_name: str
    archive_count: int
    closure_ready_count: int
    unresolved_count: int
    pending_support_confirmation_count: int
    remediation_in_progress_count: int
    blocked_count: int
    superseded_count: int
    archives: list[UatReleaseArchiveSupportHandbackArchiveItemResponse]
    summary_message: str


class UatReleaseArchiveSupportHandbackReportResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    report_version: str
    generated_at: datetime
    generated_by: str
    as_of: datetime
    review_window_days: int
    stale_reply_after_hours: int
    total_archive_count: int
    included_archive_count: int
    owner_count: int
    closure_ready_count: int
    unresolved_count: int
    pending_support_confirmation_count: int
    remediation_in_progress_count: int
    blocked_count: int
    superseded_count: int
    owners: list[UatReleaseArchiveSupportHandbackOwnerResponse]


class UatReleaseArchiveClosureHistoryTimelineEventResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    event_id: str
    export_id: str
    export_name: str
    event_type: str
    stage_label: str
    target_name: str
    delivery_channel: str | None
    external_reference: str | None
    event_notes: str | None
    occurred_at: datetime
    recorded_by: str


class UatReleaseArchiveClosureHistoryArchiveItemResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    archive_id: str
    snapshot_id: str
    cycle_id: str
    snapshot_name: str
    archive_name: str
    owner_name: str
    latest_export_id: str | None
    latest_export_name: str | None
    latest_export_status: str | None
    destination_system: str | None
    destination_reference: str | None
    notification_acknowledgement_status: str
    escalation_status: str
    closure_status: str
    latest_notification_sent_at: datetime | None
    latest_notification_acknowledged_at: datetime | None
    latest_external_handoff_at: datetime | None
    latest_escalation_outcome_at: datetime | None
    latest_support_handback_acknowledged_at: datetime | None
    latest_closure_confirmed_at: datetime | None
    unresolved_actions: list[str]
    closure_summary: str
    timeline: list[UatReleaseArchiveClosureHistoryTimelineEventResponse]


class UatReleaseArchiveClosureHistoryOwnerResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    owner_name: str
    archive_count: int
    closed_count: int
    awaiting_closure_confirmation_count: int
    awaiting_support_handback_acknowledgement_count: int
    remediation_in_progress_count: int
    blocked_count: int
    open_followup_count: int
    superseded_count: int
    archives: list[UatReleaseArchiveClosureHistoryArchiveItemResponse]
    summary_message: str


class UatReleaseArchiveClosureHistoryReportResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    report_version: str
    generated_at: datetime
    generated_by: str
    as_of: datetime
    review_window_days: int
    stale_reply_after_hours: int
    total_archive_count: int
    included_archive_count: int
    owner_count: int
    closed_count: int
    awaiting_closure_confirmation_count: int
    awaiting_support_handback_acknowledgement_count: int
    remediation_in_progress_count: int
    blocked_count: int
    open_followup_count: int
    superseded_count: int
    owners: list[UatReleaseArchiveClosureHistoryOwnerResponse]


class CreateUatCycleRequest(BaseModel):
    cycle_name: str = Field(min_length=3, max_length=255)
    environment_name: str | None = Field(default=None, min_length=2, max_length=64)
    scenario_pack_path: str | None = Field(default=None, min_length=3, max_length=255)
    summary_notes: str | None = None


class RecordUatExecutionResultRequest(BaseModel):
    status: UatExecutionStatus
    execution_notes: str | None = None
    evidence_reference: str | None = None
    executed_by: str | None = Field(default=None, min_length=2, max_length=255)


class CreateUatDefectRequest(BaseModel):
    scenario_id: str | None = Field(default=None, min_length=3, max_length=64)
    severity: UatDefectSeverity
    title: str = Field(min_length=3, max_length=255)
    description: str = Field(min_length=3)
    owner_name: str | None = Field(default=None, min_length=2, max_length=255)
    external_reference: str | None = Field(default=None, min_length=2, max_length=255)


class UpdateUatDefectRequest(BaseModel):
    status: UatDefectStatus | None = None
    owner_name: str | None = Field(default=None, min_length=2, max_length=255)
    external_reference: str | None = Field(default=None, min_length=2, max_length=255)
    resolution_notes: str | None = Field(default=None, min_length=2)

    @model_validator(mode="after")
    def validate_update_requested(self) -> UpdateUatDefectRequest:
        if not self.model_fields_set:
            raise ValueError("At least one defect field must be provided.")
        return self


class FinalizeUatCycleRequest(BaseModel):
    status: UatCycleStatus
    summary_notes: str | None = None


class CreateUatHandoffSnapshotRequest(BaseModel):
    snapshot_name: str = Field(min_length=3, max_length=255)
    distribution_summary_override: str | None = Field(default=None, min_length=3)


class CreateUatAcceptanceArtifactRequest(BaseModel):
    decision: UatAcceptanceDecision
    stakeholder_name: str = Field(min_length=2, max_length=255)
    stakeholder_role: str | None = Field(default=None, min_length=2, max_length=255)
    stakeholder_organization: str | None = Field(default=None, min_length=2, max_length=255)
    decision_notes: str | None = Field(default=None, min_length=2)


class CreateUatDistributionRecipientRequest(BaseModel):
    recipient_name: str = Field(min_length=2, max_length=255)
    recipient_role: str | None = Field(default=None, min_length=2, max_length=255)
    recipient_organization: str | None = Field(default=None, min_length=2, max_length=255)
    recipient_contact: str | None = Field(default=None, min_length=3, max_length=255)
    required_for_ack: bool = True


class CreateUatDistributionPacketRequest(BaseModel):
    packet_name: str = Field(min_length=3, max_length=255)
    channel: UatDistributionChannel = UatDistributionChannel.STAKEHOLDER_BRIEFING
    subject_line_override: str | None = Field(default=None, min_length=3, max_length=255)
    summary_excerpt_override: str | None = Field(default=None, min_length=3)
    briefing_body_override: str | None = Field(default=None, min_length=3)
    distribution_notes: str | None = Field(default=None, min_length=3)
    recipients: list[CreateUatDistributionRecipientRequest] = Field(default_factory=list)


class UpdateUatDistributionRecipientRequest(BaseModel):
    delivery_status: UatDistributionRecipientStatus | None = None
    delivery_notes: str | None = Field(default=None, min_length=2)
    acknowledgement_notes: str | None = Field(default=None, min_length=2)
    acknowledged_by: str | None = Field(default=None, min_length=2, max_length=255)

    @model_validator(mode="after")
    def validate_update_requested(self) -> UpdateUatDistributionRecipientRequest:
        if not self.model_fields_set:
            raise ValueError("At least one recipient update field must be provided.")
        return self


class CreateUatLaunchDecisionRequest(BaseModel):
    decision: UatLaunchDecisionOutcome
    reviewer_name: str = Field(min_length=2, max_length=255)
    reviewer_role: str | None = Field(default=None, min_length=2, max_length=255)
    reviewer_organization: str | None = Field(default=None, min_length=2, max_length=255)
    decision_notes: str | None = Field(default=None, min_length=2)


class CreateUatReleaseArchiveRequest(BaseModel):
    archive_name: str = Field(min_length=3, max_length=255)
    support_handoff_owner: str | None = Field(default=None, min_length=2, max_length=255)
    operations_runbook_reference: str | None = Field(
        default=None,
        min_length=2,
        max_length=255,
    )
    support_handoff_summary_override: str | None = Field(default=None, min_length=3)
    release_manifest_notes: str | None = Field(default=None, min_length=3)
    retention_review_at: datetime | None = None


class SupersedeUatReleaseArchiveRequest(BaseModel):
    superseded_by_archive_id: str = Field(min_length=3, max_length=64)
    supersession_reason: str = Field(min_length=3)


class CreateUatReleaseArchiveExportRequest(BaseModel):
    export_name: str = Field(min_length=3, max_length=255)
    export_scope: str = Field(min_length=3, max_length=64)
    destination_system: str = Field(min_length=2, max_length=128)
    destination_reference: str | None = Field(default=None, min_length=2, max_length=255)
    trigger_reason: str | None = Field(default=None, min_length=3)
    handoff_notes: str | None = Field(default=None, min_length=3)


class UpdateUatReleaseArchiveExportRequest(BaseModel):
    handoff_status: UatReleaseArchiveExportHandoffStatus | None = None
    destination_reference: str | None = Field(default=None, min_length=2, max_length=255)
    handoff_notes: str | None = Field(default=None, min_length=3)
    delivery_confirmed_by: str | None = Field(default=None, min_length=2, max_length=255)
    next_retry_at: datetime | None = None

    @model_validator(mode="after")
    def validate_update_requested(self) -> UpdateUatReleaseArchiveExportRequest:
        if not self.model_fields_set:
            raise ValueError("At least one export field must be provided.")
        return self


class CreateUatReleaseArchiveExportDeliveryEventRequest(BaseModel):
    event_type: UatReleaseArchiveExportDeliveryEventType
    target_name: str = Field(min_length=2, max_length=255)
    delivery_channel: str | None = Field(default=None, min_length=2, max_length=64)
    external_reference: str | None = Field(default=None, min_length=2, max_length=255)
    event_notes: str | None = Field(default=None, min_length=3)
    occurred_at: datetime | None = None


class CreateUatReleaseArchiveRetentionActionRequest(BaseModel):
    action_type: UatReleaseArchiveRetentionActionType
    action_notes: str | None = Field(default=None, min_length=3)
    next_retention_review_at: datetime | None = None
    related_export_id: str | None = Field(default=None, min_length=3, max_length=64)
    scheduled_retry_at: datetime | None = None


class BulkUatReleaseArchiveRetentionActionRequest(BaseModel):
    archive_ids: list[str] = Field(min_length=1)
    action_type: UatReleaseArchiveRetentionActionType
    action_notes: str | None = Field(default=None, min_length=3)
    next_retention_review_at: datetime | None = None


class ExecuteUatReleaseArchiveReexportsRequest(BaseModel):
    dry_run: bool = False
    limit: int | None = Field(default=None, ge=1, le=100)
    run_at: datetime | None = None


class ExecuteUatReleaseArchiveNotificationDispatchRequest(BaseModel):
    dry_run: bool = False
    review_window_days: int = Field(default=30, ge=1, le=365)
    recipient_limit: int | None = Field(default=None, ge=1, le=100)
    delivery_channel: str = Field(default="email", min_length=2, max_length=64)
    run_at: datetime | None = None
