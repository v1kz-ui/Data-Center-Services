from enum import StrEnum


class ScoringProfileStatus(StrEnum):
    DRAFT = "draft"
    ACTIVE = "active"
    RETIRED = "retired"


class ScoreBatchStatus(StrEnum):
    BUILDING = "building"
    FAILED = "failed"
    COMPLETED = "completed"
    ACTIVE = "active"


class ScoreRunStatus(StrEnum):
    RUNNING = "running"
    FAILED = "failed"
    COMPLETED = "completed"


class SourceSnapshotStatus(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"
    QUARANTINED = "quarantined"


class ParcelEvaluationStatus(StrEnum):
    PREFILTERED_BAND = "prefiltered_band"
    PREFILTERED_SIZE = "prefiltered_size"
    PENDING_EXCLUSION_CHECK = "pending_exclusion_check"
    PENDING_SCORING = "pending_scoring"
    SCORED = "scored"
    EXCLUDED = "excluded"


class UatCycleStatus(StrEnum):
    PLANNED = "planned"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    APPROVED = "approved"
    REWORK_REQUIRED = "rework_required"


class UatExecutionStatus(StrEnum):
    PLANNED = "planned"
    IN_PROGRESS = "in_progress"
    PASSED = "passed"
    FAILED = "failed"
    BLOCKED = "blocked"


class UatDefectSeverity(StrEnum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class UatDefectStatus(StrEnum):
    OPEN = "open"
    ACCEPTED = "accepted"
    RESOLVED = "resolved"


class UatAcceptanceDecision(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    FOLLOW_UP_REQUIRED = "follow_up_required"


class UatDistributionChannel(StrEnum):
    EMAIL = "email"
    LAUNCH_REVIEW_PACKET = "launch_review_packet"
    STAKEHOLDER_BRIEFING = "stakeholder_briefing"


class UatDistributionPacketStatus(StrEnum):
    DRAFT = "draft"
    READY = "ready"
    DISTRIBUTED = "distributed"
    COMPLETED = "completed"


class UatDistributionRecipientStatus(StrEnum):
    PENDING = "pending"
    SENT = "sent"
    ACKNOWLEDGED = "acknowledged"
    FOLLOW_UP_REQUIRED = "follow_up_required"


class UatLaunchDecisionOutcome(StrEnum):
    GO = "go"
    CONDITIONAL_GO = "conditional_go"
    HOLD = "hold"
    NO_GO = "no_go"


class UatReleaseArchiveExportHandoffStatus(StrEnum):
    PREPARED = "prepared"
    DELIVERED = "delivered"
    ACKNOWLEDGED = "acknowledged"
    FOLLOW_UP_REQUIRED = "follow_up_required"
    RE_EXPORT_SCHEDULED = "re_export_scheduled"
    RE_EXPORT_COMPLETED = "re_export_completed"


class UatReleaseArchiveExportDeliveryEventType(StrEnum):
    NOTIFICATION_SENT = "notification_sent"
    NOTIFICATION_ACKNOWLEDGED = "notification_acknowledged"
    EXTERNAL_HANDOFF_LOGGED = "external_handoff_logged"
    ESCALATION_OUTCOME_RECORDED = "escalation_outcome_recorded"
    SUPPORT_HANDBACK_ACKNOWLEDGED = "support_handback_acknowledged"
    CLOSURE_CONFIRMED = "closure_confirmed"


class UatReleaseArchiveRetentionActionType(StrEnum):
    REVIEW_COMPLETED = "review_completed"
    RETENTION_EXTENDED = "retention_extended"
    RE_EXPORT_REQUESTED = "re_export_requested"
