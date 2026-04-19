from app.db.base import Base
from app.db.models.batching import ScoreBatch, ScoreRun
from app.db.models.catalogs import (
    BonusCatalog,
    FactorCatalog,
    ScoringProfile,
    ScoringProfileFactor,
    SourceCatalog,
    SourceInterface,
)
from app.db.models.connectors import SourceRefreshCheckpoint, SourceRefreshJob
from app.db.models.evaluation import ParcelEvaluation, ParcelExclusionEvent
from app.db.models.ingestion import SourceSnapshot
from app.db.models.operations import OperatorActionEvent
from app.db.models.scoring import ScoreBonusDetail, ScoreFactorDetail, ScoreFactorInput
from app.db.models.source_data import RawZoning, SourceEvidence, SourceRecordRejection
from app.db.models.territory import CountyCatalog, MetroCatalog, ParcelRepPoint, RawParcel
from app.db.models.uat import (
    UatAcceptanceArtifact,
    UatCycle,
    UatCycleEvent,
    UatDefect,
    UatDistributionPacket,
    UatDistributionRecipient,
    UatHandoffSnapshot,
    UatLaunchDecisionRecord,
    UatReleaseArchive,
    UatReleaseArchiveEvidenceItem,
    UatReleaseArchiveExport,
    UatReleaseArchiveExportDeliveryEvent,
    UatReleaseArchiveRetentionAction,
    UatScenarioExecution,
)

MANAGED_TABLES = sorted(Base.metadata.tables.keys())

__all__ = [
    "Base",
    "BonusCatalog",
    "CountyCatalog",
    "FactorCatalog",
    "MANAGED_TABLES",
    "MetroCatalog",
    "OperatorActionEvent",
    "ParcelEvaluation",
    "ParcelExclusionEvent",
    "ParcelRepPoint",
    "RawParcel",
    "ScoreBatch",
    "ScoreBonusDetail",
    "ScoreFactorDetail",
    "ScoreFactorInput",
    "ScoreRun",
    "ScoringProfile",
    "ScoringProfileFactor",
    "SourceCatalog",
    "SourceEvidence",
    "SourceInterface",
    "SourceRefreshCheckpoint",
    "SourceRefreshJob",
    "SourceRecordRejection",
    "SourceSnapshot",
    "UatCycle",
    "UatCycleEvent",
    "UatDefect",
    "UatDistributionPacket",
    "UatDistributionRecipient",
    "UatHandoffSnapshot",
    "UatAcceptanceArtifact",
    "UatLaunchDecisionRecord",
    "UatReleaseArchive",
    "UatReleaseArchiveEvidenceItem",
    "UatReleaseArchiveExportDeliveryEvent",
    "UatReleaseArchiveExport",
    "UatReleaseArchiveRetentionAction",
    "UatScenarioExecution",
    "RawZoning",
]
