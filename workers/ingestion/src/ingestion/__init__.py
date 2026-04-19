"""Source ingestion package."""

from ingestion.adapters import SourceAdapter, StaticSourceAdapter, build_source_version
from ingestion.connectors import (
    ConnectorConfigurationError,
    ConnectorExecutionError,
    ConnectorRegistry,
    load_connector_registry,
)
from ingestion.refresh import (
    ConnectorDefinitionSummary,
    SourceRefreshBatchReport,
    SourceRefreshJobReport,
    SourceRefreshPlanItem,
    build_refresh_plan,
    list_connector_definitions,
    refresh_due_connectors,
    refresh_source_connector,
)
from ingestion.service import (
    SourceConfigurationError,
    UnsupportedSourceError,
    describe_service,
    evaluate_freshness,
    ingest_evidence_records,
    ingest_parcel_records,
    ingest_zoning_records,
    load_from_adapter,
    summarize_source_health,
)

__all__ = [
    "SourceAdapter",
    "SourceConfigurationError",
    "ConnectorConfigurationError",
    "ConnectorDefinitionSummary",
    "ConnectorExecutionError",
    "ConnectorRegistry",
    "SourceRefreshBatchReport",
    "SourceRefreshJobReport",
    "SourceRefreshPlanItem",
    "StaticSourceAdapter",
    "UnsupportedSourceError",
    "build_refresh_plan",
    "build_source_version",
    "describe_service",
    "evaluate_freshness",
    "ingest_evidence_records",
    "ingest_parcel_records",
    "ingest_zoning_records",
    "list_connector_definitions",
    "load_connector_registry",
    "load_from_adapter",
    "refresh_due_connectors",
    "refresh_source_connector",
    "summarize_source_health",
]
