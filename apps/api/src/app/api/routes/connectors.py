from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.core.security import CurrentPrincipal, require_operator_access
from app.core.settings import Settings, get_settings
from app.db.session import get_db
from app.schemas.connectors import (
    ConnectorDefinitionSummaryResponse,
    SourceRefreshBatchReportResponse,
    SourceRefreshJobReportResponse,
    SourceRefreshPlanItemResponse,
)
from ingestion.connectors import ConnectorConfigurationError, load_connector_registry
from ingestion.refresh import (
    build_refresh_plan,
    list_connector_definitions,
    refresh_due_connectors,
    refresh_source_connector,
)

router = APIRouter(dependencies=[Depends(require_operator_access)])
DbSession = Annotated[Session, Depends(get_db)]
AppSettings = Annotated[Settings, Depends(get_settings)]


@router.get(
    "/admin/connectors/definitions",
    response_model=list[ConnectorDefinitionSummaryResponse],
)
def get_connector_definitions(
    db: DbSession,
    settings: AppSettings,
) -> list[ConnectorDefinitionSummaryResponse]:
    try:
        registry = load_connector_registry(settings.source_connector_config_path)
    except ConnectorConfigurationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    definitions = list_connector_definitions(db, registry)
    return [ConnectorDefinitionSummaryResponse.model_validate(item) for item in definitions]


@router.get(
    "/admin/connectors/refresh-plan",
    response_model=list[SourceRefreshPlanItemResponse],
)
def get_connector_refresh_plan(
    db: DbSession,
    settings: AppSettings,
) -> list[SourceRefreshPlanItemResponse]:
    try:
        registry = load_connector_registry(settings.source_connector_config_path)
    except ConnectorConfigurationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    plan = build_refresh_plan(db, registry)
    return [SourceRefreshPlanItemResponse.model_validate(item) for item in plan]


@router.post(
    "/admin/connectors/{connector_key}/refresh",
    response_model=SourceRefreshJobReportResponse,
)
def refresh_connector_by_key(
    connector_key: str,
    db: DbSession,
    settings: AppSettings,
    principal: CurrentPrincipal,
) -> SourceRefreshJobReportResponse:
    try:
        registry = load_connector_registry(settings.source_connector_config_path)
        definition = registry.get_definition_by_connector_key(connector_key)
        report = refresh_source_connector(
            db,
            registry,
            connector_key=connector_key,
            source_id=definition.source_id,
            metro_id=definition.metro_id,
            trigger_mode="manual",
            actor_name=principal.display_name or principal.subject,
        )
    except ConnectorConfigurationError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return SourceRefreshJobReportResponse.model_validate(report)


@router.post(
    "/admin/connectors/{source_id}/metros/{metro_id}/refresh",
    response_model=SourceRefreshJobReportResponse,
)
def refresh_connector(
    source_id: str,
    metro_id: str,
    db: DbSession,
    settings: AppSettings,
    principal: CurrentPrincipal,
) -> SourceRefreshJobReportResponse:
    try:
        registry = load_connector_registry(settings.source_connector_config_path)
        report = refresh_source_connector(
            db,
            registry,
            source_id=source_id,
            metro_id=metro_id,
            trigger_mode="manual",
            actor_name=principal.display_name or principal.subject,
        )
    except ConnectorConfigurationError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return SourceRefreshJobReportResponse.model_validate(report)


@router.post(
    "/admin/connectors/refresh-due",
    response_model=SourceRefreshBatchReportResponse,
)
def execute_due_connector_refreshes(
    db: DbSession,
    settings: AppSettings,
    principal: CurrentPrincipal,
) -> SourceRefreshBatchReportResponse:
    try:
        registry = load_connector_registry(settings.source_connector_config_path)
    except ConnectorConfigurationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    report = refresh_due_connectors(
        db,
        registry,
        actor_name=principal.display_name or principal.subject,
    )
    return SourceRefreshBatchReportResponse.model_validate(report)
