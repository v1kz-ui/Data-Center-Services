from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.core.security import require_operator_access
from app.core.settings import Settings, get_settings
from app.schemas.source_inventory import (
    SourceInventoryCoverageResponse,
    SourceInventorySummaryResponse,
)
from app.services.source_inventory import (
    SourceInventoryConfigurationError,
    build_source_inventory_coverage,
    build_source_inventory_summary,
    load_authoritative_source_inventory,
)
from ingestion.connectors import ConnectorConfigurationError, load_connector_registry

router = APIRouter(dependencies=[Depends(require_operator_access)])
AppSettings = Annotated[Settings, Depends(get_settings)]


@router.get(
    "/admin/source-inventory",
    response_model=SourceInventorySummaryResponse,
)
def get_source_inventory(
    settings: AppSettings,
    phase: Annotated[int | None, Query(ge=1, le=3)] = None,
    category: Annotated[str | None, Query()] = None,
) -> SourceInventorySummaryResponse:
    try:
        inventory = load_authoritative_source_inventory(
            settings.authoritative_source_inventory_path
        )
    except SourceInventoryConfigurationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc

    summary = build_source_inventory_summary(
        inventory,
        phase=phase,
        category=category,
    )
    return SourceInventorySummaryResponse.model_validate(summary)


@router.get(
    "/admin/source-inventory/coverage",
    response_model=SourceInventoryCoverageResponse,
)
def get_source_inventory_coverage(
    settings: AppSettings,
    phase: Annotated[int | None, Query(ge=1, le=3)] = None,
    category: Annotated[str | None, Query()] = None,
) -> SourceInventoryCoverageResponse:
    try:
        inventory = load_authoritative_source_inventory(
            settings.authoritative_source_inventory_path
        )
        registry = load_connector_registry(settings.source_connector_config_path)
    except (SourceInventoryConfigurationError, ConnectorConfigurationError) as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc

    coverage = build_source_inventory_coverage(
        inventory,
        registry,
        phase=phase,
        category=category,
    )
    return SourceInventoryCoverageResponse.model_validate(coverage)
