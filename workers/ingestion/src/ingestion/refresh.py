from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from shapely import wkt
from shapely.errors import ShapelyError
from shapely.geometry import shape as shapely_shape
from shapely.strtree import STRtree
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models.catalogs import SourceCatalog
from app.db.models.connectors import SourceRefreshCheckpoint, SourceRefreshJob
from app.db.models.ingestion import SourceSnapshot
from app.db.models.territory import MetroCatalog, ParcelRepPoint, RawParcel
from ingestion.adapters import build_source_version
from ingestion.connectors import (
    ConnectorCheckpoint,
    ConnectorConfigurationError,
    ConnectorExecutionError,
    ConnectorRegistry,
    SourceConnectorFieldRule,
    fetch_connector_records,
    resolve_connector_field_rule_value,
)
from ingestion.service import (
    _get_cadence_window,
    _get_latest_snapshot,
    ingest_evidence_records,
    ingest_market_listing_records,
    ingest_parcel_records,
    ingest_zoning_records,
)

_BOUNDARY_GEOMETRY_CACHE: dict[str, object] = {}


@dataclass(slots=True)
class ConnectorDefinitionSummary:
    connector_key: str
    source_id: str
    metro_id: str
    interface_name: str
    adapter_type: str
    enabled: bool
    load_strategy: str
    inventory_if_codes: list[str]
    priority: int
    description: str | None
    preprocess_strategy: str | None
    latest_snapshot_ts: datetime | None
    latest_snapshot_status: str | None
    checkpoint_ts: datetime | None
    checkpoint_cursor: str | None


@dataclass(slots=True)
class SourceRefreshPlanItem:
    connector_key: str
    source_id: str
    metro_id: str
    interface_name: str
    refresh_cadence: str
    enabled: bool
    load_strategy: str
    priority: int
    preprocess_strategy: str | None
    due: bool
    due_reason: str
    next_due_at: datetime | None
    latest_snapshot_ts: datetime | None
    latest_snapshot_status: str | None
    checkpoint_ts: datetime | None
    checkpoint_cursor: str | None


@dataclass(slots=True)
class SourceRefreshJobReport:
    job_id: str
    source_id: str
    metro_id: str
    connector_key: str
    trigger_mode: str
    actor_name: str
    status: str
    started_at: datetime
    completed_at: datetime | None
    attempt_count: int
    source_version: str | None
    snapshot_id: str | None
    row_count: int
    accepted_count: int
    rejected_count: int
    checkpoint_in_ts: datetime | None
    checkpoint_out_ts: datetime | None
    checkpoint_cursor_in: str | None
    checkpoint_cursor_out: str | None
    error_message: str | None


@dataclass(slots=True)
class SourceRefreshBatchReport:
    requested_at: datetime
    total_due: int
    completed: int
    reports: list[SourceRefreshJobReport] = field(default_factory=list)


def list_connector_definitions(
    session: Session,
    registry: ConnectorRegistry,
) -> list[ConnectorDefinitionSummary]:
    summaries: list[ConnectorDefinitionSummary] = []
    for definition in registry.list_definitions():
        latest_snapshot = _get_latest_snapshot(
            session,
            definition.normalized_source_id,
            definition.normalized_metro_id,
            connector_key=definition.connector_key,
        )
        checkpoint = _get_refresh_checkpoint(
            session,
            connector_key=definition.connector_key,
        )
        summaries.append(
            ConnectorDefinitionSummary(
                connector_key=definition.connector_key,
                source_id=definition.normalized_source_id,
                metro_id=definition.normalized_metro_id,
                interface_name=definition.interface_name,
                adapter_type=definition.adapter_type,
                enabled=definition.enabled,
                load_strategy=definition.load_strategy,
                inventory_if_codes=list(definition.inventory_if_codes),
                priority=definition.priority,
                description=definition.description,
                preprocess_strategy=definition.preprocess_strategy,
                latest_snapshot_ts=_snapshot_ts(latest_snapshot),
                latest_snapshot_status=_snapshot_status(latest_snapshot),
                checkpoint_ts=(
                    _coerce_timestamp(checkpoint.checkpoint_ts)
                    if checkpoint and checkpoint.checkpoint_ts
                    else None
                ),
                checkpoint_cursor=checkpoint.checkpoint_cursor if checkpoint else None,
            )
        )
    return summaries


def build_refresh_plan(
    session: Session,
    registry: ConnectorRegistry,
    *,
    as_of: datetime | None = None,
) -> list[SourceRefreshPlanItem]:
    evaluated_at = _coerce_timestamp(as_of)
    items: list[SourceRefreshPlanItem] = []

    for definition in registry.list_definitions():
        latest_snapshot = _get_latest_snapshot(
            session,
            definition.normalized_source_id,
            definition.normalized_metro_id,
            connector_key=definition.connector_key,
        )
        checkpoint = _get_refresh_checkpoint(
            session,
            connector_key=definition.connector_key,
        )
        source_catalog = session.get(SourceCatalog, definition.normalized_source_id)
        refresh_cadence = (
            source_catalog.refresh_cadence
            if source_catalog is not None
            else "daily"
        )
        metro_catalog = session.get(MetroCatalog, definition.normalized_metro_id)
        cadence_window = _get_cadence_window(refresh_cadence)
        latest_snapshot_ts = _snapshot_ts(latest_snapshot)
        latest_snapshot_status = _snapshot_status(latest_snapshot)

        next_due_at: datetime | None = None
        due_reason = "connector_disabled"
        due = False
        if not definition.enabled:
            due_reason = "connector_disabled"
        elif source_catalog is None or not source_catalog.is_active:
            due_reason = "source_not_configured"
        elif metro_catalog is None:
            due_reason = "metro_not_configured"
        elif latest_snapshot is None:
            due = True
            due_reason = "missing_snapshot"
        elif latest_snapshot_status == "failed":
            due = True
            due_reason = "latest_snapshot_failed"
        elif latest_snapshot_status == "quarantined":
            due = True
            due_reason = "latest_snapshot_quarantined"
        else:
            next_due_at = latest_snapshot_ts + cadence_window if latest_snapshot_ts else None
            due = next_due_at is None or evaluated_at >= next_due_at
            due_reason = "cadence_due" if due else "within_cadence"

        items.append(
            SourceRefreshPlanItem(
                connector_key=definition.connector_key,
                source_id=definition.normalized_source_id,
                metro_id=definition.normalized_metro_id,
                interface_name=definition.interface_name,
                refresh_cadence=refresh_cadence,
                enabled=definition.enabled,
                load_strategy=definition.load_strategy,
                priority=definition.priority,
                preprocess_strategy=definition.preprocess_strategy,
                due=due,
                due_reason=due_reason,
                next_due_at=next_due_at,
                latest_snapshot_ts=latest_snapshot_ts,
                latest_snapshot_status=latest_snapshot_status,
                checkpoint_ts=(
                    _coerce_timestamp(checkpoint.checkpoint_ts)
                    if checkpoint and checkpoint.checkpoint_ts
                    else None
                ),
                checkpoint_cursor=checkpoint.checkpoint_cursor if checkpoint else None,
            )
        )

    return sorted(
        items,
        key=lambda item: (
            not item.due,
            item.source_id,
            item.metro_id,
            item.priority,
            item.connector_key,
        ),
    )


def refresh_source_connector(
    session: Session,
    registry: ConnectorRegistry,
    *,
    connector_key: str | None = None,
    source_id: str | None = None,
    metro_id: str | None = None,
    trigger_mode: str = "manual",
    actor_name: str = "system",
    requested_at: datetime | None = None,
) -> SourceRefreshJobReport:
    if connector_key is not None:
        definition = registry.get_definition_by_connector_key(connector_key)
    else:
        if source_id is None or metro_id is None:
            raise ConnectorConfigurationError(
                "source_id and metro_id are required when connector_key is not provided."
            )
        definition = registry.get_definition(source_id, metro_id)
    started_at = _coerce_timestamp(requested_at)
    checkpoint_model = _get_refresh_checkpoint(
        session,
        connector_key=definition.connector_key,
    )
    checkpoint = ConnectorCheckpoint(
        checkpoint_ts=(
            _coerce_timestamp(checkpoint_model.checkpoint_ts)
            if checkpoint_model and checkpoint_model.checkpoint_ts
            else None
        ),
        checkpoint_cursor=checkpoint_model.checkpoint_cursor if checkpoint_model else None,
        source_version=checkpoint_model.source_version if checkpoint_model else None,
    )
    job = SourceRefreshJob(
        source_id=definition.normalized_source_id,
        metro_id=definition.normalized_metro_id,
        connector_key=definition.connector_key,
        trigger_mode=trigger_mode,
        actor_name=actor_name,
        status="running",
        started_at=started_at,
        checkpoint_in_ts=checkpoint.checkpoint_ts,
        checkpoint_cursor_in=checkpoint.checkpoint_cursor,
    )
    session.add(job)
    session.flush()

    try:
        fetch_result = fetch_connector_records(definition, checkpoint)
        prepared_records = _preprocess_connector_records(
            session=session,
            definition=definition,
            metro_id=definition.normalized_metro_id,
            records=fetch_result.records,
        )
        snapshot_ts = _coerce_timestamp(fetch_result.checkpoint_ts or started_at)
        source_version = build_source_version(definition.interface_name, snapshot_ts)
        load_report = _ingest_connector_records(
            session=session,
            source_id=definition.normalized_source_id,
            load_strategy=definition.load_strategy,
            metro_id=definition.normalized_metro_id,
            source_version=source_version,
            records=prepared_records,
            loaded_at=snapshot_ts,
            connector_key=definition.connector_key,
            replace_existing_scope=definition.preprocess_options.get(
                "replace_existing_scope"
            ),
            listing_source_id=definition.preprocess_options.get("listing_source_id"),
        )
        checkpoint_model = _get_refresh_checkpoint(
            session,
            connector_key=definition.connector_key,
        )
        job = _ensure_refresh_job_row(session=session, job=job)
        session.flush()
        completed_at = datetime.now(UTC)
        job.status = load_report.status
        job.completed_at = completed_at
        job.attempt_count = fetch_result.attempt_count
        job.source_version = load_report.source_version
        job.snapshot_id = UUID(load_report.snapshot_id)
        job.row_count = load_report.row_count
        job.accepted_count = load_report.accepted_count
        job.rejected_count = load_report.rejected_count
        job.checkpoint_out_ts = fetch_result.checkpoint_ts or snapshot_ts
        job.checkpoint_cursor_out = fetch_result.checkpoint_cursor
        job.error_message = load_report.error_message
        _upsert_refresh_checkpoint(
            session=session,
            definition=definition,
            job=job,
            checkpoint_model=checkpoint_model,
            checkpoint_ts=job.checkpoint_out_ts,
            checkpoint_cursor=job.checkpoint_cursor_out,
            source_version=load_report.source_version,
            snapshot_id=job.snapshot_id,
            status=load_report.status,
            error_message=load_report.error_message,
            refreshed_at=completed_at,
        )
        session.commit()
        return _to_job_report(job)
    except Exception as exc:
        session.rollback()
        checkpoint_model = _get_refresh_checkpoint(
            session,
            connector_key=definition.connector_key,
        )
        job = _ensure_refresh_job_row(session=session, job=job)
        session.flush()
        job.status = "failed"
        job.completed_at = datetime.now(UTC)
        job.error_message = str(exc)
        job.attempt_count = max(job.attempt_count or 0, 1)
        _upsert_refresh_checkpoint(
            session=session,
            definition=definition,
            job=job,
            checkpoint_model=checkpoint_model,
            checkpoint_ts=checkpoint.checkpoint_ts,
            checkpoint_cursor=checkpoint.checkpoint_cursor,
            source_version=checkpoint.source_version,
            snapshot_id=checkpoint_model.snapshot_id if checkpoint_model else None,
            status="failed",
            error_message=str(exc),
            refreshed_at=job.completed_at,
        )
        session.commit()
        return _to_job_report(job)


def refresh_due_connectors(
    session: Session,
    registry: ConnectorRegistry,
    *,
    actor_name: str = "scheduler",
    requested_at: datetime | None = None,
) -> SourceRefreshBatchReport:
    started_at = _coerce_timestamp(requested_at)
    plan = build_refresh_plan(session, registry, as_of=started_at)
    due_items = [item for item in plan if item.enabled and item.due]
    reports = [
        refresh_source_connector(
            session,
            registry,
            connector_key=item.connector_key,
            source_id=item.source_id,
            metro_id=item.metro_id,
            trigger_mode="scheduled",
            actor_name=actor_name,
            requested_at=started_at,
        )
        for item in due_items
    ]
    return SourceRefreshBatchReport(
        requested_at=started_at,
        total_due=len(due_items),
        completed=len(reports),
        reports=reports,
    )


def _upsert_refresh_checkpoint(
    *,
    session: Session,
    definition,
    job: SourceRefreshJob,
    checkpoint_model: SourceRefreshCheckpoint | None,
    checkpoint_ts: datetime | None,
    checkpoint_cursor: str | None,
    source_version: str | None,
    snapshot_id: UUID | None,
    status: str,
    error_message: str | None,
    refreshed_at: datetime | None,
) -> None:
    if checkpoint_model is None:
        checkpoint_model = SourceRefreshCheckpoint(
            source_id=definition.normalized_source_id,
            metro_id=definition.normalized_metro_id,
            connector_key=definition.connector_key,
        )
        session.add(checkpoint_model)

    checkpoint_model.connector_key = definition.connector_key
    checkpoint_model.source_version = source_version
    checkpoint_model.snapshot_id = snapshot_id
    checkpoint_model.checkpoint_ts = checkpoint_ts
    checkpoint_model.checkpoint_cursor = checkpoint_cursor
    checkpoint_model.last_job_id = job.job_id
    checkpoint_model.last_status = status
    checkpoint_model.last_error_message = error_message
    checkpoint_model.last_refreshed_at = refreshed_at


def _get_refresh_checkpoint(
    session: Session,
    *,
    connector_key: str,
) -> SourceRefreshCheckpoint | None:
    statement = select(SourceRefreshCheckpoint).where(
        SourceRefreshCheckpoint.connector_key == connector_key.strip(),
    )
    return session.scalar(statement)


def _ensure_refresh_job_row(
    *,
    session: Session,
    job: SourceRefreshJob,
) -> SourceRefreshJob:
    persisted_job = session.get(SourceRefreshJob, job.job_id)
    if persisted_job is not None:
        return persisted_job

    persisted_job = SourceRefreshJob(
        job_id=job.job_id,
        source_id=job.source_id,
        metro_id=job.metro_id,
        connector_key=job.connector_key,
        trigger_mode=job.trigger_mode,
        actor_name=job.actor_name,
        status=job.status,
        started_at=job.started_at,
        checkpoint_in_ts=job.checkpoint_in_ts,
        checkpoint_cursor_in=job.checkpoint_cursor_in,
    )
    session.add(persisted_job)
    return persisted_job


def _ingest_connector_records(
    *,
    session: Session,
    source_id: str,
    load_strategy: str,
    metro_id: str,
    source_version: str,
    records: list[dict[str, object]],
    loaded_at: datetime,
    connector_key: str | None = None,
    replace_existing_scope: str | None = None,
    listing_source_id: str | None = None,
):
    normalized_strategy = load_strategy.strip().lower()
    if normalized_strategy == "parcel":
        return ingest_parcel_records(
            session=session,
            source_id=source_id,
            metro_id=metro_id,
            source_version=source_version,
            records=records,
            loaded_at=loaded_at,
            connector_key=connector_key,
            replace_existing_scope=replace_existing_scope,
        )
    if normalized_strategy == "zoning":
        return ingest_zoning_records(
            session=session,
            source_id=source_id,
            metro_id=metro_id,
            source_version=source_version,
            records=records,
            loaded_at=loaded_at,
            connector_key=connector_key,
        )
    if normalized_strategy == "market_listing":
        if listing_source_id is None:
            raise ConnectorConfigurationError(
                f"Connector `{connector_key or source_id}` requires "
                "preprocess_options.listing_source_id "
                "when using the `market_listing` load strategy."
            )
        return ingest_market_listing_records(
            session=session,
            source_id=source_id,
            metro_id=metro_id,
            source_version=source_version,
            records=records,
            listing_source_id=listing_source_id,
            loaded_at=loaded_at,
            connector_key=connector_key,
            replace_existing_scope=replace_existing_scope,
        )
    return ingest_evidence_records(
        session=session,
        source_id=source_id,
        metro_id=metro_id,
        source_version=source_version,
        records=records,
        loaded_at=loaded_at,
        connector_key=connector_key,
        replace_existing_scope=replace_existing_scope,
    )


def _preprocess_connector_records(
    *,
    session: Session,
    definition,
    metro_id: str,
    records: list[dict[str, object]],
) -> list[dict[str, object]]:
    strategy = (definition.preprocess_strategy or "").strip().lower()
    if not strategy or strategy in {"identity", "none"}:
        return [dict(record) for record in records]
    if strategy == "zoning_overlay_to_parcels":
        return _expand_zoning_overlay_records(
            session=session,
            metro_id=metro_id,
            records=records,
        )
    if strategy == "expand_evidence_attributes":
        return _expand_evidence_attribute_records(
            definition=definition,
            records=records,
        )
    if strategy == "spatial_filter_expand_evidence_attributes":
        return _spatial_filter_expand_evidence_attribute_records(
            definition=definition,
            records=records,
        )
    raise ConnectorConfigurationError(
        f"Unsupported preprocess strategy `{definition.preprocess_strategy}` for "
        f"connector `{definition.connector_key}`."
    )


def _expand_zoning_overlay_records(
    *,
    session: Session,
    metro_id: str,
    records: list[dict[str, object]],
) -> list[dict[str, object]]:
    parcel_rows = session.execute(
        select(RawParcel.parcel_id, RawParcel.county_fips, ParcelRepPoint.rep_point_wkt)
        .join(ParcelRepPoint, ParcelRepPoint.parcel_id == RawParcel.parcel_id)
        .where(RawParcel.metro_id == metro_id, RawParcel.is_active.is_(True))
        .order_by(RawParcel.parcel_id)
    ).all()
    if not parcel_rows:
        raise ConnectorExecutionError(
            "Zoning overlay expansion requires canonical parcels with representative "
            "points for the target metro."
        )

    parcel_points: list[tuple[str, str, object]] = []
    point_geometries: list[object] = []
    for parcel_id, county_fips, rep_point_wkt in parcel_rows:
        try:
            point_geometry = wkt.loads(rep_point_wkt)
        except (TypeError, ShapelyError) as exc:
            raise ConnectorExecutionError(
                f"Parcel `{parcel_id}` has an invalid representative point."
            ) from exc
        parcel_points.append((parcel_id, county_fips, point_geometry))
        point_geometries.append(point_geometry)

    spatial_index = STRtree(point_geometries)

    best_matches: dict[str, tuple[float, dict[str, object]]] = {}
    for row_number, record in enumerate(records, start=1):
        geometry_wkt = _safe_string(record.get("geometry_wkt"))
        if geometry_wkt is None:
            raise ConnectorExecutionError(
                "Zoning overlay expansion requires `geometry_wkt` on each source row."
            )

        try:
            zone_geometry = wkt.loads(geometry_wkt)
        except (TypeError, ShapelyError) as exc:
            raise ConnectorExecutionError(
                f"Zoning overlay row {row_number} has invalid geometry."
            ) from exc

        if zone_geometry.is_empty:
            continue

        county_filter = _safe_string(record.get("county_fips"))
        zone_area = float(zone_geometry.area) if not zone_geometry.is_empty else float("inf")
        lineage_prefix = _safe_string(record.get("lineage_key")) or f"zoning-overlay:{row_number}"

        # Query the parcel points spatial index first so we only test parcels
        # whose bounding boxes intersect the zoning polygon.
        candidate_indexes = spatial_index.query(zone_geometry)
        for candidate_index in candidate_indexes:
            parcel_id, county_fips, parcel_point = parcel_points[int(candidate_index)]
            if county_filter is not None and county_filter != county_fips:
                continue
            if not zone_geometry.covers(parcel_point):
                continue

            expanded_record = dict(record)
            expanded_record["parcel_id"] = parcel_id
            expanded_record["county_fips"] = county_fips
            expanded_record["lineage_key"] = f"{lineage_prefix}:{parcel_id}"

            existing_match = best_matches.get(parcel_id)
            if existing_match is None or zone_area < existing_match[0]:
                best_matches[parcel_id] = (zone_area, expanded_record)

    return [
        best_matches[parcel_id][1]
        for parcel_id in sorted(best_matches)
    ]


def _expand_evidence_attribute_records(
    *,
    definition,
    records: list[dict[str, object]],
) -> list[dict[str, object]]:
    attribute_fields = definition.preprocess_options.get("attribute_fields") or []
    if not attribute_fields:
        raise ConnectorConfigurationError(
            f"Connector `{definition.connector_key}` requires `attribute_fields` when "
            "using `expand_evidence_attributes`."
        )

    record_key_template = _safe_string(
        definition.preprocess_options.get("record_key_template")
    )
    lineage_key_template = _safe_string(
        definition.preprocess_options.get("lineage_key_template")
    ) or "{record_key}:{attribute_name}"
    county_rule = _build_optional_rule(
        target="county_fips",
        source=definition.preprocess_options.get("county_fips_source"),
        transform="strip",
    )
    parcel_rule = _build_optional_rule(
        target="parcel_id",
        source=definition.preprocess_options.get("parcel_id_source"),
        transform="strip",
    )

    expanded_records: list[dict[str, object]] = []
    for row_number, raw_record in enumerate(records, start=1):
        base_context = dict(raw_record)
        record_key = (
            record_key_template.format_map(base_context)
            if record_key_template is not None
            else str(raw_record.get("__feature_id__") or row_number)
        )
        county_fips = (
            resolve_connector_field_rule_value(
                raw_record=raw_record,
                mapped_record={},
                field_rule=county_rule,
            )
            if county_rule is not None
            else None
        )
        parcel_id = (
            resolve_connector_field_rule_value(
                raw_record=raw_record,
                mapped_record={},
                field_rule=parcel_rule,
            )
            if parcel_rule is not None
            else None
        )

        for attribute_config in attribute_fields:
            field_rule = SourceConnectorFieldRule(
                target="attribute_value",
                source=attribute_config.get("source"),
                transform=attribute_config.get("transform", "identity"),
                template=attribute_config.get("template"),
                default=attribute_config.get("default"),
                options=dict(attribute_config.get("options") or {}),
            )
            attribute_name = _safe_string(attribute_config.get("attribute_name"))
            if attribute_name is None:
                raise ConnectorConfigurationError(
                    f"Connector `{definition.connector_key}` has an evidence attribute "
                    "without `attribute_name`."
                )
            attribute_value = resolve_connector_field_rule_value(
                raw_record=raw_record,
                mapped_record={},
                field_rule=field_rule,
            )
            if attribute_value is None or str(attribute_value).strip() == "":
                continue

            lineage_context = {
                **base_context,
                "record_key": record_key,
                "attribute_name": attribute_name,
            }
            expanded_records.append(
                {
                    "record_key": record_key,
                    "attribute_name": attribute_name,
                    "attribute_value": str(attribute_value),
                    "lineage_key": lineage_key_template.format_map(lineage_context),
                    "county_fips": county_fips,
                    "parcel_id": parcel_id,
                }
            )

    return expanded_records


def _spatial_filter_expand_evidence_attribute_records(
    *,
    definition,
    records: list[dict[str, object]],
) -> list[dict[str, object]]:
    boundary_geometry = _load_boundary_geometry(definition)
    filtered_records: list[dict[str, object]] = []

    for row_number, raw_record in enumerate(records, start=1):
        geometry_payload = raw_record.get("__geometry__")
        if geometry_payload is None:
            raise ConnectorExecutionError(
                "Spatial evidence filtering requires `__geometry__` on each source row."
            )
        try:
            source_geometry = shapely_shape(geometry_payload)
        except (AttributeError, TypeError, ValueError, ShapelyError) as exc:
            raise ConnectorExecutionError(
                f"Spatial evidence row {row_number} has invalid geometry."
            ) from exc

        if source_geometry.is_empty:
            continue
        if not source_geometry.intersects(boundary_geometry):
            continue
        filtered_records.append(dict(raw_record))

    return _expand_evidence_attribute_records(
        definition=definition,
        records=filtered_records,
    )


def _load_boundary_geometry(definition):
    boundary_geojson_path = _safe_string(
        definition.preprocess_options.get("boundary_geojson_path")
    )
    boundary_wkt = _safe_string(definition.preprocess_options.get("boundary_wkt"))

    if boundary_geojson_path is not None:
        boundary_path = Path(boundary_geojson_path)
        cache_key = f"geojson:{boundary_path.resolve()}"
        cached_geometry = _BOUNDARY_GEOMETRY_CACHE.get(cache_key)
        if cached_geometry is not None:
            return cached_geometry
        if not boundary_path.exists():
            raise ConnectorConfigurationError(
                f"Connector `{definition.connector_key}` boundary path "
                f"`{boundary_path}` does not exist."
            )
        payload = json.loads(boundary_path.read_text(encoding="utf-8"))
        geometry_payload = payload.get("geometry") if payload.get("type") == "Feature" else payload
        try:
            boundary_geometry = shapely_shape(geometry_payload)
        except (AttributeError, TypeError, ValueError, ShapelyError) as exc:
            raise ConnectorConfigurationError(
                f"Connector `{definition.connector_key}` boundary GeoJSON is invalid."
            ) from exc
    elif boundary_wkt is not None:
        cache_key = f"wkt:{boundary_wkt}"
        cached_geometry = _BOUNDARY_GEOMETRY_CACHE.get(cache_key)
        if cached_geometry is not None:
            return cached_geometry
        try:
            boundary_geometry = wkt.loads(boundary_wkt)
        except (TypeError, ShapelyError) as exc:
            raise ConnectorConfigurationError(
                f"Connector `{definition.connector_key}` boundary WKT is invalid."
            ) from exc
    else:
        raise ConnectorConfigurationError(
            f"Connector `{definition.connector_key}` requires `boundary_geojson_path` "
            "or `boundary_wkt` when using `spatial_filter_expand_evidence_attributes`."
        )

    if boundary_geometry.is_empty:
        raise ConnectorConfigurationError(
            f"Connector `{definition.connector_key}` boundary geometry cannot be empty."
        )

    _BOUNDARY_GEOMETRY_CACHE[cache_key] = boundary_geometry
    return boundary_geometry


def _snapshot_ts(snapshot: SourceSnapshot | None) -> datetime | None:
    if snapshot is None:
        return None
    return _coerce_timestamp(snapshot.snapshot_ts)


def _snapshot_status(snapshot: SourceSnapshot | None) -> str | None:
    if snapshot is None:
        return None
    return snapshot.status.value


def _coerce_timestamp(timestamp: datetime | None) -> datetime:
    normalized = timestamp or datetime.now(UTC)
    if normalized.tzinfo is None:
        return normalized.replace(tzinfo=UTC)
    return normalized.astimezone(UTC)


def _to_job_report(job: SourceRefreshJob) -> SourceRefreshJobReport:
    return SourceRefreshJobReport(
        job_id=str(job.job_id),
        source_id=job.source_id,
        metro_id=job.metro_id,
        connector_key=job.connector_key,
        trigger_mode=job.trigger_mode,
        actor_name=job.actor_name,
        status=job.status,
        started_at=job.started_at,
        completed_at=job.completed_at,
        attempt_count=job.attempt_count,
        source_version=job.source_version,
        snapshot_id=str(job.snapshot_id) if job.snapshot_id is not None else None,
        row_count=job.row_count,
        accepted_count=job.accepted_count,
        rejected_count=job.rejected_count,
        checkpoint_in_ts=job.checkpoint_in_ts,
        checkpoint_out_ts=job.checkpoint_out_ts,
        checkpoint_cursor_in=job.checkpoint_cursor_in,
        checkpoint_cursor_out=job.checkpoint_cursor_out,
        error_message=job.error_message,
    )


def _safe_string(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _build_optional_rule(
    *,
    target: str,
    source: object,
    transform: str = "identity",
) -> SourceConnectorFieldRule | None:
    normalized_source = _safe_string(source)
    if normalized_source is None:
        return None
    return SourceConnectorFieldRule(
        target=target,
        source=normalized_source,
        transform=transform,
    )
