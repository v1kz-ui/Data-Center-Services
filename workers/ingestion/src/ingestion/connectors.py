from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from shapely.errors import ShapelyError
from shapely.geometry import shape as shapely_shape


class ConnectorConfigurationError(LookupError):
    """Raised when a configured connector cannot be found or parsed."""


class ConnectorExecutionError(RuntimeError):
    """Raised when a connector fails to retrieve payload records."""


@dataclass(slots=True)
class ConnectorCheckpoint:
    checkpoint_ts: datetime | None = None
    checkpoint_cursor: str | None = None
    source_version: str | None = None


@dataclass(slots=True)
class ConnectorFetchPolicy:
    max_attempts: int = 3
    backoff_seconds: float = 0.0
    rate_limit_per_minute: int | None = None
    checkpoint_field: str | None = None
    checkpoint_param: str | None = None


@dataclass(slots=True)
class ConnectorRequestPaginationConfig:
    strategy: str = "none"
    page_size: int = 1000
    max_pages: int | None = None
    offset_param: str = "resultOffset"
    page_size_param: str = "resultRecordCount"


@dataclass(slots=True)
class ConnectorRequestConfig:
    endpoint_url: str | None = None
    query_params: dict[str, str] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    auth_header_name: str | None = None
    auth_env_var: str | None = None
    record_path: list[str] = field(default_factory=list)
    timeout_seconds: float = 30.0
    pagination: ConnectorRequestPaginationConfig = field(
        default_factory=ConnectorRequestPaginationConfig
    )


@dataclass(slots=True)
class SourceConnectorFieldRule:
    target: str
    source: str | list[str] | None = None
    transform: str = "identity"
    template: str | None = None
    default: Any = None
    options: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ConnectorFetchResult:
    records: list[dict[str, Any]]
    checkpoint_ts: datetime | None
    checkpoint_cursor: str | None
    attempt_count: int


@dataclass(slots=True)
class SourceConnectorDefinition:
    connector_key: str
    source_id: str
    metro_id: str
    interface_name: str
    adapter_type: str
    enabled: bool
    inventory_if_codes: list[str] = field(default_factory=list)
    priority: int = 100
    description: str | None = None
    preprocess_strategy: str | None = None
    fetch_policy: ConnectorFetchPolicy = field(default_factory=ConnectorFetchPolicy)
    request: ConnectorRequestConfig = field(default_factory=ConnectorRequestConfig)
    field_map: dict[str, str] = field(default_factory=dict)
    field_rules: list[SourceConnectorFieldRule] = field(default_factory=list)
    static_fields: dict[str, Any] = field(default_factory=dict)
    fixture_records: list[dict[str, Any]] = field(default_factory=list)

    @property
    def normalized_source_id(self) -> str:
        return self.source_id.strip().upper()

    @property
    def normalized_metro_id(self) -> str:
        return self.metro_id.strip().upper()


class _SafeFormatDict(dict[str, Any]):
    def __missing__(self, key: str) -> str:
        return ""


class _RequestRateLimiter:
    def __init__(self) -> None:
        self._last_request_at: dict[str, float] = {}

    def wait(self, connector_key: str, rate_limit_per_minute: int | None) -> None:
        if rate_limit_per_minute is None or rate_limit_per_minute <= 0:
            return

        interval_seconds = 60.0 / rate_limit_per_minute
        last_request_at = self._last_request_at.get(connector_key)
        now = time.monotonic()
        if last_request_at is not None:
            elapsed = now - last_request_at
            if elapsed < interval_seconds:
                time.sleep(interval_seconds - elapsed)
        self._last_request_at[connector_key] = time.monotonic()


_RATE_LIMITER = _RequestRateLimiter()


class ConnectorRegistry:
    def __init__(self, definitions: list[SourceConnectorDefinition]) -> None:
        self._definitions_by_connector_key = {
            definition.connector_key: definition for definition in definitions
        }
        self._definitions_by_source_metro: dict[
            tuple[str, str], list[SourceConnectorDefinition]
        ] = {}
        for definition in definitions:
            key = (definition.normalized_source_id, definition.normalized_metro_id)
            self._definitions_by_source_metro.setdefault(key, []).append(definition)

    def get_definition(self, source_id: str, metro_id: str) -> SourceConnectorDefinition:
        key = (source_id.strip().upper(), metro_id.strip().upper())
        candidates = sorted(
            self._definitions_by_source_metro.get(key, []),
            key=lambda definition: (
                not definition.enabled,
                definition.priority,
                definition.connector_key,
            ),
        )
        if not candidates:
            raise ConnectorConfigurationError(
                f"Connector `{key[0]}` for metro `{key[1]}` is not configured."
            )

        enabled_candidates = [definition for definition in candidates if definition.enabled]
        if len(enabled_candidates) == 1:
            return enabled_candidates[0]
        if len(enabled_candidates) > 1:
            top = enabled_candidates[0]
            runner_up = enabled_candidates[1]
            if top.priority != runner_up.priority:
                return top
            raise ConnectorConfigurationError(
                f"Multiple enabled connectors are configured for source `{key[0]}` "
                f"and metro `{key[1]}`."
            )
        return candidates[0]

    def get_definition_by_connector_key(self, connector_key: str) -> SourceConnectorDefinition:
        definition = self._definitions_by_connector_key.get(connector_key.strip())
        if definition is None:
            raise ConnectorConfigurationError(
                f"Connector key `{connector_key.strip()}` is not configured."
            )
        return definition

    def list_definitions(self, *, enabled_only: bool = False) -> list[SourceConnectorDefinition]:
        definitions = list(self._definitions_by_connector_key.values())
        if enabled_only:
            definitions = [definition for definition in definitions if definition.enabled]
        return sorted(
            definitions,
            key=lambda definition: (
                definition.normalized_source_id,
                definition.normalized_metro_id,
                definition.priority,
                definition.connector_key,
            ),
        )


def load_connector_registry(config_path: str) -> ConnectorRegistry:
    path = Path(config_path)
    if not path.exists():
        raise ConnectorConfigurationError(f"Connector config path `{path}` does not exist.")

    payload = json.loads(path.read_text(encoding="utf-8"))
    definitions_payload = payload.get("definitions", [])
    if not isinstance(definitions_payload, list):
        raise ConnectorConfigurationError("Connector config must contain a `definitions` list.")

    definitions = [_parse_definition(item) for item in definitions_payload]
    return ConnectorRegistry(definitions)


def fetch_connector_records(
    definition: SourceConnectorDefinition,
    checkpoint: ConnectorCheckpoint | None = None,
) -> ConnectorFetchResult:
    if not definition.enabled:
        raise ConnectorExecutionError(
            f"Connector `{definition.connector_key}` is disabled and cannot be refreshed."
        )

    if definition.adapter_type == "fixture":
        return _fetch_fixture_records(definition, checkpoint)
    if definition.adapter_type == "http_json":
        return _fetch_http_json_records(definition, checkpoint)
    if definition.adapter_type == "arcgis_feature_service":
        return _fetch_arcgis_feature_service_records(definition, checkpoint)

    raise ConnectorConfigurationError(
        f"Connector `{definition.connector_key}` has unsupported adapter type "
        f"`{definition.adapter_type}`."
    )


def _parse_definition(payload: dict[str, Any]) -> SourceConnectorDefinition:
    fetch_policy_payload = payload.get("fetch_policy") or {}
    request_payload = payload.get("request") or {}
    return SourceConnectorDefinition(
        connector_key=str(payload["connector_key"]).strip(),
        source_id=str(payload["source_id"]).strip().upper(),
        metro_id=str(payload["metro_id"]).strip().upper(),
        interface_name=str(payload["interface_name"]).strip(),
        adapter_type=str(payload.get("adapter_type", "fixture")).strip().lower(),
        enabled=bool(payload.get("enabled", True)),
        inventory_if_codes=_normalize_string_list(payload.get("inventory_if_codes")),
        priority=int(payload.get("priority", 100)),
        description=_normalize_optional_string(payload.get("description")),
        preprocess_strategy=_normalize_optional_string(payload.get("preprocess_strategy")),
        fetch_policy=ConnectorFetchPolicy(
            max_attempts=max(int(fetch_policy_payload.get("max_attempts", 3)), 1),
            backoff_seconds=float(fetch_policy_payload.get("backoff_seconds", 0.0)),
            rate_limit_per_minute=(
                int(fetch_policy_payload["rate_limit_per_minute"])
                if fetch_policy_payload.get("rate_limit_per_minute") is not None
                else None
            ),
            checkpoint_field=_normalize_optional_string(
                fetch_policy_payload.get("checkpoint_field")
            ),
            checkpoint_param=_normalize_optional_string(
                fetch_policy_payload.get("checkpoint_param")
            ),
        ),
        request=ConnectorRequestConfig(
            endpoint_url=_normalize_optional_string(request_payload.get("endpoint_url")),
            query_params={
                str(key): str(value)
                for key, value in (request_payload.get("query_params") or {}).items()
            },
            headers={
                str(key): str(value)
                for key, value in (request_payload.get("headers") or {}).items()
            },
            auth_header_name=_normalize_optional_string(request_payload.get("auth_header_name")),
            auth_env_var=_normalize_optional_string(request_payload.get("auth_env_var")),
            record_path=_normalize_path(request_payload.get("record_path")),
            timeout_seconds=float(request_payload.get("timeout_seconds", 30.0)),
            pagination=ConnectorRequestPaginationConfig(
                strategy=_normalize_optional_string(
                    (request_payload.get("pagination") or {}).get("strategy")
                )
                or "none",
                page_size=max(
                    int((request_payload.get("pagination") or {}).get("page_size", 1000)),
                    1,
                ),
                max_pages=(
                    int((request_payload.get("pagination") or {}).get("max_pages"))
                    if (request_payload.get("pagination") or {}).get("max_pages") is not None
                    else None
                ),
                offset_param=_normalize_optional_string(
                    (request_payload.get("pagination") or {}).get("offset_param")
                )
                or "resultOffset",
                page_size_param=_normalize_optional_string(
                    (request_payload.get("pagination") or {}).get("page_size_param")
                )
                or "resultRecordCount",
            ),
        ),
        field_map={
            str(key): str(value)
            for key, value in (payload.get("field_map") or {}).items()
        },
        field_rules=[
            SourceConnectorFieldRule(
                target=str(rule_payload["target"]).strip(),
                source=_normalize_rule_source(rule_payload.get("source")),
                transform=_normalize_optional_string(rule_payload.get("transform")) or "identity",
                template=_normalize_optional_string(rule_payload.get("template")),
                default=rule_payload.get("default"),
                options=dict(rule_payload.get("options") or {}),
            )
            for rule_payload in (payload.get("field_rules") or [])
        ],
        static_fields=dict(payload.get("static_fields") or {}),
        fixture_records=[dict(record) for record in payload.get("fixture_records", [])],
    )


def _fetch_fixture_records(
    definition: SourceConnectorDefinition,
    checkpoint: ConnectorCheckpoint | None,
) -> ConnectorFetchResult:
    filtered_raw_records = _filter_incremental_records(
        definition.fixture_records,
        checkpoint,
        definition.fetch_policy.checkpoint_field,
    )
    mapped_records = [_map_record(raw_record, definition) for raw_record in filtered_raw_records]
    checkpoint_ts = _max_checkpoint_ts(
        filtered_raw_records,
        definition.fetch_policy.checkpoint_field,
    )
    return ConnectorFetchResult(
        records=mapped_records,
        checkpoint_ts=checkpoint_ts or (checkpoint.checkpoint_ts if checkpoint else None),
        checkpoint_cursor=None,
        attempt_count=1,
    )


def _fetch_http_json_records(
    definition: SourceConnectorDefinition,
    checkpoint: ConnectorCheckpoint | None,
) -> ConnectorFetchResult:
    last_exception: Exception | None = None

    for attempt_index in range(1, definition.fetch_policy.max_attempts + 1):
        try:
            _RATE_LIMITER.wait(
                definition.connector_key,
                definition.fetch_policy.rate_limit_per_minute,
            )
            request = _build_request(definition, checkpoint)
            with urlopen(request, timeout=definition.request.timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))

            raw_records = _extract_record_list(payload, definition.request.record_path)
            filtered_raw_records = _filter_incremental_records(
                raw_records,
                checkpoint,
                definition.fetch_policy.checkpoint_field,
            )
            mapped_records = [
                _map_record(raw_record, definition) for raw_record in filtered_raw_records
            ]
            checkpoint_ts = _max_checkpoint_ts(
                filtered_raw_records,
                definition.fetch_policy.checkpoint_field,
            )
            return ConnectorFetchResult(
                records=mapped_records,
                checkpoint_ts=checkpoint_ts or (checkpoint.checkpoint_ts if checkpoint else None),
                checkpoint_cursor=None,
                attempt_count=attempt_index,
            )
        except (HTTPError, URLError, TimeoutError, ValueError) as exc:
            last_exception = exc
            if attempt_index < definition.fetch_policy.max_attempts:
                time.sleep(definition.fetch_policy.backoff_seconds)

    raise ConnectorExecutionError(
        f"Connector `{definition.connector_key}` failed after "
        f"{definition.fetch_policy.max_attempts} attempt(s): {last_exception}"
    ) from last_exception


def _fetch_arcgis_feature_service_records(
    definition: SourceConnectorDefinition,
    checkpoint: ConnectorCheckpoint | None,
) -> ConnectorFetchResult:
    last_exception: Exception | None = None

    for attempt_index in range(1, definition.fetch_policy.max_attempts + 1):
        try:
            _RATE_LIMITER.wait(
                definition.connector_key,
                definition.fetch_policy.rate_limit_per_minute,
            )
            raw_records = _fetch_arcgis_feature_pages(definition, checkpoint)
            filtered_raw_records = _filter_incremental_records(
                raw_records,
                checkpoint,
                definition.fetch_policy.checkpoint_field,
            )
            mapped_records = [
                _map_record(raw_record, definition) for raw_record in filtered_raw_records
            ]
            checkpoint_ts = _max_checkpoint_ts(
                filtered_raw_records,
                definition.fetch_policy.checkpoint_field,
            )
            return ConnectorFetchResult(
                records=mapped_records,
                checkpoint_ts=checkpoint_ts or (checkpoint.checkpoint_ts if checkpoint else None),
                checkpoint_cursor=None,
                attempt_count=attempt_index,
            )
        except (HTTPError, URLError, TimeoutError, ValueError) as exc:
            last_exception = exc
            if attempt_index < definition.fetch_policy.max_attempts:
                time.sleep(definition.fetch_policy.backoff_seconds)

    raise ConnectorExecutionError(
        f"Connector `{definition.connector_key}` failed after "
        f"{definition.fetch_policy.max_attempts} attempt(s): {last_exception}"
    ) from last_exception


def _fetch_arcgis_feature_pages(
    definition: SourceConnectorDefinition,
    checkpoint: ConnectorCheckpoint | None,
) -> list[dict[str, Any]]:
    pagination = definition.request.pagination
    page_size = pagination.page_size
    max_pages = pagination.max_pages
    page_index = 0
    offset = 0
    collected_records: list[dict[str, Any]] = []

    while True:
        extra_query_params: dict[str, str] = {}
        if pagination.strategy in {"none", "arcgis_offset"}:
            extra_query_params[pagination.offset_param] = str(offset)
            extra_query_params[pagination.page_size_param] = str(page_size)
        else:
            raise ConnectorConfigurationError(
                f"Unsupported pagination strategy `{pagination.strategy}` for "
                f"connector `{definition.connector_key}`."
            )

        request = _build_request(
            definition,
            checkpoint,
            extra_query_params=extra_query_params,
            force_query_path=True,
        )
        with urlopen(request, timeout=definition.request.timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))

        page_records = _extract_arcgis_feature_list(payload)
        if not page_records:
            break

        collected_records.extend(page_records)
        page_index += 1
        if max_pages is not None and page_index >= max_pages:
            break
        if len(page_records) < page_size:
            break
        offset += len(page_records)

    return collected_records


def _build_request(
    definition: SourceConnectorDefinition,
    checkpoint: ConnectorCheckpoint | None,
    *,
    extra_query_params: dict[str, str] | None = None,
    force_query_path: bool = False,
) -> Request:
    endpoint_url = definition.request.endpoint_url
    if endpoint_url is None:
        raise ConnectorConfigurationError(
            f"Connector `{definition.connector_key}` does not define an endpoint URL."
        )

    normalized_endpoint = endpoint_url.rstrip("/")
    if force_query_path and not normalized_endpoint.endswith("/query"):
        normalized_endpoint = f"{normalized_endpoint}/query"

    query_params = dict(definition.request.query_params)
    if force_query_path:
        query_params.setdefault("where", "1=1")
        query_params.setdefault("outFields", "*")
        query_params.setdefault("returnGeometry", "true")
        query_params.setdefault("f", "geojson")
    if extra_query_params:
        query_params.update(extra_query_params)
    if (
        checkpoint is not None
        and checkpoint.checkpoint_ts is not None
        and definition.fetch_policy.checkpoint_param is not None
    ):
        query_params[definition.fetch_policy.checkpoint_param] = (
            checkpoint.checkpoint_ts.isoformat()
        )

    encoded_query = urlencode(query_params)
    target_url = (
        normalized_endpoint
        if not encoded_query
        else f"{normalized_endpoint}?{encoded_query}"
    )
    headers = dict(definition.request.headers)

    if definition.request.auth_header_name and definition.request.auth_env_var:
        token = os.getenv(definition.request.auth_env_var)
        if not token:
            raise ConnectorConfigurationError(
                f"Connector `{definition.connector_key}` requires env var "
                f"`{definition.request.auth_env_var}`."
            )
        headers[definition.request.auth_header_name] = token

    return Request(url=target_url, headers=headers)


def _extract_record_list(payload: Any, record_path: list[str]) -> list[dict[str, Any]]:
    extracted = payload
    for path_part in record_path:
        if not isinstance(extracted, dict) or path_part not in extracted:
            raise ConnectorExecutionError(
                f"Unable to resolve record path `{'/'.join(record_path)}` in connector payload."
            )
        extracted = extracted[path_part]

    if isinstance(extracted, list):
        return [dict(item) for item in extracted]
    raise ConnectorExecutionError("Connector payload did not resolve to a list of records.")


def _extract_arcgis_feature_list(payload: Any) -> list[dict[str, Any]]:
    features = payload.get("features") if isinstance(payload, dict) else None
    if not isinstance(features, list):
        raise ConnectorExecutionError("ArcGIS payload does not contain a `features` list.")

    normalized_records: list[dict[str, Any]] = []
    for feature in features:
        if not isinstance(feature, dict):
            continue
        properties = dict(feature.get("properties") or feature.get("attributes") or {})
        properties["__geometry__"] = feature.get("geometry")
        if feature.get("id") is not None:
            properties["__feature_id__"] = feature.get("id")
        normalized_records.append(properties)
    return normalized_records


def _filter_incremental_records(
    raw_records: list[dict[str, Any]],
    checkpoint: ConnectorCheckpoint | None,
    checkpoint_field: str | None,
) -> list[dict[str, Any]]:
    if checkpoint is None or checkpoint.checkpoint_ts is None or checkpoint_field is None:
        return [dict(record) for record in raw_records]

    filtered: list[dict[str, Any]] = []
    for raw_record in raw_records:
        raw_value = raw_record.get(checkpoint_field)
        checkpoint_ts = _coerce_datetime(raw_value)
        if checkpoint_ts is None or checkpoint_ts > checkpoint.checkpoint_ts:
            filtered.append(dict(raw_record))
    return filtered


def _max_checkpoint_ts(
    raw_records: list[dict[str, Any]],
    checkpoint_field: str | None,
) -> datetime | None:
    if checkpoint_field is None:
        return None

    checkpoint_values = [
        checkpoint_ts
        for checkpoint_ts in (
            _coerce_datetime(record.get(checkpoint_field)) for record in raw_records
        )
        if checkpoint_ts is not None
    ]
    return max(checkpoint_values, default=None)


def _map_record(
    raw_record: dict[str, Any],
    definition: SourceConnectorDefinition,
) -> dict[str, Any]:
    mapped_record = dict(definition.static_fields)

    if definition.field_rules:
        for field_rule in definition.field_rules:
            mapped_record[field_rule.target] = _apply_field_rule(
                raw_record=raw_record,
                mapped_record=mapped_record,
                field_rule=field_rule,
            )
        return mapped_record

    for target_key, source_key in definition.field_map.items():
        mapped_record[target_key] = _resolve_field(raw_record, source_key)
    return mapped_record


def _apply_field_rule(
    *,
    raw_record: dict[str, Any],
    mapped_record: dict[str, Any],
    field_rule: SourceConnectorFieldRule,
) -> Any:
    context = _SafeFormatDict(
        {
            **raw_record,
            **mapped_record,
        }
    )
    raw_value = _extract_source_value(raw_record, field_rule.source)
    if field_rule.template is not None:
        raw_value = field_rule.template.format_map(context)

    transformed_value = _apply_transform(
        value=raw_value,
        context=context,
        field_rule=field_rule,
    )
    if transformed_value is None and field_rule.default is not None:
        return field_rule.default
    return transformed_value


def _apply_transform(
    *,
    value: Any,
    context: dict[str, Any],
    field_rule: SourceConnectorFieldRule,
) -> Any:
    transform = field_rule.transform.lower()
    if transform == "identity":
        return value
    if transform == "stringify":
        return None if value is None else str(value)
    if transform == "strip":
        return _strip_or_none(value)
    if transform == "upper":
        normalized = _strip_or_none(value)
        return normalized.upper() if normalized is not None else None
    if transform == "lower":
        normalized = _strip_or_none(value)
        return normalized.lower() if normalized is not None else None
    if transform == "geojson_to_wkt":
        if value is None:
            return None
        try:
            return shapely_shape(value).wkt
        except (AttributeError, TypeError, ValueError, ShapelyError) as exc:
            raise ConnectorExecutionError("Unable to convert GeoJSON geometry into WKT.") from exc
    if transform == "square_feet_to_acres":
        if value is None or str(value).strip() == "":
            return None
        acreage_value = float(value) / 43560
        return f"{acreage_value:.6f}".rstrip("0").rstrip(".")
    if transform == "map_value":
        mapping = {
            str(key): mapped_value
            for key, mapped_value in (field_rule.options.get("map") or {}).items()
        }
        normalized = _strip_or_none(value)
        if normalized is None:
            return None
        if normalized in mapping:
            return mapping[normalized]
        if field_rule.options.get("preserve_unmapped", False):
            return normalized
        return field_rule.default
    if transform == "template":
        if field_rule.template is None:
            return value
        return field_rule.template.format_map(_SafeFormatDict(context))

    raise ConnectorConfigurationError(
        f"Unsupported transform `{field_rule.transform}` in connector field rule."
    )


def _extract_source_value(
    raw_record: dict[str, Any],
    source: str | list[str] | None,
) -> Any:
    if source is None:
        return None
    if isinstance(source, list):
        for candidate in source:
            value = _resolve_field(raw_record, candidate)
            if value is not None and str(value).strip() != "":
                return value
        return None
    return _resolve_field(raw_record, source)


def _resolve_field(raw_record: dict[str, Any], field_path: str) -> Any:
    current_value: Any = raw_record
    for path_part in field_path.split("."):
        if not isinstance(current_value, dict):
            return None
        current_value = current_value.get(path_part)
    return current_value


def _normalize_rule_source(value: Any) -> str | list[str] | None:
    if value is None:
        return None
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    normalized = str(value).strip()
    return normalized or None


def _normalize_path(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    normalized = str(value).strip()
    return [part for part in normalized.split(".") if part]


def _normalize_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    normalized = str(value).strip()
    return [normalized] if normalized else []


def _normalize_optional_string(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _strip_or_none(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if isinstance(value, (int, float)):
        timestamp_value = float(value)
        if timestamp_value > 10_000_000_000:
            timestamp_value /= 1000
        return datetime.fromtimestamp(timestamp_value, tz=UTC)
    try:
        normalized = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
