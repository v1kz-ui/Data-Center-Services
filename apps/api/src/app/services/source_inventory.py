from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ingestion.connectors import ConnectorRegistry


class SourceInventoryConfigurationError(LookupError):
    """Raised when the authoritative source inventory cannot be loaded or validated."""


@dataclass(slots=True)
class SourceInventoryEntry:
    if_code: str
    name: str
    category: str
    phase: int
    protocol: str
    auth: str
    url: str | None
    target_table: str | None
    secondary_url: str | None = None
    target_partition: str | None = None
    county: str | None = None
    city: str | None = None
    reference_only: bool = False
    free: bool = True
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class SourceInventoryFlag:
    flag_key: str
    name: str
    phase: int
    metro: str
    target_behavior: str
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class SourceInventoryPhaseTotal:
    phase: int
    scope: str
    source_count: int
    config_flag_count: int = 0


@dataclass(slots=True)
class SourceInventoryPhaseCount:
    phase: int
    source_count: int
    config_flag_count: int


@dataclass(slots=True)
class SourceInventoryCategoryCount:
    category: str
    source_count: int


@dataclass(slots=True)
class AuthoritativeSourceInventory:
    version: str
    captured_at: str | None
    sources: list[SourceInventoryEntry]
    config_flags: list[SourceInventoryFlag]
    phase_totals: list[SourceInventoryPhaseTotal]


@dataclass(slots=True)
class SourceInventorySummary:
    version: str
    captured_at: str | None
    total_sources: int
    total_config_flags: int
    filtered_source_count: int
    filtered_config_flag_count: int
    phase_counts: list[SourceInventoryPhaseCount]
    category_counts: list[SourceInventoryCategoryCount]
    sources: list[SourceInventoryEntry]
    config_flags: list[SourceInventoryFlag]


@dataclass(slots=True)
class SourceInventoryCoverageItem:
    if_code: str
    name: str
    category: str
    phase: int
    implemented: bool
    connector_keys: list[str] = field(default_factory=list)
    enabled_connector_keys: list[str] = field(default_factory=list)


@dataclass(slots=True)
class SourceInventoryCoveragePhase:
    phase: int
    total_sources: int
    covered_sources: int
    uncovered_sources: int


@dataclass(slots=True)
class SourceInventoryCoverage:
    total_sources: int
    covered_sources: int
    uncovered_sources: int
    coverage_percent: float
    phase_coverage: list[SourceInventoryCoveragePhase]
    covered_if_codes: list[str]
    uncovered_if_codes: list[str]
    items: list[SourceInventoryCoverageItem]
    unmapped_connector_keys: list[str]
    orphaned_connector_if_codes: list[str]


def load_authoritative_source_inventory(config_path: str) -> AuthoritativeSourceInventory:
    path = Path(config_path)
    if not path.exists():
        raise SourceInventoryConfigurationError(
            f"Authoritative source inventory path `{path}` does not exist."
        )

    payload = json.loads(path.read_text(encoding="utf-8"))
    sources_payload = payload.get("sources", [])
    if not isinstance(sources_payload, list):
        raise SourceInventoryConfigurationError(
            "Authoritative source inventory must contain a `sources` list."
        )

    inventory = AuthoritativeSourceInventory(
        version=_require_string(payload, "version"),
        captured_at=_normalize_optional_string(payload.get("captured_at")),
        sources=[_parse_source_entry(item) for item in sources_payload],
        config_flags=[
            _parse_source_flag(item)
            for item in _expect_list(
                payload.get("config_flags"),
                "config_flags",
            )
        ],
        phase_totals=[
            _parse_phase_total(item)
            for item in _expect_list(
                payload.get("phase_totals"),
                "phase_totals",
            )
        ],
    )
    _validate_inventory(inventory)
    return inventory


def list_inventory_sources(
    inventory: AuthoritativeSourceInventory,
    *,
    phase: int | None = None,
    category: str | None = None,
) -> list[SourceInventoryEntry]:
    normalized_category = _normalize_optional_string(category)
    return [
        source
        for source in inventory.sources
        if (phase is None or source.phase <= phase)
        and (normalized_category is None or source.category == normalized_category)
    ]


def list_inventory_flags(
    inventory: AuthoritativeSourceInventory,
    *,
    phase: int | None = None,
) -> list[SourceInventoryFlag]:
    return [
        flag for flag in inventory.config_flags if phase is None or flag.phase <= phase
    ]


def build_source_inventory_summary(
    inventory: AuthoritativeSourceInventory,
    *,
    phase: int | None = None,
    category: str | None = None,
) -> SourceInventorySummary:
    filtered_sources = sorted(
        list_inventory_sources(inventory, phase=phase, category=category),
        key=lambda source: (source.phase, source.if_code),
    )
    filtered_flags = sorted(
        list_inventory_flags(inventory, phase=phase),
        key=lambda flag: (flag.phase, flag.flag_key),
    )

    phase_keys = sorted(
        {source.phase for source in filtered_sources}
        | {flag.phase for flag in filtered_flags}
    )
    phase_counts = [
        SourceInventoryPhaseCount(
            phase=phase_key,
            source_count=len(
                [source for source in filtered_sources if source.phase == phase_key]
            ),
            config_flag_count=len(
                [flag for flag in filtered_flags if flag.phase == phase_key]
            ),
        )
        for phase_key in phase_keys
    ]
    category_counts = [
        SourceInventoryCategoryCount(
            category=category_key,
            source_count=len(
                [
                    source
                    for source in filtered_sources
                    if source.category == category_key
                ]
            ),
        )
        for category_key in sorted({source.category for source in filtered_sources})
    ]

    return SourceInventorySummary(
        version=inventory.version,
        captured_at=inventory.captured_at,
        total_sources=len(inventory.sources),
        total_config_flags=len(inventory.config_flags),
        filtered_source_count=len(filtered_sources),
        filtered_config_flag_count=len(filtered_flags),
        phase_counts=phase_counts,
        category_counts=category_counts,
        sources=filtered_sources,
        config_flags=filtered_flags,
    )


def build_source_inventory_coverage(
    inventory: AuthoritativeSourceInventory,
    registry: ConnectorRegistry,
    *,
    phase: int | None = None,
    category: str | None = None,
) -> SourceInventoryCoverage:
    filtered_sources = sorted(
        list_inventory_sources(inventory, phase=phase, category=category),
        key=lambda source: (source.phase, source.if_code),
    )
    all_inventory_if_codes = {source.if_code for source in inventory.sources}

    connectors_by_if_code: dict[str, list[Any]] = {}
    unmapped_connector_keys: list[str] = []
    orphaned_connector_if_codes: set[str] = set()

    for definition in registry.list_definitions():
        if not definition.inventory_if_codes:
            unmapped_connector_keys.append(definition.connector_key)
            continue

        for if_code in definition.inventory_if_codes:
            if if_code not in all_inventory_if_codes:
                orphaned_connector_if_codes.add(if_code)
                continue
            connectors_by_if_code.setdefault(if_code, []).append(definition)

    items: list[SourceInventoryCoverageItem] = []
    covered_if_codes: list[str] = []
    uncovered_if_codes: list[str] = []

    for source in filtered_sources:
        connectors = sorted(
            connectors_by_if_code.get(source.if_code, []),
            key=lambda definition: (
                not definition.enabled,
                definition.priority,
                definition.connector_key,
            ),
        )
        connector_keys = [definition.connector_key for definition in connectors]
        enabled_connector_keys = [
            definition.connector_key for definition in connectors if definition.enabled
        ]
        implemented = bool(connector_keys)
        if implemented:
            covered_if_codes.append(source.if_code)
        else:
            uncovered_if_codes.append(source.if_code)

        items.append(
            SourceInventoryCoverageItem(
                if_code=source.if_code,
                name=source.name,
                category=source.category,
                phase=source.phase,
                implemented=implemented,
                connector_keys=connector_keys,
                enabled_connector_keys=enabled_connector_keys,
            )
        )

    phase_coverage = [
        SourceInventoryCoveragePhase(
            phase=phase_key,
            total_sources=len([item for item in items if item.phase == phase_key]),
            covered_sources=len(
                [
                    item
                    for item in items
                    if item.phase == phase_key and item.implemented
                ]
            ),
            uncovered_sources=len(
                [
                    item
                    for item in items
                    if item.phase == phase_key and not item.implemented
                ]
            ),
        )
        for phase_key in sorted({item.phase for item in items})
    ]

    total_sources = len(filtered_sources)
    covered_sources = len(covered_if_codes)
    coverage_percent = round((covered_sources / total_sources) * 100, 2) if total_sources else 0.0

    return SourceInventoryCoverage(
        total_sources=total_sources,
        covered_sources=covered_sources,
        uncovered_sources=len(uncovered_if_codes),
        coverage_percent=coverage_percent,
        phase_coverage=phase_coverage,
        covered_if_codes=covered_if_codes,
        uncovered_if_codes=uncovered_if_codes,
        items=items,
        unmapped_connector_keys=sorted(unmapped_connector_keys),
        orphaned_connector_if_codes=sorted(orphaned_connector_if_codes),
    )


def _parse_source_entry(payload: dict[str, Any]) -> SourceInventoryEntry:
    return SourceInventoryEntry(
        if_code=_require_string(payload, "if_code"),
        name=_require_string(payload, "name"),
        category=_require_string(payload, "category"),
        phase=int(payload["phase"]),
        protocol=_require_string(payload, "protocol"),
        auth=_require_string(payload, "auth"),
        url=_normalize_optional_string(payload.get("url")),
        target_table=_normalize_optional_string(payload.get("target_table")),
        secondary_url=_normalize_optional_string(payload.get("secondary_url")),
        target_partition=_normalize_optional_string(payload.get("target_partition")),
        county=_normalize_optional_string(payload.get("county")),
        city=_normalize_optional_string(payload.get("city")),
        reference_only=bool(payload.get("reference_only", False)),
        free=bool(payload.get("free", True)),
        notes=[str(note).strip() for note in payload.get("notes", []) if str(note).strip()],
    )


def _parse_source_flag(payload: dict[str, Any]) -> SourceInventoryFlag:
    return SourceInventoryFlag(
        flag_key=_require_string(payload, "flag_key"),
        name=_require_string(payload, "name"),
        phase=int(payload["phase"]),
        metro=_require_string(payload, "metro"),
        target_behavior=_require_string(payload, "target_behavior"),
        notes=[str(note).strip() for note in payload.get("notes", []) if str(note).strip()],
    )


def _parse_phase_total(payload: dict[str, Any]) -> SourceInventoryPhaseTotal:
    return SourceInventoryPhaseTotal(
        phase=int(payload["phase"]),
        scope=_require_string(payload, "scope"),
        source_count=int(payload["source_count"]),
        config_flag_count=int(payload.get("config_flag_count", 0)),
    )


def _validate_inventory(inventory: AuthoritativeSourceInventory) -> None:
    if_codes = [source.if_code for source in inventory.sources]
    if len(if_codes) != len(set(if_codes)):
        raise SourceInventoryConfigurationError(
            "Authoritative source inventory contains duplicate IF codes."
        )

    if any(source.phase < 1 for source in inventory.sources):
        raise SourceInventoryConfigurationError(
            "Authoritative source inventory contains an invalid phase number."
        )

    for phase_total in inventory.phase_totals:
        cumulative_source_count = len(
            [source for source in inventory.sources if source.phase <= phase_total.phase]
        )
        cumulative_flag_count = len(
            [flag for flag in inventory.config_flags if flag.phase <= phase_total.phase]
        )
        if cumulative_source_count != phase_total.source_count:
            raise SourceInventoryConfigurationError(
                f"Phase {phase_total.phase} source total does not match the inventory payload."
            )
        if cumulative_flag_count != phase_total.config_flag_count:
            raise SourceInventoryConfigurationError(
                f"Phase {phase_total.phase} config flag total does not match the inventory payload."
            )


def _expect_list(value: Any, field_name: str) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, list):
        return [dict(item) for item in value]
    raise SourceInventoryConfigurationError(
        f"Authoritative source inventory field `{field_name}` must be a list."
    )


def _require_string(payload: dict[str, Any], key: str) -> str:
    value = _normalize_optional_string(payload.get(key))
    if value is None:
        raise SourceInventoryConfigurationError(
            f"Authoritative source inventory field `{key}` is required."
        )
    return value


def _normalize_optional_string(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None
