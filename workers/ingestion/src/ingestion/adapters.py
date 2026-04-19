from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from ingestion.models import RecordPayload


class SourceAdapter(Protocol):
    source_id: str
    interface_name: str
    source_version: str

    def fetch_records(self, metro_id: str) -> Sequence[RecordPayload]:
        """Fetch the source payload for one metro."""


@dataclass(slots=True)
class StaticSourceAdapter:
    source_id: str
    interface_name: str
    source_version: str
    records: Sequence[RecordPayload]

    def fetch_records(self, metro_id: str) -> Sequence[RecordPayload]:
        return list(self.records)


def build_source_version(interface_name: str, loaded_at: datetime) -> str:
    timestamp = loaded_at.strftime("%Y%m%dT%H%M%SZ")
    return f"{interface_name}:{timestamp}"
