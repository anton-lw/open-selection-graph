"""Transport-independent connector contract."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

from ..ids import stable_id


@dataclass(frozen=True)
class SourceEstimate:
    source_id: str
    expected_objects: int | None
    expected_bytes: int | None = None
    expected_requests: int | None = None
    method: str = "unresolved"
    confidence: str = "unknown"
    objects_per_limit_unit: float = 1.0
    requests_per_limit_unit: float | None = None


@dataclass(frozen=True)
class RawItem:
    native_id: str
    object_type: str
    payload: bytes | str
    source_url: str | None = None
    created_at: str | None = None
    modified_at: str | None = None
    licence: str | None = None
    release_class: str = "pointer_hash"
    http_status: int = 200
    etag: str | None = None
    last_modified: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FetchBatch:
    items: tuple[RawItem, ...]
    cursor: str | None
    done: bool
    request_fingerprint: str
    provider_total: int | None = None


@dataclass(frozen=True)
class NormalizedRecord:
    table: str
    row: Mapping[str, Any]


@dataclass(frozen=True)
class CoverageEvidence:
    gate_cycle_id: str
    object_type: str
    earliest_public_stage: str
    observability_grade: str
    expected_count: int | None
    found_count: int
    expected_count_method: str
    query_or_invitation: str
    known_hidden_stages: tuple[str, ...] = ()
    known_exclusions: tuple[str, ...] = ()
    missing_reason: str | None = None
    audit_status: str = "unverified"


def coverage_observation_id(source_id: str, gate_cycle_id: str, object_type: str) -> str:
    """Return the canonical FK used by records and emitted coverage evidence."""
    return stable_id("coverage", source_id, f"{gate_cycle_id}|{object_type}")


@dataclass
class ConnectorContext:
    workspace: Path
    fixture_dir: Path
    cache_dir: Path
    no_text: bool = False
    since: str | None = None
    until: str | None = None
    parameters: dict[str, Any] = field(default_factory=dict)


class Connector(ABC):
    source_id: str
    connector_version = "1"
    force_streaming = False

    @abstractmethod
    def discover(self, context: ConnectorContext) -> Iterable[Mapping[str, Any]]:
        """Discover source subdivisions (venues, sets, journals, queries)."""

    @abstractmethod
    def count(self, context: ConnectorContext) -> SourceEstimate:
        """Return a source/provider estimate without starting the full pull."""

    @abstractmethod
    def fetch(
        self,
        context: ConnectorContext,
        *,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> Iterator[FetchBatch]:
        """Yield raw batches and resumable cursor state."""

    @abstractmethod
    def normalize(self, item: RawItem, *, source_object_id: str, provenance_event_id: str) -> Iterable[NormalizedRecord]:
        """Normalize one source object without network access."""

    @abstractmethod
    def validate_fixture(self, context: ConnectorContext) -> Mapping[str, Any]:
        """Validate deterministic committed fixtures offline."""

    @abstractmethod
    def emit_coverage(self, context: ConnectorContext, *, found_count: int) -> Iterable[CoverageEvidence]:
        """Declare the observable population and denominator evidence."""
