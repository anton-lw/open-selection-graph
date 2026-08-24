"""Free-only OpenAlex singleton resolution.

As of 2026, OpenAlex list/search/content calls are metered.  This adapter
intentionally exposes only singleton ID/DOI lookups, which the official pricing
documentation marks free.  Bulk work must use the free quarterly snapshot or
the repository's frozen local corpus.
"""

from __future__ import annotations

import json
import os
from typing import Any, Iterable, Iterator, Mapping
from urllib.parse import quote

from ..connectors.base import (
    Connector,
    ConnectorContext,
    CoverageEvidence,
    FetchBatch,
    NormalizedRecord,
    RawItem,
    SourceEstimate,
)
from ..connectors.http import NetworkPolicyError, PoliteSession, RatePolicy
from ..ids import canonical_doi, stable_id
from .common import iso_datetime


class OpenAlexSingletonConnector(Connector):
    source_id = "openalex"
    connector_version = "1"
    base = "https://api.openalex.org/works/"

    def __init__(self, identifiers: Iterable[str] | None = None):
        self.identifiers = tuple(identifiers or ())

    def _ids(self, context: ConnectorContext) -> tuple[str, ...]:
        return self.identifiers or tuple(context.parameters.get("identifiers") or ())

    def _session(self, context: ConnectorContext) -> PoliteSession:
        return PoliteSession(
            cache_dir=context.cache_dir / self.source_id,
            allowed_hosts={"api.openalex.org"},
            policy=RatePolicy(min_interval_seconds=0.1, max_retries=4),
        )

    def discover(self, context: ConnectorContext) -> Iterable[Mapping[str, Any]]:
        yield {"mode": "singleton_only", "n_identifiers": len(self._ids(context)), "bulk_source": "free quarterly snapshot"}

    def count(self, context: ConnectorContext) -> SourceEstimate:
        return SourceEstimate(self.source_id, len(self._ids(context)), expected_requests=len(self._ids(context)), method="input singleton identifier count", confidence="exact")

    def fetch(self, context: ConnectorContext, *, cursor: str | None = None, limit: int | None = None) -> Iterator[FetchBatch]:
        ids = self._ids(context)
        start = int(cursor or 0)
        stop = min(len(ids), start + limit) if limit is not None else len(ids)
        session = self._session(context)
        items = []
        api_key = os.environ.get("OPENALEX_API_KEY")
        for index in range(start, stop):
            identifier = ids[index]
            # No query/list endpoint is constructible through this class.
            url = self.base + quote(identifier, safe=":./")
            params = {"api_key": api_key} if api_key else None
            response = session.get(url, params=params)
            work = response.json()
            if float((work.get("meta") or {}).get("cost_usd") or 0.0) > 0:
                raise NetworkPolicyError("OpenAlex returned a metered response; paid/freemium calls are prohibited")
            items.append(RawItem(
                native_id=str(work.get("id") or identifier), object_type="work_metadata",
                payload=json.dumps(work, sort_keys=True), source_url=work.get("id"),
                created_at=iso_datetime(work.get("publication_date")), modified_at=iso_datetime(work.get("updated_date")),
                licence="CC0", release_class="redistribute",
            ))
        next_cursor = str(stop) if stop < len(ids) else None
        yield FetchBatch(tuple(items), next_cursor, next_cursor is None, f"openalex-singleton:{start}", len(ids))

    def normalize(self, item: RawItem, *, source_object_id: str, provenance_event_id: str) -> Iterable[NormalizedRecord]:
        work = json.loads(item.payload)
        openalex_id = str(work.get("id") or item.native_id).rsplit("/", 1)[-1]
        doi = canonical_doi(work.get("doi"))
        candidate_id = stable_id("candidate", "doi", doi) if doi else stable_id("candidate", self.source_id, openalex_id)
        version_id = stable_id("candidate_version", "doi", doi) if doi else stable_id("candidate_version", self.source_id, openalex_id)
        common = {
            "source_id": self.source_id, "source_object_id": source_object_id,
            "provenance_event_id": provenance_event_id, "observed_at": item.modified_at or item.created_at,
            "record_version": 1,
        }
        yield NormalizedRecord("candidate", {
            "candidate_id": candidate_id, "first_observed_at": item.created_at,
            "domain": (work.get("primary_topic") or {}).get("field", {}).get("display_name"),
            "candidate_type": work.get("type") or "scholarly_work", "canonical_title": work.get("display_name"),
            "status": "visible", **common,
        })
        yield NormalizedRecord("candidate_version", {
            "candidate_version_id": version_id, "candidate_id": candidate_id, "native_id": openalex_id,
            "version_label": (work.get("primary_location") or {}).get("version") or "indexed",
            "version_number": None, "created_at": item.created_at, "modified_at": item.modified_at,
            "title": work.get("display_name"), "abstract": None, "content_artifact_id": None,
            "content_hash": None, "licence": (work.get("primary_location") or {}).get("license"),
            "language": work.get("language"), "authorship_visible": True, "withdrawn": False, **common,
        })
        for scheme, value in (("openalex", openalex_id), ("doi", doi)):
            if value:
                yield NormalizedRecord("identifier_alias", {
                    "identifier_alias_id": stable_id("identifier_alias", self.source_id, f"candidate|{scheme}|{value}"),
                    "entity_kind": "candidate", "entity_id": candidate_id, "scheme": scheme,
                    "value": value, "canonical_value": value.lower(), "relation": "native",
                    "confidence": 1.0, "conflict_status": "none", **common,
                })
        for topic in work.get("topics") or []:
            yield NormalizedRecord("field_assignment", {
                "field_assignment_id": stable_id("field_assignment", self.source_id, f"{candidate_id}|{topic.get('id')}"),
                "entity_kind": "candidate", "entity_id": candidate_id, "taxonomy": "openalex_topics",
                "native_label": topic.get("display_name"), "normalized_label": (topic.get("field") or {}).get("display_name"),
                "score": topic.get("score"), "mapping_version": "openalex-snapshot/API-2026", **common,
            })
        for index, cited in enumerate(work.get("referenced_works") or []):
            cited_id = str(cited).rsplit("/", 1)[-1]
            yield NormalizedRecord("reference_edge", {
                "reference_edge_id": stable_id("reference_edge", self.source_id, f"{version_id}|{index}|{cited_id}"),
                "citing_version_id": version_id, "reference_position": index,
                "cited_candidate_id": stable_id("candidate", self.source_id, cited_id),
                "cited_version_id": stable_id("candidate_version", self.source_id, cited_id),
                "cited_identifier": cited_id, "raw_citation_hash": None, "match_method": "openalex_declared",
                "confidence": 1.0, "time_valid": None, **common,
            })

    def validate_fixture(self, context: ConnectorContext) -> Mapping[str, Any]:
        fixture = context.fixture_dir / "openalex" / "work.json"
        if not fixture.exists():
            return {"passes": False, "reason": "fixture absent"}
        row = json.loads(fixture.read_text())
        return {"passes": bool(row.get("id") and row.get("display_name")), "id": row.get("id")}

    def emit_coverage(self, context: ConnectorContext, *, found_count: int) -> Iterable[CoverageEvidence]:
        expected = len(self._ids(context))
        yield CoverageEvidence(
            gate_cycle_id=stable_id("gate_cycle", self.source_id, "singleton-resolution"),
            object_type="work_metadata", earliest_public_stage="visible indexed scholarly work",
            observability_grade="D", expected_count=expected, found_count=found_count,
            expected_count_method="input singleton identifier count", query_or_invitation="free singleton endpoint only",
            known_hidden_stages=("submission and gate decisions",), audit_status="verified" if found_count == expected else "incomplete",
        )
