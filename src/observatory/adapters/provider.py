"""Crossref-backed provider adapter for publication and transparent-review layers."""

from __future__ import annotations

import json
from collections import Counter
from typing import Any, Iterable, Iterator, Mapping

from ..connectors.base import (
    Connector,
    ConnectorContext,
    CoverageEvidence,
    FetchBatch,
    NormalizedRecord,
    RawItem,
    SourceEstimate,
    coverage_observation_id,
)
from ..connectors.http import PoliteSession, RatePolicy
from ..ids import canonical_doi, stable_id
from .common import iso_datetime


class CrossrefProviderConnector(Connector):
    """Enumerate one provider/prefix without claiming a submission denominator."""

    connector_version = "3"
    endpoint = "https://api.crossref.org/works"

    def __init__(
        self,
        *,
        source_id: str,
        provider: str,
        crossref_filter: str,
        architecture: str,
        earliest_stage: str,
        grade: str = "C",
        rows: int = 200,
    ):
        self.source_id = source_id
        self.provider = provider
        self.crossref_filter = crossref_filter
        self.architecture = architecture
        self.earliest_stage = earliest_stage
        self.grade = grade
        self.rows = min(max(rows, 1), 1000)
        self._total: int | None = None
        self._cycles: Counter[str] = Counter()
        self._emitted_gates: set[str] = set()
        self._emitted_candidates: set[str] = set()
        self._emitted_versions: set[str] = set()
        self._emitted_aliases: set[str] = set()
        self._emitted_lineage_edges: set[str] = set()

    def _session(self, context: ConnectorContext) -> PoliteSession:
        return PoliteSession(
            cache_dir=context.cache_dir / self.source_id,
            allowed_hosts={"api.crossref.org"},
            policy=RatePolicy(min_interval_seconds=0.1, max_retries=5),
        )

    def discover(self, context: ConnectorContext) -> Iterable[Mapping[str, Any]]:
        yield {"provider": self.provider, "filter": self.crossref_filter, "grade": self.grade}

    def count(self, context: ConnectorContext) -> SourceEstimate:
        response = self._session(context).get(self.endpoint, params={"filter": self.crossref_filter, "rows": 0})
        self._total = int(response.json()["message"]["total-results"])
        return SourceEstimate(self.source_id, self._total, method="Crossref filtered total-results", confidence="provider deposit count")

    def fetch(self, context: ConnectorContext, *, cursor: str | None = None, limit: int | None = None) -> Iterator[FetchBatch]:
        token = cursor or "*"
        emitted = 0
        session = self._session(context)
        while token:
            size = min(self.rows, limit - emitted) if limit is not None else self.rows
            if size <= 0:
                return
            response = session.get(self.endpoint, params={"filter": self.crossref_filter, "rows": size, "cursor": token})
            message = response.json()["message"]
            self._total = int(message.get("total-results") or self._total or 0)
            items = tuple(
                RawItem(
                    native_id=str(work.get("DOI") or work.get("URL") or stable_id("provider_work", self.source_id, json.dumps(work, sort_keys=True))),
                    object_type="work_metadata", payload=json.dumps(work, sort_keys=True),
                    source_url=work.get("URL"), created_at=_crossref_date(work.get("created")),
                    modified_at=_crossref_date(work.get("indexed")), licence="Crossref-metadata",
                    release_class="pointer_hash",
                )
                for work in message.get("items", [])
            )
            emitted += len(items)
            next_token = message.get("next-cursor")
            done = not items or not next_token or next_token == token or (limit is not None and emitted >= limit)
            yield FetchBatch(items, None if done else str(next_token), done, f"crossref-provider:{token}", self._total)
            if done:
                return
            token = str(next_token)

    def normalize(self, item: RawItem, *, source_object_id: str, provenance_event_id: str) -> Iterable[NormalizedRecord]:
        work = json.loads(item.payload)
        doi = canonical_doi(work.get("DOI"))
        if not doi:
            return
        created = _published_date(work) or item.created_at
        year = int((created or "1970")[:4])
        gate_id = stable_id("gate", self.source_id, self.provider)
        cycle_id = stable_id("gate_cycle", self.source_id, str(year))
        coverage_id = coverage_observation_id(self.source_id, cycle_id, "work_metadata")
        candidate_id = stable_id("candidate", "doi", doi)
        version_id = stable_id("candidate_version", "doi", doi)
        common = {
            "source_id": self.source_id, "source_object_id": source_object_id,
            "provenance_event_id": provenance_event_id, "observed_at": item.modified_at or created,
            "record_version": 1,
        }
        if gate_id not in self._emitted_gates:
            self._emitted_gates.add(gate_id)
            yield NormalizedRecord("gate", {
                "gate_id": gate_id, "native_id": self.provider.lower().replace(" ", "_"),
                "name": self.provider, "organization": self.provider, "domain": None, "country": None,
                "architecture": self.architecture, "active_from": None, "active_to": None, **common,
            })
        if cycle_id not in self._cycles:
            yield NormalizedRecord("gate_cycle", {
                "gate_cycle_id": cycle_id, "gate_id": gate_id, "native_id": str(year),
                "name": f"{self.provider} {year}", "track": None,
                "cycle_start": f"{year}-01-01T00:00:00+00:00", "cycle_end": f"{year}-12-31T23:59:59+00:00",
                "policy_version_id": None, "architecture": self.architecture, "received_count": None,
                "observable_count": None, "evaluated_count": None, "selected_count": None,
                "status": "visible-deposit", **common,
            })
        self._cycles[cycle_id] += 1
        title = " ".join(work.get("title") or []) or None
        if candidate_id not in self._emitted_candidates:
            self._emitted_candidates.add(candidate_id)
            yield NormalizedRecord("candidate", {
                "candidate_id": candidate_id, "first_observed_at": created, "domain": None,
                "candidate_type": work.get("type") or "scholarly_work", "canonical_title": title,
                "status": "visible", **common,
            })
        if version_id not in self._emitted_versions:
            self._emitted_versions.add(version_id)
            yield NormalizedRecord("candidate_version", {
                "candidate_version_id": version_id, "candidate_id": candidate_id, "native_id": doi,
                "version_label": work.get("subtype") or work.get("type"), "version_number": None,
                "created_at": created, "modified_at": item.modified_at, "title": title,
                "abstract": None, "content_artifact_id": None, "content_hash": None,
                "licence": (work.get("license") or [{}])[0].get("URL") if work.get("license") else None,
                "language": work.get("language"), "authorship_visible": True, "withdrawn": False, **common,
            })
        yield NormalizedRecord("candidate_gate_event", {
            "candidate_gate_event_id": stable_id("candidate_gate_event", self.source_id, f"{cycle_id}|{doi}"),
            "candidate_id": candidate_id, "candidate_version_id": version_id, "gate_cycle_id": cycle_id,
            "native_id": doi, "submitted_at": None, "earliest_observed_stage": self.earliest_stage,
            "final_observed_stage": "visible_deposited_object", "coverage_observation_id": coverage_id, **common,
        })
        alias_id = stable_id("identifier_alias", self.source_id, f"candidate|doi|{doi}")
        if alias_id not in self._emitted_aliases:
            self._emitted_aliases.add(alias_id)
            yield NormalizedRecord("identifier_alias", {
                "identifier_alias_id": alias_id, "entity_kind": "candidate",
                "entity_id": candidate_id, "scheme": "doi", "value": doi,
                "canonical_value": doi, "relation": "native", "confidence": 1.0,
                "conflict_status": "none", **common,
            })
        for relation_type, values in (work.get("relation") or {}).items():
            for relation in values or []:
                target = canonical_doi(relation.get("id"))
                if target:
                    target_candidate_id = stable_id("candidate", "doi", target)
                    target_version_id = stable_id("candidate_version", "doi", target)
                    target_alias_id = stable_id(
                        "identifier_alias", self.source_id, f"candidate|doi|{target}"
                    )
                    if target_alias_id not in self._emitted_aliases:
                        self._emitted_aliases.add(target_alias_id)
                        yield NormalizedRecord("identifier_alias", {
                            "identifier_alias_id": target_alias_id,
                            "entity_kind": "candidate", "entity_id": target_candidate_id,
                            "scheme": "doi", "value": target, "canonical_value": target,
                            "relation": "native", "confidence": 1.0,
                            "conflict_status": "none", **common,
                        })
                    edge_id = stable_id(
                        "lineage_edge", self.source_id,
                        f"{candidate_id}|{relation_type}|{target_candidate_id}"
                    )
                    if edge_id not in self._emitted_lineage_edges:
                        self._emitted_lineage_edges.add(edge_id)
                        yield NormalizedRecord("lineage_edge", {
                            "lineage_edge_id": edge_id,
                            "source_candidate_id": candidate_id, "source_version_id": version_id,
                            "target_candidate_id": target_candidate_id,
                            "target_version_id": target_version_id,
                            "relation_type": relation_type, "declared": True, "confidence": 1.0,
                            "linkage_tier": "source_declared",
                            "method_version": "crossref-relation/1",
                            "evidence_json": json.dumps(relation, sort_keys=True), **common,
                        })

    def validate_fixture(self, context: ConnectorContext) -> Mapping[str, Any]:
        fixture = context.fixture_dir / self.source_id / "work.json"
        if not fixture.exists():
            return {"passes": False, "reason": "fixture absent"}
        row = json.loads(fixture.read_text())
        return {"passes": bool(row.get("DOI") and row.get("type")), "doi": row.get("DOI")}

    def emit_coverage(self, context: ConnectorContext, *, found_count: int) -> Iterable[CoverageEvidence]:
        if not self._cycles:
            yield CoverageEvidence(
                gate_cycle_id=stable_id("gate_cycle", self.source_id, "provider-all"),
                object_type="work_metadata", earliest_public_stage=self.earliest_stage,
                observability_grade=self.grade, expected_count=self._total, found_count=found_count,
                expected_count_method="Crossref filtered total-results", query_or_invitation=self.crossref_filter,
                known_hidden_stages=("submission and unpublished decisions",), audit_status="partial_snapshot",
            )
        for cycle_id, count in sorted(self._cycles.items()):
            yield CoverageEvidence(
                gate_cycle_id=cycle_id, object_type="work_metadata", earliest_public_stage=self.earliest_stage,
                observability_grade=self.grade, expected_count=None, found_count=count,
                expected_count_method="year-specific independent denominator pending provider reconciliation",
                query_or_invitation=self.crossref_filter, known_hidden_stages=("submission and unpublished decisions",),
                audit_status="unverified",
            )


def _crossref_date(value: Mapping[str, Any] | None) -> str | None:
    return iso_datetime((value or {}).get("date-time"))


def _published_date(work: Mapping[str, Any]) -> str | None:
    for key in ("published-online", "published-print", "published", "issued"):
        parts = ((work.get(key) or {}).get("date-parts") or [])
        if parts and parts[0] and parts[0][0] is not None:
            values = [value if value is not None else 1 for value in list(parts[0])] + [1, 1]
            return f"{values[0]:04d}-{values[1]:02d}-{values[2]:02d}T00:00:00+00:00"
    return None
