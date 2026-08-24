"""Crossref peer-review discovery and relation adapter."""

from __future__ import annotations

import json
from math import ceil
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
from ..connectors.http import PoliteSession, RatePolicy
from ..ids import canonical_doi, content_hash, stable_id
from .common import candidate_from_doi, iso_datetime


class CrossrefPeerReviewConnector(Connector):
    source_id = "crossref"
    connector_version = "3"
    endpoint = "https://api.crossref.org/types/peer-review/works"

    def __init__(self, *, rows: int = 200, bundle_pages: bool = False):
        self.rows = min(max(rows, 1), 1000)
        self.bundle_pages = bundle_pages
        self.force_streaming = bundle_pages
        self._total: int | None = None
        self._logical_found = 0
        self._emitted: dict[str, set[str]] = {"gate": set(), "gate_cycle": set(), "candidate": set(), "candidate_version": set()}

    def _session(self, context: ConnectorContext) -> PoliteSession:
        return PoliteSession(
            cache_dir=context.cache_dir / self.source_id,
            allowed_hosts={"api.crossref.org"},
            policy=RatePolicy(min_interval_seconds=0.1, max_retries=5),
        )

    def discover(self, context: ConnectorContext) -> Iterable[Mapping[str, Any]]:
        yield {"endpoint": self.endpoint, "type": "peer-review", "cursor": True}

    def count(self, context: ConnectorContext) -> SourceEstimate:
        response = self._session(context).get(self.endpoint, params={"rows": 0, "mailto": context.parameters.get("contact", "")})
        self._total = int(response.json()["message"]["total-results"])
        if self.bundle_pages:
            bundles = ceil(self._total / self.rows)
            return SourceEstimate(
                self.source_id,
                bundles,
                expected_bytes=self._total * 2_000,
                expected_requests=bundles + 1,
                method=(
                    f"{self._total} Crossref peer-review works in immutable API-page "
                    f"bundles of at most {self.rows}"
                ),
                confidence="provider total-results; lossless page bundling",
                requests_per_limit_unit=1.0,
            )
        return SourceEstimate(
            self.source_id, self._total, method="Crossref total-results", confidence="provider"
        )

    def fetch(self, context: ConnectorContext, *, cursor: str | None = None, limit: int | None = None) -> Iterator[FetchBatch]:
        session = self._session(context)
        token = cursor or "*"
        emitted = 0
        previous_page_hash: str | None = None
        while token:
            if self.bundle_pages:
                rows = self.rows
            else:
                rows = min(self.rows, limit - emitted) if limit is not None else self.rows
            if rows <= 0:
                return
            params = {"rows": rows, "cursor": token}
            if context.parameters.get("from_index_date"):
                params["filter"] = f"from-index-date:{context.parameters['from_index_date']}"
            response = session.get(self.endpoint, params=params)
            message = response.json()["message"]
            self._total = int(message.get("total-results") or self._total or 0)
            works = message.get("items", [])
            page_hash = content_hash(json.dumps(
                [work.get("DOI") or work.get("URL") or content_fallback(work) for work in works]
            ))
            if works and page_hash == previous_page_hash:
                raise RuntimeError("Crossref cursor repeated an identical non-empty page")
            previous_page_hash = page_hash
            self._logical_found += len(works)
            items = []
            if self.bundle_pages and works:
                payload = json.dumps(
                    {
                        "cursor": token,
                        "next_cursor": message.get("next-cursor"),
                        "total_results": self._total,
                        "items": works,
                    },
                    sort_keys=True,
                )
                items.append(RawItem(
                    native_id=f"page:{content_hash(str(token))[:24]}",
                    object_type="peer_review_metadata_page",
                    payload=payload,
                    source_url=self.endpoint,
                    created_at=min(filter(None, (_date(work.get("created")) for work in works)), default=None),
                    modified_at=max(filter(None, (_date(work.get("indexed")) for work in works)), default=None),
                    licence="Crossref-metadata-with-abstract-exception",
                    release_class="pointer_hash",
                    metadata={"cursor": token, "work_count": len(works)},
                ))
            else:
                for work in works:
                    doi = work.get("DOI") or work.get("URL") or content_fallback(work)
                    items.append(RawItem(
                        native_id=str(doi), object_type="peer_review_metadata",
                        payload=json.dumps(work, sort_keys=True), source_url=work.get("URL"),
                        created_at=_date(work.get("created")), modified_at=_date(work.get("indexed")),
                        licence="Crossref-metadata-with-abstract-exception", release_class="pointer_hash",
                        metadata={"cursor": token},
                    ))
            next_token = message.get("next-cursor")
            emitted += len(items)
            done = (
                not items
                or not next_token
                or (self._total is not None and self._logical_found >= self._total)
                or (limit is not None and emitted >= limit)
            )
            yield FetchBatch(tuple(items), None if done else str(next_token), done, f"crossref:{quote(str(token), safe='')}", self._total)
            if done:
                return
            token = str(next_token)

    def normalize(self, item: RawItem, *, source_object_id: str, provenance_event_id: str) -> Iterable[NormalizedRecord]:
        payload = json.loads(item.payload)
        works = payload.get("items") if item.object_type == "peer_review_metadata_page" else [payload]
        for index, work in enumerate(works or []):
            yield from self._normalize_work(
                work,
                item=item,
                item_index=index if item.object_type == "peer_review_metadata_page" else None,
                source_object_id=source_object_id,
                provenance_event_id=provenance_event_id,
            )

    def _normalize_work(
        self,
        work: Mapping[str, Any],
        *,
        item: RawItem,
        item_index: int | None,
        source_object_id: str,
        provenance_event_id: str,
    ) -> Iterable[NormalizedRecord]:
        work_native = str(work.get("DOI") or work.get("URL") or content_fallback(work))
        created_at = _date(work.get("created"))
        modified_at = _date(work.get("indexed"))
        common = {
            "source_id": self.source_id, "source_object_id": source_object_id,
            "provenance_event_id": provenance_event_id,
            "observed_at": modified_at or created_at or item.modified_at or item.created_at,
            "record_version": 1,
        }
        review_doi = canonical_doi(work.get("DOI"))
        gate_id = stable_id("gate", self.source_id, "peer-review-discovery")
        cycle_id = stable_id("gate_cycle", self.source_id, "peer-review-discovery")
        if gate_id not in self._emitted["gate"]:
            self._emitted["gate"].add(gate_id)
            yield NormalizedRecord("gate", {
                "gate_id": gate_id, "native_id": "peer-review-discovery",
                "name": "Crossref peer-review deposits", "organization": "Crossref depositors",
                "domain": None, "country": None, "architecture": "unknown",
                "active_from": None, "active_to": None, **common,
            })
        if cycle_id not in self._emitted["gate_cycle"]:
            self._emitted["gate_cycle"].add(cycle_id)
            yield NormalizedRecord("gate_cycle", {
                "gate_cycle_id": cycle_id, "gate_id": gate_id, "native_id": "peer-review-discovery",
                "name": "Crossref peer-review discovery snapshot", "track": None,
                "cycle_start": None, "cycle_end": None, "policy_version_id": None,
                "architecture": "unknown", "received_count": None, "observable_count": None,
                "evaluated_count": None, "selected_count": None, "status": "discovery-only", **common,
            })
        if review_doi:
            yield NormalizedRecord("identifier_alias", {
                "identifier_alias_id": stable_id("identifier_alias", self.source_id, f"content|doi|{review_doi}"),
                "entity_kind": "content_artifact", "entity_id": stable_id("content_artifact", "doi", review_doi),
                "scheme": "doi", "value": review_doi, "canonical_value": review_doi,
                "relation": "native", "confidence": 1.0, "conflict_status": "none", **common,
            })
        yield NormalizedRecord("content_artifact", {
            "content_artifact_id": stable_id("content_artifact", self.source_id, work_native),
            "object_type": "peer_review_metadata", "media_type": "application/json",
            "byte_hash": content_hash(json.dumps(work, sort_keys=True)),
            "normalized_text_hash": None, "source_url": work.get("URL"),
            "local_pointer": f"items[{item_index}]" if item_index is not None else None,
            "licence": "Crossref-metadata-with-abstract-exception",
            "release_class": "pointer_hash",
            "size_bytes": len(json.dumps(work, sort_keys=True).encode()),
            "language": work.get("language"),
            "parser_version": self.connector_version, **common,
        })
        for relation_type, values in (work.get("relation") or {}).items():
            for relation in values or []:
                target = canonical_doi(relation.get("id"))
                if not target:
                    continue
                candidate_id, version_id, alias_id = candidate_from_doi(self.source_id, target)
                if candidate_id not in self._emitted["candidate"]:
                    self._emitted["candidate"].add(candidate_id)
                    yield NormalizedRecord("candidate", {
                        "candidate_id": candidate_id, "first_observed_at": created_at,
                        "domain": None, "candidate_type": "scholarly_work", "canonical_title": None,
                        "status": "visible", **common,
                    })
                if version_id not in self._emitted["candidate_version"]:
                    self._emitted["candidate_version"].add(version_id)
                    yield NormalizedRecord("candidate_version", {
                        "candidate_version_id": version_id, "candidate_id": candidate_id, "native_id": target,
                        "version_label": "reviewed_object", "version_number": None, "created_at": None,
                        "modified_at": None, "title": None, "abstract": None, "content_artifact_id": None,
                        "content_hash": None, "licence": None, "language": None,
                        "authorship_visible": None, "withdrawn": None, **common,
                    })
                yield NormalizedRecord("identifier_alias", {
                    "identifier_alias_id": alias_id, "entity_kind": "candidate", "entity_id": candidate_id,
                    "scheme": "doi", "value": target, "canonical_value": target,
                    "relation": "native", "confidence": 1.0, "conflict_status": "none", **common,
                })
                yield NormalizedRecord("evaluation", {
                    "evaluation_id": stable_id(
                        "evaluation", self.source_id, f"{review_doi or work_native}|{relation_type}|{target}"
                    ),
                    "candidate_version_id": version_id, "gate_cycle_id": cycle_id,
                    "native_id": str(review_doi or item.native_id),
                    "evaluation_type": str(work.get("subtype") or work.get("type") or "peer-review"),
                    "evaluator_role": None, "evaluator_public_id": None,
                    "evaluator_protected_id": None,
                    "anonymous": any("anonymous" in str(a).lower() for a in work.get("author") or []),
                    "official": None, "criterion_native": work.get("stage"),
                    "criterion_normalized": None, "criterion_value": work.get("recommendation"),
                    "criterion_value_numeric": None, "scale_json": None,
                    "confidence_value": None,
                    "text_artifact_id": stable_id("content_artifact", self.source_id, work_native),
                    "created_at": created_at, **common,
                })

    def validate_fixture(self, context: ConnectorContext) -> Mapping[str, Any]:
        fixture = context.fixture_dir / "crossref" / "peer_review.json"
        if not fixture.exists():
            return {"passes": False, "reason": "fixture absent"}
        work = json.loads(fixture.read_text())
        return {"passes": bool(work.get("DOI") and work.get("type") == "peer-review"), "doi": work.get("DOI")}

    def emit_coverage(self, context: ConnectorContext, *, found_count: int) -> Iterable[CoverageEvidence]:
        yield CoverageEvidence(
            gate_cycle_id=stable_id("gate_cycle", self.source_id, "peer-review-discovery"),
            object_type="peer_review_metadata", earliest_public_stage="deposited visible review object",
            observability_grade="D", expected_count=self._total,
            found_count=self._logical_found if self.bundle_pages else found_count,
            expected_count_method="Crossref total-results", query_or_invitation=self.endpoint,
            known_hidden_stages=("reviews not deposited", "rejected/hidden manuscripts"),
            known_exclusions=("provider-specific content and stage completeness",),
            audit_status=(
                "verified_lossless_page_bundles"
                if self._total is not None
                and (self._logical_found if self.bundle_pages else found_count) == self._total
                else "partial_snapshot"
            ),
        )


def _date(value: Mapping[str, Any] | None) -> str | None:
    return iso_datetime((value or {}).get("date-time"))


def content_fallback(work: Mapping[str, Any]) -> str:
    return stable_id("crossref_work", "crossref", json.dumps(work, sort_keys=True))
