"""Crossref posted-content census for the Copernicus discussion population."""

from __future__ import annotations

import json
from collections import Counter
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
    coverage_observation_id,
)
from ..connectors.http import PoliteSession, RatePolicy
from ..ids import canonical_doi, content_hash, stable_id
from .common import iso_datetime, json_text


def _date(value: Any) -> str | None:
    if isinstance(value, Mapping):
        value = value.get("date-time") or value.get("timestamp")
    return iso_datetime(value)


def _kind(work: Mapping[str, Any]) -> str:
    doi = str(work.get("DOI") or "").lower()
    subtype = str(work.get("subtype") or "").lower()
    if subtype == "preprint":
        return "discussion_preprint"
    if subtype == "other" and doi.split("/", 1)[-1].startswith("egusphere-egu"):
        return "conference_abstract"
    return "public_review_or_other_posted_content"


def _year(work: Mapping[str, Any]) -> int:
    for key in ("published", "created", "deposited", "indexed"):
        value = work.get(key)
        if isinstance(value, Mapping):
            parts = value.get("date-parts") or []
            if parts and parts[0]:
                return int(parts[0][0])
            if stamp := _date(value):
                return int(stamp[:4])
    return 1970


def _journal(doi: str) -> str:
    return doi.split("/", 1)[-1].split("-", 1)[0].lower()


def _title(work: Mapping[str, Any]) -> str | None:
    value = work.get("title")
    if isinstance(value, list):
        return str(value[0]) if value else None
    return str(value) if value else None


class CopernicusCrossrefPostedConnector(Connector):
    source_id = "copernicus_crossref"
    connector_version = "1"
    force_streaming = True
    endpoint = "https://api.crossref.org/prefixes/10.5194/works"

    def __init__(self, *, rows: int = 1000):
        self.rows = min(max(rows, 1), 1000)
        self._total: int | None = None
        self._logical_found = 0
        self._kind_counts: Counter[str] = Counter()
        self._cycle_counts: Counter[str] = Counter()
        self._emitted: dict[str, set[str]] = {
            "gate": set(),
            "gate_cycle": set(),
            "candidate": set(),
            "candidate_version": set(),
            "evaluation": set(),
            "lineage_edge": set(),
        }

    @property
    def provider_total(self) -> int | None:
        return self._total

    @property
    def kind_counts(self) -> dict[str, int]:
        return dict(self._kind_counts)

    def _session(self, context: ConnectorContext) -> PoliteSession:
        return PoliteSession(
            cache_dir=context.cache_dir / self.source_id,
            allowed_hosts={"api.crossref.org"},
            policy=RatePolicy(
                min_interval_seconds=0.1,
                max_retries=5,
                timeout_seconds=120,
                daily_request_ceiling=2_000,
            ),
        )

    def discover(self, context: ConnectorContext) -> Iterable[Mapping[str, Any]]:
        yield {
            "endpoint": self.endpoint,
            "filter": "type:posted-content",
            "cursor": True,
            "candidate_subtype": "preprint",
            "conference_subtype": "other + egusphere-egu DOI grammar",
        }

    def count(self, context: ConnectorContext) -> SourceEstimate:
        response = self._session(context).get(
            self.endpoint,
            params={"filter": "type:posted-content", "rows": 0},
        )
        self._total = int(response.json()["message"]["total-results"])
        pages = ceil(self._total / self.rows)
        return SourceEstimate(
            self.source_id,
            pages,
            expected_requests=pages + 1,
            expected_bytes=self._total * 4_000,
            method=(
                f"{self._total} Crossref 10.5194 posted-content works in immutable "
                f"API-page bundles of at most {self.rows}"
            ),
            confidence="provider total-results and subtype deposits",
            requests_per_limit_unit=1.0,
        )

    def fetch(
        self,
        context: ConnectorContext,
        *,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> Iterator[FetchBatch]:
        token = cursor or "*"
        emitted_pages = 0
        previous_page_hash: str | None = None
        session = self._session(context)
        while token:
            response = session.get(
                self.endpoint,
                params={
                    "filter": "type:posted-content",
                    "rows": self.rows,
                    "cursor": token,
                },
            )
            message = response.json()["message"]
            works = message.get("items") or []
            self._total = int(message.get("total-results") or self._total or 0)
            page_hash = content_hash(json.dumps(
                [work.get("DOI") or work.get("URL") for work in works]
            ))
            if works and page_hash == previous_page_hash:
                raise RuntimeError("Crossref posted-content cursor repeated an identical page")
            previous_page_hash = page_hash
            self._logical_found += len(works)
            payload = json.dumps({
                "cursor": token,
                "next_cursor": message.get("next-cursor"),
                "total_results": self._total,
                "items": works,
            }, sort_keys=True)
            item = RawItem(
                native_id=f"posted-page:{content_hash(str(token))[:24]}:{emitted_pages}",
                object_type="copernicus_posted_content_page",
                payload=payload,
                source_url=response.url,
                created_at=min(
                    filter(None, (_date(work.get("created")) for work in works)),
                    default=None,
                ),
                modified_at=max(
                    filter(None, (_date(work.get("indexed")) for work in works)),
                    default=None,
                ),
                licence="Crossref-CC0-with-abstract-exception",
                release_class="pointer_hash",
                metadata={"cursor": token, "work_count": len(works)},
            )
            emitted_pages += 1
            next_token = message.get("next-cursor")
            done = bool(
                not works
                or not next_token
                or (self._total is not None and self._logical_found >= self._total)
                or (limit is not None and emitted_pages >= limit)
            )
            yield FetchBatch(
                (item,),
                None if done else str(next_token),
                done,
                f"copernicus-crossref:{quote(str(token), safe='')}",
                self._total,
            )
            if done:
                return
            token = str(next_token)

    def normalize(
        self,
        item: RawItem,
        *,
        source_object_id: str,
        provenance_event_id: str,
    ) -> Iterable[NormalizedRecord]:
        page = json.loads(item.payload)
        for index, work in enumerate(page.get("items") or []):
            yield from self._normalize_work(
                work,
                index=index,
                item=item,
                source_object_id=source_object_id,
                provenance_event_id=provenance_event_id,
            )

    def _normalize_work(
        self,
        work: Mapping[str, Any],
        *,
        index: int,
        item: RawItem,
        source_object_id: str,
        provenance_event_id: str,
    ) -> Iterable[NormalizedRecord]:
        doi = canonical_doi(work.get("DOI"))
        if not doi:
            return
        kind = _kind(work)
        self._kind_counts[kind] += 1
        observed = _date(work.get("indexed")) or _date(work.get("created"))
        common = {
            "source_id": self.source_id,
            "source_object_id": source_object_id,
            "provenance_event_id": provenance_event_id,
            "observed_at": observed,
            "record_version": 1,
        }
        artifact_id = stable_id("content_artifact", self.source_id, doi)
        yield NormalizedRecord("content_artifact", {
            "content_artifact_id": artifact_id,
            "object_type": f"copernicus_crossref_{kind}_metadata",
            "media_type": "application/json",
            "byte_hash": content_hash(json.dumps(work, sort_keys=True)),
            "normalized_text_hash": None,
            "source_url": f"https://doi.org/{doi}",
            "local_pointer": f"items[{index}]",
            "licence": item.licence,
            "release_class": item.release_class,
            "size_bytes": len(json.dumps(work, sort_keys=True).encode()),
            "language": work.get("language"),
            "parser_version": self.connector_version,
            **common,
        })
        if kind == "conference_abstract":
            return
        if kind == "discussion_preprint":
            yield from self._emit_candidate(
                work,
                doi=doi,
                kind=kind,
                common=common,
            )
        for relation_type, values in (work.get("relation") or {}).items():
            for relation in values or []:
                target = canonical_doi(relation.get("id"))
                if not target:
                    continue
                if relation_type in {"is-preprint-of", "has-version", "is-version-of"}:
                    edge_id = stable_id(
                        "lineage_edge", self.source_id, f"{doi}|{relation_type}|{target}"
                    )
                    if edge_id not in self._emitted["lineage_edge"]:
                        self._emitted["lineage_edge"].add(edge_id)
                        yield NormalizedRecord("lineage_edge", {
                            "lineage_edge_id": edge_id,
                            "source_candidate_id": stable_id("candidate", "doi", doi),
                            "source_version_id": stable_id("candidate_version", "doi", doi),
                            "target_candidate_id": stable_id("candidate", "doi", target),
                            "target_version_id": stable_id("candidate_version", "doi", target),
                            "relation_type": relation_type,
                            "declared": True,
                            "confidence": 1.0,
                            "linkage_tier": "source_declared",
                            "method_version": "crossref-posted-relation/1",
                            "evidence_json": json_text(relation),
                            **common,
                        })
                if kind != "discussion_preprint":
                    evaluation_id = stable_id(
                        "evaluation", self.source_id, f"{doi}|{relation_type}|{target}"
                    )
                    if evaluation_id in self._emitted["evaluation"]:
                        continue
                    self._emitted["evaluation"].add(evaluation_id)
                    cycle_id = stable_id(
                        "gate_cycle", self.source_id, f"{_journal(target)}|{_year(work)}"
                    )
                    yield NormalizedRecord("evaluation", {
                        "evaluation_id": evaluation_id,
                        "candidate_version_id": stable_id(
                            "candidate_version", "doi", target
                        ),
                        "gate_cycle_id": cycle_id,
                        "native_id": doi,
                        "evaluation_type": str(work.get("subtype") or "posted-content"),
                        "evaluator_role": None,
                        "evaluator_public_id": None,
                        "evaluator_protected_id": None,
                        "anonymous": None,
                        "official": None,
                        "criterion_native": relation_type,
                        "criterion_normalized": None,
                        "criterion_value": None,
                        "criterion_value_numeric": None,
                        "scale_json": None,
                        "confidence_value": None,
                        "text_artifact_id": artifact_id,
                        "created_at": _date(work.get("created")),
                        "forum_native_id": target,
                        "invitation_native": relation_type,
                        "readers_json": json_text(["public Crossref deposit"]),
                        "signatures_json": json_text(work.get("author")),
                        "reply_to_native_id": target,
                        **common,
                    })

    def _emit_candidate(
        self,
        work: Mapping[str, Any],
        *,
        doi: str,
        kind: str,
        common: Mapping[str, Any],
    ) -> Iterable[NormalizedRecord]:
        journal = _journal(doi)
        year = _year(work)
        gate_id = stable_id("gate", self.source_id, journal)
        cycle_id = stable_id("gate_cycle", self.source_id, f"{journal}|{year}")
        candidate_id = stable_id("candidate", "doi", doi)
        version_id = stable_id("candidate_version", "doi", doi)
        self._cycle_counts[cycle_id] += 1
        if gate_id not in self._emitted["gate"]:
            self._emitted["gate"].add(gate_id)
            yield NormalizedRecord("gate", {
                "gate_id": gate_id,
                "native_id": journal,
                "name": journal,
                "organization": "Copernicus Publications",
                "domain": "earth and environmental sciences",
                "country": "DE",
                "architecture": "access_public_discussion",
                "active_from": None,
                "active_to": None,
                **common,
            })
        if cycle_id not in self._emitted["gate_cycle"]:
            self._emitted["gate_cycle"].add(cycle_id)
            yield NormalizedRecord("gate_cycle", {
                "gate_cycle_id": cycle_id,
                "gate_id": gate_id,
                "native_id": f"{journal}|{year}",
                "name": f"{journal} {year}",
                "track": None,
                "cycle_start": f"{year}-01-01T00:00:00+00:00",
                "cycle_end": f"{year}-12-31T23:59:59+00:00",
                "policy_version_id": None,
                "architecture": "access_public_discussion",
                "received_count": None,
                "observable_count": None,
                "evaluated_count": None,
                "selected_count": None,
                "status": "Crossref posted-content subtype census",
                **common,
            })
        if candidate_id not in self._emitted["candidate"]:
            self._emitted["candidate"].add(candidate_id)
            title = _title(work)
            yield NormalizedRecord("candidate", {
                "candidate_id": candidate_id,
                "first_observed_at": _date(work.get("created")),
                "domain": "earth and environmental sciences",
                "candidate_type": kind,
                "canonical_title": title,
                "status": "public_discussion_visible",
                **common,
            })
        if version_id not in self._emitted["candidate_version"]:
            self._emitted["candidate_version"].add(version_id)
            yield NormalizedRecord("candidate_version", {
                "candidate_version_id": version_id,
                "candidate_id": candidate_id,
                "native_id": doi,
                "version_label": "public_discussion_preprint",
                "version_number": None,
                "created_at": _date(work.get("created")),
                "modified_at": _date(work.get("indexed")),
                "title": _title(work),
                "abstract": work.get("abstract"),
                "content_artifact_id": stable_id("content_artifact", self.source_id, doi),
                "content_hash": None,
                "licence": None,
                "language": work.get("language"),
                "authorship_visible": bool(work.get("author")),
                "withdrawn": False,
                **common,
            })
        yield NormalizedRecord("identifier_alias", {
            "identifier_alias_id": stable_id(
                "identifier_alias", self.source_id, f"candidate|doi|{doi}"
            ),
            "entity_kind": "candidate",
            "entity_id": candidate_id,
            "scheme": "doi",
            "value": doi,
            "canonical_value": doi,
            "relation": "native",
            "confidence": 1.0,
            "conflict_status": "none",
            **common,
        })
        yield NormalizedRecord("candidate_gate_event", {
            "candidate_gate_event_id": stable_id(
                "candidate_gate_event", self.source_id, f"{cycle_id}|{doi}"
            ),
            "candidate_id": candidate_id,
            "candidate_version_id": version_id,
            "gate_cycle_id": cycle_id,
            "native_id": doi,
            "submitted_at": _date(work.get("created")),
            "earliest_observed_stage": "post_access_public_discussion",
            "final_observed_stage": "outcome_unresolved_or_censored",
            "coverage_observation_id": coverage_observation_id(
                self.source_id, cycle_id, "discussion_preprint"
            ),
            **common,
        })

    def validate_fixture(self, context: ConnectorContext) -> Mapping[str, Any]:
        fixture = context.fixture_dir / self.source_id / "posted_page.json"
        if not fixture.exists():
            return {"passes": False, "reason": "fixture absent"}
        page = json.loads(fixture.read_text())
        works = page.get("items") or []
        return {
            "passes": bool(works and all(work.get("DOI") for work in works)),
            "work_count": len(works),
            "preprint_count": sum(_kind(work) == "discussion_preprint" for work in works),
        }

    def emit_coverage(
        self,
        context: ConnectorContext,
        *,
        found_count: int,
    ) -> Iterable[CoverageEvidence]:
        yield CoverageEvidence(
            gate_cycle_id=stable_id("gate_cycle", self.source_id, "posted-content-all"),
            object_type="posted_content_page_bundle",
            earliest_public_stage="Crossref-deposited public object",
            observability_grade="D",
            expected_count=ceil(self._total / self.rows) if self._total else None,
            found_count=found_count,
            expected_count_method="Crossref total-results divided into cursor page bundles",
            query_or_invitation=f"{self.endpoint}?filter=type:posted-content",
            known_hidden_stages=("access review before public discussion",),
            audit_status="cursor_exact" if self._total else "unresolved",
        )
        for cycle_id, count in sorted(self._cycle_counts.items()):
            yield CoverageEvidence(
                gate_cycle_id=cycle_id,
                object_type="discussion_preprint",
                earliest_public_stage="post-access public discussion",
                observability_grade="B",
                expected_count=count,
                found_count=count,
                expected_count_method="complete Crossref posted-content subtype census",
                query_or_invitation=f"{self.endpoint}?filter=type:posted-content",
                known_hidden_stages=("access review",),
                known_exclusions=("conference subtype other excluded",),
                audit_status="provider_subtype_exact",
            )
