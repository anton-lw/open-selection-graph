"""Europe PMC search and open-full-text/JATS adapter."""

from __future__ import annotations

import json
from typing import Any, Iterable, Iterator, Mapping

from ..connectors.base import (
    Connector,
    ConnectorContext,
    CoverageEvidence,
    FetchBatch,
    NormalizedRecord,
    RawItem,
    SourceEstimate,
)
from ..connectors.formats import parse_jats
from ..connectors.http import PoliteSession, RatePolicy
from ..ids import canonical_doi, content_hash, stable_id
from ..licensing import canonical_licence, decide_release
from .common import iso_datetime


class EuropePMCConnector(Connector):
    source_id = "europe_pmc"
    connector_version = "5"
    search_endpoint = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
    fulltext_template = "https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML"

    def __init__(self, *, query: str = "OPEN_ACCESS:Y", page_size: int = 1000):
        self.query = query
        self.page_size = min(max(page_size, 1), 1000)
        self._total: int | None = None
        self._emitted: set[str] = set()

    def _session(self, context: ConnectorContext) -> PoliteSession:
        return PoliteSession(
            cache_dir=context.cache_dir / self.source_id,
            allowed_hosts={"www.ebi.ac.uk"},
            policy=RatePolicy(min_interval_seconds=0.2, max_retries=5),
        )

    def discover(self, context: ConnectorContext) -> Iterable[Mapping[str, Any]]:
        yield {"query": context.parameters.get("query", self.query), "include_fulltext": bool(context.parameters.get("include_fulltext"))}

    def count(self, context: ConnectorContext) -> SourceEstimate:
        query = context.parameters.get("query", self.query)
        response = self._session(context).get(
            self.search_endpoint,
            params={"query": query, "format": "json", "pageSize": 1, "resultType": "core"},
        )
        self._total = int(response.json().get("hitCount") or 0)
        include_fulltext = bool(context.parameters.get("include_fulltext")) and not context.no_text
        expected_objects = self._total * (2 if include_fulltext else 1)
        expected_requests = (
            (self._total + self.page_size - 1) // self.page_size
            + (self._total if include_fulltext else 0)
        )
        return SourceEstimate(
            self.source_id, expected_objects, expected_requests=expected_requests,
            method=(
                "Europe PMC hitCount plus one planned JATS object per matching work"
                if include_fulltext else "Europe PMC hitCount"
            ),
            confidence=(
                "provider metadata count; conservative JATS availability plan"
                if include_fulltext else "provider"
            ),
            objects_per_limit_unit=2.0 if include_fulltext else 1.0,
            requests_per_limit_unit=(
                1.0 + 1.0 / self.page_size if include_fulltext else 1.0 / self.page_size
            ),
        )

    def fetch(self, context: ConnectorContext, *, cursor: str | None = None, limit: int | None = None) -> Iterator[FetchBatch]:
        session = self._session(context)
        query = context.parameters.get("query", self.query)
        mark = cursor or "*"
        emitted = 0
        include_fulltext = bool(context.parameters.get("include_fulltext")) and not context.no_text
        while mark:
            size = min(self.page_size, limit - emitted) if limit is not None else self.page_size
            if size <= 0:
                return
            response = session.get(self.search_endpoint, params={
                "query": query, "format": "json", "pageSize": size,
                "resultType": "core", "cursorMark": mark,
            })
            body = response.json()
            self._total = int(body.get("hitCount") or self._total or 0)
            results = body.get("resultList", {}).get("result", [])
            items: list[RawItem] = []
            for result in results:
                native = result.get("pmcid") or result.get("doi") or f"{result.get('source')}:{result.get('id')}"
                items.append(RawItem(
                    native_id=f"work:{native}", object_type="work_metadata", payload=json.dumps(result, sort_keys=True),
                    source_url=(f"https://europepmc.org/article/{result.get('source')}/{result.get('id')}" if result.get("source") and result.get("id") else None),
                    created_at=iso_datetime(result.get("firstPublicationDate") or result.get("electronicPublicationDate")),
                    modified_at=None, licence="Europe-PMC-metadata-with-abstract-exception",
                    release_class="pointer_hash",
                ))
                if include_fulltext and result.get("pmcid"):
                    xml_response = session.get(self.fulltext_template.format(pmcid=result["pmcid"]))
                    items.append(RawItem(
                        native_id=f"fulltext:{result['pmcid']}", object_type="fulltext_xml", payload=xml_response.content,
                        source_url=xml_response.url, created_at=None, modified_at=None,
                        licence=result.get("license") or "per-object", release_class="derived_only",
                        metadata={"pmcid": result["pmcid"], "doi": result.get("doi")},
                    ))
                emitted += 1
                if limit is not None and emitted >= limit:
                    break
            next_mark = body.get("nextCursorMark")
            done = not results or not next_mark or next_mark == mark or (limit is not None and emitted >= limit)
            yield FetchBatch(tuple(items), None if done else str(next_mark), done, f"epmc:{mark}", self._total)
            if done:
                return
            mark = str(next_mark)

    def normalize(self, item: RawItem, *, source_object_id: str, provenance_event_id: str) -> Iterable[NormalizedRecord]:
        common = {
            "source_id": self.source_id, "source_object_id": source_object_id,
            "provenance_event_id": provenance_event_id, "observed_at": item.modified_at or item.created_at,
            "record_version": 1,
        }
        if item.object_type == "fulltext_xml":
            parsed = parse_jats(item.payload)
            doi = canonical_doi(parsed.get("doi") or item.metadata.get("doi"))
            pmcid = parsed.get("pmcid") or item.metadata.get("pmcid")
            declared_licence = next((
                canonical_licence(row.get("href") or row.get("text"))
                for row in parsed.get("licences") or []
                if canonical_licence(row.get("href") or row.get("text"))
            ), None) or canonical_licence(item.licence)
            licence_decision = decide_release(
                object_type="fulltext_xml", licence=declared_licence,
                source_allows_redistribution=True if declared_licence else None,
            )
            content_id = stable_id("content_artifact", self.source_id, f"fulltext:{pmcid or doi}")
            yield NormalizedRecord("content_artifact", {
                "content_artifact_id": content_id, "object_type": "fulltext_xml", "media_type": "application/xml",
                "byte_hash": content_hash(item.payload), "normalized_text_hash": content_hash(parsed.get("body_text") or ""),
                "source_url": item.source_url, "local_pointer": None,
                "licence": licence_decision.licence,
                "release_class": licence_decision.release_class.value,
                "size_bytes": len(item.payload), "language": None,
                "parser_version": self.connector_version, **common,
            })
            for sub_article in parsed.get("sub_articles") or []:
                article_type = str(sub_article.get("article_type") or "").lower()
                if not any(token in article_type for token in ("review", "ref-report", "response")):
                    continue
                native = str(sub_article.get("id") or content_hash(json.dumps(sub_article, sort_keys=True)))
                text = sub_article.get("body_text") or ""
                yield NormalizedRecord("content_artifact", {
                    "content_artifact_id": stable_id(
                        "content_artifact", self.source_id, f"{pmcid or doi}|sub-article|{native}"
                    ),
                    "object_type": f"jats_sub_article:{article_type or 'review_material'}",
                    "media_type": "application/xml", "byte_hash": content_hash(text),
                    "normalized_text_hash": content_hash(" ".join(text.split())),
                    "source_url": item.source_url, "local_pointer": f"sub-article[@id='{native}']",
                    "licence": licence_decision.licence,
                    "release_class": licence_decision.release_class.value,
                    "size_bytes": len(text.encode()), "language": None,
                    "parser_version": self.connector_version, **common,
                })
            citing_native = doi or pmcid
            if citing_native:
                namespace = "doi" if doi else "pmcid"
                candidate_id = stable_id("candidate", namespace, citing_native)
                version_id = stable_id("candidate_version", namespace, citing_native)
                for index, reference in enumerate(parsed.get("references") or []):
                    cited = canonical_doi(reference.get("doi"))
                    yield NormalizedRecord("reference_edge", {
                        "reference_edge_id": stable_id("reference_edge", self.source_id, f"{version_id}|{index}|{cited or reference.get('text')}"),
                        "citing_version_id": version_id, "reference_position": index,
                        "cited_candidate_id": stable_id("candidate", "doi", cited) if cited else None,
                        "cited_version_id": stable_id("candidate_version", "doi", cited) if cited else None,
                        "cited_identifier": cited, "raw_citation_hash": content_hash(reference.get("text") or ""),
                        "match_method": "structured_doi" if cited else "unresolved_text_hash",
                        "confidence": 1.0 if cited else 0.0, "time_valid": None, **common,
                    })
                for index, relation in enumerate(parsed.get("related_articles") or []):
                    target_doi = canonical_doi(relation.get("href"))
                    target_pmcid = (
                        str(relation.get("href") or "").rsplit("/", 1)[-1]
                        if "PMC" in str(relation.get("href") or "").upper() else None
                    )
                    target_native = target_doi or target_pmcid
                    if not target_native:
                        continue
                    target_namespace = "doi" if target_doi else "pmcid"
                    yield NormalizedRecord("lineage_edge", {
                        "lineage_edge_id": stable_id(
                            "lineage_edge", self.source_id,
                            f"jats-related|{candidate_id}|{relation.get('relation_type')}|{target_native}|{index}"
                        ),
                        "source_candidate_id": candidate_id, "source_version_id": version_id,
                        "target_candidate_id": stable_id("candidate", target_namespace, target_native),
                        "target_version_id": stable_id(
                            "candidate_version", target_namespace, target_native
                        ),
                        "relation_type": str(relation.get("relation_type") or "jats_related_article"),
                        "declared": True, "confidence": 1.0,
                        "linkage_tier": "source_declared", "method_version": "jats-related-article/1",
                        "evidence_json": json.dumps(relation, sort_keys=True), **common,
                    })
            return

        result = json.loads(item.payload)
        doi = canonical_doi(result.get("doi"))
        native = doi or result.get("pmcid") or f"{result.get('source')}:{result.get('id')}"
        candidate_id = stable_id("candidate", "doi" if doi else self.source_id, native)
        version_id = stable_id("candidate_version", "doi" if doi else self.source_id, native)
        publication_type_values = (
            (result.get("pubTypeList") or {}).get("pubType")
            if isinstance(result.get("pubTypeList"), dict) else result.get("pubTypeList") or []
        )
        if isinstance(publication_type_values, str):
            publication_type_values = [publication_type_values]
        publication_types = " ".join(str(value) for value in publication_type_values or []).lower()
        title_low = str(result.get("title") or "").lower()
        retracted = str(result.get("isRetracted") or "").upper() in {"Y", "TRUE", "1"}
        withdrawn = "withdraw" in publication_types or title_low.startswith(("withdrawn", "removed"))
        if candidate_id not in self._emitted:
            self._emitted.add(candidate_id)
            yield NormalizedRecord("candidate", {
                "candidate_id": candidate_id, "first_observed_at": item.created_at,
                "domain": "biomedical and life sciences", "candidate_type": result.get("pubType") or "scholarly_work",
                "canonical_title": result.get("title"),
                "status": "retracted" if retracted else ("withdrawn" if withdrawn else "visible"),
                **common,
            })
            yield NormalizedRecord("candidate_version", {
                "candidate_version_id": version_id, "candidate_id": candidate_id, "native_id": str(native),
                "version_label": result.get("pubType") or "indexed", "version_number": None,
                "created_at": item.created_at, "modified_at": None, "title": result.get("title"),
                "abstract": result.get("abstractText") if item.release_class != "pointer_hash" else None,
                "content_artifact_id": None, "content_hash": None, "licence": result.get("license"),
                "language": result.get("language"), "authorship_visible": True,
                "withdrawn": withdrawn, **common,
            })
        for scheme, value in (("doi", doi), ("pmcid", result.get("pmcid")), ("pmid", result.get("pmid"))):
            if not value:
                continue
            yield NormalizedRecord("identifier_alias", {
                "identifier_alias_id": stable_id("identifier_alias", self.source_id, f"candidate|{scheme}|{value}"),
                "entity_kind": "candidate", "entity_id": candidate_id, "scheme": scheme,
                "value": str(value), "canonical_value": str(value).lower(), "relation": "native",
                "confidence": 1.0, "conflict_status": "none", **common,
            })
        if result.get("journalTitle"):
            yield NormalizedRecord("field_assignment", {
                "field_assignment_id": stable_id("field_assignment", self.source_id, f"{candidate_id}|journal|{result['journalTitle']}"),
                "entity_kind": "candidate", "entity_id": candidate_id, "taxonomy": "europe_pmc_journal",
                "native_label": result["journalTitle"], "normalized_label": None, "score": 1.0,
                "mapping_version": "native/1", **common,
            })
        if retracted or withdrawn:
            outcome_type = "retraction" if retracted else "withdrawal"
            yield NormalizedRecord("downstream_outcome", {
                "downstream_outcome_id": stable_id(
                    "downstream_outcome", self.source_id, f"{candidate_id}|{outcome_type}"
                ),
                "candidate_id": candidate_id, "candidate_version_id": version_id,
                "outcome_type": outcome_type, "native_id": str(result.get("id") or native),
                "doi": doi, "venue": result.get("journalTitle"), "occurred_at": None,
                "window_years": None, "value_numeric": None,
                "value_json": json.dumps({
                    "isRetracted": result.get("isRetracted"), "pubTypeList": result.get("pubTypeList"),
                    "title_status_prefix": title_low.split(":", 1)[0] if withdrawn else None,
                }, sort_keys=True),
                "censoring_date": item.created_at, **common,
            })
        corrections = result.get("commentCorrectionList") or {}
        corrections = corrections.get("commentCorrection") if isinstance(corrections, dict) else corrections
        if isinstance(corrections, dict):
            corrections = [corrections]
        for index, correction in enumerate(corrections or []):
            target_native = str(correction.get("id") or "")
            target_source = str(correction.get("source") or self.source_id).lower()
            target_id = stable_id("candidate", target_source, target_native) if target_native else None
            yield NormalizedRecord("downstream_outcome", {
                "downstream_outcome_id": stable_id(
                    "downstream_outcome", self.source_id,
                    f"{candidate_id}|correction|{index}|{target_source}|{target_native}"
                ),
                "candidate_id": candidate_id, "candidate_version_id": version_id,
                "outcome_type": str(correction.get("type") or "comment_correction"),
                "native_id": target_native or None, "doi": None,
                "venue": result.get("journalTitle"), "occurred_at": None,
                "window_years": None, "value_numeric": None,
                "value_json": json.dumps(correction, sort_keys=True),
                "censoring_date": item.created_at, **common,
            })
            if target_id:
                yield NormalizedRecord("lineage_edge", {
                    "lineage_edge_id": stable_id(
                        "lineage_edge", self.source_id,
                        f"{candidate_id}|{target_id}|{correction.get('type')}"
                    ),
                    "source_candidate_id": candidate_id, "source_version_id": version_id,
                    "target_candidate_id": target_id, "target_version_id": None,
                    "relation_type": str(correction.get("type") or "comment_correction"),
                    "declared": True, "confidence": 1.0, "linkage_tier": "source_declared",
                    "method_version": "europe-pmc-comment-correction/1",
                    "evidence_json": json.dumps(correction, sort_keys=True), **common,
                })

    def validate_fixture(self, context: ConnectorContext) -> Mapping[str, Any]:
        fixture = context.fixture_dir / "europe_pmc" / "work.json"
        if not fixture.exists():
            return {"passes": False, "reason": "fixture absent"}
        row = json.loads(fixture.read_text())
        return {"passes": bool(row.get("id") and (row.get("doi") or row.get("pmcid")))}

    def emit_coverage(self, context: ConnectorContext, *, found_count: int) -> Iterable[CoverageEvidence]:
        yield CoverageEvidence(
            gate_cycle_id=stable_id("gate_cycle", self.source_id, content_hash(context.parameters.get("query", self.query))),
            object_type="indexed_work", earliest_public_stage="Europe-PMC-indexed public work",
            observability_grade="D", expected_count=self._total, found_count=found_count,
            expected_count_method="Europe PMC hitCount", query_or_invitation=context.parameters.get("query", self.query),
            known_hidden_stages=("submission and editorial selection",), audit_status="verified" if found_count == self._total else "partial_snapshot",
        )
