"""arXiv OAI metadata/version adapter."""

from __future__ import annotations

import json
from typing import Any, Iterable, Iterator, Mapping

from lxml import etree

from ..connectors.base import (
    Connector,
    ConnectorContext,
    CoverageEvidence,
    FetchBatch,
    NormalizedRecord,
    RawItem,
    SourceEstimate,
)
from ..connectors.formats import parse_oai
from ..connectors.http import PoliteSession, RatePolicy
from ..identity import canonical_identifier
from ..ids import canonical_doi, stable_id
from .common import iso_datetime


class ArxivOAIConnector(Connector):
    source_id = "arxiv"
    connector_version = "3"
    endpoint = "https://export.arxiv.org/oai2"

    def __init__(self, *, metadata_prefix: str = "arXivRaw"):
        self.metadata_prefix = metadata_prefix
        self._emitted: set[str] = set()

    def _session(self, context: ConnectorContext) -> PoliteSession:
        return PoliteSession(
            cache_dir=context.cache_dir / self.source_id,
            allowed_hosts={"export.arxiv.org"},
            policy=RatePolicy(min_interval_seconds=3.0, max_retries=5, timeout_seconds=60),
        )

    def discover(self, context: ConnectorContext) -> Iterable[Mapping[str, Any]]:
        response = self._session(context).get(self.endpoint, params={"verb": "ListSets"})
        root = etree.fromstring(response.content)
        ns = {"oai": "http://www.openarchives.org/OAI/2.0/"}
        for node in root.xpath(".//oai:set", namespaces=ns):
            yield {"setSpec": node.findtext("oai:setSpec", namespaces=ns), "setName": node.findtext("oai:setName", namespaces=ns)}

    def count(self, context: ConnectorContext) -> SourceEstimate:
        return SourceEstimate(self.source_id, None, method="OAI has no stable preflight total; reconcile with completed cursor/bulk manifest", confidence="unknown")

    def fetch(self, context: ConnectorContext, *, cursor: str | None = None, limit: int | None = None) -> Iterator[FetchBatch]:
        session = self._session(context)
        token = cursor
        emitted = 0
        while True:
            params = {"verb": "ListRecords", "resumptionToken": token} if token else {"verb": "ListRecords", "metadataPrefix": self.metadata_prefix}
            if not token:
                if context.since:
                    params["from"] = context.since
                if context.until:
                    params["until"] = context.until
                if context.parameters.get("set"):
                    params["set"] = context.parameters["set"]
            response = session.get(self.endpoint, params=params)
            rows, next_token = parse_oai(response.content)
            items = []
            for row in rows:
                if limit is not None and emitted >= limit:
                    break
                items.append(RawItem(
                    native_id=row.get("identifier") or f"row:{emitted}", object_type="oai_record",
                    payload=json.dumps(row, sort_keys=True), modified_at=iso_datetime(row.get("datestamp")),
                    licence="arXiv-metadata-with-per-submission-content", release_class="pointer_hash",
                    metadata={"metadata_prefix": self.metadata_prefix},
                ))
                emitted += 1
            done = not next_token or (limit is not None and emitted >= limit)
            yield FetchBatch(tuple(items), None if done else next_token, done, f"arxiv-oai:{token or 'start'}")
            if done:
                return
            token = next_token

    def normalize(self, item: RawItem, *, source_object_id: str, provenance_event_id: str) -> Iterable[NormalizedRecord]:
        wrapper = json.loads(item.payload)
        if wrapper.get("deleted") or not wrapper.get("metadata_xml"):
            return
        root = etree.fromstring(wrapper["metadata_xml"].encode())
        def text(name: str) -> str | None:
            return " ".join(root.xpath(f"string(.//*[local-name()='{name}'])").split()) or None
        def values(name: str) -> list[str]:
            return [" ".join(str(value).split()) for value in root.xpath(f".//*[local-name()='{name}']/text()")]

        identifiers = values("identifier")
        arxiv_id = canonical_identifier(
            "arxiv",
            text("id") or next((value for value in identifiers if "arxiv.org/" in value), None)
            or str(wrapper.get("identifier", "")).split(":")[-1],
        )
        if not arxiv_id:
            return
        descriptions = values("description")
        title = text("title")
        abstract = text("abstract") or (descriptions[0] if descriptions else None)
        doi = canonical_doi(text("doi")) or next((canonical_doi(value) for value in identifiers if canonical_doi(value)), None)
        version_entries = []
        for index, node in enumerate(root.xpath(".//*[local-name()='version']"), 1):
            label = str(node.get("version") or f"v{index}")
            date_text = " ".join(node.xpath("string(.//*[local-name()='date'][1])").split())
            date = iso_datetime(date_text)
            if date:
                version_entries.append((label, date))
        if not version_entries:
            fallback_dates = values("date") or [text("created") or wrapper.get("datestamp")]
            version_entries = [
                (f"v{index}", normalized)
                for index, value in enumerate(fallback_dates, 1)
                if (normalized := iso_datetime(value))
            ]
        normalized_dates = [date for _, date in version_entries]
        created = normalized_dates[0] if normalized_dates else iso_datetime(wrapper.get("datestamp"))
        candidate_id = stable_id("candidate", self.source_id, arxiv_id)
        common = {
            "source_id": self.source_id, "source_object_id": source_object_id,
            "provenance_event_id": provenance_event_id, "observed_at": item.modified_at or created,
            "record_version": 1,
        }
        if candidate_id not in self._emitted:
            self._emitted.add(candidate_id)
            yield NormalizedRecord("candidate", {
                "candidate_id": candidate_id, "first_observed_at": created, "domain": text("categories"),
                "candidate_type": "preprint", "canonical_title": title, "status": "public", **common,
            })
            previous_id = None
            for number, (version_label, version_date) in enumerate(
                version_entries or [("v1", created)], 1
            ):
                version_id = stable_id(
                    "candidate_version", self.source_id, f"{arxiv_id}|{version_label}"
                )
                yield NormalizedRecord("candidate_version", {
                    "candidate_version_id": version_id, "candidate_id": candidate_id,
                    "native_id": f"{arxiv_id}{version_label}", "version_label": version_label,
                    "version_number": number, "created_at": version_date,
                    "modified_at": version_date, "title": title, "abstract": abstract,
                    "content_artifact_id": None, "content_hash": None,
                    "licence": text("license") or "per-submission", "language": None,
                    "authorship_visible": True, "withdrawn": False, **common,
                })
                yield NormalizedRecord("identity_visibility", {
                    "identity_visibility_id": stable_id(
                        "identity_visibility", self.source_id, f"{version_id}|authors"
                    ),
                    "candidate_version_id": version_id, "identity_kind": "authors",
                    "visible_from": version_date, "visible_to": None, "audience": "public",
                    "source_evidence": json.dumps({
                        "authors": text("authors"), "metadata_prefix": item.metadata.get("metadata_prefix")
                    }, sort_keys=True), **common,
                })
                if previous_id:
                    yield NormalizedRecord("lineage_edge", {
                        "lineage_edge_id": stable_id("lineage_edge", self.source_id, f"{previous_id}|{version_id}"),
                        "source_candidate_id": candidate_id, "source_version_id": previous_id,
                        "target_candidate_id": candidate_id, "target_version_id": version_id,
                        "relation_type": "source_declared_version", "declared": True,
                        "confidence": 1.0, "linkage_tier": "source_declared",
                        "method_version": "arxiv-oai-dates/1",
                        "evidence_json": json.dumps({"version_date": version_date, "version_number": number}),
                        **common,
                    })
                previous_id = version_id
        for scheme, value in (("arxiv", arxiv_id), ("doi", doi)):
            if value:
                yield NormalizedRecord("identifier_alias", {
                    "identifier_alias_id": stable_id("identifier_alias", self.source_id, f"candidate|{scheme}|{value}"),
                    "entity_kind": "candidate", "entity_id": candidate_id, "scheme": scheme,
                    "value": value, "canonical_value": value.lower(), "relation": "native" if scheme == "arxiv" else "publication",
                    "confidence": 1.0, "conflict_status": "none", **common,
                })
        journal_ref = text("journal-ref")
        if journal_ref:
            yield NormalizedRecord("identifier_alias", {
                "identifier_alias_id": stable_id(
                    "identifier_alias", self.source_id, f"candidate|journal_ref|{journal_ref}"
                ),
                "entity_kind": "candidate", "entity_id": candidate_id,
                "scheme": "journal_ref", "value": journal_ref,
                "canonical_value": " ".join(journal_ref.lower().split()),
                "relation": "source_declared_publication", "confidence": 0.8,
                "conflict_status": "none", **common,
            })

    def validate_fixture(self, context: ConnectorContext) -> Mapping[str, Any]:
        fixture = context.fixture_dir / "arxiv" / "oai_record.json"
        if not fixture.exists():
            return {"passes": False, "reason": "fixture absent"}
        row = json.loads(fixture.read_text())
        return {"passes": bool(row.get("identifier") and row.get("metadata_xml"))}

    def emit_coverage(self, context: ConnectorContext, *, found_count: int) -> Iterable[CoverageEvidence]:
        yield CoverageEvidence(
            gate_cycle_id=stable_id("gate_cycle", self.source_id, context.parameters.get("set", "all")),
            object_type="preprint_metadata", earliest_public_stage="public arXiv preprint",
            observability_grade="D", expected_count=None, found_count=found_count,
            expected_count_method="completed OAI cursor or bulk manifest required", query_or_invitation=self.endpoint,
            known_hidden_stages=("submission moderation and non-public submissions",), audit_status="unverified",
        )
