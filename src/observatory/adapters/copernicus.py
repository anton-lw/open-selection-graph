"""Copernicus OAI-PMH metadata/full-text adapter."""

from __future__ import annotations

import json
import math
import re
from collections import Counter
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
    coverage_observation_id,
)
from ..connectors.formats import parse_oai
from ..connectors.http import PoliteSession, RatePolicy
from ..ids import canonical_doi, stable_id
from .common import iso_datetime


class CopernicusOAIConnector(Connector):
    source_id = "copernicus"
    connector_version = "5"
    force_streaming = True
    endpoint = "https://oai-pmh.copernicus.org/oai.php"

    def __init__(self, *, metadata_prefix: str = "oai_dc"):
        self.metadata_prefix = metadata_prefix
        self._complete_list_size: int | None = None
        self._page_size: int | None = None
        self._record_count = 0
        self._cycle_counts: Counter[str] = Counter()
        self._cycle_kind_counts: Counter[tuple[str, str]] = Counter()
        self._recovered_response_count = 0
        self._emitted: dict[str, set[str]] = {"gate": set(), "gate_cycle": set(), "candidate": set()}

    @property
    def provider_record_count(self) -> int | None:
        return self._complete_list_size

    @staticmethod
    def _parse_provider_page(payload: bytes) -> tuple[list[dict[str, Any]], str | None, int]:
        try:
            rows, token = parse_oai(payload)
            return rows, token, 0
        except etree.XMLSyntaxError:
            repaired, substitutions = re.subn(
                rb"&(?!amp;|lt;|gt;|quot;|apos;|#\d+;|#x[0-9A-Fa-f]+;)",
                b"&amp;",
                payload,
            )
            if not substitutions:
                raise
            rows, token = parse_oai(repaired)
            return rows, token, substitutions

    def _session(self, context: ConnectorContext) -> PoliteSession:
        return PoliteSession(
            cache_dir=context.cache_dir / self.source_id,
            allowed_hosts={"oai-pmh.copernicus.org"},
            policy=RatePolicy(min_interval_seconds=0.5, max_retries=5),
        )

    def discover(self, context: ConnectorContext) -> Iterable[Mapping[str, Any]]:
        response = self._session(context).get(self.endpoint, params={"verb": "ListSets"})
        root = etree.fromstring(response.content)
        ns = {"oai": "http://www.openarchives.org/OAI/2.0/"}
        for node in root.xpath(".//oai:set", namespaces=ns):
            yield {"setSpec": node.findtext("oai:setSpec", namespaces=ns), "setName": node.findtext("oai:setName", namespaces=ns)}

    def count(self, context: ConnectorContext) -> SourceEstimate:
        response = self._session(context).get(
            self.endpoint,
            params={"verb": "ListRecords", "metadataPrefix": self.metadata_prefix, "until": context.until} if context.until else {"verb": "ListRecords", "metadataPrefix": self.metadata_prefix},
        )
        rows, _, _ = self._parse_provider_page(response.content)
        root = etree.fromstring(response.content)
        token = root.find(".//{http://www.openarchives.org/OAI/2.0/}resumptionToken")
        self._complete_list_size = int(token.get("completeListSize")) if token is not None and token.get("completeListSize") else None
        self._page_size = len(rows) or None
        expected_pages = (
            math.ceil(self._complete_list_size / self._page_size)
            if self._complete_list_size and self._page_size else None
        )
        return SourceEstimate(
            self.source_id,
            expected_pages,
            expected_requests=expected_pages,
            method=(
                "lossless OAI response-page bundles; provider completeListSize is retained "
                "as the exact record denominator"
            ),
            confidence="provider record total; exact initial page size",
            requests_per_limit_unit=1.0,
        )

    def fetch(self, context: ConnectorContext, *, cursor: str | None = None, limit: int | None = None) -> Iterator[FetchBatch]:
        session = self._session(context)
        token = cursor
        emitted_pages = 0
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
            rows, next_token, repair_count = self._parse_provider_page(response.content)
            if repair_count:
                self._recovered_response_count += 1
            self._record_count += len(rows)
            item = RawItem(
                native_id=f"oai-page:{stable_id('oai_page', self.source_id, token or 'start')}",
                object_type="oai_response_page",
                payload=response.content,
                source_url=response.url,
                licence="CC-BY-4.0",
                release_class="redistribute",
                metadata={
                    "metadata_prefix": self.metadata_prefix,
                    "bare_ampersand_repairs_for_parsing": repair_count,
                    "response_byte_count": len(response.content),
                    "record_count": len(rows),
                },
            )
            emitted_pages += 1
            done = not next_token or (
                limit is not None and emitted_pages >= limit
            )
            yield FetchBatch(
                (item,),
                None if done else next_token,
                done,
                f"oai:{token or 'start'}",
                self._complete_list_size,
            )
            if done:
                return
            token = next_token

    def normalize(self, item: RawItem, *, source_object_id: str, provenance_event_id: str) -> Iterable[NormalizedRecord]:
        if item.object_type == "oai_response_page":
            rows, _, repair_count = self._parse_provider_page(
                item.payload if isinstance(item.payload, bytes) else item.payload.encode()
            )
            for index, row in enumerate(rows):
                yield from self.normalize(
                    RawItem(
                        native_id=str(
                            row.get("identifier")
                            or stable_id("oai_record", self.source_id, json.dumps(row, sort_keys=True))
                        ),
                        object_type="oai_record",
                        payload=json.dumps(row, sort_keys=True),
                        source_url=item.source_url,
                        modified_at=iso_datetime(row.get("datestamp")),
                        licence=item.licence,
                        release_class=item.release_class,
                        metadata={
                            **dict(item.metadata),
                            "bundle_record_index": index,
                            "bare_ampersand_repairs_for_parsing": repair_count,
                        },
                    ),
                    source_object_id=source_object_id,
                    provenance_event_id=provenance_event_id,
                )
            return
        wrapper = json.loads(item.payload)
        metadata_xml = wrapper.get("metadata_xml")
        common = {
            "source_id": self.source_id, "source_object_id": source_object_id,
            "provenance_event_id": provenance_event_id,
            "observed_at": item.modified_at,
            "record_version": 1,
        }
        native = str(wrapper.get("identifier") or item.native_id)
        if wrapper.get("deleted") or not metadata_xml:
            yield NormalizedRecord("content_artifact", {
                "content_artifact_id": stable_id(
                    "content_artifact", self.source_id, f"wrapper|{native}"
                ),
                "object_type": "oai_record_wrapper__deleted",
                "media_type": "application/xml", "byte_hash": None,
                "normalized_text_hash": None, "source_url": item.source_url,
                "local_pointer": f"bundle.records[{item.metadata.get('bundle_record_index')}]",
                "licence": item.licence, "release_class": item.release_class,
                "size_bytes": 0, "language": None,
                "parser_version": self.connector_version, **common,
            })
            return
        root = etree.fromstring(metadata_xml.encode())
        ns = {"dc": "http://purl.org/dc/elements/1.1/"}
        def values(name: str) -> list[str]:
            return [
                " ".join(text.split())
                for text in root.xpath(f".//dc:{name}/text()", namespaces=ns)
            ]
        ids = values("identifier")
        doi = next((canonical_doi(value) for value in ids if canonical_doi(value)), None)
        if not doi:
            yield NormalizedRecord("content_artifact", {
                "content_artifact_id": stable_id(
                    "content_artifact", self.source_id, f"wrapper|{native}"
                ),
                "object_type": "oai_record_wrapper__unclassified",
                "media_type": "application/xml", "byte_hash": None,
                "normalized_text_hash": None, "source_url": item.source_url,
                "local_pointer": f"bundle.records[{item.metadata.get('bundle_record_index')}]",
                "licence": item.licence, "release_class": item.release_class,
                "size_bytes": len(metadata_xml.encode()),
                "language": (values("language") or [None])[0],
                "parser_version": self.connector_version, **common,
            })
            return
        title = (values("title") or [None])[0]
        date = iso_datetime((values("date") or [wrapper.get("datestamp")])[0])
        journal = _journal_from_doi(doi)
        object_kind = _object_kind(doi, ids)
        year = int((date or wrapper.get("datestamp") or "1970")[:4])
        gate_id = stable_id("gate", self.source_id, journal)
        cycle_id = stable_id("gate_cycle", self.source_id, f"{journal}|{year}")
        candidate_id = stable_id("candidate", "doi", doi)
        version_id = stable_id("candidate_version", "doi", doi)
        self._cycle_counts[cycle_id] += 1
        common["observed_at"] = item.modified_at or date
        yield NormalizedRecord("content_artifact", {
            "content_artifact_id": stable_id(
                "content_artifact", self.source_id, f"wrapper|{native}"
            ),
            "object_type": f"oai_record_wrapper__{object_kind}",
            "media_type": "application/xml", "byte_hash": None,
            "normalized_text_hash": None, "source_url": item.source_url,
            "local_pointer": f"bundle.records[{item.metadata.get('bundle_record_index')}]",
            "licence": item.licence, "release_class": item.release_class,
            "size_bytes": len(metadata_xml.encode()),
            "language": (values("language") or [None])[0],
            "parser_version": self.connector_version, **common,
        })
        yield NormalizedRecord("content_artifact", {
            "content_artifact_id": stable_id("content_artifact", self.source_id, doi),
            "object_type": f"copernicus_{object_kind}_metadata", "media_type": "application/xml",
            "byte_hash": None, "normalized_text_hash": None,
            "source_url": f"https://doi.org/{doi}",
            "local_pointer": f"bundle.records[{item.metadata.get('bundle_record_index')}]",
            "licence": (values("rights") or ["CC-BY-4.0"])[0],
            "release_class": "redistribute", "size_bytes": len(metadata_xml.encode()),
            "language": (values("language") or [None])[0], "parser_version": self.connector_version,
            **common,
        })
        if object_kind == "conference_abstract":
            return
        self._cycle_kind_counts[(cycle_id, object_kind)] += 1
        if gate_id not in self._emitted["gate"]:
            self._emitted["gate"].add(gate_id)
            yield NormalizedRecord("gate", {
                "gate_id": gate_id, "native_id": journal, "name": journal,
                "organization": "Copernicus Publications", "domain": "earth and environmental sciences",
                "country": "DE", "architecture": "access_public_discussion", "active_from": None,
                "active_to": None, **common,
            })
        if cycle_id not in self._emitted["gate_cycle"]:
            self._emitted["gate_cycle"].add(cycle_id)
            yield NormalizedRecord("gate_cycle", {
                "gate_cycle_id": cycle_id, "gate_id": gate_id, "native_id": f"{journal}|{year}",
                "name": f"{journal} {year}", "track": None, "cycle_start": f"{year}-01-01T00:00:00+00:00",
                "cycle_end": f"{year}-12-31T23:59:59+00:00", "policy_version_id": None,
                "architecture": "access_public_discussion", "received_count": None,
                "observable_count": None, "evaluated_count": None, "selected_count": None,
                "status": "observed", **common,
            })
        if candidate_id not in self._emitted["candidate"]:
            self._emitted["candidate"].add(candidate_id)
            yield NormalizedRecord("candidate", {
                "candidate_id": candidate_id, "first_observed_at": date, "domain": "earth and environmental sciences",
                "candidate_type": object_kind, "canonical_title": title, "status": "visible", **common,
            })
            yield NormalizedRecord("candidate_version", {
                "candidate_version_id": version_id, "candidate_id": candidate_id, "native_id": doi,
                "version_label": "public_discussion_preprint" if object_kind == "discussion_preprint" else "final_article",
                "version_number": None, "created_at": date,
                "modified_at": item.modified_at, "title": title,
                "abstract": "\n".join(values("description")) or None, "content_artifact_id": None,
                "content_hash": None, "licence": (values("rights") or ["CC-BY-4.0"])[0],
                "language": (values("language") or [None])[0], "authorship_visible": True,
                "withdrawn": False, **common,
            })
        yield NormalizedRecord("identifier_alias", {
            "identifier_alias_id": stable_id("identifier_alias", self.source_id, f"candidate|doi|{doi}"),
            "entity_kind": "candidate", "entity_id": candidate_id, "scheme": "doi", "value": doi,
            "canonical_value": doi, "relation": "native", "confidence": 1.0,
            "conflict_status": "none", **common,
        })
        if object_kind == "discussion_preprint":
            yield NormalizedRecord("candidate_gate_event", {
                "candidate_gate_event_id": stable_id(
                    "candidate_gate_event", self.source_id, f"{cycle_id}|{doi}"
                ),
                "candidate_id": candidate_id, "candidate_version_id": version_id,
                "gate_cycle_id": cycle_id, "native_id": doi, "submitted_at": None,
                "earliest_observed_stage": "post_access_public_discussion",
                "final_observed_stage": "outcome_unresolved_or_censored",
                "coverage_observation_id": coverage_observation_id(
                    self.source_id, cycle_id, "discussion_preprint"
                ),
                **common,
            })

    def validate_fixture(self, context: ConnectorContext) -> Mapping[str, Any]:
        fixture = context.fixture_dir / "copernicus" / "oai_page.xml"
        if not fixture.exists():
            return {"passes": False, "reason": "fixture absent"}
        rows, token, repairs = self._parse_provider_page(fixture.read_bytes())
        return {
            "passes": bool(rows and rows[0].get("identifier")),
            "record_count": len(rows),
            "resumption_token_present": bool(token),
            "bare_ampersand_repairs": repairs,
        }

    def emit_coverage(self, context: ConnectorContext, *, found_count: int) -> Iterable[CoverageEvidence]:
        if not self._cycle_counts:
            yield CoverageEvidence(
                gate_cycle_id=stable_id("gate_cycle", self.source_id, "oai-all"), object_type="oai_record",
                earliest_public_stage="published article OAI record", observability_grade="D",
                expected_count=self._complete_list_size, found_count=self._record_count,
                expected_count_method="OAI completeListSize", query_or_invitation=self.endpoint,
                known_hidden_stages=("access review", "discussion-stage records require Crossref/page relation join"),
                audit_status="partial_snapshot",
            )
        for (cycle_id, object_kind), count in sorted(self._cycle_kind_counts.items()):
            yield CoverageEvidence(
                gate_cycle_id=cycle_id, object_type=object_kind,
                earliest_public_stage=(
                    "post-access public discussion" if object_kind == "discussion_preprint"
                    else "final published article"
                ),
                observability_grade="U" if object_kind == "discussion_preprint" else "D",
                expected_count=None, found_count=count,
                expected_count_method="cycle denominator unresolved pending Crossref/journal reconciliation",
                query_or_invitation=self.endpoint,
                known_hidden_stages=("access review",) if object_kind == "discussion_preprint" else (),
                known_exclusions=("conference abstracts classified and excluded",),
                audit_status=(
                    "unverified; recovered_malformed_oai_responses="
                    f"{self._recovered_response_count}"
                ),
            )


def _journal_from_doi(doi: str) -> str:
    tail = doi.split("/", 1)[1]
    return tail.split("-")[0].lower() if "-" in tail else "copernicus"


def _object_kind(doi: str, identifiers: list[str]) -> str:
    low = " ".join([doi, *identifiers]).lower()
    if any(token in low for token in ("egusphere-egu", "meetings.copernicus", "/conference")):
        return "conference_abstract"
    if "/preprints/" in low or re.match(r"^10\.5194/[a-z0-9]+-\d{4}-\d+$", doi):
        return "discussion_preprint"
    if "/articles/" in low:
        return "final_article"
    return "other_posted_content"
