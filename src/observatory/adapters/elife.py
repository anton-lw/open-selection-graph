"""eLife Reviewed Preprint process connector.

The public API provides the denominator and assessment terms; each public page
contains an immutable Next.js data object with sent-for-review dates, versions,
reviews, author responses, and assessment text.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterable, Iterator, Mapping

from bs4 import BeautifulSoup

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
from ..connectors.http import NetworkPolicyError, PoliteSession, RatePolicy
from ..ids import canonical_doi, content_hash, stable_id
from ..licensing import canonical_licence, decide_release
from .common import iso_datetime, json_text

SIGNIFICANCE_TERMS = ("landmark", "fundamental", "important", "valuable", "useful")
STRENGTH_TERMS = ("exceptional", "compelling", "convincing", "solid", "incomplete", "inadequate")
NEW_MODEL_EFFECTIVE = "2023-01-31T00:00:00+00:00"


def _structured_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return BeautifulSoup(value, "lxml").get_text("\n", strip=True)
    if isinstance(value, Mapping):
        if isinstance(value.get("text"), str):
            return str(value["text"])
        return " ".join(_structured_text(part) for part in value.values()).strip()
    if isinstance(value, list):
        return " ".join(_structured_text(part) for part in value).strip()
    return str(value)


def _date(value: Any) -> str | None:
    parsed = iso_datetime(value)
    if parsed:
        return parsed
    try:
        return datetime.strptime(str(value), "%a %b %d %Y").replace(
            tzinfo=timezone.utc
        ).isoformat()
    except (TypeError, ValueError):
        return None


def _page_props(payload: bytes | str) -> dict[str, Any]:
    soup = BeautifulSoup(payload, "lxml")
    node = soup.select_one("#__NEXT_DATA__")
    if node is None or not node.string:
        raise ValueError("eLife page lacks __NEXT_DATA__")
    return json.loads(node.string)["props"]["pageProps"]


def _process_props(payload: bytes | str) -> dict[str, Any]:
    data = payload.decode() if isinstance(payload, bytes) else payload
    if data.lstrip().startswith("{"):
        row = json.loads(data)
        assessment = row.get("elifeAssessment") or {}
        return {
            "metaData": {
                "msid": row.get("id"), "title": row.get("title"),
                "article": {"title": row.get("title")}, "published": row.get("published"),
                "doi": row.get("doi"), "version": row.get("version"),
            },
            "timeline": [],
            "peerReview": {
                "evaluationSummary": {
                    "doi": assessment.get("doi"), "date": row.get("reviewedDate"),
                    "reviewType": "evaluation-summary",
                    "text": _structured_text(assessment.get("content")),
                    "participants": [],
                } if assessment else None,
                "reviews": [], "authorResponse": None,
            },
        }
    return _page_props(payload)


class ELifeProcessConnector(Connector):
    source_id = "elife_process"
    connector_version = "5"
    # Each process page expands to many evaluations and field-provenance rows,
    # so source-object count alone understates compilation memory.
    force_streaming = True
    api_endpoint = "https://api.elifesciences.org/reviewed-preprints"
    page_template = "https://elifesciences.org/reviewed-preprints/{identifier}v{version}"

    def __init__(self, *, page_size: int = 50):
        self.page_size = min(max(page_size, 1), 100)
        self._total: int | None = None
        self._cycles: dict[str, int] = {}
        self._api_detail_fallback_count = 0
        self._emitted: dict[str, set[str]] = {
            "gate": set(), "gate_cycle": set(), "candidate": set(), "candidate_version": set(),
        }

    def _session(self, context: ConnectorContext) -> PoliteSession:
        return PoliteSession(
            cache_dir=context.cache_dir / self.source_id,
            allowed_hosts={"api.elifesciences.org", "elifesciences.org"},
            policy=RatePolicy(min_interval_seconds=0.25, max_retries=5),
        )

    def discover(self, context: ConnectorContext) -> Iterable[Mapping[str, Any]]:
        yield {
            "collection": "reviewed-preprints", "entry_stage": "sent_for_review",
            "new_model_effective": NEW_MODEL_EFFECTIVE,
            "assessment_policy": "https://elifesciences.org/about/elife-assessments",
        }

    def count(self, context: ConnectorContext) -> SourceEstimate:
        body = self._session(context).get(
            self.api_endpoint, params={"page": 1, "per-page": 1}
        ).json()
        self._total = int(body["total"])
        return SourceEstimate(
            self.source_id, self._total,
            expected_requests=(
                1 + (self._total + self.page_size - 1) // self.page_size + 2 * self._total
            ),
            method=(
                "eLife reviewed-preprints API total plus public process page per item; "
                "conservative allowance for an official detail-API fallback on every item"
            ),
            confidence="provider collection count",
            requests_per_limit_unit=2.0 + 1.0 / self.page_size,
        )

    def fetch(
        self, context: ConnectorContext, *, cursor: str | None = None, limit: int | None = None
    ) -> Iterator[FetchBatch]:
        session = self._session(context)
        page = int(cursor or 1)
        emitted = 0
        while True:
            size = min(self.page_size, limit - emitted) if limit is not None else self.page_size
            if size <= 0:
                return
            body = session.get(
                self.api_endpoint, params={"page": page, "per-page": size}
            ).json()
            self._total = int(body["total"])
            items = []
            for summary in body.get("items") or []:
                identifier = str(summary["id"])
                version = int(summary.get("version") or 1)
                url = self.page_template.format(identifier=identifier, version=version)
                page_access = "public_process_page"
                page_error = None
                try:
                    response = session.get(
                        url, use_cache=False, accepted_statuses={403, 404}
                    )
                    if response.status_code != 200:
                        raise ValueError(f"process page HTTP {response.status_code}")
                    process_props = _page_props(response.content)
                except (NetworkPolicyError, ValueError) as exc:
                    detail_url = f"{self.api_endpoint}/{identifier}"
                    response = session.get(detail_url, use_cache=False)
                    process_props = _process_props(response.content)
                    page_access = "official_detail_api_fallback"
                    page_error = str(exc)[:300]
                    self._api_detail_fallback_count += 1
                page_metadata = process_props.get("metaData") or {}
                declared_licence = canonical_licence(page_metadata.get("license"))
                assessment = summary.get("elifeAssessment") or {}
                items.append(RawItem(
                    native_id=f"{identifier}v{version}",
                    object_type=(
                        "reviewed_preprint_process_page"
                        if page_access == "public_process_page"
                        else "reviewed_preprint_api_detail"
                    ),
                    payload=response.content, source_url=response.url,
                    created_at=_date(summary.get("reviewedDate") or summary.get("published")),
                    modified_at=_date(summary.get("versionDate") or summary.get("statusDate")),
                    # The article/review licence is captured separately below;
                    # the complete web shell is retained as controlled raw
                    # evidence, not asserted to be redistributable wholesale.
                    licence=declared_licence, release_class="pointer_hash",
                    metadata={
                        "id": identifier, "version": version, "doi": summary.get("doi"),
                        "status": summary.get("status"),
                        "significance": assessment.get("significance") or [],
                        "strength": assessment.get("strength") or [],
                        "declared_content_licence": declared_licence,
                        "page_access": page_access,
                        "page_error": page_error,
                        "intended_process_url": url,
                    },
                ))
                emitted += 1
                if limit is not None and emitted >= limit:
                    break
            next_page = page + 1
            done = not items or page * size >= self._total or (
                limit is not None and emitted >= limit
            )
            yield FetchBatch(
                tuple(items), None if done else str(next_page), done, f"elife-reviewed-preprints:{page}",
                self._total,
            )
            if done:
                return
            page = next_page

    def normalize(
        self, item: RawItem, *, source_object_id: str, provenance_event_id: str
    ) -> Iterable[NormalizedRecord]:
        props = _process_props(item.payload)
        metadata = props.get("metaData") or {}
        peer = props.get("peerReview") or {}
        msid = str(metadata.get("msid") or item.metadata.get("id"))
        sent_for_review = _date(metadata.get("sentForReview"))
        published = _date(metadata.get("published")) or item.created_at
        cohort = (
            "reviewed_preprint_model" if sent_for_review and sent_for_review >= NEW_MODEL_EFFECTIVE
            else "reviewed_preprint_pilot_or_legacy_transition"
        )
        year = int((sent_for_review or published or "1970")[:4])
        gate_id = stable_id("gate", self.source_id, "elife")
        cycle_native = f"{cohort}|{year}"
        cycle_id = stable_id("gate_cycle", self.source_id, cycle_native)
        policy_id = stable_id("policy_version", self.source_id, cohort)
        candidate_id = stable_id("candidate", self.source_id, msid)
        common = {
            "source_id": self.source_id, "source_object_id": source_object_id,
            "provenance_event_id": provenance_event_id,
            "observed_at": item.modified_at or published, "record_version": 1,
        }
        if gate_id not in self._emitted["gate"]:
            self._emitted["gate"].add(gate_id)
            yield NormalizedRecord("gate", {
                "gate_id": gate_id, "native_id": "elife", "name": "eLife",
                "organization": "eLife Sciences Publications", "domain": "life sciences",
                "country": None, "architecture": "publish_review_curate",
                "active_from": None, "active_to": None, **common,
            })
        if policy_id not in self._emitted.setdefault("policy_version", set()):
            self._emitted["policy_version"].add(policy_id)
            yield NormalizedRecord("policy_version", {
                "policy_version_id": policy_id, "gate_id": gate_id, "native_id": cohort,
                "effective_at": NEW_MODEL_EFFECTIVE if cohort == "reviewed_preprint_model" else None,
                "valid_to": None,
                "criteria_json": json_text({
                    "significance": SIGNIFICANCE_TERMS, "strength_of_evidence": STRENGTH_TERMS,
                }),
                "rubric_json": json_text({
                    "significance_high_to_low": SIGNIFICANCE_TERMS,
                    "strength_high_to_low": STRENGTH_TERMS,
                    "numeric_direction": "higher_is_stronger",
                }),
                "stage_rules_json": json_text({
                    "entry": "sent_for_review", "post_review": "every reviewed item is public",
                    "version_of_record": "author choice, not an accept/reject decision",
                }),
                "quota_or_cap": None, "anonymity_model": "public reviewer identity where declared",
                "revision_rules": json_text({"reviewed_preprint_versions": True}),
                "policy_url": "https://elifesciences.org/about/elife-assessments",
                "content_hash": content_hash(json.dumps({
                    "significance": SIGNIFICANCE_TERMS, "strength": STRENGTH_TERMS,
                })),
                "date_confidence": 1.0 if cohort == "reviewed_preprint_model" else 0.5,
                **common,
            })
        if cycle_id not in self._emitted["gate_cycle"]:
            self._emitted["gate_cycle"].add(cycle_id)
            yield NormalizedRecord("gate_cycle", {
                "gate_cycle_id": cycle_id, "gate_id": gate_id, "native_id": cycle_native,
                "name": f"eLife {cohort} {year}", "track": None,
                "cycle_start": f"{year}-01-01T00:00:00+00:00",
                "cycle_end": f"{year}-12-31T23:59:59+00:00",
                "policy_version_id": policy_id, "architecture": "publish_review_curate",
                "received_count": None, "observable_count": None, "evaluated_count": None,
                "selected_count": None, "status": "reviewed-preprint collection", **common,
            })
        self._cycles[cycle_id] = self._cycles.get(cycle_id, 0) + 1
        article = metadata.get("article") or {}
        declared_licence = canonical_licence(metadata.get("license") or item.licence)
        if candidate_id not in self._emitted["candidate"]:
            self._emitted["candidate"].add(candidate_id)
            yield NormalizedRecord("candidate", {
                "candidate_id": candidate_id, "first_observed_at": sent_for_review,
                "domain": "life sciences", "candidate_type": "manuscript",
                "canonical_title": article.get("title") or metadata.get("title"),
                "status": "reviewed_preprint", **common,
            })
        timeline = sorted(
            props.get("timeline") or [], key=lambda row: int(row.get("version") or 0)
        )
        previous = None
        current_version_id = stable_id("candidate_version", self.source_id, f"{msid}|current")
        if not timeline:
            version_number = int(item.metadata.get("version") or metadata.get("version") or 1)
            native = f"{msid}v{version_number}"
            current_version_id = stable_id("candidate_version", self.source_id, native)
            self._emitted["candidate_version"].add(current_version_id)
            yield NormalizedRecord("candidate_version", {
                "candidate_version_id": current_version_id, "candidate_id": candidate_id,
                "native_id": native, "version_label": "reviewed_preprint_api_record",
                "version_number": version_number, "created_at": published, "modified_at": None,
                "title": article.get("title") or metadata.get("title"), "abstract": None,
                "content_artifact_id": None, "content_hash": None,
                "licence": declared_licence, "language": "en", "authorship_visible": None,
                "withdrawn": False, **common,
            })
        for index, version in enumerate(timeline, 1):
            native = str(version.get("url") or f"{msid}|v{version.get('version') or index}")
            version_id = stable_id("candidate_version", self.source_id, native)
            current_version_id = version_id
            if version_id not in self._emitted["candidate_version"]:
                self._emitted["candidate_version"].add(version_id)
                yield NormalizedRecord("candidate_version", {
                    "candidate_version_id": version_id, "candidate_id": candidate_id,
                    "native_id": native,
                    "version_label": str(
                        version.get("name") or version.get("title") or "public_version"
                    ),
                    "version_number": int(version.get("version") or index),
                    "created_at": _date(version.get("date")), "modified_at": None,
                    "title": article.get("title"), "abstract": None,
                    "content_artifact_id": None, "content_hash": None,
                    "licence": declared_licence, "language": "en",
                    "authorship_visible": True, "withdrawn": False, **common,
                })
            if previous:
                yield NormalizedRecord("lineage_edge", {
                    "lineage_edge_id": stable_id(
                        "lineage_edge", self.source_id, f"{previous}|{version_id}"
                    ),
                    "source_candidate_id": candidate_id, "source_version_id": previous,
                    "target_candidate_id": candidate_id, "target_version_id": version_id,
                    "relation_type": "source_declared_version", "declared": True,
                    "confidence": 1.0, "linkage_tier": "source_declared",
                    "method_version": "elife-timeline/1",
                    "evidence_json": json_text(version), **common,
                })
            previous = version_id
        yield NormalizedRecord("candidate_gate_event", {
            "candidate_gate_event_id": stable_id(
                "candidate_gate_event", self.source_id, f"{cycle_id}|{msid}"
            ),
            "candidate_id": candidate_id, "candidate_version_id": current_version_id,
            "gate_cycle_id": cycle_id, "native_id": msid, "submitted_at": sent_for_review,
            "earliest_observed_stage": "sent_for_review",
            "final_observed_stage": "reviewed_preprint_public",
            "coverage_observation_id": coverage_observation_id(
                self.source_id, cycle_id, "reviewed_preprint"
            ), **common,
        })
        review_objects = []
        if peer.get("evaluationSummary"):
            review_objects.append(peer["evaluationSummary"])
        review_objects.extend(peer.get("reviews") or [])
        if peer.get("authorResponse"):
            review_objects.append(peer["authorResponse"])
        assessment_artifact_id = None
        for index, review in enumerate(review_objects):
            native = str(review.get("doi") or f"{msid}|review|{index}")
            text = _structured_text(review.get("text"))
            artifact_id = stable_id("content_artifact", self.source_id, native)
            release = decide_release(
                object_type="review_material",
                licence=declared_licence,
                source_allows_redistribution=None,
            )
            if review.get("reviewType") == "evaluation-summary":
                assessment_artifact_id = artifact_id
            yield NormalizedRecord("content_artifact", {
                "content_artifact_id": artifact_id,
                "object_type": str(review.get("reviewType") or "review_material"),
                "media_type": "text/html", "byte_hash": content_hash(str(review.get("text") or "")),
                "normalized_text_hash": content_hash(" ".join(text.split())),
                "source_url": item.source_url, "local_pointer": f"__NEXT_DATA__.peerReview[{index}]",
                "licence": release.licence,
                "release_class": release.release_class.value,
                "size_bytes": len(text.encode()),
                "language": "en", "parser_version": self.connector_version, **common,
            })
            participants = review.get("participants") or []
            roles = [str(row.get("role")) for row in participants if row.get("role")]
            review_type = str(review.get("reviewType") or "review_material")
            yield NormalizedRecord("evaluation", {
                "evaluation_id": stable_id("evaluation", self.source_id, native),
                "candidate_version_id": current_version_id, "gate_cycle_id": cycle_id,
                "native_id": native, "evaluation_type": review_type,
                "evaluator_role": "author" if review_type == "author-response" else (
                    roles[0] if roles else "reviewer"
                ),
                "evaluator_public_id": None, "evaluator_protected_id": None,
                "anonymous": any(
                    str(row.get("name") or "").lower() == "anonymous" for row in participants
                ),
                "official": review_type != "author-response", "criterion_native": None,
                "criterion_normalized": None, "criterion_value": None,
                "criterion_value_numeric": None, "scale_json": None,
                "confidence_value": None, "text_artifact_id": artifact_id,
                "created_at": _date(review.get("date")), "forum_native_id": msid,
                "invitation_native": review_type,
                "readers_json": json_text(["public"]),
                "signatures_json": json_text(participants), "reply_to_native_id": msid,
                **common,
            })
        for criterion, allowed in (
            ("significance", SIGNIFICANCE_TERMS), ("strength", STRENGTH_TERMS)
        ):
            for value in item.metadata.get(criterion) or []:
                yield NormalizedRecord("evaluation", {
                    "evaluation_id": stable_id(
                        "evaluation", self.source_id, f"{msid}|assessment|{criterion}|{value}"
                    ),
                    "candidate_version_id": current_version_id, "gate_cycle_id": cycle_id,
                    "native_id": f"{msid}:{criterion}:{value}",
                    "evaluation_type": "elife_assessment", "evaluator_role": "editor",
                    "evaluator_public_id": None, "evaluator_protected_id": None,
                    "anonymous": False, "official": True, "criterion_native": criterion,
                    "criterion_normalized": f"elife_{criterion}", "criterion_value": str(value),
                    "criterion_value_numeric": (
                        float(len(allowed) - allowed.index(value)) if value in allowed else None
                    ),
                    "scale_json": json_text({
                        "ordered_terms_high_to_low": allowed,
                        "numeric_direction": "higher_is_stronger",
                    }),
                    "confidence_value": None, "text_artifact_id": assessment_artifact_id,
                    "created_at": published, "forum_native_id": msid,
                    "invitation_native": "elife_assessment", "readers_json": json_text(["public"]),
                    "signatures_json": None, "reply_to_native_id": msid, **common,
                })
        for scheme, value in (
            ("doi", canonical_doi(metadata.get("umbrellaDoi") or metadata.get("doi"))),
            ("doi", canonical_doi(metadata.get("preprintDoi"))),
            ("elife_msid", msid),
        ):
            if value:
                yield NormalizedRecord("identifier_alias", {
                    "identifier_alias_id": stable_id(
                        "identifier_alias", self.source_id, f"candidate|{scheme}|{value}"
                    ),
                    "entity_kind": "candidate", "entity_id": candidate_id, "scheme": scheme,
                    "value": str(value), "canonical_value": str(value).lower(),
                    "relation": "native" if scheme == "elife_msid" else "source_declared",
                    "confidence": 1.0, "conflict_status": "none", **common,
                })

    def validate_fixture(self, context: ConnectorContext) -> Mapping[str, Any]:
        fixture = context.fixture_dir / self.source_id / "process_page.html"
        if not fixture.exists():
            return {"passes": False, "reason": "fixture absent"}
        props = _page_props(fixture.read_bytes())
        return {
            "passes": bool((props.get("metaData") or {}).get("msid") and props.get("peerReview")),
            "contains_sent_for_review": bool((props.get("metaData") or {}).get("sentForReview")),
        }

    def emit_coverage(
        self, context: ConnectorContext, *, found_count: int
    ) -> Iterable[CoverageEvidence]:
        for cycle_id, count in sorted(self._cycles.items()):
            yield CoverageEvidence(
                gate_cycle_id=cycle_id, object_type="reviewed_preprint",
                earliest_public_stage="sent for review", observability_grade="B",
                expected_count=None, found_count=count,
                expected_count_method=(
                    "provider collection total is exact globally; cohort denominator is reconstructed "
                    "from sent-for-review dates after completed cursor"
                ),
                query_or_invitation=self.api_endpoint,
                known_hidden_stages=("editorial selection before sent for review",),
                known_exclusions=("manuscripts not sent for review",),
                audit_status=(
                    "verified_public_reviewed_collection; official_detail_api_fallbacks="
                    f"{self._api_detail_fallback_count}"
                ),
            )
