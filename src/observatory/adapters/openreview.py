"""OpenReview local-corpus adapter and lossless stage normalization.

The existing P2 pull is the authoritative network implementation for ICLR and
TMLR.  This adapter consumes those checkpointed raw-normalized files into the
OSG process graph without re-downloading 500 MB or treating their
presence as proof of denominator completeness.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path
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
from ..ids import content_hash, stable_id
from .common import epoch_ms, json_text, year_quarter


def _number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    found = re.search(r"[-+]?\d+(?:\.\d+)?", str(value))
    return float(found.group()) if found else None


def _kind(path: Path) -> str:
    return "review" if "_reviews" in path.stem else "submission"


def _venue_cycle(path: Path, row: Mapping[str, Any]) -> tuple[str, str, str, str]:
    name = path.stem.replace("_reviews", "")
    if name.startswith("ICLR_"):
        year = name.split("_", 1)[1]
        return "iclr", f"iclr-{year}", f"ICLR {year}", "competitive_quota"
    stamp = epoch_ms(row.get("cdate"))
    year, quarter = year_quarter(stamp)
    return "tmlr", f"tmlr-{year}-q{quarter}", f"TMLR {year} Q{quarter}", "rolling_threshold"


class OpenReviewLocalConnector(Connector):
    source_id = "openreview"
    connector_version = "2"

    def __init__(self, files: Iterable[Path] | None = None, *, batch_size: int = 500):
        self.files = tuple(files or ())
        self.batch_size = batch_size
        self._emitted: dict[str, set[str]] = {"gate": set(), "gate_cycle": set(), "candidate": set(), "candidate_version": set()}
        self._cycle_counts: Counter[str] = Counter()
        self._cycle_kind_counts: Counter[tuple[str, str]] = Counter()
        self._seen_native_hashes: dict[tuple[str, str], str] = {}

    def _files(self, context: ConnectorContext) -> tuple[Path, ...]:
        if self.files:
            return self.files
        configured = context.parameters.get("files")
        if configured:
            return tuple((context.workspace / value).resolve() for value in configured)
        root = context.workspace / "data" / "p2" / "openreview"
        return tuple(sorted(root.glob("ICLR_20*.jsonl"))) + tuple(
            p for p in (root / "TMLR.jsonl", root / "TMLR_Rejected.jsonl", root / "TMLR_Withdrawn_Submission.jsonl") if p.exists()
        )

    def discover(self, context: ConnectorContext) -> Iterable[Mapping[str, Any]]:
        for path in self._files(context):
            yield {"path": str(path), "object_type": _kind(path), "bytes": path.stat().st_size}

    def count(self, context: ConnectorContext) -> SourceEstimate:
        total = 0
        for path in self._files(context):
            with path.open("rb") as fh:
                total += sum(1 for line in fh if line.strip())
        return SourceEstimate(self.source_id, total, method="committed JSONL line count", confidence="exact")

    def fetch(
        self,
        context: ConnectorContext,
        *,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> Iterator[FetchBatch]:
        files = self._files(context)
        start_file, start_line = (map(int, cursor.split(":")) if cursor else (0, 0))
        emitted = 0
        batch: list[RawItem] = []
        for file_index, path in enumerate(files):
            if file_index < start_file:
                continue
            with path.open("r", encoding="utf-8") as fh:
                for line_index, line in enumerate(fh):
                    if file_index == start_file and line_index < start_line:
                        continue
                    if not line.strip():
                        continue
                    row = json.loads(line)
                    if context.no_text:
                        row.pop("abstract", None)
                        row.pop("review_text", None)
                    item = RawItem(
                        native_id=f"{path.name}:{row.get('id') or line_index}",
                        object_type=_kind(path),
                        payload=json.dumps(row, sort_keys=True),
                        source_url=(
                            f"https://openreview.net/forum?id={row.get('forum') or row.get('id')}"
                            if row.get("forum") or row.get("id") else None
                        ),
                        created_at=epoch_ms(row.get("cdate")),
                        modified_at=epoch_ms(row.get("mdate") or row.get("odate")),
                        licence="per-object" if _kind(path) == "submission" else "CC-BY-4.0",
                        release_class="pointer_hash" if _kind(path) == "submission" else "redistribute",
                        metadata={
                            "path": str(path), "filename": path.name, "line": line_index,
                            "no_text": context.no_text,
                        },
                    )
                    batch.append(item)
                    emitted += 1
                    if len(batch) >= self.batch_size:
                        next_cursor = f"{file_index}:{line_index + 1}"
                        yield FetchBatch(tuple(batch), next_cursor, False, f"local:{next_cursor}")
                        batch = []
                    if limit is not None and emitted >= limit:
                        if batch:
                            next_cursor = f"{file_index}:{line_index + 1}"
                            yield FetchBatch(tuple(batch), next_cursor, True, f"local:{next_cursor}")
                        return
        if batch:
            yield FetchBatch(tuple(batch), None, True, "local:complete")
        elif not files or emitted == 0:
            yield FetchBatch((), None, True, "local:empty")

    def normalize(self, item: RawItem, *, source_object_id: str, provenance_event_id: str) -> Iterable[NormalizedRecord]:
        row = json.loads(item.payload)
        path = Path(str(item.metadata["path"]))
        native_key = (
            item.object_type,
            str(row.get("id") or row.get("forum") or item.native_id),
        )
        payload_hash = content_hash(item.payload)
        prior_hash = self._seen_native_hashes.get(native_key)
        if prior_hash is not None:
            if prior_hash != payload_hash:
                raise ValueError(f"conflicting duplicate OpenReview native object: {native_key}")
            return
        self._seen_native_hashes[native_key] = payload_hash
        gate_native, cycle_native, cycle_name, architecture = _venue_cycle(path, row)
        gate_id = stable_id("gate", self.source_id, gate_native)
        cycle_id = stable_id("gate_cycle", self.source_id, cycle_native)
        self._cycle_counts[cycle_id] += 1
        self._cycle_kind_counts[(cycle_id, item.object_type)] += 1
        common = {
            "source_id": self.source_id,
            "source_object_id": source_object_id,
            "provenance_event_id": provenance_event_id,
            "observed_at": item.modified_at or item.created_at,
            "record_version": 1,
        }
        if gate_id not in self._emitted["gate"]:
            self._emitted["gate"].add(gate_id)
            yield NormalizedRecord("gate", {
                "gate_id": gate_id, "native_id": gate_native, "name": gate_native.upper(),
                "organization": "OpenReview venue", "domain": "machine learning", "country": None,
                "architecture": architecture, "active_from": None, "active_to": None, **common,
            })
        if cycle_id not in self._emitted["gate_cycle"]:
            self._emitted["gate_cycle"].add(cycle_id)
            yield NormalizedRecord("gate_cycle", {
                "gate_cycle_id": cycle_id, "gate_id": gate_id, "native_id": cycle_native,
                "name": cycle_name, "track": None, "cycle_start": None, "cycle_end": None,
                "policy_version_id": stable_id("policy_version", self.source_id, cycle_native),
                "architecture": architecture, "received_count": None, "observable_count": None,
                "evaluated_count": None, "selected_count": None, "status": "observed", **common,
            })
        forum = str(row.get("forum") or row.get("id") or "")
        if not forum:
            return
        candidate_id = stable_id("candidate", self.source_id, forum)
        version_id = stable_id("candidate_version", self.source_id, forum)
        if item.object_type == "review":
            evaluation_id = stable_id("evaluation", self.source_id, row.get("id") or item.native_id)
            text = row.get("review_text")
            text_artifact_id = (
                stable_id("content_artifact", self.source_id, f"review|{row.get('id')}")
                if text not in (None, "") else None
            )
            if text_artifact_id:
                normalized_text = " ".join(str(text).split())
                yield NormalizedRecord("content_artifact", {
                    "content_artifact_id": text_artifact_id, "object_type": "review_text",
                    "media_type": "text/plain", "byte_hash": content_hash(str(text)),
                    "normalized_text_hash": content_hash(normalized_text), "source_url": item.source_url,
                    "local_pointer": f"{item.metadata.get('path')}#line={item.metadata.get('line')}",
                    "licence": item.licence, "release_class": item.release_class,
                    "size_bytes": len(str(text).encode()), "language": None,
                    "parser_version": self.connector_version, **common,
                })
            yield NormalizedRecord("evaluation", {
                "evaluation_id": evaluation_id, "candidate_version_id": version_id,
                "gate_cycle_id": cycle_id, "native_id": str(row.get("id") or item.native_id),
                "evaluation_type": "official_review", "evaluator_role": "reviewer",
                "evaluator_public_id": None, "evaluator_protected_id": None, "anonymous": True,
                "official": True, "criterion_native": "overall_rating", "criterion_normalized": "overall_recommendation",
                "criterion_value": row.get("rating_raw"),
                "criterion_value_numeric": _number(row.get("rating_raw")),
                "scale_json": json_text({"rating_raw": row.get("rating_raw")}),
                "confidence_value": _number(row.get("confidence_raw")),
                "text_artifact_id": text_artifact_id,
                "created_at": epoch_ms(row.get("cdate")), "forum_native_id": forum,
                "invitation_native": None, "readers_json": None, "signatures_json": None,
                "reply_to_native_id": forum, **common,
            })
            for criterion, normalized in (
                ("confidence_raw", "reviewer_confidence"),
                ("technical_novelty_raw", "technical_novelty"),
                ("empirical_novelty_raw", "empirical_novelty"),
            ):
                if row.get(criterion) not in (None, ""):
                    yield NormalizedRecord("evaluation", {
                        "evaluation_id": stable_id("evaluation", self.source_id, f"{row.get('id')}|{criterion}"),
                        "candidate_version_id": version_id, "gate_cycle_id": cycle_id,
                        "native_id": f"{row.get('id')}:{criterion}", "evaluation_type": "official_review",
                        "evaluator_role": "reviewer", "evaluator_public_id": None,
                        "evaluator_protected_id": None, "anonymous": True, "official": True,
                        "criterion_native": criterion, "criterion_normalized": normalized,
                        "criterion_value": str(row.get(criterion)),
                        "criterion_value_numeric": _number(row.get(criterion)),
                        "scale_json": None, "confidence_value": None,
                        "text_artifact_id": text_artifact_id,
                        "created_at": epoch_ms(row.get("cdate")), "forum_native_id": forum,
                        "invitation_native": None, "readers_json": None, "signatures_json": None,
                        "reply_to_native_id": forum, **common,
                    })
            return

        if candidate_id not in self._emitted["candidate"]:
            self._emitted["candidate"].add(candidate_id)
            yield NormalizedRecord("candidate", {
                "candidate_id": candidate_id, "first_observed_at": epoch_ms(row.get("cdate")),
                "domain": "machine learning", "candidate_type": "manuscript",
                "canonical_title": row.get("title"), "status": row.get("outcome"), **common,
            })
        if version_id not in self._emitted["candidate_version"]:
            self._emitted["candidate_version"].add(version_id)
            yield NormalizedRecord("candidate_version", {
                "candidate_version_id": version_id, "candidate_id": candidate_id, "native_id": forum,
                "version_label": "submitted", "version_number": 1,
                "created_at": epoch_ms(row.get("cdate")), "modified_at": epoch_ms(row.get("odate")),
                "title": row.get("title"), "abstract": None if item.metadata.get("no_text") else row.get("abstract"),
                "content_artifact_id": None, "content_hash": None, "licence": "per-object",
                "language": None, "authorship_visible": None,
                "withdrawn": row.get("outcome") == "withdrawn", **common,
            })
        outcome = str(row.get("outcome") or "unknown")
        stage = {
            "accepted": "selected", "rejected": "rejected", "withdrawn": "withdrawn",
            "desk_rejected": "desk_rejected",
        }.get(outcome, "outcome_unknown")
        coverage_id = coverage_observation_id(self.source_id, cycle_id, "submission")
        yield NormalizedRecord("candidate_gate_event", {
            "candidate_gate_event_id": stable_id("candidate_gate_event", self.source_id, f"{cycle_id}|{forum}"),
            "candidate_id": candidate_id, "candidate_version_id": version_id, "gate_cycle_id": cycle_id,
            "native_id": forum, "submitted_at": epoch_ms(row.get("cdate")),
            "earliest_observed_stage": "submission_record", "final_observed_stage": stage,
            "coverage_observation_id": coverage_id, **common,
        })
        yield NormalizedRecord("decision_event", {
            "decision_event_id": stable_id("decision_event", self.source_id, f"{cycle_id}|{forum}|{outcome}"),
            "candidate_version_id": version_id, "gate_cycle_id": cycle_id,
            "native_id": f"{forum}:{outcome}", "stage_native": str(row.get("venueid") or "decision"),
            "stage_normalized": "final_observed_decision", "outcome_native": str(row.get("venue") or outcome),
            "outcome_normalized": outcome, "tier_or_band": row.get("tier"), "reason": None,
            "deciding_body": gate_native.upper(), "decided_at": epoch_ms(row.get("pdate") or row.get("odate")),
            "policy_version_id": stable_id("policy_version", self.source_id, cycle_native), **common,
        })
        yield NormalizedRecord("identifier_alias", {
            "identifier_alias_id": stable_id("identifier_alias", self.source_id, f"candidate|openreview|{forum}"),
            "entity_kind": "candidate", "entity_id": candidate_id, "scheme": "openreview_forum",
            "value": forum, "canonical_value": forum, "relation": "native", "confidence": 1.0,
            "conflict_status": "none", **common,
        })
        prior = row.get("prior_submission")
        if prior:
            prior_forum = str(prior).split("id=")[-1]
            yield NormalizedRecord("lineage_edge", {
                "lineage_edge_id": stable_id("lineage_edge", self.source_id, f"{prior_forum}|{forum}"),
                "source_candidate_id": stable_id("candidate", self.source_id, prior_forum),
                "source_version_id": stable_id("candidate_version", self.source_id, prior_forum),
                "target_candidate_id": candidate_id, "target_version_id": version_id,
                "relation_type": "source_declared_resubmission", "declared": True,
                "confidence": 1.0, "linkage_tier": "source_declared", "method_version": "openreview-prior/1",
                "evidence_json": json_text({"prior_submission": prior}), **common,
            })

    def validate_fixture(self, context: ConnectorContext) -> Mapping[str, Any]:
        files = self._files(context)
        readable = [path for path in files if path.exists() and path.stat().st_size > 0]
        return {"passes": len(readable) == len(files) and bool(files), "n_files": len(files), "n_readable": len(readable)}

    def emit_coverage(self, context: ConnectorContext, *, found_count: int) -> Iterable[CoverageEvidence]:
        expected = context.parameters.get("provider_totals") or {}
        grades = context.parameters.get("grades") or {}
        for cycle_id in sorted(self._cycle_counts):
            for object_type in ("submission", "review"):
                count = self._cycle_kind_counts[(cycle_id, object_type)]
                if not count:
                    continue
                denominator_key = f"{cycle_id}|{object_type}"
                total = expected.get(denominator_key, expected.get(cycle_id) if object_type == "submission" else None)
                grade = grades.get(denominator_key, grades.get(cycle_id, "U"))
                yield CoverageEvidence(
                    gate_cycle_id=cycle_id,
                    object_type=object_type,
                    earliest_public_stage="venue-year-specific invitation audit required",
                    observability_grade=grade,
                    expected_count=total,
                    found_count=count,
                    expected_count_method="provider total supplied in run config" if total is not None else "unresolved",
                    query_or_invitation="committed P2 JSONL; source invitation recorded upstream",
                    known_hidden_stages=() if grade == "A" else ("desk/access stages may be hidden",),
                    audit_status="verified" if total and count / total >= 0.95 else "unverified",
                )
