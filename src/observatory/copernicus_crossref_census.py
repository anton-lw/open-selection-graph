"""Acceptance audit for the Crossref Copernicus posted-content population."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .ids import content_hash


def _rows(lake_root: Path, table: str, query_hash: str) -> list[dict[str, Any]]:
    prefix = f"run-{query_hash[:16]}"
    files = sorted(
        (lake_root / table / "source_id=copernicus_crossref").glob(
            f"{prefix}*.parquet"
        )
    )
    if not files:
        return []
    return pa.concat_tables([pq.ParquetFile(path).read() for path in files]).to_pylist()


def build_copernicus_crossref_report(
    lake_root: Path,
    *,
    query_hash: str,
    provider_total_results: int,
    expected_page_bundles: int,
    found_page_bundles: int,
) -> dict[str, Any]:
    artifacts = _rows(lake_root, "content_artifact", query_hash)
    candidates = _rows(lake_root, "candidate", query_hash)
    events = _rows(lake_root, "candidate_gate_event", query_hash)
    evaluations = _rows(lake_root, "evaluation", query_hash)
    cycles = _rows(lake_root, "gate_cycle", query_hash)
    kinds = Counter(
        str(row["object_type"]).removeprefix("copernicus_crossref_").removesuffix(
            "_metadata"
        )
        for row in artifacts
    )
    candidate_ids = {row["candidate_id"] for row in candidates}
    event_ids = {row["candidate_id"] for row in events}
    missing_cycle = [
        row["candidate_id"] for row in events if not row.get("gate_cycle_id")
    ]
    missing_outcome = [
        row["candidate_id"] for row in events if not row.get("final_observed_stage")
    ]
    report: dict[str, Any] = {
        "schema": "observatory.copernicus-crossref-posted-census/1",
        "source_id": "copernicus_crossref",
        "query_hash": query_hash,
        "endpoint": "https://api.crossref.org/prefixes/10.5194/works",
        "filter": "type:posted-content",
        "cursor_initial": "*",
        "provider_total_results": provider_total_results,
        "expected_page_bundles": expected_page_bundles,
        "found_page_bundles": found_page_bundles,
        "cursor_complete": found_page_bundles == expected_page_bundles,
        "normalized_posted_content_count": len(artifacts),
        "object_kind_counts": dict(sorted(kinds.items())),
        "discussion_candidate_count": len(candidate_ids),
        "discussion_event_count": len(events),
        "discussion_candidates_without_event": sorted(candidate_ids - event_ids),
        "discussion_events_without_cycle": missing_cycle,
        "discussion_events_without_outcome_or_censoring": missing_outcome,
        "conference_abstract_count": kinds.get("conference_abstract", 0),
        "conference_abstracts_excluded_from_candidate_pool": all(
            row.get("candidate_type") != "conference_abstract" for row in candidates
        ),
        "public_review_relation_count": len(evaluations),
        "cycle_count": len(cycles),
        "hidden_stage": "access review before public discussion",
        "absence_of_final_relation_means_rejection": False,
        "scope_warning": (
            "Crossref subtype deposits are the population denominator; provider OAI/pages "
            "supply independent metadata and outcome evidence. Deposit presence alone never "
            "implies a final decision."
        ),
    }
    report["passes"] = bool(
        report["cursor_complete"]
        and len(artifacts) == provider_total_results
        and candidate_ids
        and len(events) == len(candidate_ids)
        and not report["discussion_candidates_without_event"]
        and not missing_cycle
        and not missing_outcome
        and report["conference_abstracts_excluded_from_candidate_pool"]
        and report["hidden_stage"]
        and not report["absence_of_final_relation_means_rejection"]
    )
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    return report


def write_copernicus_crossref_report(
    lake_root: Path,
    output: Path,
    **kwargs: Any,
) -> Path:
    report = build_copernicus_crossref_report(lake_root, **kwargs)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return output
