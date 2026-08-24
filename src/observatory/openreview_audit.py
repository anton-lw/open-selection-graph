"""Streaming audit of the existing ICLR/TMLR OpenReview corpus."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .ids import content_hash


def _rows(path: Path):
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if line.strip():
                try:
                    yield line_number, json.loads(line)
                except json.JSONDecodeError as exc:
                    yield line_number, {"__parse_error__": str(exc)}


def _checkpoint(path: Path) -> dict[str, Any] | None:
    checkpoint = path.with_suffix(".ckpt.json")
    return json.loads(checkpoint.read_text()) if checkpoint.exists() else None


def audit_local_openreview(root: Path) -> dict[str, Any]:
    submission_paths = sorted(
        path for path in root.glob("*.jsonl")
        if path.name.startswith(("ICLR_", "TMLR")) and "_reviews" not in path.stem
    )
    review_paths = sorted(root.glob("ICLR_*_reviews.jsonl"))
    candidate_forums_by_year: dict[str, set[str]] = defaultdict(set)
    all_candidate_forums: set[str] = set()
    all_candidate_ids: Counter[str] = Counter()
    submission_reports = []
    for path in submission_paths:
        count = invalid = 0
        outcomes: Counter[str] = Counter()
        missing: Counter[str] = Counter()
        file_ids: Counter[str] = Counter()
        file_forums: Counter[str] = Counter()
        prior_links = 0
        for _, row in _rows(path):
            count += 1
            if "__parse_error__" in row:
                invalid += 1
                continue
            native_id, forum = str(row.get("id") or ""), str(row.get("forum") or "")
            file_ids[native_id] += 1
            file_forums[forum] += 1
            all_candidate_ids[native_id] += 1
            if forum:
                all_candidate_forums.add(forum)
                if path.name.startswith("ICLR_"):
                    candidate_forums_by_year[path.stem.split("_")[1]].add(forum)
            outcomes[str(row.get("outcome") or "missing")] += 1
            prior_links += bool(row.get("prior_submission"))
            for field in ("id", "forum", "cdate", "outcome", "venueid", "title"):
                if row.get(field) in (None, ""):
                    missing[field] += 1
        checkpoint = _checkpoint(path)
        expected = (checkpoint or {}).get("offset")
        submission_reports.append({
            "file": str(path), "rows": count, "invalid_json": invalid,
            "distinct_ids": len(file_ids), "duplicate_id_rows": sum(n - 1 for n in file_ids.values() if n > 1),
            "distinct_forums": len(file_forums),
            "duplicate_forum_rows": sum(n - 1 for n in file_forums.values() if n > 1),
            "outcomes": dict(sorted(outcomes.items())), "missing_fields": dict(sorted(missing.items())),
            "source_declared_prior_links": prior_links, "checkpoint_expected_rows": expected,
            "checkpoint_reconciliation_ratio": count / expected if expected else None,
            "checkpoint_status": "exact" if expected == count else ("missing" if expected is None else "disagrees"),
        })

    review_reports = []
    all_review_ids: Counter[str] = Counter()
    all_review_hashes: dict[str, set[str]] = defaultdict(set)
    for path in review_paths:
        year = path.stem.split("_")[1]
        candidate_forums = candidate_forums_by_year[year]
        count = invalid = 0
        ids: Counter[str] = Counter()
        id_hashes: dict[str, set[str]] = defaultdict(set)
        forums: Counter[str] = Counter()
        orphan_rows = 0
        missing: Counter[str] = Counter()
        for _, row in _rows(path):
            count += 1
            if "__parse_error__" in row:
                invalid += 1
                continue
            native_id, forum = str(row.get("id") or ""), str(row.get("forum") or "")
            ids[native_id] += 1
            row_hash = content_hash(json.dumps(row, sort_keys=True))
            id_hashes[native_id].add(row_hash)
            forums[forum] += 1
            all_review_ids[native_id] += 1
            all_review_hashes[native_id].add(row_hash)
            orphan_rows += bool(forum and forum not in candidate_forums)
            for field in ("id", "forum", "cdate", "rating_raw"):
                if row.get(field) in (None, ""):
                    missing[field] += 1
            # These fields were discarded by the legacy P2 transformation.
            for field in ("invitation", "readers", "signatures", "replyto", "original"):
                if row.get(field) in (None, "", []):
                    missing[field] += 1
        checkpoint = _checkpoint(path)
        completed_forums = set((checkpoint or {}).get("forums") or [])
        review_reports.append({
            "file": str(path), "rows": count, "invalid_json": invalid,
            "distinct_ids": len(ids), "duplicate_id_rows": sum(n - 1 for n in ids.values() if n > 1),
            "exact_duplicate_id_rows": sum(
                n - 1 for native_id, n in ids.items() if n > 1 and len(id_hashes[native_id]) == 1
            ),
            "conflicting_duplicate_ids": sum(
                len(id_hashes[native_id]) > 1 for native_id, n in ids.items() if n > 1
            ),
            "distinct_forums": len(forums), "orphan_rows": orphan_rows,
            "orphan_rate": orphan_rows / count if count else None,
            "missing_fields": dict(sorted(missing.items())),
            "checkpoint_completed_forums": len(completed_forums),
            "candidate_forums": len(candidate_forums),
            "checkpoint_forum_reconciliation_ratio": (
                len(completed_forums & candidate_forums) / len(candidate_forums)
                if candidate_forums else None
            ),
        })

    report: dict[str, Any] = {
        "schema": "observatory.openreview-local-audit/1",
        "submission_files": submission_reports,
        "review_files": review_reports,
        "totals": {
            "submission_rows": sum(row["rows"] for row in submission_reports),
            "review_rows": sum(row["rows"] for row in review_reports),
            "candidate_forums": len(all_candidate_forums),
            "cross_file_duplicate_candidate_ids": sum(n - 1 for n in all_candidate_ids.values() if n > 1),
            "duplicate_review_rows": sum(n - 1 for n in all_review_ids.values() if n > 1),
            "exact_duplicate_review_rows": sum(
                n - 1 for native_id, n in all_review_ids.items()
                if n > 1 and len(all_review_hashes[native_id]) == 1
            ),
            "conflicting_duplicate_review_ids": sum(
                len(all_review_hashes[native_id]) > 1
                for native_id, n in all_review_ids.items() if n > 1
            ),
            "submission_checkpoint_disagreements": sum(
                row["checkpoint_status"] == "disagrees" for row in submission_reports
            ),
            "submission_checkpoint_missing": sum(
                row["checkpoint_status"] == "missing" for row in submission_reports
            ),
            "review_orphan_rows": sum(row["orphan_rows"] for row in review_reports),
        },
        "authoritative_fields": {
            "legacy_local_p2": [
                "normalized final outcome", "title", "abstract", "forum", "rating/confidence text"
            ],
            "required_raw_notes_reharvest": [
                "invitation", "readers", "signatures", "replyto", "original", "mdate/tmdate",
                "revision edits", "comments/rebuttals/meta-reviews/ethics roles",
            ],
        },
        "grade_decision": {
            "candidate_selection": "U until invitation-specific public-reader and denominator audit passes",
            "evaluation_constructs": "derived corpus usable for exploratory P2; not a lossless E3 archive",
        },
    }
    report["audit_hash"] = content_hash(json.dumps(report, sort_keys=True))
    return report


def write_local_openreview_audit(root: Path, output: Path) -> Path:
    report = audit_local_openreview(root)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return output
