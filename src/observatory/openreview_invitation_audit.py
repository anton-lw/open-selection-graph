"""Audit OpenReview invitation readers and exact public Note denominators."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from .adapters.openreview_api import OpenReviewAPINotesConnector
from .connectors.base import ConnectorContext
from .ids import content_hash


def _strings(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, Mapping):
        for child in value.values():
            yield from _strings(child)
    elif isinstance(value, list):
        for child in value:
            yield from _strings(child)


def _manifest(path: Path, *, public_only: bool) -> list[dict[str, Any]]:
    body = json.loads(path.read_text())
    rows = body.get("invitations", body)
    selected = []
    for row in rows:
        if public_only and row.get("public_configuration_flag") is not True:
            continue
        selected.append(dict(row))
    return selected


def audit_openreview_invitations(
    context: ConnectorContext,
    *,
    manifest_path: Path,
    public_only: bool = True,
    checkpoint_path: Path | None = None,
) -> dict[str, Any]:
    connector = OpenReviewAPINotesConnector(page_size=500, include_edits=False)
    rows = _manifest(manifest_path, public_only=public_only)
    checkpoint: dict[str, Any] = {
        "schema": "observatory.openreview-invitation-checkpoint/2",
        "results": {},
    }
    if checkpoint_path and checkpoint_path.exists():
        checkpoint = json.loads(checkpoint_path.read_text())
        if checkpoint.get("schema") != "observatory.openreview-invitation-checkpoint/2":
            raise ValueError("OpenReview invitation checkpoint schema mismatch")
    cached = checkpoint.setdefault("results", {})
    audited = []
    for index, row in enumerate(rows):
        version = "v1" if row.get("api_version") == "v1" else "v2"
        invitation_id = str(row["invitation"])
        checkpoint_key = f"{version}|{invitation_id}"
        if checkpoint_key in cached:
            result = {**cached[checkpoint_key], "audit_index": index}
            audited.append(result)
            continue
        result: dict[str, Any] = {
            **row,
            "audit_index": index,
            "invitation_exists": False,
            "invitation_configuration_public": None,
            "note_reader_rule": None,
            "note_readers_explicitly_public": False,
            "provider_note_count": None,
            "runtime_reader_sample_count": 0,
            "runtime_reader_sample_public_count": 0,
            "error_class": None,
            "error_detail": None,
        }
        try:
            # Submission Invitation objects are frequently not independently
            # readable even when the provider's venue-group configuration
            # explicitly declares the state public. Audit that declaration
            # against the actual invitation-specific Note endpoint instead of
            # converting an Invitation 403 into a false absence.
            notes = connector._get_json(
                context,
                version,
                "/notes",
                {
                    "invitation": invitation_id,
                    "limit": 100,
                    "count": "true",
                    "trash": "true",
                },
            )
            sample = notes.get("notes") or []
            provider_count = int(notes.get("count", len(sample)))
            public_in_sample = sum(
                "everyone" in set(_strings(note.get("readers") or []))
                for note in sample
            )
            surface_public = row.get("public_configuration_flag") is True
            sample_public = not sample or public_in_sample == len(sample)
            result.update({
                "invitation_exists": True,
                "invitation_configuration_public": surface_public,
                "note_reader_rule": {
                    "provider_group_public_configuration": surface_public,
                    "runtime_note_sample_all_everyone": sample_public,
                },
                "note_readers_explicitly_public": surface_public and sample_public,
                "provider_note_count": provider_count,
                "runtime_reader_sample_count": len(sample),
                "runtime_reader_sample_public_count": public_in_sample,
                "audit_evidence_method": (
                    "provider venue-group public flag plus invitation-specific "
                    "count endpoint and up-to-100 Note reader sample"
                ),
            })
        except Exception as exc:
            result["error_class"] = type(exc).__name__
            result["error_detail"] = str(exc)[:500]
        audited.append(result)
        if checkpoint_path and result["error_class"] is None:
            cached[checkpoint_key] = result
            checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            temporary = checkpoint_path.with_suffix(".tmp.json")
            temporary.write_text(json.dumps(checkpoint, indent=2, sort_keys=True) + "\n")
            temporary.replace(checkpoint_path)

    by_cycle: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in audited:
        by_cycle[str(row.get("venue_id") or row["invitation"].split("/-/")[0])].append(row)
    cycles = []
    for venue_id, cycle_rows in sorted(by_cycle.items()):
        by_kind = defaultdict(list)
        for row in cycle_rows:
            by_kind[str(row.get("kind") or "unknown")].append(row)
        submissions = by_kind.get("submission") or []
        all_named_state_invitations_audited = all(
            row["invitation_exists"]
            and row["provider_note_count"] is not None
            and row["error_class"] is None
            for row in cycle_rows
        )
        submission_public = bool(
            submissions
            and all(row["note_readers_explicitly_public"] for row in submissions)
        )
        state_count = sum(
            int(row["provider_note_count"] or 0) for row in cycle_rows
        )
        grade = "B" if all_named_state_invitations_audited and submission_public else "U"
        cycles.append({
            "venue_id": venue_id,
            "observability_grade": grade,
            "grade_reason": (
                "provider venue-group public configuration, invitation-specific exact "
                "Note counts, and runtime public-reader samples; hidden pre-publication/"
                "blind stages remain"
                if grade == "B"
                else "state count or provider/runtime public Note-reader evidence unresolved"
            ),
            "state_invitation_count": len(cycle_rows),
            "state_counts_sum_before_note_id_deduplication": state_count,
            "all_named_state_invitations_audited": all_named_state_invitations_audited,
            "submission_notes_explicitly_public": submission_public,
            "invitations": [row["invitation"] for row in cycle_rows],
        })

    report: dict[str, Any] = {
        "schema": "observatory.openreview-public-invitation-audit/1",
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "manifest": str(manifest_path),
        "public_configuration_candidates_only": public_only,
        "manifest_row_count": len(rows),
        "audited_row_count": len(audited),
        "error_classes": dict(Counter(
            row["error_class"] for row in audited if row["error_class"]
        )),
        "explicit_public_note_reader_count": sum(
            bool(row["note_readers_explicitly_public"]) for row in audited
        ),
        "provider_note_count_sum_before_note_id_deduplication": sum(
            int(row["provider_note_count"] or 0) for row in audited
        ),
        "grade_counts": dict(Counter(row["observability_grade"] for row in cycles)),
        "passing_cycle_count": sum(row["observability_grade"] == "B" for row in cycles),
        "cycles": cycles,
        "invitations": audited,
        "scope_warning": (
            "Grade B begins at the provider-visible public Note stage and does not imply "
            "observability during confidential submission or access screening."
        ),
    }
    report["passes"] = bool(
        len(audited) == len(rows)
        and not report["error_classes"]
        and report["passing_cycle_count"] > 0
    )
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True, default=str))
    return report


def write_openreview_invitation_audit(
    context: ConnectorContext,
    output: Path,
    *,
    manifest_path: Path,
    public_only: bool = True,
    checkpoint_path: Path | None = None,
) -> Path:
    report = audit_openreview_invitations(
        context,
        manifest_path=manifest_path,
        public_only=public_only,
        checkpoint_path=checkpoint_path,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return output
