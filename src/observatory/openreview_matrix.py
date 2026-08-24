"""Build the venue-year/API/invitation observability matrix from raw groups."""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any

from .adapters.openreview_surface import _architecture, _public_configuration, _year
from .ids import content_hash


def build_openreview_matrix(raw_root: Path) -> dict[str, Any]:
    manifest = raw_root / "manifests" / "openreview_surface.jsonl"
    latest: dict[str, dict[str, Any]] = {}
    for line in manifest.read_text().splitlines():
        row = json.loads(line)
        if row["object_type"] != "venue_group_configuration":
            continue
        latest[row["native_id"]] = row
    records = []
    for venue_id, receipt in sorted(latest.items()):
        group = json.loads(gzip.decompress(Path(receipt["raw_pointer"]).read_bytes()))
        config = _public_configuration(group) if not group.get("missing") else {}
        public_submission = config.get("public_submissions") is True
        public_withdrawn = config.get("public_withdrawn_submissions") is True
        public_desk = config.get("public_desk_rejected_submissions") is True
        invitation = config.get("submission_id") or config.get("submission_invitation")
        if public_submission and invitation:
            earliest = "submission_note_configured_public; runtime readers audit still required"
        elif invitation:
            earliest = "submission invitation named but public readers unresolved"
        else:
            earliest = "unknown invitation semantics"
        metadata = receipt.get("metadata") or {}
        api_version = metadata.get("resolved_api")
        if not api_version and metadata.get("present") is False:
            api_version = "missing_v2_and_v1"
        records.append({
            "venue_id": venue_id, "year": _year(venue_id),
            "api_version": api_version or metadata.get("requested_api") or "unresolved",
            "architecture": _architecture(venue_id, config), "submission_invitation": invitation,
            "withdrawn_invitation": config.get("withdrawn_submission_id"),
            "desk_rejected_invitation": config.get("desk_rejected_submission_id"),
            "decision_invitation": config.get("decision_id") or config.get("decision_invitation_id"),
            "review_invitation": config.get("review_id") or config.get("review_invitation_id"),
            "public_submissions": public_submission, "public_withdrawn": public_withdrawn,
            "public_desk_rejected": public_desk, "earliest_observable_stage": earliest,
            "observability_grade": "U", "grade_reason": "candidate readers and denominator not audited",
            "retrieval_recipe": "group config -> invitation-specific Notes/Edits pull -> provider/venue count reconciliation",
            "raw_hash": receipt["byte_hash"],
        })
    return {
        "schema": "observatory.openreview-surface-matrix/1", "records": records,
        "record_count": len(records),
        "provider_group_missing_count": sum(row["api_version"] == "missing_v2_and_v1" for row in records),
        "invitation_unresolved_count": sum(not row["submission_invitation"] for row in records),
        "matrix_hash": content_hash(json.dumps(records, sort_keys=True)),
    }


def write_openreview_matrix(raw_root: Path, output: Path) -> Path:
    matrix = build_openreview_matrix(raw_root)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(matrix, indent=2, sort_keys=True) + "\n")
    return output


def write_openreview_invitation_manifest(matrix_path: Path, output: Path) -> Path:
    """Materialize the explicit, deduplicated input to the lossless API pull."""
    matrix = json.loads(matrix_path.read_text())
    invitations = []
    seen = set()
    for row in matrix.get("records") or []:
        for kind, field in (
            ("submission", "submission_invitation"),
            ("withdrawal", "withdrawn_invitation"),
            ("desk_rejection", "desk_rejected_invitation"),
            ("decision", "decision_invitation"),
            ("review", "review_invitation"),
        ):
            invitation = row.get(field)
            if not invitation:
                continue
            key = (row.get("api_version"), invitation)
            if key in seen:
                continue
            seen.add(key)
            invitations.append({
                "invitation": invitation, "kind": kind,
                "api_version": row.get("api_version"), "venue_id": row.get("venue_id"),
                "surface_grade": row.get("observability_grade"),
                "public_configuration_flag": row.get(
                    {
                        "submission": "public_submissions", "withdrawal": "public_withdrawn",
                        "desk_rejection": "public_desk_rejected",
                    }.get(kind, "")
                ),
            })
    report = {
        "schema": "observatory.openreview-invitation-manifest/1",
        "matrix_hash": matrix.get("matrix_hash"), "invitation_count": len(invitations),
        "invitations": invitations,
        "scope_warning": (
            "Configuration-named invitations are discovery inputs only; runtime invitation readers, "
            "Note counts, and accessibility determine cycle/object coverage grades."
        ),
    }
    report["manifest_hash"] = content_hash(json.dumps(report, sort_keys=True))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return output
