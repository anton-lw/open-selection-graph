"""Release diffing and provenance-preserving migration validation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Mapping

import yaml

from .ids import content_hash

CHANGE_CLASSES = (
    "row_additions",
    "row_removals",
    "source_corrections",
    "grade_changes",
    "id_merges",
    "id_splits",
    "schema_changes",
    "feature_version_changes",
)


def validate_migration(events: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows = [dict(row) for row in events]
    failures = []
    for index, row in enumerate(rows):
        change_class = str(row.get("change_class") or "")
        if change_class not in CHANGE_CLASSES:
            failures.append({"row": index, "reason": "unknown_change_class"})
        if not row.get("reason") or not row.get("evidence"):
            failures.append({"row": index, "reason": "missing_reason_or_evidence"})
        if change_class in {"id_merges", "id_splits"} and (
            not row.get("old_ids") or not row.get("new_ids")
        ):
            failures.append({"row": index, "reason": "ambiguous_id_mapping"})
        if row.get("mutates_frozen_release"):
            failures.append({"row": index, "reason": "frozen_release_mutation"})
    return {
        "schema": "observatory.migration-validation/1",
        "event_count": len(rows),
        "failures": failures,
        "passes": not failures,
    }


def build_initial_release_diff(workspace: Path, output: Path) -> dict[str, Any]:
    versions = yaml.safe_load((workspace / "configs" / "observatory" / "versions.yaml").read_text())
    current = versions["release"]
    events = [
        {
            "change_class": "schema_changes",
            "reason": "integrated R2-R5 products join the R1 architecture triangle",
            "evidence": "configs/observatory/release_components.yaml",
            "old": "0.1.0",
            "new": current["schema_version"],
            "breaking": True,
            "mutates_frozen_release": False,
        },
        {
            "change_class": "feature_version_changes",
            "reason": "construct, lineage, outcomes, funding and patent feature families released",
            "evidence": "configs/observatory/versions.yaml",
            "old": ["institutional_regimes", "evaluation_objects", "semantic_novelty"],
            "new": sorted(current["feature_versions"]),
            "breaking": False,
            "mutates_frozen_release": False,
        },
        {
            "change_class": "row_additions",
            "reason": "new immutable source snapshots and derived products",
            "evidence": "results/observatory/r1 through results/observatory/r5",
            "old_release": "observatory-0.1.0-r1",
            "new_release": current["release_version"],
            "breaking": False,
            "mutates_frozen_release": False,
        },
    ]
    validation = validate_migration(events)
    body = {
        "schema": "observatory.release-diff/1",
        "from_release": "0.1.0-r1",
        "to_release": current["release_version"],
        "change_classes": list(CHANGE_CLASSES),
        "events": events,
        "migration_note": "docs/observatory/RELEASE_MIGRATIONS.md",
        "validation": validation,
    }
    body["diff_hash"] = content_hash(json.dumps(body, sort_keys=True))
    body["passes"] = validation["passes"]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n")
    return body
