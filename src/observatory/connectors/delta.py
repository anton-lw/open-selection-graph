"""Deterministic change logs between immutable source manifests."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _rows(path: Path) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if not path.exists():
        return out
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        # Source-object IDs include the byte hash so revisions retain distinct
        # immutable identities.  Deltas compare the stable provider identity.
        key = f"{row.get('source_id', '')}|{row['native_id']}"
        out[key] = row
    return out


def manifest_diff(before: Path, after: Path) -> dict[str, Any]:
    old, new = _rows(before), _rows(after)
    added = sorted(set(new) - set(old))
    removed = sorted(set(old) - set(new))
    modified = sorted(
        key for key in set(old) & set(new) if old[key].get("byte_hash") != new[key].get("byte_hash")
    )
    unchanged = len(set(old) & set(new)) - len(modified)
    return {
        "added": added,
        "removed": removed,
        "modified": modified,
        "unchanged_count": unchanged,
        "before_count": len(old),
        "after_count": len(new),
        "changes": [
            {"change_type": "addition", "native_key": key, "after": new[key].get("source_object_id")}
            for key in added
        ] + [
            {"change_type": "modification", "native_key": key,
             "before": old[key].get("source_object_id"), "after": new[key].get("source_object_id")}
            for key in modified
        ] + [
            {"change_type": "removal", "native_key": key, "before": old[key].get("source_object_id")}
            for key in removed
        ],
    }


def coverage_grade_diff(before: list[dict[str, Any]], after: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def indexed(rows):
        return {
            f"{row['gate_cycle_id']}|{row['object_type']}": row.get("observability_grade")
            for row in rows
        }
    old, new = indexed(before), indexed(after)
    return [
        {"change_type": "grade_change", "coverage_key": key, "before": old[key], "after": new[key]}
        for key in sorted(set(old) & set(new)) if old[key] != new[key]
    ]
