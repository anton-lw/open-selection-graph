"""Object-specific licensing matrix and release-package firewall."""

from __future__ import annotations

from typing import Any, Iterable, Mapping

from .licensing import ReleaseClass, decide_release


def object_license_matrix(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    matrix = []
    for row in rows:
        declared = row.get("release_class")
        licence = row.get("licence")
        affirmative = declared == ReleaseClass.REDISTRIBUTE.value
        decision = decide_release(
            object_type=str(row.get("object_type") or "unknown"),
            licence=str(licence) if licence is not None else None,
            source_allows_redistribution=True if affirmative else None,
        )
        # A connector can be more restrictive than the generic classifier but
        # never less restrictive without an affirmative per-object licence.
        if declared in {c.value for c in ReleaseClass} and declared != ReleaseClass.REDISTRIBUTE.value:
            release_class = str(declared)
            reason = "source-object declaration is more restrictive"
        elif affirmative and decision.release_class is not ReleaseClass.REDISTRIBUTE:
            release_class = ReleaseClass.POINTER_HASH.value
            reason = "claimed redistribution lacks an affirmative recognized object licence"
        else:
            release_class = decision.release_class.value
            reason = decision.reason
        matrix.append({
            "source_object_id": row["source_object_id"], "source_id": row["source_id"],
            "object_type": row.get("object_type"), "licence": licence,
            "release_class": release_class,
            "attribution_required": decision.attribution_required,
            "share_alike": decision.share_alike, "noncommercial": decision.noncommercial,
            "licence_unknown": licence in (None, "", "unknown", "per-object"),
            "reason": reason,
        })
    return matrix


def validate_release_bundle(rows: Iterable[Mapping[str, Any]], *, include_content: bool) -> None:
    for row in rows:
        release_class = row.get("release_class")
        if include_content and release_class != ReleaseClass.REDISTRIBUTE.value:
            raise ValueError(f"content object is not redistributable: {row.get('source_object_id')}")
        if row.get("noncommercial") and not row.get("bundle_noncommercial"):
            raise ValueError("noncommercial object requires a separately compatible bundle")
