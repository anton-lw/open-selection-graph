"""Predictive storage stops and exact recoverability manifests."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from .ids import content_hash
from .registry import source_cards


def storage_preflight(
    path: Path,
    *,
    projected_input_bytes: int,
    projected_output_bytes: int,
    reserve_bytes: int = 2_000_000_000,
) -> dict[str, Any]:
    """Fail before a job can consume the configured free-space reserve."""
    if min(projected_input_bytes, projected_output_bytes, reserve_bytes) < 0:
        raise ValueError("storage projections and reserve must be non-negative")
    target = path
    while not target.exists() and target != target.parent:
        target = target.parent
    usage = shutil.disk_usage(target)
    required = int(projected_input_bytes) + int(projected_output_bytes) + int(reserve_bytes)
    report = {
        "schema": "observatory.storage-preflight/1",
        "target": str(path.resolve()),
        "filesystem_probe": str(target.resolve()),
        "free_bytes": int(usage.free),
        "projected_input_bytes": int(projected_input_bytes),
        "projected_output_bytes": int(projected_output_bytes),
        "reserve_bytes": int(reserve_bytes),
        "required_free_bytes": required,
        "passes": int(usage.free) >= required,
    }
    if not report["passes"]:
        raise RuntimeError(
            f"unsafe disk pressure: free={usage.free}, required={required}, target={path.resolve()}"
        )
    return report


def build_recoverability_manifest(raw_root: Path) -> dict[str, Any]:
    """Describe exact raw manifests and their public refetch authorities."""
    cards = {card.source_id: card for card in source_cards()}
    rows = []
    manifests = raw_root / "manifests"
    for path in sorted(manifests.glob("*.jsonl")) if manifests.exists() else []:
        source_id = path.stem
        card = cards.get(source_id)
        rows.append(
            {
                "source_id": source_id,
                "manifest": str(path),
                "manifest_sha256": content_hash(path.read_bytes()),
                "receipt_count": sum(1 for line in path.read_text().splitlines() if line.strip()),
                "refetchable": card is not None and card.status != "failed",
                "official_url": card.official_url if card else None,
                "access_mode": card.access_mode if card else None,
            }
        )
    report: dict[str, Any] = {
        "schema": "observatory.recoverability-manifest/1",
        "raw_root": str(raw_root.resolve()),
        "sources": rows,
        "source_count": len(rows),
    }
    report["manifest_hash"] = content_hash(json.dumps(report, sort_keys=True))
    return report


def write_recoverability_manifest(raw_root: Path, output: Path) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(build_recoverability_manifest(raw_root), indent=2, sort_keys=True) + "\n")
    return output

