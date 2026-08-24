"""Iterate lossless Crossref work objects from legacy or page-bundled raw receipts."""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any, Iterator


def iter_crossref_peer_reviews(raw_root: Path) -> Iterator[dict[str, Any]]:
    manifest = raw_root / "manifests" / "crossref.jsonl"
    if not manifest.exists():
        return
    latest: dict[str, dict[str, Any]] = {}
    for line in manifest.read_text().splitlines():
        if not line.strip():
            continue
        receipt = json.loads(line)
        if receipt.get("object_type") in {
            "peer_review_metadata", "peer_review_metadata_page"
        }:
            latest[str(receipt["source_object_id"])] = receipt
    receipts = list(latest.values())
    page_receipts = [
        receipt for receipt in receipts
        if receipt.get("object_type") == "peer_review_metadata_page"
    ]
    if page_receipts:
        # A raw lake may contain an earlier bounded proof followed by the full
        # cursor census.  Every batch in one runner invocation shares its
        # retrieval timestamp, so the newest timestamp isolates the dated
        # snapshot and prevents proof pages from inflating its denominator.
        newest_snapshot = max(str(receipt.get("retrieved_at") or "") for receipt in page_receipts)
        page_receipts = [
            receipt
            for receipt in page_receipts
            if str(receipt.get("retrieved_at") or "") == newest_snapshot
        ]
    # A full page-bundled run supersedes the 100-object single-work proof.  Do
    # not double count proof objects that also occur in the complete cursor.
    for receipt in page_receipts or receipts:
        payload = json.loads(gzip.decompress(Path(receipt["raw_pointer"]).read_bytes()))
        works = (
            payload.get("items")
            if receipt.get("object_type") == "peer_review_metadata_page"
            else [payload]
        )
        for index, work in enumerate(works or []):
            yield {
                "work": work,
                "receipt": receipt,
                "item_index": index,
                "bundle_cursor": payload.get("cursor") if isinstance(payload, dict) else None,
                "bundle_total_results": (
                    payload.get("total_results") if isinstance(payload, dict) else None
                ),
            }
