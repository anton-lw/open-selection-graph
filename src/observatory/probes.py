"""Build reproducible 100-object proof-of-access fixtures and manifests."""

from __future__ import annotations

import gzip
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .connectors.http import PoliteSession, RatePolicy
from .registry import source_by_id
from .storage import RawStore

GOLDEN_NAMES = {
    "crossref": "peer_review.json",
    "copernicus": "oai_page.xml",
    "copernicus_crossref": "posted_page.json",
    "arxiv": "oai_record.json",
    "europe_pmc": "work.json",
    "openreview_surface": "venue_group.json",
    "openreview": "review.json",
    "openreview_api": "note.json",
    "elife_process": "process_page.html",
    "f1000_process": "article.xml",
    "scipost_process": "submission_bundle.json",
    "elife": "work.json",
    "f1000research": "work.json",
    "scipost": "work.json",
    "peerj": "work.json",
    "plos_review_history": "work.json",
    "embo_transparent_review": "work.json",
    "royal_society_review": "work.json",
    "bmc_open_review": "work.json",
    "qeios": "work.json",
}


def _latest_run(run_root: Path, source_id: str) -> tuple[Path, dict[str, Any]]:
    candidates = []
    for path in (run_root / "runs").glob(f"{source_id}-*.json"):
        row = json.loads(path.read_text())
        if row.get("found_count", 0) >= 100:
            candidates.append((path, row))
    if not candidates:
        raise FileNotFoundError(f"no completed 100-object run for {source_id}")
    return max(candidates, key=lambda pair: pair[1].get("completed_at") or "")


def _object_rows(
    raw_root: Path, source_id: str, *, run_root: Path | None = None, run: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    manifest = raw_root / "manifests" / f"{source_id}.jsonl"
    rows = [json.loads(line) for line in manifest.read_text().splitlines() if line.strip()]
    latest: dict[str, dict[str, Any]] = {}
    for row in reversed(rows):
        if row["object_type"] in {"coverage_assessment", "terms_snapshot"}:
            continue
        latest.setdefault(row["source_object_id"], row)
    if run_root is not None and run is not None:
        stage_dir = run_root / "staging" / source_id / str(run["query_hash"])[:16]
        ordered_ids = []
        paths = sorted((*stage_dir.glob("batch-*.json"), *stage_dir.glob("batch-*.json.gz")))
        for path in paths:
            if path.name.startswith("batch-coverage"):
                continue
            stage = json.loads(gzip.decompress(path.read_bytes())) if path.suffix == ".gz" else json.loads(path.read_text())
            ordered_ids.extend(
                row["source_object_id"] for row in (stage.get("tables") or {}).get("source_object", [])
                if row.get("object_type") != "coverage_assessment"
            )
        selected = [latest[source_object_id] for source_object_id in ordered_ids if source_object_id in latest]
        if selected:
            return selected
    return list(latest.values())


def snapshot_terms(
    *, source_id: str, raw_root: Path, cache_root: Path, retrieved_at: str
) -> dict[str, Any]:
    card = source_by_id(source_id)
    url = card.terms_url or card.official_url
    host = (urlparse(url).hostname or "").lower()
    try:
        response = PoliteSession(
            cache_dir=cache_root / "terms",
            allowed_hosts={host},
            policy=RatePolicy(min_interval_seconds=0.5, max_retries=3),
        ).get(url)
        receipt = RawStore(raw_root).put(
            source_id=source_id, native_id=f"terms:{url}", object_type="terms_snapshot",
            payload=response.content, metadata={"url": url, "status": response.status_code},
            retrieved_at=retrieved_at,
        )
        return {
            "url": url, "retrieved_at": retrieved_at, "http_status": response.status_code,
            "byte_hash": receipt.byte_hash, "raw_pointer": receipt.raw_pointer, "passes": True,
        }
    except Exception as exc:
        return {
            "url": url, "retrieved_at": retrieved_at, "passes": False,
            "error_class": type(exc).__name__, "error": str(exc)[:300],
        }


def _golden_row(
    source_id: str, rows: list[dict[str, Any]], raw_root: Path
) -> dict[str, Any]:
    """Choose a fixture that exercises the source's defining structure."""
    if source_id == "f1000_process":
        store = RawStore(raw_root)
        for row in rows:
            if b"<sub-article" in store.get(row["byte_hash"]):
                return row
        raise ValueError("F1000 proof sample contains no review/response sub-article")
    return rows[0]


def build_probe_manifest(
    *,
    source_id: str,
    raw_root: Path,
    run_root: Path,
    fixture_root: Path,
    cache_root: Path,
) -> Path:
    run_path, run = _latest_run(run_root, source_id)
    rows = _object_rows(raw_root, source_id, run_root=run_root, run=run)[:100]
    if len(rows) != 100:
        raise ValueError(f"{source_id}: expected exactly 100 distinct proof objects, found {len(rows)}")
    retrieved_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    terms = snapshot_terms(
        source_id=source_id, raw_root=raw_root, cache_root=cache_root, retrieved_at=retrieved_at
    )
    fixture_dir = fixture_root / source_id
    fixture_dir.mkdir(parents=True, exist_ok=True)
    golden_name = GOLDEN_NAMES.get(source_id)
    golden_hash = None
    if golden_name:
        golden_row = _golden_row(source_id, rows, raw_root)
        payload = gzip.decompress(Path(golden_row["raw_pointer"]).read_bytes())
        golden = fixture_dir / golden_name
        golden.write_bytes(payload)
        golden_hash = golden_row["byte_hash"]
    manifest = {
        "schema": "observatory.proof-of-access/1",
        "constitution_version": "0.1.0",
        "source_id": source_id,
        "created_at": retrieved_at,
        "run_manifest": str(run_path),
        "query_hash": run["query_hash"],
        "connector_version": run["connector_version"],
        "expected_count_method": run["estimate"]["method"],
        "provider_expected_objects": run["estimate"]["expected_objects"],
        "object_count": len(rows),
        "object_hashes": [row["byte_hash"] for row in rows],
        "object_size_bytes": [row["size_bytes"] for row in rows],
        "native_ids": [row["native_id"] for row in rows],
        "golden_fixture": golden_name,
        "golden_fixture_hash": golden_hash,
        "terms_snapshot": terms,
        "pagination": {
            "batch_count": run["batch_count"], "found_count": run["found_count"],
            "checkpointed": True,
        },
        "deletions": "source-specific delta probe required; absence is never silently dropped",
        "passes": len(rows) == 100 and bool(golden_name) and terms["passes"],
    }
    output = fixture_dir / "probe_manifest.json"
    output.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return output
