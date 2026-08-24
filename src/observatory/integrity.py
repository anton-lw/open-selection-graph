"""Hash, relational, provenance, and coverage invariants for releases."""

from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Iterable

from .ids import content_hash
from .storage import RawStore


def verify_raw_manifests(
    raw_root: Path, *, source_ids: Iterable[str] | None = None
) -> dict[str, Any]:
    """Verify immutable objects referenced by all or selected source manifests."""
    store = RawStore(raw_root)
    checked = 0
    corrupt: list[dict[str, str]] = []
    verified_hashes: set[str] = set()
    first_receipt: dict[str, dict[str, str]] = {}
    duplicate_receipts = 0
    selected = set(source_ids) if source_ids is not None else None
    manifests = (
        [store.manifests / f"{source_id}.jsonl" for source_id in sorted(selected)]
        if selected is not None
        else sorted(store.manifests.glob("*.jsonl"))
    )
    for manifest in manifests:
        if not manifest.exists():
            corrupt.append({"manifest": str(manifest), "source_object_id": "manifest_absent"})
            continue
        for line in manifest.read_text().splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            checked += 1
            byte_hash = str(row["byte_hash"])
            if byte_hash in verified_hashes:
                duplicate_receipts += 1
                continue
            verified_hashes.add(byte_hash)
            first_receipt[byte_hash] = {
                "manifest": str(manifest),
                "source_object_id": str(row["source_object_id"]),
            }
    thread_state = threading.local()

    def verify_hash(byte_hash: str) -> tuple[str, bool]:
        worker_store = getattr(thread_state, "store", None)
        if worker_store is None:
            worker_store = RawStore(raw_root)
            thread_state.store = worker_store
        return byte_hash, worker_store.verify(byte_hash)

    hashes = sorted(verified_hashes)
    with ThreadPoolExecutor(max_workers=min(8, max(len(hashes), 1))) as executor:
        results = dict(executor.map(verify_hash, hashes))
    corrupt.extend(
        first_receipt[byte_hash]
        for byte_hash in hashes
        if not results.get(byte_hash, False)
    )
    return {
        "passes": not corrupt,
        "checked": checked,
        "unique_objects_checked": len(verified_hashes),
        "duplicate_receipts_skipped": duplicate_receipts,
        "corrupt": corrupt,
    }


def parquet_checksums(lake_root: Path) -> dict[str, str]:
    return {
        str(path.relative_to(lake_root)): content_hash(path.read_bytes())
        for path in sorted(lake_root.glob("**/*.parquet"))
    }


def relational_audit(connection) -> dict[str, Any]:
    tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
    issues: dict[str, int] = {}
    if {"candidate_gate_event", "coverage_observation"} <= tables:
        issues["orphan_candidate_gate_coverage"] = connection.execute("""
            SELECT count(*) FROM candidate_gate_event e
            LEFT JOIN coverage_observation c USING (coverage_observation_id)
            WHERE c.coverage_observation_id IS NULL
        """).fetchone()[0]
        issues["ambiguous_candidate_gate_coverage"] = connection.execute("""
            SELECT count(*) FROM (
              SELECT e.candidate_gate_event_id, count(*) n
              FROM candidate_gate_event e JOIN coverage_observation c USING (coverage_observation_id)
              GROUP BY 1 HAVING n <> 1
            )
        """).fetchone()[0]
    if {"field_provenance", "source_object", "provenance_event"} <= tables:
        issues["orphan_field_source"] = connection.execute("""
            SELECT count(*) FROM field_provenance f LEFT JOIN source_object s USING(source_object_id)
            WHERE s.source_object_id IS NULL
        """).fetchone()[0]
        issues["orphan_field_event"] = connection.execute("""
            SELECT count(*) FROM field_provenance f LEFT JOIN provenance_event p USING(provenance_event_id)
            WHERE p.provenance_event_id IS NULL
        """).fetchone()[0]
    return {"passes": not any(issues.values()), "issues": issues}


def trace_field(connection, table: str, record_id: str, field_name: str) -> list[dict[str, Any]]:
    """Trace a normalized field to raw bytes and transformation metadata."""
    cursor = connection.execute("""
        SELECT f.*, s.byte_hash, s.raw_pointer, s.source_url,
               p.parser_name, p.parser_version, p.code_hash, p.input_hash, p.output_hash
        FROM field_provenance f
        JOIN source_object s USING(source_object_id)
        JOIN provenance_event p USING(provenance_event_id)
        WHERE f.table_name = ? AND f.record_id = ? AND f.field_name = ?
        ORDER BY f.observed_at
    """, [table, record_id, field_name])
    columns = [description[0] for description in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]
