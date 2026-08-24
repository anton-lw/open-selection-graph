"""Audited checkpoint migration for independent F1000-family host shards."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse

from .adapters.f1000 import PLATFORMS
from .connectors.checkpoint import query_hash
from .ids import content_hash


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def migrate_f1000_prefix_checkpoint(
    run_root: Path,
    *,
    enumeration_snapshot: Path,
    old_query_hash: str,
    new_parameters: Mapping[str, Any],
    platform_id: str,
    connector_version: str = "6",
) -> dict[str, Any]:
    """Atomically clone a monolithic prefix into an equivalent host shard.

    The migration is allowed only when the completed portion of the globally
    sorted provider enumeration is byte-for-byte the same prefix as the chosen
    platform shard. The old checkpoint and stage tree are retained as rollback
    evidence.
    """
    if platform_id not in PLATFORMS:
        raise ValueError(f"unknown F1000-family platform: {platform_id}")
    snapshot = json.loads(enumeration_snapshot.read_text())
    declared_hash = snapshot.pop("snapshot_hash", None)
    if declared_hash != content_hash(json.dumps(snapshot, sort_keys=True)):
        raise ValueError("F1000 enumeration snapshot hash mismatch")
    host = PLATFORMS[platform_id]["host"]
    shard_urls = [
        str(url)
        for url in snapshot.get("urls") or []
        if urlparse(str(url)).hostname == host
    ]
    if not shard_urls:
        raise ValueError(f"enumeration contains no URLs for {platform_id}")

    old_checkpoint_path = (
        run_root / "checkpoints" / f"f1000_process-{old_query_hash[:12]}.json"
    )
    old_checkpoint = json.loads(old_checkpoint_path.read_text())
    if old_checkpoint.get("source_id") != "f1000_process":
        raise ValueError("checkpoint source_id mismatch")
    if old_checkpoint.get("query_hash") != old_query_hash:
        raise ValueError("checkpoint query hash mismatch")
    if old_checkpoint.get("complete") or old_checkpoint.get("fetch_complete"):
        raise ValueError("only an incomplete fetch checkpoint can be migrated")
    cursor = int(old_checkpoint.get("cursor") or 0)
    if cursor <= 0 or cursor > len(shard_urls):
        raise ValueError("checkpoint cursor is outside the selected platform prefix")
    if old_checkpoint.get("last_native_id") != shard_urls[cursor - 1]:
        raise ValueError("checkpoint does not end on the selected platform prefix")

    old_stage = run_root / "staging" / "f1000_process" / old_query_hash[:16]
    stage_files = sorted(old_stage.glob("batch-*.json.gz"))
    if len(stage_files) != int(old_checkpoint.get("batch_count") or 0):
        raise ValueError("checkpoint batch count does not match durable stage files")
    expected_names = [f"batch-{index:08d}.json.gz" for index in range(1, len(stage_files) + 1)]
    if [path.name for path in stage_files] != expected_names:
        raise ValueError("durable stage batch sequence is not contiguous")

    params = {
        "source_id": "f1000_process",
        "version": connector_version,
        "parameters": dict(new_parameters),
        "since": None,
        "until": None,
        "limit": None,
        "fixture": False,
        "no_text": False,
    }
    new_query_hash = query_hash(params)
    new_checkpoint_path = (
        run_root / "checkpoints" / f"f1000_process-{new_query_hash[:12]}.json"
    )
    new_stage = run_root / "staging" / "f1000_process" / new_query_hash[:16]
    receipt_path = run_root / "f1000_checkpoint_migration_receipt.json"
    if new_checkpoint_path.exists() or new_stage.exists():
        if receipt_path.exists():
            existing = json.loads(receipt_path.read_text())
            if existing.get("new_query_hash") == new_query_hash:
                return existing
        raise FileExistsError("target F1000 shard checkpoint or stage already exists")

    temporary_stage = new_stage.with_name(f".{new_stage.name}.migrating")
    if temporary_stage.exists():
        shutil.rmtree(temporary_stage)
    try:
        shutil.copytree(old_stage, temporary_stage)
        copied = sorted(temporary_stage.glob("batch-*.json.gz"))
        if [path.name for path in copied] != expected_names:
            raise ValueError("copied F1000 stage sequence failed verification")
        os.replace(temporary_stage, new_stage)
        new_checkpoint = {**old_checkpoint, "query_hash": new_query_hash}
        _atomic_json(new_checkpoint_path, new_checkpoint)
    finally:
        if temporary_stage.exists():
            shutil.rmtree(temporary_stage)

    receipt: dict[str, Any] = {
        "schema": "observatory.f1000-checkpoint-migration/1",
        "source_id": "f1000_process",
        "platform_id": platform_id,
        "old_query_hash": old_query_hash,
        "new_query_hash": new_query_hash,
        "cursor": cursor,
        "batch_count": len(stage_files),
        "last_native_id": old_checkpoint["last_native_id"],
        "selected_platform_expected_count": len(shard_urls),
        "old_checkpoint_retained": True,
        "old_stage_retained": True,
        "passes": True,
    }
    receipt["receipt_hash"] = content_hash(json.dumps(receipt, sort_keys=True))
    _atomic_json(receipt_path, receipt)
    return receipt
