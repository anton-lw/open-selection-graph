"""Atomic cursor/checkpoint state with query-integrity checks."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from ..ids import content_hash


@dataclass
class Checkpoint:
    source_id: str
    query_hash: str
    cursor: str | None = None
    last_native_id: str | None = None
    found_count: int = 0
    batch_count: int = 0
    last_batch_hash: str | None = None
    started_at: str | None = None
    updated_at: str | None = None
    fetch_complete: bool = False
    complete: bool = False


def query_hash(parameters: object) -> str:
    return content_hash(json.dumps(parameters, sort_keys=True, separators=(",", ":"), default=str))


class CheckpointStore:
    def __init__(self, path: Path):
        self.path = path

    def load(self, *, source_id: str, expected_query_hash: str) -> Checkpoint:
        if not self.path.exists():
            return Checkpoint(source_id=source_id, query_hash=expected_query_hash)
        row = json.loads(self.path.read_text())
        current = Checkpoint(**row)
        if current.source_id != source_id:
            raise ValueError(f"checkpoint source mismatch: {current.source_id} != {source_id}")
        if current.query_hash != expected_query_hash:
            raise ValueError("checkpoint query changed; use a new checkpoint path or deliberately restart")
        return current

    def save(self, checkpoint: Checkpoint) -> None:
        checkpoint.updated_at = datetime.now(timezone.utc).isoformat(timespec="microseconds")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(dir=self.path.parent, prefix=f".{self.path.name}.")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(asdict(checkpoint), fh, indent=2, sort_keys=True)
                fh.write("\n")
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(temp_name, self.path)
        finally:
            if os.path.exists(temp_name):
                os.unlink(temp_name)
