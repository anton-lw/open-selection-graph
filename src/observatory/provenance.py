"""Retrieval/transformation provenance and field-level traceability."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .ids import content_hash, stable_id


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


@dataclass(frozen=True)
class ProvenanceEvent:
    provenance_event_id: str
    source_id: str
    source_object_id: str
    event_type: str
    occurred_at: str
    parser_name: str | None = None
    parser_version: str | None = None
    code_hash: str | None = None
    input_hash: str | None = None
    output_hash: str | None = None
    parameters_json: str | None = None
    parent_event_ids: list[str] = field(default_factory=list)
    success: bool = True
    error: str | None = None


def make_event(
    *,
    source_id: str,
    source_object_id: str,
    event_type: str,
    parser_name: str | None = None,
    parser_version: str | None = None,
    code_hash: str | None = None,
    input_hash: str | None = None,
    output_hash: str | None = None,
    parameters: dict[str, Any] | None = None,
    parent_event_ids: Iterable[str] = (),
    success: bool = True,
    error: str | None = None,
    occurred_at: str | None = None,
) -> ProvenanceEvent:
    stamp = occurred_at or utc_now()
    native = content_hash(
        json.dumps(
            [source_id, source_object_id, event_type, stamp, input_hash, output_hash],
            separators=(",", ":"),
        )
    )
    return ProvenanceEvent(
        provenance_event_id=stable_id("provenance", source_id, native),
        source_id=source_id,
        source_object_id=source_object_id,
        event_type=event_type,
        occurred_at=stamp,
        parser_name=parser_name,
        parser_version=parser_version,
        code_hash=code_hash,
        input_hash=input_hash,
        output_hash=output_hash,
        parameters_json=json.dumps(parameters, sort_keys=True) if parameters is not None else None,
        parent_event_ids=list(parent_event_ids),
        success=success,
        error=error,
    )


class ProvenanceIndex:
    """Small portable trace index used for audits and fixtures."""

    def __init__(self, path: Path):
        self.path = path

    def append(self, event: ProvenanceEvent) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(asdict(event), sort_keys=True) + "\n")

    def events(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        return [json.loads(line) for line in self.path.read_text().splitlines() if line.strip()]

    def trace(self, provenance_event_id: str) -> list[dict[str, Any]]:
        by_id = {row["provenance_event_id"]: row for row in self.events()}
        out: list[dict[str, Any]] = []
        todo = [provenance_event_id]
        seen: set[str] = set()
        while todo:
            current = todo.pop()
            if current in seen or current not in by_id:
                continue
            seen.add(current)
            row = by_id[current]
            out.append(row)
            todo.extend(row.get("parent_event_ids") or [])
        return out
