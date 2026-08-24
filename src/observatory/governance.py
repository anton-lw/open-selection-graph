"""Executable governance for claims, partitions, source transitions, and stops."""

from __future__ import annotations

import hashlib
import inspect
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from .constitution import SourceStatus


def canonical_digest(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


def partition_manifest(registry: Mapping[str, Any]) -> dict[str, Any]:
    """Freeze only prospective design facts, excluding the digest itself."""
    body = {k: v for k, v in registry.items() if k != "registry_sha256"}
    return {**body, "registry_sha256": canonical_digest(body)}


def validate_partition_manifest(registry: Mapping[str, Any]) -> None:
    expected = registry.get("registry_sha256")
    if not expected:
        raise ValueError("partition registry is not frozen")
    actual = canonical_digest({k: v for k, v in registry.items() if k != "registry_sha256"})
    if expected != actual:
        raise ValueError("partition registry changed after freeze; create a new version")
    for name, row in (registry.get("partitions") or {}).items():
        if name.startswith("confirmatory") and row.get("status") == "exploratory":
            raise ValueError(f"confirmatory partition cannot be relabelled exploratory: {name}")


@dataclass(frozen=True)
class SourceTransition:
    source_id: str
    from_status: str
    to_status: str
    effective_at: str
    evidence: tuple[str, ...]
    migration_note: str

    def validate(self) -> None:
        SourceStatus(self.from_status)
        SourceStatus(self.to_status)
        datetime.fromisoformat(self.effective_at.replace("Z", "+00:00"))
        if not self.evidence or not self.migration_note.strip():
            raise ValueError("source transitions require dated evidence and a migration note")


def validate_source_transitions(rows: Iterable[Mapping[str, Any]], source_ids: set[str]) -> None:
    for row in rows:
        transition = SourceTransition(
            source_id=str(row["source_id"]),
            from_status=str(row["from_status"]),
            to_status=str(row["to_status"]),
            effective_at=str(row["effective_at"]),
            evidence=tuple(row.get("evidence") or ()),
            migration_note=str(row.get("migration_note") or ""),
        )
        transition.validate()
        if transition.source_id not in source_ids:
            raise ValueError(f"transition references unregistered source: {transition.source_id}")


def validate_claim_rows(rows: Iterable[Mapping[str, Any]]) -> None:
    required = {
        "id", "statement", "status", "dataset_version", "query", "code_hash",
        "rows", "observability_grade", "policy_version", "validation_artifact", "scope",
    }
    for row in rows:
        missing = sorted(required - set(row))
        if missing:
            raise ValueError(f"claim {row.get('id')} lacks ledger fields: {missing}")
        if row["status"] == "headline" and (not row["query"] or not row["validation_artifact"]):
            raise ValueError(f"headline claim lacks reproducibility evidence: {row['id']}")


def make_registration_receipt(plan: Mapping[str, Any], *, created_at: str | None = None) -> dict[str, Any]:
    stamp = created_at or datetime.now(timezone.utc).isoformat(timespec="seconds")
    return {"created_at": stamp, "plan_sha256": canonical_digest(plan), "plan": dict(plan)}


def execute_claim_ledger(
    connection,
    rows: Iterable[Mapping[str, Any]],
    *,
    dataset_version: str,
) -> dict[str, Any]:
    """Execute read-only claim queries and bind results to code and data."""
    code_hash = hashlib.sha256(inspect.getsource(execute_claim_ledger).encode()).hexdigest()
    executed = []
    failures = []
    for claim in rows:
        query = str(claim["query"]).strip()
        if not re.match(r"(?is)^select\b", query) or ";" in query:
            raise ValueError(f"claim query must be one read-only SELECT: {claim['id']}")
        cursor = connection.execute(query)
        values = cursor.fetchall()
        scalar = values[0][0] if len(values) == 1 and len(values[0]) == 1 else None
        expected = claim.get("rows")
        passes = expected is None or scalar == expected
        receipt = {
            "id": claim["id"], "statement": claim["statement"],
            "dataset_version": dataset_version, "query": query, "code_hash": code_hash,
            "result_row_count": len(values), "scalar_result": scalar,
            "result_sha256": canonical_digest(values), "expected_scalar": expected,
            "observability_grade": claim["observability_grade"],
            "policy_version": claim["policy_version"],
            "validation_artifact": claim["validation_artifact"], "scope": claim["scope"],
            "passes": passes,
        }
        executed.append(receipt)
        if not passes:
            failures.append({"id": claim["id"], "expected": expected, "actual": scalar})
    return {
        "schema": "observatory.executed-claim-ledger/1", "dataset_version": dataset_version,
        "code_hash": code_hash, "claims": executed, "failures": failures, "passes": not failures,
        "ledger_hash": canonical_digest(executed),
    }
