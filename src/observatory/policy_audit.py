"""Independent, frozen-source audit of institutional policy extraction."""

from __future__ import annotations

import gzip
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .ids import content_hash
from .storage import ObservatoryCatalog

_ALLOWED = (
    "submission",
    "review",
    "decision",
    "comment",
    "rebuttal",
    "response",
    "revision",
    "withdraw",
    "desk_reject",
    "public_",
    "venue",
    "title",
    "subtitle",
    "date",
    "start_date",
)
_SENSITIVE = ("email", "contact", "message_sender")


def _native_value(value: Any) -> Any:
    return value.get("value") if isinstance(value, dict) and "value" in value else value


def independently_extract_public_configuration(group: dict[str, Any]) -> dict[str, Any]:
    """Re-express the public-field contract without invoking adapter code."""
    return {
        str(key): _native_value(value)
        for key, value in (group.get("content") or {}).items()
        if any(pattern in str(key).lower() for pattern in _ALLOWED)
        and not any(pattern in str(key).lower() for pattern in _SENSITIVE)
    }


def _architecture(group_id: str, config: dict[str, Any]) -> str:
    low = group_id.lower()
    if low == "tmlr" or "transactions_on_machine_learning_research" in low:
        return "rolling_threshold"
    if any(token in low for token in ("conference", "workshop", "symposium", "meeting")):
        return "competitive_quota"
    return "rolling_threshold" if config.get("public_submissions") is True else "unknown"


def _epoch(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    return datetime.fromtimestamp(float(value) / 1000, tz=timezone.utc)


def _same_instant(left: Any, right: datetime | None) -> bool:
    if left is None or right is None:
        return left is None and right is None
    if left.tzinfo is None:
        left = left.replace(tzinfo=timezone.utc)
    return left.astimezone(timezone.utc) == right.astimezone(timezone.utc)


def _read_raw(pointer: str) -> dict[str, Any]:
    path = Path(pointer)
    payload = path.read_bytes()
    if path.suffix == ".gz":
        payload = gzip.decompress(payload)
    value = json.loads(payload)
    if not isinstance(value, dict):
        raise ValueError(f"policy audit expected an object: {path}")
    return value


def build_policy_extraction_audit(
    lake_root: Path,
    output: Path,
    *,
    sample_size: int = 250,
) -> dict[str, Any]:
    """Compare stored facts to an independent raw-configuration extraction."""
    connection = ObservatoryCatalog(lake_root).connect()
    rows = connection.execute(
        """SELECT p.policy_version_id, p.native_id, p.effective_at,
                  p.criteria_json, p.rubric_json, p.content_hash,
                  gc.architecture, s.raw_pointer, s.byte_hash, p.source_object_id
           FROM policy_version p
           JOIN gate_cycle gc ON gc.policy_version_id=p.policy_version_id
           JOIN source_object s ON s.source_object_id=p.source_object_id
           WHERE p.source_id='openreview_surface' AND s.raw_pointer IS NOT NULL"""
    ).fetchall()
    # Hash ordering is a deterministic pseudo-random audit sample and remains
    # stable if file ordering or DuckDB planning changes.
    rows = sorted(rows, key=lambda row: content_hash(str(row[0])))[:sample_size]
    audited: list[dict[str, Any]] = []
    for row in rows:
        (policy_id, native_id, effective_at, criteria_raw, rubric_raw,
         stored_config_hash, stored_architecture, raw_pointer, raw_byte_hash,
         source_object_id) = row
        group = _read_raw(str(raw_pointer))
        config = independently_extract_public_configuration(group)
        expected_criteria = {key: value for key, value in config.items() if "review" in key.lower()}
        expected_rubric = {
            key: value
            for key, value in config.items()
            if any(token in key.lower() for token in ("rating", "score", "confidence"))
        }
        observed_criteria = json.loads(criteria_raw or "{}")
        observed_rubric = json.loads(rubric_raw or "{}")
        expected_effective = _epoch(group.get("tmdate") or group.get("tcdate"))
        checks = {
            "effective_date": _same_instant(effective_at, expected_effective),
            "architecture": stored_architecture == _architecture(str(native_id), config),
            "criteria": observed_criteria == expected_criteria,
            "scale": observed_rubric == expected_rubric,
            "configuration_hash": stored_config_hash == content_hash(json.dumps(config, sort_keys=True)),
            "raw_byte_hash": str(raw_pointer).endswith(f"{raw_byte_hash}.gz"),
        }
        audited.append(
            {
                "policy_version_id": policy_id,
                "venue_native_id": native_id,
                "source_object_id": source_object_id,
                "raw_byte_hash": raw_byte_hash,
                "checks": checks,
                "passes": all(checks.values()),
                "audit_sampling": "deterministic sha256 order over eligible policy ids",
                "auditor_class": "research_team_source_audit_without_recruited_participants",
            }
        )
    field_totals = {
        field: {
            "audited": len(audited),
            "exact": sum(bool(item["checks"][field]) for item in audited),
        }
        for field in (
            "effective_date",
            "architecture",
            "criteria",
            "scale",
            "configuration_hash",
            "raw_byte_hash",
        )
    }
    for totals in field_totals.values():
        totals["agreement"] = totals["exact"] / totals["audited"] if totals["audited"] else 0.0
    report: dict[str, Any] = {
        "schema": "observatory.policy-extraction-audit/1",
        "sample_size_requested": sample_size,
        "sample_size_audited": len(audited),
        "population_eligible": len(connection.execute(
            """SELECT 1 FROM policy_version p JOIN source_object s USING(source_object_id)
               WHERE p.source_id='openreview_surface' AND s.raw_pointer IS NOT NULL"""
        ).fetchall()),
        "field_agreement": field_totals,
        "threshold": 0.95,
        "lower_performing_field_treatment": "source pointer only; never normalized as fact",
        "failure_taxonomy": {
            "effective_date": "provider timestamp mismatch",
            "architecture": "rule or configuration conflict",
            "criteria": "lossy public-configuration extraction",
            "scale": "lossy rating/score/confidence extraction",
            "configuration_hash": "non-reproducible structured document hash",
            "raw_byte_hash": "raw pointer/content-address mismatch",
        },
        "audited_rows": audited,
    }
    required_fields = ("effective_date", "architecture", "criteria", "scale")
    report["passes"] = bool(audited) and all(
        field_totals[field]["agreement"] >= report["threshold"] for field in required_fields
    )
    report["audit_hash"] = content_hash(json.dumps(report, sort_keys=True))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report

