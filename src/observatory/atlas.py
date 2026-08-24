"""Generate the source/coverage atlas from cards and run manifests."""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .ids import content_hash
from .registry import source_cards

RELEASE_RULES = {
    "R1": {
        "purpose": "candidate-pool and public-stage process panel",
        "allowed_grades": ["A", "B"],
        "minimum_coverage_ratio": 0.95,
        "requires_exact_or_audited_denominator": True,
    },
    "R2": {
        "purpose": "descriptive discovery and selected-history layer",
        "allowed_grades": ["A", "B", "C", "D"],
        "minimum_coverage_ratio": None,
        "requires_exact_or_audited_denominator": False,
    },
}
RELEASE_SOURCE_STATUSES = {"included", "pointer_only", "derived_only"}


def count_coverage_grade(coverage: dict[str, Any], *, default_threshold: float = 0.95) -> dict[str, Any]:
    """Automatically downgrade unsupported A/B count claims before release."""
    declared = str(coverage.get("observability_grade") or "U")
    expected = coverage.get("expected_count")
    found = coverage.get("found_count")
    ratio = coverage.get("coverage_ratio")
    if ratio is None and expected not in (None, 0) and found is not None:
        ratio = float(found) / float(expected)
    threshold = float(coverage.get("minimum_coverage_ratio") or default_threshold)
    effective = declared
    reason = None
    if declared in {"A", "B"} and (ratio is None or float(ratio) < threshold):
        effective = "U"
        reason = f"automatic count-coverage downgrade: {ratio} < {threshold}"
    return {
        "declared_grade": declared,
        "effective_grade": effective,
        "coverage_ratio": ratio,
        "threshold": threshold,
        "downgraded": effective != declared,
        "reason": reason,
    }


def release_cycle_decision(
    coverage: dict[str, Any],
    *,
    source_status: str,
    release: str,
    required_ticket_statuses: dict[str, str] | None = None,
) -> dict[str, Any]:
    if release not in RELEASE_RULES:
        raise KeyError(release)
    rule = RELEASE_RULES[release]
    reasons = []
    grade_decision = count_coverage_grade(coverage)
    grade = grade_decision["effective_grade"]
    if source_status not in RELEASE_SOURCE_STATUSES:
        reasons.append(f"source_status={source_status}")
    if grade not in rule["allowed_grades"]:
        reasons.append(f"grade={grade}")
    ratio = coverage.get("coverage_ratio")
    if ratio is None:
        expected = coverage.get("expected_count")
        found = coverage.get("found_count")
        ratio = found / expected if expected not in (None, 0) else None
    minimum = rule["minimum_coverage_ratio"]
    if minimum is not None and (ratio is None or float(ratio) < float(minimum)):
        reasons.append(f"coverage_ratio={ratio}")
    audit_status = str(coverage.get("audit_status") or "unresolved").lower()
    if rule["requires_exact_or_audited_denominator"] and any(
        token in audit_status for token in ("unresolved", "unverified", "partial")
    ):
        reasons.append(f"audit_status={audit_status}")
    if not coverage.get("earliest_public_stage"):
        reasons.append("earliest_public_stage=missing")
    if coverage.get("known_hidden_stages") is None:
        reasons.append("known_hidden_stages=undeclared")
    for ticket_id, status in sorted((required_ticket_statuses or {}).items()):
        if status != "complete":
            reasons.append(f"ticket_gate={ticket_id}:{status}")
    return {
        "release": release,
        "eligible": not reasons,
        "reasons": reasons,
        "grade": grade,
        "coverage_ratio": ratio,
        "source_status": source_status,
        "required_ticket_statuses": required_ticket_statuses or {},
        "count_coverage_grade": grade_decision,
    }


def _evidence_files(run_root: Path) -> list[dict[str, Any]]:
    patterns = (
        "*receipt.json",
        "*report.json",
        "*census.json",
        "*audit.json",
        "*reconciliation.json",
        "*benchmark*.json",
        "*coverage*.json",
    )
    paths = {
        path
        for pattern in patterns
        for path in run_root.glob(pattern)
        if path.name not in {"source_coverage_atlas.json", "ticket_evidence_audit.json"}
    }
    return [
        {
            "path": str(path),
            "byte_hash": content_hash(path.read_bytes()),
            "size_bytes": path.stat().st_size,
        }
        for path in sorted(paths)
    ]


def write_lake_population_coverage(
    lake_root: Path,
    output: Path,
    *,
    source_id: str,
    query_hashes: list[str] | tuple[str, ...],
) -> Path:
    """Freeze uncapped coverage rows for explicit completed run hashes."""
    import pyarrow.parquet as pq

    hashes = tuple(str(value) for value in query_hashes)
    if not hashes or len(set(hashes)) != len(hashes):
        raise ValueError("coverage export query hashes must be non-empty and unique")
    partition = lake_root / "coverage_observation" / f"source_id={source_id}"
    rows_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    files_by_hash: dict[str, list[str]] = {}
    keep = {
        "coverage_observation_id",
        "gate_cycle_id",
        "object_type",
        "earliest_public_stage",
        "observability_grade",
        "expected_count",
        "found_count",
        "coverage_ratio",
        "expected_count_method",
        "query_or_invitation",
        "known_hidden_stages",
        "known_exclusions",
        "missing_reason",
        "audit_status",
    }
    for qhash in hashes:
        matches = sorted(partition.glob(f"run-{qhash[:16]}*.parquet"))
        if not matches:
            raise FileNotFoundError(f"no coverage shards for {source_id} run {qhash}")
        files_by_hash[qhash] = [str(path.relative_to(lake_root)) for path in matches]
        for path in matches:
            for source_row in pq.ParquetFile(path).read().to_pylist():
                if str(source_row.get("source_id")) != source_id:
                    raise ValueError(f"coverage shard source mismatch: {path}")
                row = {name: source_row.get(name) for name in keep}
                for name in ("known_hidden_stages", "known_exclusions"):
                    value = row.get(name)
                    if isinstance(value, str):
                        try:
                            parsed = json.loads(value)
                        except json.JSONDecodeError:
                            parsed = [value]
                        row[name] = parsed
                key = (str(row["gate_cycle_id"]), str(row["object_type"]))
                previous = rows_by_key.get(key)
                if previous is not None and previous != row:
                    raise ValueError(f"conflicting full coverage row for {source_id}: {key}")
                rows_by_key[key] = row
    rows = [rows_by_key[key] for key in sorted(rows_by_key)]
    body: dict[str, Any] = {
        "schema": "observatory.population-coverage/1",
        "source_id": source_id,
        "query_hashes": list(hashes),
        "coverage_shards": files_by_hash,
        "row_count": len(rows),
        "rows": rows,
    }
    body["export_hash"] = content_hash(json.dumps(body, sort_keys=True, default=str))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(body, indent=2, sort_keys=True, default=str) + "\n")
    return output


def _coverage_exports(run_root: Path) -> dict[str, dict[str, Any]]:
    exports: dict[str, dict[str, Any]] = {}
    for path in sorted(run_root.glob("*_population_coverage.json")):
        body = json.loads(path.read_text())
        if body.get("schema") != "observatory.population-coverage/1":
            raise ValueError(f"unsupported population coverage schema: {path}")
        declared = body.get("export_hash")
        unhashed = {key: value for key, value in body.items() if key != "export_hash"}
        if not declared or declared != content_hash(json.dumps(unhashed, sort_keys=True, default=str)):
            raise ValueError(f"population coverage self-hash failed: {path}")
        rows = body.get("rows") or []
        if int(body.get("row_count") or -1) != len(rows) or not rows:
            raise ValueError(f"population coverage row count failed: {path}")
        keys = [(str(row.get("gate_cycle_id")), str(row.get("object_type"))) for row in rows]
        if any("None" in key for key in keys) or len(keys) != len(set(keys)):
            raise ValueError(f"population coverage keys are incomplete or duplicate: {path}")
        source_id = str(body.get("source_id") or "")
        if not source_id or source_id in exports:
            raise ValueError(f"population coverage source is missing or duplicate: {path}")
        exports[source_id] = {
            **body,
            "path": str(path),
            "byte_hash": content_hash(path.read_bytes()),
        }
    return exports


def build_source_atlas(run_root: Path) -> dict[str, Any]:
    import yaml

    ticket_config = yaml.safe_load(
        (Path(__file__).resolve().parents[2] / "configs" / "observatory" / "ticket_evidence.yaml").read_text()
    )
    ticket_status = {
        str(row["id"]): str(row["status"])
        for row in ticket_config.get("tickets") or []
    }
    runs = {}
    for path in sorted((run_root / "runs").glob("*.json")):
        row = json.loads(path.read_text())
        runs.setdefault(row["source_id"], []).append({
            "manifest": str(path),
            "manifest_hash": content_hash(path.read_bytes()),
            "status": row.get("status"),
            "connector_version": row.get("connector_version"),
            "query_hash": row.get("query_hash"),
            "found_count": row.get("found_count"),
            "coverage": row.get("coverage"),
            "coverage_count": row.get("coverage_count"),
            "coverage_truncated": row.get("coverage_truncated_in_manifest", False),
            "completed_at": row.get("completed_at"),
        })
    coverage_exports = _coverage_exports(run_root)
    sources = []
    coverage_export_required_sources = []
    for card in source_cards():
        source_runs = runs.get(card.source_id, [])
        coverage_export = coverage_exports.get(card.source_id)
        if (
            card.status in RELEASE_SOURCE_STATUSES
            and any(run.get("coverage_truncated") for run in source_runs)
            and coverage_export is None
        ):
            coverage_export_required_sources.append(card.source_id)
        required_ticket_statuses = {
            ticket_id: ticket_status.get(ticket_id, "missing")
            for ticket_id in card.release_gate_tickets
        }
        release_decisions = []
        coverage_rows = (
            list(coverage_export["rows"])
            if coverage_export is not None
            else [
                coverage
                for run in source_runs
                for coverage in (run.get("coverage") or [])
            ]
        )
        for coverage in coverage_rows:
                release_decisions.append({
                    "gate_cycle_id": coverage.get("gate_cycle_id"),
                    "venue_id": coverage.get("venue_id"),
                    "architecture": coverage.get("architecture", card.architecture),
                    "object_type": coverage.get("object_type"),
                    "R1": release_cycle_decision(
                        coverage,
                        source_status=card.status,
                        release="R1",
                        required_ticket_statuses=required_ticket_statuses,
                    ),
                    "R2": release_cycle_decision(
                        coverage,
                        source_status=card.status,
                        release="R2",
                        required_ticket_statuses=required_ticket_statuses,
                    ),
                })
        sources.append({
            **asdict(card),
            "runs": source_runs,
            "population_coverage_export": (
                None
                if coverage_export is None
                else {
                    key: coverage_export[key]
                    for key in ("path", "byte_hash", "export_hash", "row_count")
                }
            ),
            "release_decisions": release_decisions,
            "required_ticket_statuses": required_ticket_statuses,
            "registry_exclusion": (
                None if card.status in RELEASE_SOURCE_STATUSES
                else {"status": card.status, "reason": card.notes}
            ),
        })
    body = {
        "schema": "observatory.source-atlas/2",
        "release_rules": RELEASE_RULES,
        "sources": sources,
        "unregistered_run_sources": sorted(set(runs) - {card.source_id for card in source_cards()}),
        "unregistered_coverage_sources": sorted(
            set(coverage_exports) - {card.source_id for card in source_cards()}
        ),
        "coverage_export_required_sources": sorted(coverage_export_required_sources),
        "evidence_files": _evidence_files(run_root),
        "coverage_manifest_limitation": (
            "Run manifests cap embedded coverage rows at 100; a source with "
            "coverage_truncated=true cannot be frozen for R1 until its population report "
            "or a verified full coverage export is included in evidence_files and used for decisions."
        ),
    }
    body["frozen"] = not (
        body["unregistered_run_sources"]
        or body["unregistered_coverage_sources"]
        or body["coverage_export_required_sources"]
    )
    body["release_snapshot_id"] = f"obs-release-{content_hash(json.dumps(body, sort_keys=True))[:20]}"
    body["atlas_hash"] = content_hash(json.dumps(body, sort_keys=True))
    return body


def write_source_atlas(run_root: Path, output: Path) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(build_source_atlas(run_root), indent=2, sort_keys=True) + "\n")
    return output
