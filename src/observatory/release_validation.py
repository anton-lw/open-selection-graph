"""Machine-readable, fail-closed validation for OSG release packages."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import yaml

from .audit import audit_no_paid_api_policy
from .ids import content_hash
from .limitations import audit_pointer_rebuild_registry
from .operations import governance_audit
from .ticket_evidence import audit_ticket_evidence


def evaluate_release_gate(
    *,
    release_id: str,
    requirements: Iterable[Mapping[str, Any]],
    evidence: Iterable[Mapping[str, Any]],
    ticket_structure_passes: bool,
    policy_passes: bool,
    governance_passes: bool,
    pointer_registry_passes: bool = True,
    required_ticket_ids: Iterable[str] | None = None,
    scope_reductions: Mapping[str, Mapping[str, str]] | None = None,
) -> dict[str, Any]:
    """Evaluate P0 tickets after explicit, documented component removals.

    A scope reduction removes the affected component from the release. It is
    not a waiver: each row must name the component, reason, affected release,
    and rollback/narrowing path, and the excluded ticket is recorded.
    """
    requirement_rows = [dict(row) for row in requirements]
    evidence_by_id = {str(row["id"]): dict(row) for row in evidence}
    requested = (
        {str(value) for value in required_ticket_ids}
        if required_ticket_ids is not None
        else {str(row["id"]) for row in requirement_rows}
    )
    reductions = {str(key): dict(value) for key, value in (scope_reductions or {}).items()}
    required_reduction_fields = {
        "component", "reason", "affected_release", "rollback_or_narrowing_path",
    }
    invalid_reductions = []
    for ticket_id, row in reductions.items():
        missing = sorted(required_reduction_fields - set(row))
        if ticket_id not in requested:
            missing.append("ticket_not_in_release_scope")
        if any(not str(row.get(name) or "").strip() for name in required_reduction_fields):
            missing.append("empty_scope_reduction_field")
        if "waiver" in row:
            missing.append("waivers_are_prohibited")
        if missing:
            invalid_reductions.append({"ticket_id": ticket_id, "issues": sorted(set(missing))})
    active = requested - set(reductions)
    p0_tickets = {
        str(row["id"])
        for row in requirement_rows
        if str(row.get("priority")) == "P0" and str(row["id"]) in active
    }
    missing_evidence_rows = sorted(active - set(evidence_by_id))
    p0_failures = [
        {
            "ticket_id": ticket_id,
            "status": evidence_by_id.get(ticket_id, {}).get("status", "missing"),
            "gap": evidence_by_id.get(ticket_id, {}).get("gap"),
        }
        for ticket_id in sorted(p0_tickets)
        if evidence_by_id.get(ticket_id, {}).get("status") != "complete"
    ]
    checks = {
        "ticket_structure": bool(ticket_structure_passes),
        "no_paid_api_policy": bool(policy_passes),
        "governance_field_catalogue": bool(governance_passes),
        "pointer_rebuild_registry": bool(pointer_registry_passes),
        "scope_reductions_valid": not invalid_reductions,
        "required_ticket_rows_present": not missing_evidence_rows,
        "all_active_p0_tickets_complete": not p0_failures,
    }
    report: dict[str, Any] = {
        "schema": "observatory.release-validation/1",
        "release_id": release_id,
        "required_ticket_count": len(requested),
        "active_ticket_count": len(active),
        "active_p0_ticket_count": len(p0_tickets),
        "scope_reductions": reductions,
        "invalid_scope_reductions": invalid_reductions,
        "missing_evidence_rows": missing_evidence_rows,
        "p0_failures": p0_failures,
        "checks": checks,
        "packaging_allowed": all(checks.values()),
    }
    report["validation_hash"] = content_hash(json.dumps(report, sort_keys=True))
    return report


def build_release_validation(
    workspace: Path,
    *,
    release_id: str,
    required_ticket_ids: Iterable[str] | None = None,
    scope_reductions: Mapping[str, Mapping[str, str]] | None = None,
) -> dict[str, Any]:
    requirements_path = workspace / "configs" / "observatory" / "ticket_requirements.yaml"
    evidence_path = workspace / "configs" / "observatory" / "ticket_evidence.yaml"
    requirements = yaml.safe_load(requirements_path.read_text())
    evidence = yaml.safe_load(evidence_path.read_text())
    ticket_audit = audit_ticket_evidence(workspace)
    policy = audit_no_paid_api_policy(workspace)
    governance = governance_audit()
    pointer_registry = audit_pointer_rebuild_registry(workspace)
    report = evaluate_release_gate(
        release_id=release_id,
        requirements=requirements.get("tickets") or [],
        evidence=evidence.get("tickets") or [],
        ticket_structure_passes=bool(ticket_audit["passes_structure"]),
        policy_passes=bool(policy["passes"]),
        governance_passes=bool(governance["passes"]),
        pointer_registry_passes=bool(pointer_registry["passes"]),
        required_ticket_ids=required_ticket_ids,
        scope_reductions=scope_reductions,
    )
    unhashed = {key: value for key, value in report.items() if key != "validation_hash"}
    unhashed.update(
        {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "ticket_audit_hash": ticket_audit["audit_hash"],
            "governance_catalogue_hash": governance["catalogue_hash"],
        }
    )
    unhashed["validation_hash"] = content_hash(json.dumps(unhashed, sort_keys=True))
    return unhashed


def write_release_validation(
    workspace: Path,
    output: Path,
    *,
    release_id: str,
    required_ticket_ids: Iterable[str] | None = None,
    scope_reductions: Mapping[str, Mapping[str, str]] | None = None,
) -> Path:
    report = build_release_validation(
        workspace,
        release_id=release_id,
        required_ticket_ids=required_ticket_ids,
        scope_reductions=scope_reductions,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return output


def assert_release_packagable(report: Mapping[str, Any]) -> None:
    if not report.get("packaging_allowed"):
        failures = ",".join(str(row["ticket_id"]) for row in report.get("p0_failures") or [])
        raise RuntimeError(f"release packaging blocked by validation report; P0 failures={failures}")
