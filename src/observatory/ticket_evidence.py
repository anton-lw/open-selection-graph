"""Machine-readable acceptance evidence for the complete A-Q ticketbook."""

from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

import yaml

from .ids import content_hash

TICKETBOOK_EPICS = tuple("ABCDEFGHIJKLMNOPQ")


def ticketbook_ids(ticketbook: Path) -> tuple[str, ...]:
    found = re.findall(r"^### ([A-Z]\d+) —", ticketbook.read_text(), flags=re.MULTILINE)
    return tuple(found)


def audit_ticket_evidence(workspace: Path) -> dict[str, Any]:
    config_path = workspace / "configs" / "observatory" / "ticket_evidence.yaml"
    config = yaml.safe_load(config_path.read_text())
    requirements_path = workspace / str(config.get("requirements") or "")
    requirements = yaml.safe_load(requirements_path.read_text())
    waves_path = workspace / "configs" / "observatory" / "release_waves.yaml"
    waves = yaml.safe_load(waves_path.read_text())
    expected = ticketbook_ids(workspace / "TICKETBOOK_OPEN_SELECTION_GRAPH.md")
    requirement_rows = requirements.get("tickets") or []
    requirement_ids = tuple(str(row["id"]) for row in requirement_rows)
    requirement_by_id = {str(row["id"]): row for row in requirement_rows}
    wave_by_id = {
        str(ticket_id): str(wave)
        for wave, wave_row in (waves.get("waves") or {}).items()
        for ticket_id in wave_row.get("tickets") or []
    }
    wave_ids = [
        str(ticket_id)
        for wave_row in (waves.get("waves") or {}).values()
        for ticket_id in wave_row.get("tickets") or []
    ]
    wave_duplicates = sorted(ticket for ticket, count in Counter(wave_ids).items() if count > 1)
    wave_missing = sorted(set(expected) - set(wave_ids))
    wave_unexpected = sorted(set(wave_ids) - set(expected))
    rows = config.get("tickets") or []
    configured = tuple(str(row.get("id")) for row in rows)
    duplicates = sorted(ticket for ticket, count in Counter(configured).items() if count > 1)
    missing = sorted(set(expected) - set(configured))
    unexpected = sorted(set(configured) - set(expected))
    audited = []
    for row in rows:
        evidence = []
        for relative in row.get("evidence") or []:
            path = workspace / str(relative)
            evidence.append({
                "path": str(relative), "exists": path.exists(),
                "kind": "directory" if path.is_dir() else "file",
                "size_bytes": path.stat().st_size if path.is_file() else None,
            })
        issues = []
        status = row.get("status")
        if status in {"complete", "partial"} and not evidence:
            issues.append("no evidence paths")
        if any(not item["exists"] for item in evidence):
            issues.append("missing evidence path")
        if status == "complete" and row.get("gap"):
            issues.append("complete ticket declares a gap")
        if status in {"partial", "pending"} and not row.get("gap"):
            issues.append("incomplete ticket omits its acceptance gap")
        if status not in {"complete", "partial", "pending"}:
            issues.append("invalid status")
        audited.append(
            {
                **row,
                "release_wave": wave_by_id.get(str(row.get("id"))),
                "evidence": evidence,
                "issues": issues,
            }
        )
    counts = Counter(str(row.get("status")) for row in rows)
    status_by_id = {str(row.get("id")): str(row.get("status")) for row in rows}

    def grouped_counts(key) -> dict[str, dict[str, int]]:
        groups: dict[str, Counter[str]] = {}
        for ticket_id in expected:
            group = str(key(ticket_id))
            groups.setdefault(group, Counter())[status_by_id.get(ticket_id, "missing")] += 1
        return {group: dict(counter) for group, counter in sorted(groups.items())}

    result: dict[str, Any] = {
        "schema": "observatory.ticket-evidence-audit/1",
        "scope": list(TICKETBOOK_EPICS), "expected_ticket_count": len(expected),
        "configured_ticket_count": len(rows), "expected_ids": list(expected),
        "requirements_path": str(requirements_path.relative_to(workspace)),
        "requirements_ticket_count": len(requirement_ids),
        "requirements_match_ticketbook": requirement_ids == expected,
        "release_waves_path": str(waves_path.relative_to(workspace)),
        "release_wave_ticket_count": len(wave_ids),
        "release_wave_duplicates": wave_duplicates,
        "release_wave_missing": wave_missing,
        "release_wave_unexpected": wave_unexpected,
        "duplicates": duplicates, "missing": missing, "unexpected": unexpected,
        "status_counts": dict(counts),
        "status_by_epic": grouped_counts(lambda ticket_id: requirement_by_id[ticket_id]["epic"]),
        "status_by_release_wave": grouped_counts(
            lambda ticket_id: wave_by_id.get(ticket_id, "missing")
        ),
        "status_by_priority": grouped_counts(
            lambda ticket_id: requirement_by_id[ticket_id]["priority"]
        ),
        "gate_status_counts": grouped_counts(
            lambda ticket_id: "gate" if requirement_by_id[ticket_id]["gate"] else "non_gate"
        ),
        "tickets": audited,
    }
    result["passes_structure"] = not (
        duplicates
        or missing
        or unexpected
        or requirement_ids != expected
        or wave_duplicates
        or wave_missing
        or wave_unexpected
        or tuple(config.get("scope") or []) != TICKETBOOK_EPICS
        or any(row["issues"] for row in audited)
    )
    result["all_acceptance_complete"] = (
        result["passes_structure"] and counts["complete"] == len(expected)
    )
    result["audit_hash"] = content_hash(json.dumps(result, sort_keys=True))
    return result


def write_ticket_evidence_audit(workspace: Path, output: Path) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(audit_ticket_evidence(workspace), indent=2, sort_keys=True) + "\n"
    )
    return output
