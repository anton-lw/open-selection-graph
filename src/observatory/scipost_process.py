"""Acceptance report for a dated SciPost current-public process census."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .ids import content_hash


def _rows(
    lake_root: Path, table: str, *, source_id: str, query_hash: str
) -> list[dict[str, Any]]:
    prefix = f"run-{query_hash[:16]}"
    files = sorted(
        (lake_root / table / f"source_id={source_id}").glob(f"{prefix}*.parquet")
    )
    if not files:
        return []
    return pa.concat_tables([pq.ParquetFile(path).read() for path in files]).to_pylist()


def build_scipost_process_report(
    lake_root: Path,
    *,
    query_hash: str,
    provider_expected_series: int,
    found_series: int,
) -> dict[str, Any]:
    source_id = "scipost_process"
    gates = {
        row["gate_id"]: row
        for row in _rows(lake_root, "gate", source_id=source_id, query_hash=query_hash)
    }
    cycles = {
        row["gate_cycle_id"]: row
        for row in _rows(
            lake_root, "gate_cycle", source_id=source_id, query_hash=query_hash
        )
    }
    candidates = _rows(
        lake_root, "candidate", source_id=source_id, query_hash=query_hash
    )
    versions = _rows(
        lake_root, "candidate_version", source_id=source_id, query_hash=query_hash
    )
    events = _rows(
        lake_root, "candidate_gate_event", source_id=source_id, query_hash=query_hash
    )
    evaluations = _rows(
        lake_root, "evaluation", source_id=source_id, query_hash=query_hash
    )
    decisions = _rows(
        lake_root, "decision_event", source_id=source_id, query_hash=query_hash
    )
    lineages = _rows(
        lake_root, "lineage_edge", source_id=source_id, query_hash=query_hash
    )
    policies = _rows(
        lake_root, "policy_version", source_id=source_id, query_hash=query_hash
    )
    coverage = _rows(
        lake_root, "coverage_observation", source_id=source_id, query_hash=query_hash
    )

    versions_by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in versions:
        versions_by_candidate[str(row["candidate_id"])].append(row)
    version_gaps = {}
    expected_lineages = 0
    for candidate_id, rows in versions_by_candidate.items():
        numbers = {int(row["version_number"]) for row in rows}
        expected = set(range(1, max(numbers) + 1)) if numbers else set()
        if numbers != expected:
            version_gaps[candidate_id] = sorted(expected - numbers)
        expected_lineages += max(len(rows) - 1, 0)

    cycle_journal_year = {}
    for cycle_id, cycle in cycles.items():
        gate = gates.get(cycle["gate_id"], {})
        cycle_journal_year[cycle_id] = {
            "journal": gate.get("native_id"),
            "year": str(cycle.get("native_id") or "").rsplit("|", 1)[-1],
        }
    event_cycles = Counter(row["gate_cycle_id"] for row in events)
    cycle_counts = {
        cycle_id: {**details, "version_events": event_cycles.get(cycle_id, 0)}
        for cycle_id, details in sorted(cycle_journal_year.items())
    }
    evaluation_types = Counter(str(row["evaluation_type"]) for row in evaluations)
    report_types = {
        key: value for key, value in evaluation_types.items() if key.endswith("_report")
    }
    decision_outcomes = Counter(str(row.get("outcome_normalized")) for row in decisions)
    entry_stages = Counter(str(row["earliest_observed_stage"]) for row in events)
    policy_definitions = []
    for row in policies:
        policy_definitions.append({
            "native_id": row["native_id"],
            "policy_url": row.get("policy_url"),
            "stage_rules": json.loads(row["stage_rules_json"]),
            "anonymity_model": row.get("anonymity_model"),
        })
    attrition_declared = bool(
        coverage
        and all(
            "rejected or withdrawn pages removed"
            in " ".join(row.get("known_exclusions") or [])
            for row in coverage
        )
    )

    report: dict[str, Any] = {
        "schema": "observatory.scipost-process/1",
        "source_id": source_id,
        "query_hash": query_hash,
        "provider_expected_current_public_series": provider_expected_series,
        "found_current_public_series": found_series,
        "current_public_series_census_complete": found_series == provider_expected_series,
        "candidate_series_count": len(candidates),
        "version_count": len(versions),
        "candidate_gate_event_count": len(events),
        "lineage_edge_count": len(lineages),
        "expected_lineage_edge_count": expected_lineages,
        "unobserved_version_number_gap_count": len(version_gaps),
        "unobserved_version_number_gaps": version_gaps,
        "version_gap_scope_rule": (
            "only provider-linked public version pages are in scope; missing lower "
            "version numbers are structural public-page attrition, not invented records"
        ),
        "journal_year_graphs": cycle_counts,
        "journal_count": len({row["journal"] for row in cycle_counts.values()}),
        "year_count": len({row["year"] for row in cycle_counts.values()}),
        "evaluation_count": len(evaluations),
        "evaluation_types": dict(sorted(evaluation_types.items())),
        "report_types": dict(sorted(report_types.items())),
        "decision_count": len(decisions),
        "decision_outcomes": dict(sorted(decision_outcomes.items())),
        "entry_stages": dict(sorted(entry_stages.items())),
        "policy_definitions": policy_definitions,
        "structural_attrition_declared": attrition_declared,
        "observability_grade": "U",
        "entry_selection_estimands_admissible": False,
        "scope_statement": (
            "complete graph of every version/report/reply currently public at the "
            "post-assignment stage; not a historical or entry-complete submission pool"
        ),
    }
    required_rules = {
        "entry",
        "report_types",
        "all_public_contributions_vetted",
        "reporter_may_choose_public_anonymity",
        "binding_publish_or_reject_vote",
        "submission_exclusivity",
        "rejected_or_withdrawn_page_default",
    }
    report["passes"] = bool(
        report["current_public_series_census_complete"]
        and len(candidates) == found_series
        and len(versions) == len(events)
        and len(lineages) == expected_lineages
        and cycle_counts
        and all(row["version_events"] > 0 for row in cycle_counts.values())
        and set(entry_stages) == {"public_page_after_editor_assignment"}
        and report_types
        and "invited_report" in report_types
        and "contributed_report" in report_types
        and evaluation_types.get("author_reply", 0) > 0
        and policy_definitions
        and all(
            required_rules <= set(row["stage_rules"])
            and row["stage_rules"]["all_public_contributions_vetted"] is True
            for row in policy_definitions
        )
        and attrition_declared
        and report["observability_grade"] == "U"
        and not report["entry_selection_estimands_admissible"]
    )
    report["report_hash"] = content_hash(
        json.dumps(report, sort_keys=True, default=str)
    )
    return report


def write_scipost_process_report(
    lake_root: Path,
    output: Path,
    *,
    query_hash: str,
    provider_expected_series: int,
    found_series: int,
) -> Path:
    report = build_scipost_process_report(
        lake_root,
        query_hash=query_hash,
        provider_expected_series=provider_expected_series,
        found_series=found_series,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return output
