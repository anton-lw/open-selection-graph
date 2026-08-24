"""Acceptance-grade cohort report for provider-native eLife process data."""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .adapters.elife import NEW_MODEL_EFFECTIVE, SIGNIFICANCE_TERMS, STRENGTH_TERMS
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
    combined = pa.concat_tables([pq.ParquetFile(path).read() for path in files])
    return combined.to_pylist()


def build_elife_cohort_report(
    lake_root: Path,
    *,
    query_hash: str,
    provider_expected_objects: int,
    found_count: int,
) -> dict[str, Any]:
    """Summarize only one exact run, never all historical eLife shards."""
    source_id = "elife_process"
    cycles = {
        row["gate_cycle_id"]: row
        for row in _rows(lake_root, "gate_cycle", source_id=source_id, query_hash=query_hash)
    }
    events = _rows(
        lake_root, "candidate_gate_event", source_id=source_id, query_hash=query_hash
    )
    versions = _rows(
        lake_root, "candidate_version", source_id=source_id, query_hash=query_hash
    )
    evaluations = _rows(
        lake_root, "evaluation", source_id=source_id, query_hash=query_hash
    )
    policies = _rows(
        lake_root, "policy_version", source_id=source_id, query_hash=query_hash
    )
    coverage = _rows(
        lake_root, "coverage_observation", source_id=source_id, query_hash=query_hash
    )
    fallback_count = 0
    for row in coverage:
        match = re.search(
            r"official_detail_api_fallbacks=(\d+)", str(row.get("audit_status") or "")
        )
        if match:
            fallback_count = max(fallback_count, int(match.group(1)))

    cohort_counts: Counter[str] = Counter()
    year_counts: Counter[str] = Counter()
    entry_stages: Counter[str] = Counter()
    for event in events:
        cycle = cycles.get(event["gate_cycle_id"], {})
        native = str(cycle.get("native_id") or "unknown|unknown")
        cohort, _, year = native.partition("|")
        cohort_counts[cohort] += 1
        year_counts[f"{cohort}|{year}"] += 1
        entry_stages[str(event.get("earliest_observed_stage"))] += 1

    evaluation_types = Counter(str(row.get("evaluation_type")) for row in evaluations)
    criterion_values: dict[str, Counter[str]] = {}
    for row in evaluations:
        criterion = row.get("criterion_normalized")
        value = row.get("criterion_value")
        if criterion and value:
            criterion_values.setdefault(str(criterion), Counter())[str(value)] += 1

    version_labels = Counter(str(row.get("version_label")) for row in versions)
    vor_candidates = {
        row["candidate_id"]
        for row in versions
        if "version of record" in str(row.get("version_label") or "").lower()
    }
    reviewed_candidates = {row["candidate_id"] for row in versions}
    policy_definitions = []
    for row in sorted(policies, key=lambda item: str(item["native_id"])):
        policy_definitions.append({
            "cohort": row["native_id"],
            "effective_at": row.get("effective_at"),
            "policy_url": row.get("policy_url"),
            "criteria": json.loads(row["criteria_json"]),
            "rubric": json.loads(row["rubric_json"]),
            "stage_rules": json.loads(row["stage_rules_json"]),
            "date_confidence": row.get("date_confidence"),
        })

    report: dict[str, Any] = {
        "schema": "observatory.elife-cohorts/1",
        "source_id": source_id,
        "query_hash": query_hash,
        "provider_expected_objects": provider_expected_objects,
        "found_count": found_count,
        "count_reconciliation_ratio": (
            found_count / provider_expected_objects if provider_expected_objects else None
        ),
        "completed_cursor": found_count == provider_expected_objects,
        "official_detail_api_fallback_count": fallback_count,
        "process_page_complete_count": found_count - fallback_count,
        "process_page_complete_fraction": (
            (found_count - fallback_count) / found_count if found_count else None
        ),
        "full_process_page_census_complete": fallback_count == 0,
        "model_effective_at": NEW_MODEL_EFFECTIVE,
        "cohort_rule": (
            "sent_for_review >= model_effective_at => reviewed_preprint_model; "
            "earlier or unresolved => reviewed_preprint_pilot_or_legacy_transition"
        ),
        "cohort_counts": dict(sorted(cohort_counts.items())),
        "cohort_year_counts": dict(sorted(year_counts.items())),
        "entry_stages": dict(sorted(entry_stages.items())),
        "reviewed_candidate_count": len(reviewed_candidates),
        "version_of_record_candidate_count": len(vor_candidates),
        "version_of_record_is_decision": False,
        "version_labels": dict(sorted(version_labels.items())),
        "evaluation_types": dict(sorted(evaluation_types.items())),
        "observed_assessment_values": {
            key: dict(sorted(values.items()))
            for key, values in sorted(criterion_values.items())
        },
        "declared_assessment_vocabulary": {
            "significance_high_to_low": list(SIGNIFICANCE_TERMS),
            "strength_high_to_low": list(STRENGTH_TERMS),
            "numeric_direction": "higher_is_stronger",
        },
        "policy_definitions": policy_definitions,
        "hidden_stage": "editorial selection before sent for review",
        "entry_selection_estimands_admissible": False,
    }
    report["passes"] = bool(
        report["completed_cursor"]
        and set(entry_stages) == {"sent_for_review"}
        and policy_definitions
        and all(
            definition["stage_rules"].get("entry") == "sent_for_review"
            for definition in policy_definitions
        )
        and not report["version_of_record_is_decision"]
    )
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True, default=str))
    return report


def write_elife_cohort_report(
    lake_root: Path,
    output: Path,
    *,
    query_hash: str,
    provider_expected_objects: int,
    found_count: int,
) -> Path:
    report = build_elife_cohort_report(
        lake_root,
        query_hash=query_hash,
        provider_expected_objects=provider_expected_objects,
        found_count=found_count,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True, default=str) + "\n")
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lake-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--query-hash", required=True)
    parser.add_argument("--expected", type=int, required=True)
    parser.add_argument("--found", type=int, required=True)
    args = parser.parse_args()
    output = write_elife_cohort_report(
        args.lake_root,
        args.output,
        query_hash=args.query_hash,
        provider_expected_objects=args.expected,
        found_count=args.found,
    )
    report = json.loads(output.read_text())
    print(output)
    return 0 if report["passes"] else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
