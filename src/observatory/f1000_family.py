"""Run-specific acceptance report for F1000-family process graphs."""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Mapping

import pyarrow as pa
import pyarrow.parquet as pq

from .ids import content_hash


def _rows(
    lake_root: Path,
    table: str,
    *,
    source_id: str,
    query_hashes: Sequence[str],
) -> list[dict[str, Any]]:
    partition = lake_root / table / f"source_id={source_id}"
    files = sorted({path for query_hash in query_hashes for path in partition.glob(f"run-{query_hash[:16]}*.parquet")})
    if not files:
        return []
    combined = pa.concat_tables([pq.ParquetFile(path).read() for path in files])
    return combined.to_pylist()


def build_f1000_family_report(
    lake_root: Path,
    *,
    query_hash: str,
    query_hashes: Sequence[str] | None = None,
    provider_expected_objects: int,
    found_count: int,
    platform_census: Mapping[str, Mapping[str, Any]],
    acquired_by_platform: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    """Audit exact platform censuses, version chains, and review semantics."""
    source_id = "f1000_process"
    run_hashes = tuple(query_hashes or (query_hash,))
    if not run_hashes or len(set(run_hashes)) != len(run_hashes):
        raise ValueError("F1000 report query hashes must be non-empty and unique")
    gates = {row["gate_id"]: row for row in _rows(lake_root, "gate", source_id=source_id, query_hashes=run_hashes)}
    cycles = {
        row["gate_cycle_id"]: row
        for row in _rows(lake_root, "gate_cycle", source_id=source_id, query_hashes=run_hashes)
    }
    versions = _rows(lake_root, "candidate_version", source_id=source_id, query_hashes=run_hashes)
    events = _rows(lake_root, "candidate_gate_event", source_id=source_id, query_hashes=run_hashes)
    evaluations = _rows(lake_root, "evaluation", source_id=source_id, query_hashes=run_hashes)
    lineages = _rows(lake_root, "lineage_edge", source_id=source_id, query_hashes=run_hashes)
    policies = _rows(lake_root, "policy_version", source_id=source_id, query_hashes=run_hashes)
    decision_events = _rows(lake_root, "decision_event", source_id=source_id, query_hashes=run_hashes)
    artifacts = _rows(lake_root, "content_artifact", source_id=source_id, query_hashes=run_hashes)
    artifact_types = Counter(str(row.get("object_type")) for row in artifacts)

    platform_by_cycle = {
        cycle_id: str(gates.get(cycle["gate_id"], {}).get("native_id") or "unknown")
        for cycle_id, cycle in cycles.items()
    }
    event_platform_counts = Counter(platform_by_cycle.get(row["gate_cycle_id"], "unknown") for row in events)
    entry_stages = Counter(str(row.get("earliest_observed_stage")) for row in events)
    final_statuses = Counter(str(row.get("final_observed_stage")) for row in events)

    versions_by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in versions:
        versions_by_candidate[str(row["candidate_id"])].append(row)
    expected_lineages = sum(max(len(rows) - 1, 0) for rows in versions_by_candidate.values())
    multi_version_candidates = sum(len(rows) > 1 for rows in versions_by_candidate.values())
    version_gaps: dict[str, list[int]] = {}
    leading_version_gaps_only = True
    for candidate_id, rows in versions_by_candidate.items():
        observed = {int(row["version_number"]) for row in rows}
        expected = set(range(1, max(observed) + 1)) if observed else set()
        if observed != expected:
            version_gaps[candidate_id] = sorted(expected - observed)
            leading_version_gaps_only &= max(expected - observed) < min(observed)
    version_gap_slot_count = sum(len(gaps) for gaps in version_gaps.values())
    expected_lineages_with_unresolved_predecessors = expected_lineages + len(version_gaps)

    evaluation_types = Counter(str(row.get("evaluation_type")) for row in evaluations)
    recommendation_values = Counter(
        str(row["criterion_value"]) for row in evaluations if row.get("criterion_normalized") == "f1000_approval_status"
    )
    policy_rows = []
    for row in policies:
        policy_rows.append(
            {
                "platform_id": gates.get(row["gate_id"], {}).get("native_id"),
                "native_id": row["native_id"],
                "policy_url": row.get("policy_url"),
                "rubric": json.loads(row["rubric_json"]),
                "stage_rules": json.loads(row["stage_rules_json"]),
            }
        )

    census = {key: dict(value) for key, value in platform_census.items()}
    included = {key: row for key, row in census.items() if row.get("included")}
    excluded = {key: row for key, row in census.items() if not row.get("included")}
    expected_by_platform = {key: int(row.get("expected_version_count") or 0) for key, row in included.items()}
    normalized_by_platform = dict(sorted(event_platform_counts.items()))
    acquired = {key: int((acquired_by_platform or expected_by_platform).get(key, 0)) for key in sorted(included)}
    reconciliation = {
        key: {
            "provider_expected": expected_by_platform[key],
            "acquired": acquired[key],
            "normalized_versions": int(event_platform_counts.get(key, 0)),
            "normalization_ratio": (event_platform_counts.get(key, 0) / acquired[key] if acquired[key] else None),
            "unresolved_after_acquisition": max(acquired[key] - int(event_platform_counts.get(key, 0)), 0),
        }
        for key in sorted(included)
    }
    normalized_count = len(versions)
    normalization_ratio = normalized_count / found_count if found_count else None

    report: dict[str, Any] = {
        "schema": "observatory.f1000-family-process/1",
        "source_id": source_id,
        "query_hash": query_hash,
        "shard_query_hashes": list(run_hashes),
        "provider_expected_objects": provider_expected_objects,
        "provider_acquired_objects": found_count,
        "found_count": found_count,
        "completed_acquisition_census": found_count == provider_expected_objects,
        "completed_census": found_count == provider_expected_objects,
        "platform_census": census,
        "included_platforms": sorted(included),
        "excluded_platforms": sorted(excluded),
        "platform_reconciliation": reconciliation,
        "acquired_by_platform": acquired,
        "normalized_by_platform": normalized_by_platform,
        "normalized_version_count": normalized_count,
        "normalization_ratio": normalization_ratio,
        "unresolved_after_acquisition": max(found_count - normalized_count, 0),
        "normalization_missingness_rule": (
            "Every provider-listed URL is acquired. A URL that cannot be normalized after the "
            "documented recovery cascade remains explicitly unresolved/censored; its metadata, "
            "reviews, responses, and status are never imputed."
        ),
        "candidate_series_count": len(versions_by_candidate),
        "version_count": len(versions),
        "multi_version_candidate_count": multi_version_candidates,
        "version_gap_count": len(version_gaps),
        "version_gap_slot_count": version_gap_slot_count,
        "leading_version_gaps_only": leading_version_gaps_only,
        "version_gaps": version_gaps,
        "lineage_edge_count": len(lineages),
        "expected_observed_only_lineage_edge_count": expected_lineages,
        "expected_lineage_edge_count": expected_lineages_with_unresolved_predecessors,
        "unresolved_predecessor_rule": (
            "A normalized later version may retain one declared edge to its immediately "
            "preceding provider-listed version even when that predecessor is among the "
            "explicitly unresolved acquisitions. Leading version slots remain missing; "
            "no candidate-version row is synthesized."
        ),
        "candidate_gate_event_count": len(events),
        "entry_stages": dict(sorted(entry_stages.items())),
        "final_statuses": dict(sorted(final_statuses.items())),
        "evaluation_count": len(evaluations),
        "evaluation_types": dict(sorted(evaluation_types.items())),
        "approval_statuses": dict(sorted(recommendation_values.items())),
        "not_approved_is_rejection": False,
        "decision_event_count": len(decision_events),
        "acquisition_artifact_types": dict(sorted(artifact_types.items())),
        "later_version_recovery_count": artifact_types.get("article_process_later_version_recovery", 0),
        "crossref_metadata_recovery_count": artifact_types.get("article_process_crossref_metadata_recovery", 0),
        "crossref_metadata_recovery_rule": (
            "When an enumerated XML and its public HTML both fail, an explicitly "
            "hash-pinned Crossref bibliographic row may recover only the parent DOI, "
            "title, date, and licence. No review or response content is imputed."
        ),
        "later_version_recovery_rule": (
            "An enumerated empty XML plus failing HTML may use the immediately later "
            "valid JATS version for parent metadata only; intended version identity is "
            "retained and later-version review sub-articles are excluded from the surrogate."
        ),
        "policy_definitions": sorted(policy_rows, key=lambda row: str(row["platform_id"])),
        "indexing_status_scope": (
            "native public peer-review status retained; bibliographic indexing requires "
            "the separately versioned Crossref/Europe PMC relation join"
        ),
        "entry_selection_estimands_admissible": False,
        "hidden_stage": "pre-publication editorial screen",
    }
    report["passes"] = bool(
        report["completed_acquisition_census"]
        and len(included) >= 4
        and all(row.get("exclusion_reason") for row in excluded.values())
        and all(row["provider_expected"] == row["acquired"] for row in reconciliation.values())
        and all((row["normalization_ratio"] or 0) >= 0.99 for row in reconciliation.values())
        and len(versions) == len(events)
        and set(entry_stages) == {"publication_after_editorial_screen"}
        and (
            not version_gaps
            or (leading_version_gaps_only and version_gap_slot_count <= report["unresolved_after_acquisition"])
        )
        and len(lineages) == expected_lineages_with_unresolved_predecessors
        and multi_version_candidates > 0
        and evaluations
        and any("report" in kind for kind in evaluation_types)
        and any(kind in {"response", "author-comment"} for kind in evaluation_types)
        and len(policy_rows) == len(included)
        and all(row["stage_rules"].get("not_approved_is_rejection") is False for row in policy_rows)
        and not decision_events
    )
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True, default=str))
    return report


def write_f1000_family_report(
    lake_root: Path,
    output: Path,
    *,
    query_hash: str,
    query_hashes: Sequence[str] | None = None,
    provider_expected_objects: int,
    found_count: int,
    platform_census: Mapping[str, Mapping[str, Any]],
    acquired_by_platform: Mapping[str, int] | None = None,
) -> Path:
    report = build_f1000_family_report(
        lake_root,
        query_hash=query_hash,
        query_hashes=query_hashes,
        provider_expected_objects=provider_expected_objects,
        found_count=found_count,
        platform_census=platform_census,
        acquired_by_platform=acquired_by_platform,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lake-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--query-hash", required=True)
    parser.add_argument("--expected", type=int, required=True)
    parser.add_argument("--found", type=int, required=True)
    parser.add_argument("--platform-census", type=Path, required=True)
    args = parser.parse_args()
    output = write_f1000_family_report(
        args.lake_root,
        args.output,
        query_hash=args.query_hash,
        provider_expected_objects=args.expected,
        found_count=args.found,
        platform_census=json.loads(args.platform_census.read_text()),
    )
    return 0 if json.loads(output.read_text())["passes"] else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
