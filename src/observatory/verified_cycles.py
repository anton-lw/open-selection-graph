"""Build the audited OpenReview cycle-level analytical input for OSG.

The row-level OpenReview census is retained in the durable acquisition lake.
This module exports the non-identifying cycle aggregates needed by the public
analytical release and binds them to the passing union audit.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .ids import content_hash
from .storage import ObservatoryCatalog


def _context(venue_id: str) -> str:
    lowered = venue_id.lower()
    for token, label in (
        ("research_fund", "funding"),
        ("oi_fund", "funding"),
        ("competition", "competition"),
        ("challenge", "competition"),
        ("workshop", "workshop"),
        ("conference", "conference"),
        ("proceedings", "proceedings"),
        ("papers", "journal_or_series"),
        ("university", "course_or_training"),
        ("hackathon", "course_or_training"),
        ("research_camp", "course_or_training"),
    ):
        if token in lowered:
            return label
    return "other_selection_process"


def _outcome_class(value: Any) -> str:
    label = str(value or "").strip().lower()
    if label.startswith(("accept", "conditional accept", "conditionally accept", "strong accept")):
        return "accepted"
    if label.startswith("reject"):
        return "rejected"
    if label.startswith("withdraw"):
        return "withdrawn"
    return "other_or_unresolved"


def build_verified_openreview_cycles(
    workspace: Path,
    normalized_snapshot: Path,
    output_path: Path,
) -> dict[str, Any]:
    evidence_root = workspace / "results" / "observatory"
    coverage_path = evidence_root / "openreview_api_population_coverage.json"
    audit_path = evidence_root / "openreview_process_audit.json"
    receipt_path = evidence_root / "modal_openreview_process_receipt.json"
    coverage = json.loads(coverage_path.read_text())
    audit = json.loads(audit_path.read_text())
    receipt = json.loads(receipt_path.read_text())
    candidate_rows = [row for row in coverage["rows"] if row["object_type"] == "candidate_state"]
    cycle_ids = [str(row["gate_cycle_id"]) for row in candidate_rows]
    if len(candidate_rows) != 176 or len(set(cycle_ids)) != 176:
        raise RuntimeError("verified OpenReview population must contain 176 unique cycles")

    with ObservatoryCatalog(normalized_snapshot).connect() as connection:
        connection.execute("CREATE TEMP TABLE verified_cycle_ids(gate_cycle_id VARCHAR)")
        connection.executemany(
            "INSERT INTO verified_cycle_ids VALUES (?)",
            [(cycle_id,) for cycle_id in cycle_ids],
        )
        cycle_metadata = {
            str(row[0]): {
                "gate_name": row[1],
                "field_of_study": row[2],
                "architecture": row[3],
                "cycle_start": row[4],
                "cycle_end": row[5],
            }
            for row in connection.execute(
                """SELECT gc.gate_cycle_id, g.name, g.domain, gc.architecture,
                          gc.cycle_start, gc.cycle_end
                   FROM gate_cycle gc
                   JOIN gate g USING(gate_id)
                   JOIN verified_cycle_ids v USING(gate_cycle_id)"""
            ).fetchall()
        }
        review_metrics = {
            str(row[0]): {
                "official_review_count": int(row[1]),
                "reviewed_candidate_count": int(row[2]),
            }
            for row in connection.execute(
                """SELECT v.gate_cycle_id,
                          count(DISTINCT e.evaluation_id) FILTER (
                            WHERE e.official OR e.evaluation_type='official_review'
                          ) AS official_reviews,
                          count(DISTINCT e.candidate_version_id) FILTER (
                            WHERE e.official OR e.evaluation_type='official_review'
                          ) AS reviewed_candidates
                   FROM verified_cycle_ids v
                   LEFT JOIN evaluation e USING(gate_cycle_id)
                   GROUP BY 1"""
            ).fetchall()
        }
        latest_decisions = connection.execute(
            """WITH ranked AS (
                   SELECT d.gate_cycle_id, d.candidate_version_id, d.outcome_normalized,
                          row_number() OVER (
                            PARTITION BY d.gate_cycle_id, d.candidate_version_id
                            ORDER BY d.decided_at DESC NULLS LAST,
                                     d.observed_at DESC NULLS LAST,
                                     d.decision_event_id DESC
                          ) AS position
                   FROM decision_event d
                   JOIN verified_cycle_ids v USING(gate_cycle_id)
               )
               SELECT gate_cycle_id, outcome_normalized
               FROM ranked WHERE position=1"""
        ).fetchall()
    decisions: dict[str, dict[str, int]] = {
        cycle_id: {label: 0 for label in ("accepted", "rejected", "withdrawn", "other_or_unresolved")}
        for cycle_id in cycle_ids
    }
    for cycle_id, outcome in latest_decisions:
        decisions[str(cycle_id)][_outcome_class(outcome)] += 1

    output: list[dict[str, Any]] = []
    for coverage_row in candidate_rows:
        cycle_id = str(coverage_row["gate_cycle_id"])
        venue_id = str(coverage_row["venue_id"])
        expected = int(coverage_row["expected_count"])
        found = int(coverage_row["found_count"])
        metadata = cycle_metadata.get(cycle_id, {})
        reviews = review_metrics.get(
            cycle_id,
            {"official_review_count": 0, "reviewed_candidate_count": 0},
        )
        outcome_counts = decisions[cycle_id]
        decided = sum(outcome_counts.values())
        output.append(
            {
                "gate_cycle_id": cycle_id,
                "venue_id": venue_id,
                "gate_name": metadata.get("gate_name") or venue_id.split("/", 1)[0],
                "platform": "OpenReview",
                "venue_family": venue_id.split("/", 1)[0],
                "selection_context": _context(venue_id),
                "field_of_study": metadata.get("field_of_study") or "unclassified",
                "architecture": metadata.get("architecture") or "unknown",
                "cycle_start": metadata.get("cycle_start"),
                "cycle_end": metadata.get("cycle_end"),
                "received_count": expected,
                "observable_count": found,
                "official_review_count": reviews["official_review_count"],
                "reviewed_candidate_count": reviews["reviewed_candidate_count"],
                "reviews_per_reviewed_candidate": (
                    reviews["official_review_count"] / reviews["reviewed_candidate_count"]
                    if reviews["reviewed_candidate_count"]
                    else None
                ),
                "accepted_count": outcome_counts["accepted"],
                "rejected_count": outcome_counts["rejected"],
                "withdrawn_count": outcome_counts["withdrawn"],
                "other_decision_count": outcome_counts["other_or_unresolved"],
                "decided_candidate_count": decided,
                "decision_observation_ratio": decided / found if found else None,
                "observability_grade": coverage_row["observability_grade"],
                "coverage_ratio": float(coverage_row["coverage_ratio"]),
                "earliest_public_stage": coverage_row["earliest_public_stage"],
                "known_hidden_stages": json.dumps(coverage_row["known_hidden_stages"], sort_keys=True),
                "audit_status": coverage_row["audit_status"],
                "source_id": "openreview_api",
                "union_audit_hash": receipt["union_hash"],
            }
        )
    output.sort(key=lambda row: row["venue_id"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(output), output_path, compression="zstd")
    populated = [row for row in output if row["observable_count"] > 0]
    report: dict[str, Any] = {
        "schema": "observatory.verified-openreview-cycle-metrics/1",
        "cycle_count": len(output),
        "populated_cycle_count": len(populated),
        "candidate_count": sum(row["observable_count"] for row in output),
        "official_review_count": sum(row["official_review_count"] for row in output),
        "reviewed_candidate_count_sum": sum(row["reviewed_candidate_count"] for row in output),
        "mean_official_reviews_per_cycle_all": sum(row["official_review_count"] for row in output)
        / len(output),
        "mean_official_reviews_per_populated_cycle": sum(
            row["official_review_count"] for row in populated
        )
        / len(populated),
        "platforms": {"OpenReview": len(output)},
        "fields": {
            field: sum(row["field_of_study"] == field for row in output)
            for field in sorted({row["field_of_study"] for row in output})
        },
        "selection_contexts": {
            context: sum(row["selection_context"] == context for row in output)
            for context in sorted({row["selection_context"] for row in output})
        },
        "checks": {
            "provider_union_audit_passes": bool(audit["passes"]),
            "receipt_complete": receipt["status"] == "complete",
            "all_cycles_grade_b": all(row["observability_grade"] == "B" for row in output),
            "all_denominators_reconcile": all(
                row["received_count"] == row["observable_count"] for row in output
            ),
            "all_coverage_ratios_one": all(row["coverage_ratio"] == 1.0 for row in output),
            "all_counts_nonnegative": all(
                all(row[field] >= 0 for field in (
                    "received_count",
                    "observable_count",
                    "official_review_count",
                    "reviewed_candidate_count",
                    "accepted_count",
                    "rejected_count",
                    "withdrawn_count",
                    "other_decision_count",
                ))
                for row in output
            ),
        },
        "source_hashes": {
            str(path.relative_to(workspace)): content_hash(path.read_bytes())
            for path in (coverage_path, audit_path, receipt_path)
        },
        "artifact": str(output_path.relative_to(workspace)),
        "artifact_hash": content_hash(output_path.read_bytes()),
        "scope_note": (
            "Grade B begins at the provider-audited public submission invitation; "
            "confidential screening and objects unreadable by the audited account remain outside the denominator."
        ),
    }
    report["passes"] = all(report["checks"].values())
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    report_path = output_path.with_name("openreview_verified_cycle_metrics_report.json")
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    parser.add_argument("--normalized-snapshot", type=Path, required=True)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("results/observatory/openreview_verified_cycle_metrics.parquet"),
    )
    arguments = parser.parse_args()
    workspace = arguments.workspace.resolve()
    output = arguments.output if arguments.output.is_absolute() else workspace / arguments.output
    report = build_verified_openreview_cycles(
        workspace,
        arguments.normalized_snapshot.resolve(),
        output,
    )
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
