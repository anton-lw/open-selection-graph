"""Evidence-bounded historical policy archive.

Only observed, dated versions can support a policy-change row.  Undated live
pages remain current pointers and are never projected backward.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .ids import content_hash
from .storage import ObservatoryCatalog
from .storage_guard import storage_preflight


def build_policy_history(lake: Path, output: Path) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    storage_preflight(output, projected_input_bytes=0, projected_output_bytes=32 * 1024 * 1024)
    with ObservatoryCatalog(lake).connect() as connection:
        policies = connection.execute(
            """
            SELECT policy_version_id, gate_id, effective_at, valid_to, policy_url,
                   content_hash, date_confidence, source_id, source_object_id,
                   observed_at, criteria_json, rubric_json, stage_rules_json
            FROM policy_version
            ORDER BY gate_id, effective_at NULLS LAST, observed_at, policy_version_id
            """
        ).fetchdf()
    rows: list[dict[str, Any]] = []
    undated = 0
    for gate_id, frame in policies.groupby("gate_id", dropna=False):
        previous = None
        for item in frame.itertuples(index=False):
            effective_at = None if pd.isna(item.effective_at) else item.effective_at
            valid_to = None if pd.isna(item.valid_to) else item.valid_to
            observed_at = None if pd.isna(item.observed_at) else item.observed_at
            dated = effective_at is not None and float(item.date_confidence or 0.0) > 0
            if not dated:
                undated += 1
            change_claim = bool(
                dated
                and previous is not None
                and previous["effective_at"] is not None
                and previous["content_hash"] != item.content_hash
            )
            evidence = {
                "pre_policy_version_id": previous["policy_version_id"] if change_claim else None,
                "pre_hash": previous["content_hash"] if change_claim else None,
                "post_policy_version_id": item.policy_version_id if change_claim else None,
                "post_hash": item.content_hash if change_claim else None,
            }
            rows.append(
                {
                    "policy_version_id": item.policy_version_id,
                    "gate_id": gate_id,
                    "effective_at": effective_at,
                    "valid_to": valid_to,
                    "observed_at": observed_at,
                    "source_id": item.source_id,
                    "policy_url": item.policy_url,
                    "source_object_id": item.source_object_id,
                    "content_hash": item.content_hash,
                    "date_confidence": item.date_confidence,
                    "date_status": "dated_observation" if dated else "current_pointer_not_backprojected",
                    "supports_policy_change_claim": change_claim,
                    "pre_post_evidence_json": json.dumps(evidence, sort_keys=True),
                    "structured_fact_hash": content_hash(
                        json.dumps(
                            [item.criteria_json, item.rubric_json, item.stage_rules_json],
                            sort_keys=True,
                            default=str,
                        )
                    ),
                }
            )
            if dated:
                previous = {
                    "policy_version_id": item.policy_version_id,
                    "effective_at": effective_at,
                    "content_hash": item.content_hash,
                }
    path = output / "historical_policy_archive.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path, compression="zstd")
    change_rows = [row for row in rows if row["supports_policy_change_claim"]]
    invalid_changes = [
        row
        for row in change_rows
        if not json.loads(row["pre_post_evidence_json"])["pre_hash"]
        or not json.loads(row["pre_post_evidence_json"])["post_hash"]
    ]
    report: dict[str, Any] = {
        "schema": "observatory.policy-history-report/1",
        "policy_versions": len(rows),
        "gates": int(policies["gate_id"].nunique()),
        "dated_versions": len(rows) - undated,
        "undated_current_pointers": undated,
        "policy_change_claims": len(change_rows),
        "change_claims_without_pre_post_evidence": len(invalid_changes),
        "backprojected_undated_pages": 0,
        "allowed_evidence_modes": ["dated source snapshot", "source repository history", "archived pointer/hash"],
    }
    report["passes"] = not invalid_changes and report["backprojected_undated_pages"] == 0
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    (output / "policy_history_report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report
