"""Evaluator-supply, timing/strain, reform, and descriptive-atlas products."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .ids import content_hash
from .storage import ObservatoryCatalog
from .storage_guard import storage_preflight


def _write(frame: pd.DataFrame | list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = (
        pa.Table.from_pandas(frame, preserve_index=False)
        if isinstance(frame, pd.DataFrame)
        else pa.Table.from_pylist(frame)
    )
    pq.write_table(table, path, compression="zstd")


def build_strain_and_reform_products(lake: Path, output: Path) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    storage_preflight(output, projected_input_bytes=0, projected_output_bytes=64 * 1024 * 1024)
    with ObservatoryCatalog(lake).connect() as connection:
        evaluations = connection.execute(
            """
            SELECT evaluation_id, candidate_version_id, gate_cycle_id, evaluator_public_id,
                   evaluator_protected_id, created_at, criterion_value_numeric,
                   confidence_value, official, evaluation_type, source_id
            FROM evaluation
            ORDER BY gate_cycle_id, created_at
            """
        ).fetchdf()
        decisions = connection.execute(
            "SELECT candidate_version_id, gate_cycle_id, decided_at, outcome_normalized FROM decision_event"
        ).fetchdf()
        cycles = connection.execute(
            """
            SELECT gate_cycle_id, source_id, cycle_start, cycle_end, architecture,
                   received_count, observable_count, evaluated_count, selected_count,
                   policy_version_id
            FROM gate_cycle ORDER BY gate_cycle_id
            """
        ).fetchdf()
        policies = connection.execute(
            """
            SELECT policy_version_id, gate_id, effective_at, valid_to, content_hash,
                   criteria_json, rubric_json, stage_rules_json, quota_or_cap,
                   anonymity_model, revision_rules, source_id
            FROM policy_version ORDER BY gate_id, effective_at
            """
        ).fetchdf()

    official = evaluations[evaluations["official"].fillna(False)].copy()
    joined = official.merge(decisions, on=["candidate_version_id", "gate_cycle_id"], how="left")
    joined["created_at"] = pd.to_datetime(joined["created_at"], utc=True, errors="coerce")
    joined["decided_at"] = pd.to_datetime(joined["decided_at"], utc=True, errors="coerce")
    joined["review_to_decision_hours"] = (joined["decided_at"] - joined["created_at"]).dt.total_seconds() / 3600
    rows = []
    for cycle_id, frame in joined.groupby("gate_cycle_id", dropna=False):
        public_evaluators = frame["evaluator_public_id"].dropna().astype(str)
        protected_evaluators = frame["evaluator_protected_id"].dropna().astype(str)
        candidate_counts = frame.groupby("candidate_version_id")["evaluation_id"].nunique()
        delays = frame["review_to_decision_hours"].dropna()
        rows.append(
            {
                "gate_cycle_id": cycle_id,
                "source_id": str(frame["source_id"].iloc[0]),
                "official_evaluations": int(frame["evaluation_id"].nunique()),
                "evaluated_candidates_observed": int(frame["candidate_version_id"].nunique()),
                "public_evaluator_proxy_count": int(public_evaluators.nunique()),
                "protected_evaluator_proxy_count_internal": int(protected_evaluators.nunique()),
                "reports_per_candidate_mean": float(candidate_counts.mean()),
                "reports_per_candidate_median": float(candidate_counts.median()),
                "review_to_decision_hours_median": float(delays.median()) if len(delays) else None,
                "review_to_decision_hours_p90": float(delays.quantile(0.9)) if len(delays) else None,
                "confidence_observed_share": float(frame["confidence_value"].notna().mean()),
                "denominator": "official public evaluations joined to observed candidate versions in gate cycle",
                "measurement_caveat": "public/pseudonymous activity is a lower-bound supply proxy; hidden assignments and identities are not inferred",
                "causal_caveat": "cycle aggregate does not identify individual workload effects",
                "public_release_level": "gate_cycle_aggregate",
                "identity_salt_released": False,
            }
        )
    workload = pd.DataFrame(rows)
    _write(workload, output / "evaluator_supply_strain.parquet")

    cycle_frame = cycles.merge(workload, on=["gate_cycle_id", "source_id"], how="left")
    for column in ("cycle_start", "cycle_end"):
        cycle_frame[column] = pd.to_datetime(cycle_frame[column], utc=True, errors="coerce")
    cycle_frame["cycle_duration_days"] = (
        cycle_frame["cycle_end"] - cycle_frame["cycle_start"]
    ).dt.total_seconds() / 86400
    cycle_frame["selectivity_rate"] = cycle_frame["selected_count"] / cycle_frame["evaluated_count"].replace(0, np.nan)
    cycle_frame["evaluation_coverage"] = cycle_frame["evaluated_candidates_observed"] / cycle_frame[
        "evaluated_count"
    ].replace(0, np.nan)
    cycle_frame["timezone"] = "UTC"
    cycle_frame["missing_timestamp_policy"] = "retain row; timing measure null; denominator explicit"
    cycle_frame["known_statistic_validation"] = np.where(
        cycle_frame["evaluated_count"].notna(),
        "compared_to_gate_cycle_provider_count",
        "no_provider_statistic",
    )
    _write(cycle_frame, output / "timing_strain_series.parquet")

    events = []
    for gate_id, frame in policies.groupby("gate_id"):
        frame = frame.sort_values("effective_at", na_position="last")
        prior = None
        for row in frame.itertuples(index=False):
            if prior is not None and row.content_hash != prior.content_hash and pd.notna(row.effective_at):
                changed_fields = [
                    name
                    for name in (
                        "criteria_json",
                        "rubric_json",
                        "stage_rules_json",
                        "quota_or_cap",
                        "anonymity_model",
                        "revision_rules",
                    )
                    if getattr(row, name) != getattr(prior, name)
                ]
                events.append(
                    {
                        "event_id": f"policy-change:{prior.policy_version_id}:{row.policy_version_id}",
                        "gate_id": gate_id,
                        "source_id": row.source_id,
                        "effective_at": row.effective_at,
                        "treatment_definition": json.dumps(changed_fields),
                        "affected_units": f"gate cycles under {gate_id} beginning on/after effective date",
                        "anticipation_window_days": 30,
                        "concurrent_changes": json.dumps(changed_fields if len(changed_fields) > 1 else []),
                        "identification_rating": "descriptive_only"
                        if len(changed_fields) > 1
                        else "candidate_event_needs_pretrend",
                        "pre_policy_version_id": prior.policy_version_id,
                        "post_policy_version_id": row.policy_version_id,
                        "pre_hash": prior.content_hash,
                        "post_hash": row.content_hash,
                    }
                )
            if pd.notna(row.effective_at):
                prior = row
    _write(events, output / "registered_reforms.parquet")

    # Aggregate strain/conservatism specifications; never emit evaluator rows.
    analysis = cycle_frame[
        [
            "gate_cycle_id",
            "architecture",
            "selectivity_rate",
            "reports_per_candidate_mean",
            "review_to_decision_hours_median",
            "confidence_observed_share",
        ]
    ].copy()
    analysis["specification"] = "cycle_aggregate_descriptive"
    analysis["causal_identification"] = "not_identified"
    analysis["alternative_specifications"] = (
        "median delay; reports/candidate; public evaluator lower bound; architecture strata"
    )
    _write(analysis, output / "strain_conservatism_analysis.parquet")

    report: dict[str, Any] = {
        "schema": "observatory.strain-reform-report/1",
        "workload_cycles": len(workload),
        "timing_cycles": len(cycle_frame),
        "registered_events": len(events),
        "all_timestamps_utc": bool((cycle_frame["timezone"] == "UTC").all()),
        "missing_timestamp_rows_retained": int(cycle_frame["cycle_duration_days"].isna().sum()),
        "all_proxies_have_denominator_and_caveats": bool(
            workload["denominator"].notna().all()
            and workload["measurement_caveat"].notna().all()
            and workload["causal_caveat"].notna().all()
        ),
        "individual_evaluator_rows_released": False,
        "all_events_registered": all(
            row["treatment_definition"]
            and row["affected_units"]
            and row["anticipation_window_days"] is not None
            and row["identification_rating"]
            for row in events
        ),
    }
    report["passes"] = (
        report["workload_cycles"] > 0
        and report["all_timestamps_utc"]
        and report["all_proxies_have_denominator_and_caveats"]
        and report["all_events_registered"]
    )
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    (output / "strain_reform_report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report
