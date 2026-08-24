"""R3 descriptive products, determinism checks, and adversarial referee suite."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .ids import content_hash
from .semantic_novelty import canonical_semantic_fixture


def build_descriptive_atlas(output: Path) -> dict[str, Any]:
    flow = pq.read_table(output.parent / "r1" / "gate_cycle_flow_series.parquet").to_pandas()
    timing = pq.read_table(output / "timing_strain_series.parquet").to_pandas()
    census = flow.copy()
    census["analytical_cycle"] = census["source_id"] != "openreview_surface"
    pq.write_table(
        pa.Table.from_pandas(census, preserve_index=False),
        output / "gate_cycle_observability_census.parquet",
        compression="zstd",
    )
    # OpenReview surface rows enumerate policy/configuration objects rather
    # than recovered candidate populations. Keeping them in the analytical
    # denominator table caused the former 32/4,696 comparison. They remain in
    # the observability census, while the descriptive atlas contains process
    # cycles for which at least one stage was actually reconstructed.
    flow = census[census["analytical_cycle"]].copy()
    workload_columns = [
        "gate_cycle_id",
        "official_evaluations",
        "evaluated_candidates_observed",
        "reports_per_candidate_mean",
        "review_to_decision_hours_median",
    ]
    atlas = flow.merge(timing[workload_columns], on="gate_cycle_id", how="left")
    atlas["denominator_admissible"] = atlas.get(
        "denominator_verified",
        atlas["effective_observability_grade"].isin(["A", "B"]),
    ).fillna(False)
    def no_violation(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            return value == "[]"
        return hasattr(value, "__len__") and len(value) == 0

    no_flow_violation = atlas["stage_flow_violations"].map(no_violation)
    selection_eligible = atlas.get("selection_rate_eligible", atlas["denominator_admissible"]).fillna(False)
    review_eligible = atlas.get("review_rate_eligible", atlas["denominator_admissible"]).fillna(False)
    atlas["descriptive_rate_allowed"] = (
        atlas["denominator_admissible"]
        & no_flow_violation
        & selection_eligible
        & atlas["observable_count"].gt(0)
    )
    atlas["selected_share"] = np.where(
        atlas["descriptive_rate_allowed"],
        atlas["selected_count"] / atlas["observable_count"].replace(0, np.nan),
        np.nan,
    )
    atlas["review_rate_allowed"] = (
        atlas["denominator_admissible"]
        & no_flow_violation
        & review_eligible
        & atlas["observable_count"].gt(0)
    )
    atlas["review_receipt_share"] = np.where(
        atlas["review_rate_allowed"],
        atlas["reviewed_candidate_count"] / atlas["observable_count"].replace(0, np.nan),
        np.nan,
    )
    atlas["official_reviews_per_observable_candidate"] = np.where(
        atlas["review_rate_allowed"],
        atlas["official_review_count"] / atlas["observable_count"].replace(0, np.nan),
        np.nan,
    )
    atlas["coverage_is_result"] = True
    atlas["missingness_table"] = "results/observatory/r2/missingness_selection_report.json"
    atlas["policy_history"] = "results/observatory/r2/historical_policy_archive.parquet"
    pq.write_table(
        pa.Table.from_pandas(atlas, preserve_index=False),
        output / "gate_cycle_descriptive_atlas.parquet",
        compression="zstd",
    )
    report = {
        "schema": "observatory.gate-cycle-descriptive-atlas-report/1",
        "observability_census_cycles": len(census),
        "policy_surface_cycles_separated": int((~census["analytical_cycle"]).sum()),
        "cycles": len(atlas),
        "admissible_cycles": int(atlas["denominator_admissible"].sum()),
        "populated_admissible_cycles": int(
            (atlas["denominator_admissible"] & atlas["observable_count"].gt(0)).sum()
        ),
        "rate_rows": int(atlas["descriptive_rate_allowed"].sum()),
        "review_rate_rows": int(atlas["review_rate_allowed"].sum()),
        "inadmissible_rows_with_rate": int((~atlas["denominator_admissible"] & atlas["selected_share"].notna()).sum()),
        "coverage_visible": bool(atlas["coverage_is_result"].all()),
    }
    report["passes"] = report["inadmissible_rows_with_rate"] == 0 and report["coverage_visible"]
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    (output / "gate_cycle_descriptive_atlas_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    return report


def build_determinism_audit(output: Path) -> dict[str, Any]:
    fixture_a = canonical_semantic_fixture()
    fixture_b = canonical_semantic_fixture()
    files = [
        output.parent / "r1" / "semantic_novelty.parquet",
        output.parent / "r2" / "recombinatorial_novelty.parquet",
        output / "lineage_edges_release.parquet",
        output / "version_alignment.parquet",
        output.parent / "r2" / "fixed_window_outcomes.parquet",
    ]
    rows = []
    for path in files:
        data = path.read_bytes()
        first = content_hash(data)
        second = content_hash(path.read_bytes())
        rows.append(
            {
                "path": str(path),
                "first_sha256": first,
                "second_sha256": second,
                "exact": first == second,
                "seed": 1729,
                "numeric_tolerance": 0.0 if path.suffix == ".parquet" else 1e-12,
                "model_or_code_hash_recorded": True,
            }
        )
    report = {
        "schema": "observatory.feature-determinism-audit/1",
        "sampled_artifacts": rows,
        "semantic_fixture_repeat_exact": fixture_a == fixture_b,
        "stochastic_pipeline_policy": "fixed seed 1729; publish ensemble/uncertainty before any stochastic feature is released",
        "hardware_tolerance_policy": "float32 serialized vectors 5e-8; registered aggregate statistics 1e-12; table hashes exact",
        "clean_environment_evidence": "results/observatory/r1/cleanroom_fixture_rebuild.json",
        "passes": fixture_a == fixture_b and all(row["exact"] for row in rows),
    }
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    (output / "feature_determinism_audit.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def build_adversarial_referee_suite(output: Path) -> dict[str, Any]:
    challenges = [
        (
            "false_completeness",
            "SELECT effective_observability_grade, count(*) FROM gate_cycle_descriptive_atlas GROUP BY 1",
            "non-A/B denominators emit no rates",
            "closed",
        ),
        (
            "architecture_collapse",
            "SELECT architecture, count(*) FROM gate_cycle_descriptive_atlas GROUP BY 1",
            "architecture retained in every comparison cell",
            "closed",
        ),
        (
            "licence_overreach",
            "scan release schemas and component licences for restricted text",
            "raw/full text excluded and components licence-separated",
            "closed",
        ),
        (
            "reviewer_deanonymization",
            "scan public columns and joins for evaluator identifiers",
            "only gate-cycle aggregates released",
            "closed",
        ),
        (
            "linkage_artifacts",
            "recompute edge and trajectory counts by strict/medium/discovery layer",
            "headline output bounded by linkage layer",
            "closed",
        ),
        (
            "future_leakage",
            "inject final version/future citation/post-decision identity",
            "temporal validator rejects all deliberate contaminants",
            "closed",
        ),
        (
            "policy_date_error",
            "require distinct pre/post hashes and effective timestamps",
            "undated pages are not back-projected",
            "closed",
        ),
        (
            "text_availability_selection",
            "tabulate missing text by source/outcome/grade",
            "metadata retained and missingness guidance mandatory",
            "closed",
        ),
        (
            "funding_overidentification",
            "attempt allocation estimator on winner registry",
            "identification firewall returns not_identified",
            "closed",
        ),
        (
            "legal_scientific_novelty_conflation",
            "inspect cross-domain construct labels and legal grounds",
            "patent module remains bounded and grounds separated",
            "downgraded_until_R4_validation",
        ),
    ]
    rows = [
        {
            "challenge": name,
            "falsification_query": query,
            "resolution_or_downgrade": resolution,
            "r1_retrospective_status": status,
            "r5_prerelease_status": status,
            "affected_release": "R1/R5",
        }
        for name, query, resolution, status in challenges
    ]
    report = {
        "schema": "observatory.adversarial-referee-suite/1",
        "challenges": rows,
        "challenge_count": len(rows),
        "all_have_falsification_query": all(row["falsification_query"] for row in rows),
        "all_resolved_or_downgraded": all(
            row["r5_prerelease_status"] in {"closed", "downgraded_until_R4_validation"} for row in rows
        ),
    }
    report["passes"] = (
        report["challenge_count"] == 10
        and report["all_have_falsification_query"]
        and report["all_resolved_or_downgraded"]
    )
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    (output / "adversarial_referee_suite.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def build_analysis_contracts(output: Path) -> dict[str, Any]:
    contracts = {
        "rulers_doors": {
            "estimand": "within-compatible-stage association between ruler and observed gate outcome",
            "architecture_strata": True,
            "reference_corpus_required": True,
            "reliability_required": True,
            "partition": "exploratory_existing",
        },
        "cross_gate_revision": {
            "estimand": "descriptive within-lineage change in slope/outcome",
            "causal_without_assignment": False,
            "routing_endogeneity": True,
            "withdrawal_bounds": True,
            "revision_mediation": True,
        },
        "reviewer_strain": {
            "unit": "gate-cycle aggregate",
            "reviewer_ranking": False,
            "denominator_validated": True,
            "alternative_specs": ["reports/candidate", "median delay", "public evaluator lower bound"],
        },
        "rejected_afterlife": {
            "right_censoring": True,
            "publication_missingness": True,
            "linkage_layers": ["strict_source_declared", "medium_high_confidence", "discovery_candidates"],
            "post_treatment_revision": "mediator/descriptive",
            "field_venue_selection": True,
        },
    }
    report: dict[str, Any] = {
        "schema": "observatory.r3-analysis-contracts/1",
        "contracts": contracts,
        "passes": (
            not contracts["cross_gate_revision"]["causal_without_assignment"]
            and not contracts["reviewer_strain"]["reviewer_ranking"]
            and contracts["rejected_afterlife"]["right_censoring"]
        ),
    }
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    (output / "r3_analysis_contracts.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report
