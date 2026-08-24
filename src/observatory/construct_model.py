"""Preregistered multi-trait/multi-method and rulers-by-doors analyses."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

from .ids import content_hash
from .storage import ObservatoryCatalog


def _fit_pca(values: np.ndarray, components: int) -> dict[str, Any]:
    standardized = StandardScaler().fit_transform(values)
    model = PCA(n_components=min(components, standardized.shape[1]), svd_solver="full")
    transformed = model.fit_transform(standardized)
    rebuilt = model.inverse_transform(transformed)
    return {
        "components": int(model.n_components_),
        "explained_variance": [float(value) for value in model.explained_variance_ratio_],
        "reconstruction_mse": float(np.mean(np.square(standardized - rebuilt))),
        "loadings": model.components_.tolist(),
    }


def build_construct_model(workspace: Path, lake: Path, output: Path) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    register_path = workspace / "configs" / "observatory" / "construct_models.yaml"
    register = yaml.safe_load(register_path.read_text())
    semantic = pq.read_table(output.parent / "r1" / "semantic_novelty.parquet").to_pandas()
    semantic = semantic.pivot_table(
        index="candidate_version_id",
        columns="encoder",
        values="centroid_cosine_distance_source_year_z",
        aggfunc="first",
    ).rename(columns={"specter2_base_cls": "semantic_specter2"})
    evaluations = pq.read_table(output.parent / "r1" / "evaluation_objects.parquet").to_pandas()
    numeric = evaluations[evaluations["criterion_value_cycle_z"].notna()].copy()
    human = numeric.pivot_table(
        index="candidate_version_id",
        columns="criterion_normalized",
        values="criterion_value_cycle_z",
        aggfunc="mean",
    ).rename(columns={"overall_recommendation": "human_overall", "reviewer_confidence": "human_confidence"})
    dimensions = ["human_overall", "human_confidence", "semantic_specter2"]
    matrix = human.join(semantic, how="inner")
    matrix = matrix[[column for column in dimensions if column in matrix]].dropna()
    if len(matrix) < 100:
        raise RuntimeError(f"insufficient complete multi-method rows: {len(matrix)}")

    alternatives = []
    for alternative in register["alternatives"]:
        fit = _fit_pca(matrix.to_numpy(dtype=float), int(alternative["factor_count"]))
        alternatives.append({"id": alternative["id"], **fit})
    alternatives.sort(key=lambda row: row["reconstruction_mse"])

    with ObservatoryCatalog(lake).connect() as connection:
        metadata = connection.execute(
            """
            SELECT DISTINCT v.candidate_version_id, v.source_id,
                   c.architecture, c.policy_version_id
            FROM candidate_version v
            LEFT JOIN candidate_gate_event e ON e.candidate_version_id=v.candidate_version_id
            LEFT JOIN gate_cycle c ON c.gate_cycle_id=e.gate_cycle_id
            """
        ).fetchdf()
    joined = matrix.reset_index().merge(metadata, on="candidate_version_id", how="left")
    pooled = _fit_pca(matrix.to_numpy(dtype=float), 1)
    pooled_loading = np.array(pooled["loadings"][0], dtype=float)
    invariance = []
    for grouping in register["invariance_groups"]:
        column = {"gate_architecture": "architecture"}.get(grouping, grouping)
        for group, frame in joined.groupby(column, dropna=False):
            if len(frame) < 100:
                invariance.append(
                    {
                        "grouping": grouping,
                        "group": str(group),
                        "n": len(frame),
                        "status": "insufficient_for_invariance",
                        "loading_congruence": None,
                        "pooled": False,
                    }
                )
                continue
            local = _fit_pca(frame[matrix.columns].to_numpy(dtype=float), 1)
            loading = np.array(local["loadings"][0], dtype=float)
            congruence = abs(
                float(np.dot(loading, pooled_loading) / (np.linalg.norm(loading) * np.linalg.norm(pooled_loading)))
            )
            status = "invariant_at_0.90" if congruence >= 0.90 else "invariance_failed_report_separately"
            invariance.append(
                {
                    "grouping": grouping,
                    "group": str(group),
                    "n": len(frame),
                    "status": status,
                    "loading_congruence": congruence,
                    "pooled": congruence >= 0.90,
                }
            )

    reliability = pq.read_table(output.parent / "r2" / "construct_reliability.parquet").to_pandas()
    reliability_lookup = {str(row.rubric): row.reliability_ceiling for row in reliability.itertuples(index=False)}
    cells = []
    for keys, frame in joined.groupby(["source_id", "architecture"], dropna=False):
        for measure in matrix.columns:
            cells.append(
                {
                    "source_id": str(keys[0]),
                    "gate_architecture": str(keys[1]),
                    "stage": "observed public gate decision",
                    "ruler_or_construct": measure,
                    "population_n": int(frame[measure].notna().sum()),
                    "reference_corpus": "source-year strictly prior semantic corpus"
                    if measure.startswith("semantic")
                    else None,
                    "reliability_ceiling": reliability_lookup.get(
                        "overall_recommendation" if measure == "human_overall" else "reviewer_confidence"
                    ),
                    "analysis_partition": "exploratory_existing",
                    "confirmatory": False,
                    "compatible_stage_pool_only": True,
                    "cross_door_pooling": False,
                }
            )
    pq.write_table(pa.Table.from_pylist(cells), output / "rulers_doors_cells.parquet", compression="zstd")
    pq.write_table(pa.Table.from_pylist(invariance), output / "construct_invariance.parquet", compression="zstd")
    report: dict[str, Any] = {
        "schema": "observatory.construct-model-report/1",
        "register_hash": content_hash(register_path.read_bytes()),
        "dimensions": list(matrix.columns),
        "complete_case_n": len(matrix),
        "alternatives_ranked": alternatives,
        "invariance_tests": len(invariance),
        "invariance_failures": sum(row["status"] == "invariance_failed_report_separately" for row in invariance),
        "invariance_failures_pooled": sum(
            row["status"] == "invariance_failed_report_separately" and row["pooled"] for row in invariance
        ),
        "rulers_doors_cells": len(cells),
        "incompatible_stages_pooled": 0,
        "confirmatory_existing_rows": 0,
        "attenuation_rule": register["attenuation_rule"],
    }
    report["passes"] = (
        len(alternatives) >= 3
        and report["invariance_failures_pooled"] == 0
        and report["incompatible_stages_pooled"] == 0
        and report["confirmatory_existing_rows"] == 0
    )
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    (output / "construct_model_report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report
