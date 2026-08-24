"""Read-only reproduction checks against related programme artifacts.

This module never imports or writes a paper project. It treats the named files
as immutable external fixtures, independently recomputes the registered
semantic aggregations/validation statistics, and writes evidence only to the
OSG result tree.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .ids import content_hash


def reproduce_p2_semantic_fixture(workspace: Path, output: Path) -> dict[str, Any]:
    float32_csv_tolerance = 5e-8
    root = workspace / "results" / "p2" / "candidate"
    paths = {
        "measures": root / "semantic_cube_measures.csv",
        "frame": root / "analysis_frame.csv",
        "artifact": root / "semantic_cube.json",
        "sample": root / "cube_sample.csv",
        "ref100k_topk": root / "ref100k_topk.npy",
        "ref100k_centroid": root / "ref100k_centroid.npy",
        "ref680k_topk": root / "ref680k_topk.npy",
        "ref680k_centroid": root / "ref680k_centroid.npy",
    }
    missing = [str(path) for path in paths.values() if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"P2 read-only semantic fixtures missing: {missing}")
    measures = pd.read_csv(paths["measures"])
    frame = pd.read_csv(paths["frame"])
    sample = pd.read_csv(paths["sample"])
    artifact = json.loads(paths["artifact"].read_text())
    aggregation_checks = []
    for reference in ("100k", "680k"):
        topk = np.load(paths[f"ref{reference}_topk"])
        centroid = np.load(paths[f"ref{reference}_centroid"])
        aggregation_checks.append(
            {
                "reference": reference,
                "row_count": len(topk),
                "row_ids_exact": np.array_equal(measures["row_id"].to_numpy(), sample["row_id"].to_numpy()),
                "centroid_within_float32_csv_tolerance": bool(
                    np.allclose(
                        measures[f"sem_ref{reference}_centroid"].to_numpy(),
                        centroid,
                        rtol=0,
                        atol=float32_csv_tolerance,
                        equal_nan=True,
                    )
                ),
                "knn_within_float32_csv_tolerance": {
                    str(k): bool(
                        np.allclose(
                            measures[f"sem_ref{reference}_knn{k}"].to_numpy(),
                            np.nanmean(topk[:, :k], axis=1),
                            rtol=0,
                            atol=float32_csv_tolerance,
                            equal_nan=True,
                        )
                    )
                    for k in (1, 5, 10, 25, 50)
                },
            }
        )
    joined = frame.merge(measures, on="row_id", how="inner", validate="one_to_one")
    validation: dict[str, float] = {}
    for reference in ("100k", "680k"):
        committed = joined[f"novelty_sem_{reference}"].to_numpy(dtype=float)
        rebuilt = joined[f"sem_ref{reference}_knn10"].to_numpy(dtype=float)
        valid = np.isfinite(committed) & np.isfinite(rebuilt)
        validation[f"pearson_recomputed_vs_committed_{reference}_knn10"] = float(
            np.corrcoef(committed[valid], rebuilt[valid])[0, 1]
        )
        validation[f"max_abs_diff_{reference}"] = float(
            np.max(np.abs(committed[valid] - rebuilt[valid]))
        )
    expected = artifact["validation_against_committed_columns"]
    registered_checks = {
        "pearson_100k": abs(
            validation["pearson_recomputed_vs_committed_100k_knn10"]
            - float(expected["pearson_recomputed_vs_committed_100k_knn10"])
        )
        <= 1e-12,
        "pearson_680k": abs(
            validation["pearson_recomputed_vs_committed_680k_knn10"]
            - float(expected["pearson_recomputed_vs_committed_680k_knn10"])
        )
        <= 1e-12,
        "max_abs_diff_680k": abs(
            validation["max_abs_diff_680k"] - float(expected["max_abs_diff"])
        )
        <= 1e-12,
        "cell_count": int(artifact["n_cells"]) == 12,
        "positive_sign": bool(artifact["sign_stable_positive"]),
    }
    report: dict[str, Any] = {
        "schema": "observatory.external-p2-semantic-reproduction/1",
        "mode": "read_only_external_fixture",
        "paper_project_modified": False,
        "external_fixture_hashes": {
            name: content_hash(path.read_bytes()) for name, path in paths.items()
        },
        "row_count": len(measures),
        "aggregation_checks": aggregation_checks,
        "independent_validation": validation,
        "registered_artifact_checks": registered_checks,
        "registered_statistic_tolerance": 1e-12,
        "float32_csv_tolerance": float32_csv_tolerance,
    }
    report["passes"] = (
        all(
            row["row_ids_exact"]
            and row["centroid_within_float32_csv_tolerance"]
            and all(row["knn_within_float32_csv_tolerance"].values())
            for row in aggregation_checks
        )
        and all(registered_checks.values())
    )
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report
