"""Modern open-weights semantic ruler export and triangulation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .ids import content_hash
from .storage import ObservatoryCatalog

QWEN_MODEL = "Qwen/Qwen3-Embedding-0.6B"
QWEN_REVISION = "97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3"


def export_modern_ruler_input(lake: Path, destination: Path) -> dict[str, Any]:
    """Export the bounded text input sent to the Modal encoder."""
    with ObservatoryCatalog(lake).connect() as connection:
        frame = connection.execute(
            """
            SELECT candidate_version_id, source_id, created_at, observed_at, title, abstract
            FROM candidate_version
            WHERE created_at IS NOT NULL AND (title IS NOT NULL OR abstract IS NOT NULL)
            ORDER BY created_at, candidate_version_id
            """
        ).fetchdf()
    created = pd.to_datetime(frame["created_at"], utc=True, errors="coerce")
    observed = pd.to_datetime(frame["observed_at"], utc=True, errors="coerce")
    frame = frame[created.notna() & (observed.isna() | (created <= observed))].copy()
    frame["created_at"] = created.loc[frame.index]
    frame["text"] = (
        frame["title"].fillna("").astype(str) + "\n" + frame["abstract"].fillna("").astype(str)
    ).str.strip()
    frame = frame[frame["text"].str.len() > 0][
        ["candidate_version_id", "source_id", "created_at", "text"]
    ].reset_index(drop=True)
    destination.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), destination, compression="zstd")
    report = {
        "schema": "open-selection-graph.modern-ruler-input/1",
        "rows": len(frame),
        "model": QWEN_MODEL,
        "model_revision": QWEN_REVISION,
        "text_scope": "title and abstract",
        "future_dated_rows_excluded": True,
        "outcomes_included": False,
        "temporary_input": True,
        "input_bytes": destination.stat().st_size,
        "input_sha256": content_hash(destination.read_bytes()),
        "passes": len(frame) > 8_000,
    }
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    report_path = destination.with_suffix(".report.json")
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def build_modern_ruler_triangulation(workspace: Path, output: Path) -> dict[str, Any]:
    """Compare Qwen3 and SPECTER2 without declaring either construct-correct."""
    qwen_path = output / "qwen3_semantic_novelty.parquet"
    qwen_report_path = output / "qwen3_semantic_novelty_report.json"
    if not qwen_path.is_file() or not qwen_report_path.is_file():
        raise FileNotFoundError("run the pinned Modal Qwen3 encoder before validity-build")
    qwen_report = json.loads(qwen_report_path.read_text())
    if not qwen_report.get("passes"):
        raise RuntimeError("Qwen3 semantic novelty report failed")
    qwen = pd.read_parquet(qwen_path)
    specter = pd.read_parquet(
        workspace / "results" / "observatory" / "r1" / "semantic_novelty.parquet"
    )
    shared = specter.merge(
        qwen,
        on="candidate_version_id",
        suffixes=("_specter2", "_qwen3"),
    )
    groups = ["source_id_specter2", "target_year_specter2"]
    for field in ("centroid_cosine_distance", "nearest_cosine_distance"):
        for ruler in ("specter2", "qwen3"):
            column = f"{field}_{ruler}"
            shared[f"{field}_{ruler}_percentile"] = shared.groupby(groups)[column].transform(
                lambda values: values.rank(pct=True, method="average")
            )
        shared[f"{field}_absolute_percentile_disagreement"] = (
            shared[f"{field}_specter2_percentile"] - shared[f"{field}_qwen3_percentile"]
        ).abs()
    rows = pd.DataFrame(
        {
            "candidate_version_id": shared["candidate_version_id"],
            "source_id": shared["source_id_specter2"],
            "target_year": shared["target_year_specter2"],
            "specter2_centroid_percentile": shared["centroid_cosine_distance_specter2_percentile"],
            "qwen3_centroid_percentile": shared["centroid_cosine_distance_qwen3_percentile"],
            "centroid_absolute_percentile_disagreement": shared[
                "centroid_cosine_distance_absolute_percentile_disagreement"
            ],
            "specter2_nearest_percentile": shared["nearest_cosine_distance_specter2_percentile"],
            "qwen3_nearest_percentile": shared["nearest_cosine_distance_qwen3_percentile"],
            "nearest_absolute_percentile_disagreement": shared[
                "nearest_cosine_distance_absolute_percentile_disagreement"
            ],
            "rulers_equated": False,
        }
    )
    triangulation_path = output / "semantic_ruler_triangulation.parquet"
    pq.write_table(pa.Table.from_pandas(rows, preserve_index=False), triangulation_path, compression="zstd")
    centroid_spearman = float(
        rows["specter2_centroid_percentile"].corr(rows["qwen3_centroid_percentile"], method="spearman")
    )
    nearest_spearman = float(
        rows["specter2_nearest_percentile"].corr(rows["qwen3_nearest_percentile"], method="spearman")
    )
    report = {
        "schema": "open-selection-graph.semantic-ruler-triangulation/1",
        "primary_ruler": {
            "model": "allenai/specter2_base",
            "revision": "3447645e1def9117997203454fa4495937bfbd83",
            "dimension": 768,
            "scientific_domain_model": True,
        },
        "independent_ruler": {
            "model": QWEN_MODEL,
            "revision": QWEN_REVISION,
            "parameters": 595_776_512,
            "dimension": 1024,
            "license": "Apache-2.0",
            "purpose_built_text_embedding_model": True,
            "scientific_domain_model": False,
        },
        "qwen3_rows": len(qwen),
        "specter2_rows": len(specter),
        "shared_rows": len(shared),
        "within_source_year_centroid_percentile_spearman": centroid_spearman,
        "within_source_year_nearest_percentile_spearman": nearest_spearman,
        "median_centroid_percentile_disagreement": float(
            rows["centroid_absolute_percentile_disagreement"].median()
        ),
        "median_nearest_percentile_disagreement": float(
            rows["nearest_absolute_percentile_disagreement"].median()
        ),
        "construct_rule": (
            "report both rulers and their disagreement; neither embedding distance is treated as ground truth"
        ),
        "no_tfidf_or_bag_of_words_ruler": True,
        "outcomes_used_for_encoding_or_reference_selection": False,
        "qwen_report_hash": qwen_report["report_hash"],
        "passes": len(shared) / max(len(qwen), 1) >= 0.99
        and qwen_report["model_revision"] == QWEN_REVISION,
    }
    report["artifact_sha256"] = content_hash(triangulation_path.read_bytes())
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    (output / "semantic_ruler_triangulation_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    return report
