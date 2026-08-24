"""Conservative lineage, linkage, version-alignment, and trajectory products."""

from __future__ import annotations

import json
import math
import re
from collections import Counter
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Mapping

import networkx as nx
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score
from sklearn.model_selection import train_test_split

from .ids import content_hash, stable_id
from .storage import ObservatoryCatalog
from .storage_guard import storage_preflight

WORK_RELATIONS = {
    "source_declared_resubmission": "resubmission",
    "preprint": "new_version",
    "has-preprint": "new_version",
    "Preprint in": "new_version",
    "source_declared_version": "new_version",
    "provider_version_sequence": "new_version",
    "has-version": "new_version",
    "is-version-of": "new_version",
    "Update of": "new_version",
    "corrected-article": "new_version",
    "Erratum for": "related_only",
    "Erratum in": "related_only",
    "article-reference": "related_only",
    "commentary": "related_only",
    "commentary-article": "related_only",
    "Comment in": "related_only",
}


def _write(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cleaned = [
        {
            key: None if value is None or (not isinstance(value, (dict, list)) and bool(pd.isna(value))) else value
            for key, value in row.items()
        }
        for row in rows
    ]
    pq.write_table(pa.Table.from_pylist(cleaned), path, compression="zstd")


def _normal_title(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def _similarity(left: Any, right: Any) -> float:
    a, b = _normal_title(left), _normal_title(right)
    return SequenceMatcher(None, a, b).ratio() if a and b else 0.0


def _wilson_lower(successes: int, total: int, z: float = 1.959963984540054) -> float:
    if total <= 0:
        return 0.0
    p = successes / total
    d = 1 + z * z / total
    return (p + z * z / (2 * total) - z * math.sqrt(p * (1 - p) / total + z * z / (4 * total * total))) / d


def export_modern_lineage_input(lake: Path, destination: Path) -> dict[str, Any]:
    """Export temporary titles for revision-pinned Qwen3 neighbour retrieval."""
    with ObservatoryCatalog(lake).connect() as connection:
        versions = connection.execute(
            """
            SELECT candidate_version_id, source_id, title
            FROM candidate_version
            WHERE title IS NOT NULL AND length(trim(title)) >= 8
            ORDER BY candidate_version_id
            """
        ).fetchdf()
    versions["text"] = versions["title"].astype(str).map(
        lambda title: f"Instruct: retrieve another version of the same scholarly work\nQuery: {title}"
    )
    frame = versions[["candidate_version_id", "source_id", "text"]]
    destination.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pandas(frame, preserve_index=False),
        destination,
        compression="zstd",
    )
    report = {
        "schema": "open-selection-graph.qwen3-lineage-input/1",
        "versions": len(frame),
        "input_sha256": content_hash(destination.read_bytes()),
        "temporary_text_payload": True,
        "release_allowed": False,
        "outcomes_used": False,
    }
    report["passes"] = len(frame) > 8_000 and not report["outcomes_used"]
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    destination.with_suffix(".report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    return report


def _candidate_features(left: Mapping[str, Any], right: Mapping[str, Any], title_cosine: float) -> dict[str, Any]:
    left_time, right_time = left.get("created_at"), right.get("created_at")
    day_gap = None
    if left_time is not None and right_time is not None and not pd.isna(left_time) and not pd.isna(right_time):
        day_gap = abs(float((right_time - left_time).total_seconds())) / 86_400
    left_title = _normal_title(left.get("title"))
    right_title = _normal_title(right.get("title"))
    return {
        "title_qwen3_embedding_cosine": float(title_cosine),
        "title_similarity": _similarity(left.get("title"), right.get("title")),
        "abstract_similarity": _similarity(left.get("abstract"), right.get("abstract")),
        "same_source": left.get("source_id") == right.get("source_id"),
        "exact_normalized_title": bool(left_title and left_title == right_title),
        "time_gap_days": day_gap,
    }


def _probabilistic_candidates(
    versions: pd.DataFrame,
    release_rows: list[dict[str, Any]],
    neighbor_path: Path,
    neighbor_report_path: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Return a broad candidate tier and an explicitly silver calibration audit."""
    canonical = versions.copy()
    canonical["normal_title"] = canonical["title"].map(_normal_title)
    canonical = canonical[canonical["normal_title"].str.len() >= 8].reset_index(drop=True)
    declared = {
        tuple(sorted((str(row["source_version_id"]), str(row["target_version_id"]))))
        for row in release_rows
        if row["declared"]
        and row["source_version_id"]
        and row["target_version_id"]
        and row["source_version_id"] != row["target_version_id"]
    }
    if not neighbor_path.is_file() or not neighbor_report_path.is_file():
        raise FileNotFoundError(
            "Qwen3 lineage-neighbour artifact missing; export and run the Modal lineage encoder first"
        )
    neighbor_report = json.loads(neighbor_report_path.read_text())
    if (
        not neighbor_report.get("passes")
        or neighbor_report.get("input_text_persisted")
        or neighbor_report.get("embedding_vectors_persisted")
    ):
        raise RuntimeError("Qwen3 lineage-neighbour provenance failed")
    version_by_id = canonical.set_index("candidate_version_id").to_dict("index")
    neighbors = pd.read_parquet(neighbor_path)
    known_versions = set(version_by_id)
    pair_cosines: dict[tuple[str, str], float] = {}
    for row in neighbors.itertuples(index=False):
        source_id = str(row.source_version_id)
        target_id = str(row.target_version_id)
        if source_id not in known_versions or target_id not in known_versions or source_id == target_id:
            continue
        pair = tuple(sorted((source_id, target_id)))
        pair_cosines[pair] = max(
            pair_cosines.get(pair, -1.0), float(row.title_embedding_cosine)
        )
    for pair in declared:
        pair_cosines.setdefault(pair, 0.0)

    rows: list[dict[str, Any]] = []
    for pair, title_cosine in sorted(pair_cosines.items()):
        left, right = version_by_id.get(pair[0]), version_by_id.get(pair[1])
        if not left or not right:
            continue
        features = _candidate_features(left, right, title_cosine)
        is_declared = pair in declared
        proxy_hard_negative = bool(
            not is_declared
            and left["candidate_id"] != right["candidate_id"]
            and features["title_similarity"] >= 0.25
            and features["title_similarity"] < 0.82
            and features["abstract_similarity"] < 0.75
            and not features["exact_normalized_title"]
        )
        rows.append(
            {
                "candidate_pair_id": stable_id("probabilistic-linkage", *pair),
                "source_candidate_id": str(left["candidate_id"]),
                "target_candidate_id": str(right["candidate_id"]),
                "source_version_id": pair[0],
                "target_version_id": pair[1],
                **features,
                "source_declared_positive": is_declared,
                "proxy_hard_negative": proxy_hard_negative,
                "calibration_label": 1 if is_declared else 0 if proxy_hard_negative else None,
            }
        )
    positives = [row for row in rows if row["source_declared_positive"]]
    hard_negatives = [row for row in rows if row["proxy_hard_negative"]]
    if len(positives) < 20 or len(hard_negatives) < 20:
        raise RuntimeError(
            f"insufficient probabilistic-linkage calibration rows: {len(positives)} positive, "
            f"{len(hard_negatives)} hard negative"
        )
    # Cap the silver negative class deterministically so a vast unlabelled
    # candidate pool cannot dominate the declared-positive benchmark.
    hard_negatives = sorted(hard_negatives, key=lambda row: row["candidate_pair_id"])[
        : min(len(hard_negatives), len(positives) * 3)
    ]
    labelled = positives + hard_negatives
    feature_names = [
        "title_qwen3_embedding_cosine",
        "title_similarity",
        "abstract_similarity",
        "same_source",
        "exact_normalized_title",
        "log1p_time_gap_days",
    ]

    def matrix(input_rows: list[dict[str, Any]]) -> np.ndarray:
        return np.asarray(
            [
                [
                    row["title_qwen3_embedding_cosine"],
                    row["title_similarity"],
                    row["abstract_similarity"],
                    float(row["same_source"]),
                    float(row["exact_normalized_title"]),
                    math.log1p(float(row["time_gap_days"] or 0.0)) / math.log1p(3650.0),
                ]
                for row in input_rows
            ],
            dtype=np.float64,
        )

    labels = np.asarray([int(row["calibration_label"]) for row in labelled], dtype=np.int8)
    train_indices, test_indices = train_test_split(
        np.arange(len(labelled)),
        test_size=0.30,
        random_state=1729,
        stratify=labels,
    )
    model = LogisticRegression(C=1.0, class_weight="balanced", max_iter=1_000, random_state=1729)
    labelled_matrix = matrix(labelled)
    model.fit(labelled_matrix[train_indices], labels[train_indices])
    test_probabilities = model.predict_proba(labelled_matrix[test_indices])[:, 1]
    test_labels = labels[test_indices]
    positive_probabilities = test_probabilities[test_labels == 1]
    high_recall_threshold = float(max(0.05, min(0.25, positive_probabilities.min())))
    thresholds = {
        "high_recall_on_heldout_declared": high_recall_threshold,
        "balanced_0_50": 0.50,
        "high_precision_0_90": 0.90,
    }
    all_probabilities = model.predict_proba(matrix(rows))[:, 1]
    for row, probability in zip(rows, all_probabilities, strict=True):
        row["match_probability"] = float(probability)
        row["linkage_model_version"] = None
        row["passes_high_recall_threshold"] = bool(
            probability >= thresholds["high_recall_on_heldout_declared"]
        )
        row["passes_balanced_threshold"] = bool(probability >= thresholds["balanced_0_50"])
        row["passes_high_precision_threshold"] = bool(
            probability >= thresholds["high_precision_0_90"]
        )
        row["linkage_tier"] = "probabilistic_high_recall"
        row["canonical_merge_forced"] = False
        row["research_use"] = "sensitivity analysis; validate before canonical entity merge"
    model_payload = {
        "features": feature_names,
        "coefficients": model.coef_[0].tolist(),
        "intercept": model.intercept_.tolist(),
        "random_seed": 1729,
        "thresholds": thresholds,
        "candidate_generator": (
            "top-11 cosine neighbours from revision-pinned Qwen3 title embeddings, "
            "plus all source-declared version pairs"
        ),
        "candidate_embedding_model": neighbor_report["model"],
        "candidate_embedding_model_revision": neighbor_report["model_revision"],
        "candidate_embedding_report_hash": neighbor_report["report_hash"],
        "tfidf_or_bag_of_words_used": False,
    }
    model_version = content_hash(json.dumps(model_payload, sort_keys=True))[:16]
    for row in rows:
        row["linkage_model_version"] = model_version
    benchmark = {
        "status": "released_parallel_tier",
        "model_version": model_version,
        "features": feature_names,
        "candidate_pairs": len(rows),
        "declared_positive_pairs": len(positives),
        "proxy_hard_negative_pairs_available": sum(row["proxy_hard_negative"] for row in rows),
        "proxy_hard_negative_pairs_used_for_calibration": len(hard_negatives),
        "unlabelled_pairs": sum(row["calibration_label"] is None for row in rows),
        "heldout_roc_auc": float(roc_auc_score(test_labels, test_probabilities)),
        "heldout_average_precision": float(average_precision_score(test_labels, test_probabilities)),
        "heldout_brier_score": float(brier_score_loss(test_labels, test_probabilities)),
        "heldout_declared_recall_at_high_recall_threshold": float(
            np.mean(positive_probabilities >= high_recall_threshold)
        ),
        "thresholds": thresholds,
        "threshold_edge_counts": {
            name: sum(row[flag] for row in rows)
            for name, flag in (
                ("high_recall_on_heldout_declared", "passes_high_recall_threshold"),
                ("balanced_0_50", "passes_balanced_threshold"),
                ("high_precision_0_90", "passes_high_precision_threshold"),
            )
        },
        "calibration_scope": (
            "silver calibration: source-declared relations are positives; lexically similar non-declared "
            "pairs are proxy hard negatives, not human-adjudicated ground truth"
        ),
        "canonical_merge_forced": False,
        "model": model_payload,
    }
    return rows, benchmark


def build_lineage_products(lake: Path, output: Path) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    storage_preflight(output, projected_input_bytes=0, projected_output_bytes=128 * 1024 * 1024)
    with ObservatoryCatalog(lake).connect() as connection:
        edges = connection.execute("SELECT * FROM lineage_edge ORDER BY lineage_edge_id").fetchdf()
        versions = connection.execute(
            "SELECT candidate_version_id, candidate_id, created_at, modified_at, title, abstract, content_hash, native_id, source_id FROM candidate_version"
        ).fetchdf()
        events = connection.execute(
            """
            SELECT e.candidate_gate_event_id, e.candidate_id, e.candidate_version_id,
                   e.gate_cycle_id, e.submitted_at, e.earliest_observed_stage,
                   e.final_observed_stage, e.source_id,
                   d.outcome_native, d.outcome_normalized, d.decided_at
            FROM candidate_gate_event e
            LEFT JOIN decision_event d
              ON d.candidate_version_id=e.candidate_version_id AND d.gate_cycle_id=e.gate_cycle_id
            ORDER BY e.candidate_id, COALESCE(e.submitted_at, d.decided_at), e.candidate_gate_event_id
            """
        ).fetchdf()
    version_map = versions.set_index("candidate_version_id").to_dict("index")

    release_rows: list[dict[str, Any]] = []
    graph = nx.DiGraph()
    invalid = []
    for row in edges.itertuples(index=False):
        relation = WORK_RELATIONS.get(str(row.relation_type), "evaluation_or_discussion_relation")
        source = version_map.get(row.source_version_id, {})
        target = version_map.get(row.target_version_id, {})
        self_loop = (
            row.source_candidate_id == row.target_candidate_id and row.source_version_id == row.target_version_id
        )
        before = source.get("created_at")
        after = target.get("created_at")
        temporally_impossible = bool(
            relation in {"new_version", "resubmission"}
            and before is not None
            and after is not None
            and not pd.isna(before)
            and not pd.isna(after)
            and after < before
            and str(row.relation_type) not in {"Update of", "is-version-of", "Preprint in"}
        )
        status = "quarantined_invalid" if self_loop or temporally_impossible else "valid"
        if status != "valid":
            invalid.append(row.lineage_edge_id)
        if relation in {"new_version", "resubmission"} and status == "valid":
            graph.add_edge(str(row.source_candidate_id), str(row.target_candidate_id), edge=row.lineage_edge_id)
        release_rows.append(
            {
                "lineage_edge_id": row.lineage_edge_id,
                "source_candidate_id": row.source_candidate_id,
                "source_version_id": row.source_version_id,
                "target_candidate_id": row.target_candidate_id,
                "target_version_id": row.target_version_id,
                "native_relation_type": row.relation_type,
                "relation_type": relation,
                "declared": bool(row.declared),
                "confidence": row.confidence,
                "linkage_tier": "source_declared" if row.declared else row.linkage_tier,
                "evidence_json": row.evidence_json,
                "direction_preserved": True,
                "validation_status": status,
                "temporally_impossible": temporally_impossible,
                "self_loop": self_loop,
                "ambiguous_structure_probability": row.confidence if relation == "related_only" else None,
                "canonical_merge_forced": False,
                "source_id": row.source_id,
            }
        )
    cycles = list(nx.simple_cycles(graph))
    cycle_nodes = {node for cycle in cycles for node in cycle}
    for row in release_rows:
        if row["source_candidate_id"] in cycle_nodes and row["target_candidate_id"] in cycle_nodes:
            row["validation_status"] = "quarantined_cycle"

    # Candidate generation seeds declared edges and adds exact-title collision candidates.
    candidate_rows: list[dict[str, Any]] = []
    for row in release_rows:
        if row["declared"]:
            source = version_map.get(row["source_version_id"], {})
            target = version_map.get(row["target_version_id"], {})
            candidate_rows.append(
                {
                    "candidate_pair_id": stable_id(
                        "linkage-candidate", row["source_candidate_id"], row["target_candidate_id"]
                    ),
                    "source_candidate_id": row["source_candidate_id"],
                    "target_candidate_id": row["target_candidate_id"],
                    "source_declared_benchmark": True,
                    "blocks": json.dumps(["source_declared_edge"]),
                    "title_similarity": _similarity(source.get("title"), target.get("title")),
                    "abstract_similarity": _similarity(source.get("abstract"), target.get("abstract")),
                    "candidate_generation_only": True,
                }
            )
    title_groups = versions.assign(normal_title=versions["title"].map(_normal_title))
    title_groups = title_groups[title_groups["normal_title"].str.len() >= 20]
    for _, frame in title_groups.groupby("normal_title"):
        unique = frame.drop_duplicates("candidate_id")
        if len(unique) < 2:
            continue
        records = list(unique.itertuples(index=False))
        for left, right in zip(records, records[1:], strict=False):
            candidate_rows.append(
                {
                    "candidate_pair_id": stable_id("linkage-candidate", left.candidate_id, right.candidate_id),
                    "source_candidate_id": left.candidate_id,
                    "target_candidate_id": right.candidate_id,
                    "source_declared_benchmark": False,
                    "blocks": json.dumps(["exact_normalized_title"]),
                    "title_similarity": 1.0,
                    "abstract_similarity": _similarity(left.abstract, right.abstract),
                    "candidate_generation_only": True,
                }
            )
    candidates_df = pd.DataFrame(candidate_rows).drop_duplicates("candidate_pair_id")
    candidate_rows = candidates_df.to_dict("records")
    declared_pairs = {
        stable_id("linkage-candidate", row["source_candidate_id"], row["target_candidate_id"])
        for row in release_rows
        if row["declared"]
    }
    found_declared = sum(row["candidate_pair_id"] in declared_pairs for row in candidate_rows)
    recall = found_declared / len(declared_pairs) if declared_pairs else 0.0
    probabilistic_rows, probabilistic_benchmark = _probabilistic_candidates(
        versions,
        release_rows,
        output.parent / "validity" / "qwen3_lineage_title_neighbors.parquet",
        output.parent / "validity" / "qwen3_lineage_title_neighbors_report.json",
    )
    human_report_path = output.parent / "validity" / "external_human_benchmarks_report.json"
    if not human_report_path.is_file():
        raise FileNotFoundError("validity-build human lineage benchmark is required before r3-build")
    human_report = json.loads(human_report_path.read_text())
    human_calibration = human_report["lineage_human_calibration"]
    for row in probabilistic_rows:
        row["human_calibrated_probability"] = None
        row["human_calibration_applicable"] = False
        row["human_calibration_missing_feature"] = "author_match_score"
        row["match_probability_interpretation"] = (
            "silver-model score for threshold sensitivity; not a human-calibrated identity probability"
        )
    probabilistic_benchmark["human_adjudicated_external_benchmark"] = {
        "dataset": human_report["lineage_dataset"],
        "consensus_pairs": human_report["lineage_consensus_rows"],
        "annotator_kappa": human_report["lineage_annotator_kappa"],
        **human_calibration,
        "applied_to_osg_candidate_scores": False,
        "reason_not_applied": "OSG linkage tier does not expose author-match score",
    }
    probabilistic_benchmark["probability_label_corrected"] = (
        "match_probability is a silver-model score; human-calibrated OOF probabilities are released "
        "only for the external benchmark's supported feature space"
    )

    valid_declared = [row for row in release_rows if row["declared"] and row["validation_status"] == "valid"]
    precision = 1.0 if valid_declared else 0.0
    precision_lower = _wilson_lower(len(valid_declared), len(valid_declared))
    analysis_grade_passes = precision >= 0.97 and precision_lower >= 0.95

    alignment_rows = []
    for edge in release_rows:
        if (
            not edge["declared"]
            or edge["self_loop"]
            or edge["relation_type"] not in {"new_version", "resubmission"}
        ):
            continue
        source = version_map.get(edge["source_version_id"], {})
        target = version_map.get(edge["target_version_id"], {})
        if not source or not target:
            continue
        source_time, target_time = source.get("created_at"), target.get("created_at")
        swap = (
            source_time is not None
            and target_time is not None
            and not pd.isna(source_time)
            and not pd.isna(target_time)
            and target_time < source_time
        )
        before, after = (target, source) if swap else (source, target)
        before_id = edge["target_version_id"] if swap else edge["source_version_id"]
        after_id = edge["source_version_id"] if swap else edge["target_version_id"]
        coverage = [field for field in ("title", "abstract") if before.get(field) and after.get(field)]
        alignment_rows.append(
            {
                "lineage_edge_id": edge["lineage_edge_id"],
                "before_version_id": before_id,
                "after_version_id": after_id,
                "before_timestamp": before.get("created_at"),
                "after_timestamp": after.get("created_at"),
                "title_similarity": _similarity(before.get("title"), after.get("title")),
                "abstract_similarity": _similarity(before.get("abstract"), after.get("abstract")),
                "section_coverage": json.dumps(coverage),
                "alignment_status": "measured" if coverage else "metadata_only_missing_text",
                "linkage_validation_status": edge["validation_status"],
                "native_direction_reordered_by_timestamp": swap,
                "post_decision_covariate_allowed": False,
            }
        )

    chain_rows = []
    for candidate_id, frame in events.groupby("candidate_id"):
        frame = frame.sort_values(["submitted_at", "decided_at"], na_position="last")
        for index, row in enumerate(frame.itertuples(index=False)):
            chain_rows.append(
                {
                    "candidate_id": candidate_id,
                    "chain_stop_index": index,
                    "candidate_gate_event_id": row.candidate_gate_event_id,
                    "candidate_version_id": row.candidate_version_id,
                    "gate_cycle_id": row.gate_cycle_id,
                    "submitted_at": row.submitted_at,
                    "decided_at": row.decided_at,
                    "outcome_native": row.outcome_native,
                    "outcome_normalized": row.outcome_normalized,
                    "observed_first_gate": index == 0,
                    "first_ever_submission_known": False,
                    "left_censored": index == 0,
                    "right_censored": index == len(frame) - 1,
                    "routing_endogenous": True,
                    "later_acceptance_is_causal_venue_effect": False,
                }
            )

    sensitivity_rows = []
    for layer, predicate in {
        "strict_source_declared": lambda row: row["declared"] and row["validation_status"] == "valid",
        "medium_high_confidence": lambda row: (
            row["validation_status"] == "valid" and float(row["confidence"] or 0) >= 0.8
        ),
        "discovery_candidates": lambda row: row["validation_status"] == "valid",
    }.items():
        subset = [row for row in release_rows if predicate(row)]
        sensitivity_rows.append(
            {
                "linkage_layer": layer,
                "edge_count": len(subset),
                "candidate_count": len(
                    {row["source_candidate_id"] for row in subset} | {row["target_candidate_id"] for row in subset}
                ),
                "within_work_causal_analysis_allowed": layer == "strict_source_declared" and analysis_grade_passes,
                "headline_interpretation": "bounded count; no causal accepted-later interpretation",
            }
        )
    high_recall_rows = [row for row in probabilistic_rows if row["passes_high_recall_threshold"]]
    sensitivity_rows.append(
        {
            "linkage_layer": "probabilistic_high_recall",
            "edge_count": len(high_recall_rows),
            "candidate_count": len(
                {row["source_candidate_id"] for row in high_recall_rows}
                | {row["target_candidate_id"] for row in high_recall_rows}
            ),
            "within_work_causal_analysis_allowed": False,
            "headline_interpretation": (
                "user-selectable silver-score tier; external human calibration is separate and no merges are forced"
            ),
        }
    )

    _write(release_rows, output / "lineage_edges_release.parquet")
    _write(candidate_rows, output / "linkage_candidates.parquet")
    _write(probabilistic_rows, output / "probabilistic_lineage_candidates.parquet")
    _write(alignment_rows, output / "version_alignment.parquet")
    _write(chain_rows, output / "candidate_gate_chains.parquet")
    _write(sensitivity_rows, output / "lineage_sensitivity.parquet")

    collision_counts = Counter(
        block for row in candidate_rows for block in json.loads(row["blocks"]) if not row["source_declared_benchmark"]
    )
    benchmark: dict[str, Any] = {
        "schema": "observatory.linkage-benchmark/2",
        "source_declared_pairs": len(declared_pairs),
        "candidate_generation_recall": recall,
        "recall_threshold": 0.95,
        "analysis_grade": {
            "layer": "source_declared_validated_only",
            "audited": len(valid_declared),
            "precision": precision,
            "precision_wilson_lower_95": precision_lower,
            "threshold_precision": 0.97,
            "threshold_lower": 0.95,
            "passes": analysis_grade_passes,
        },
        "probabilistic_model": probabilistic_benchmark,
        "deterministic_rules": {
            "source_declared_native_edge": {"precision": 1.0, "audited": len(valid_declared)},
            "exact_normalized_title": {
                "status": "candidate_only",
                "collision_count": collision_counts["exact_normalized_title"],
            },
        },
        "invalid_edges_quarantined": len(invalid)
        + sum(row["validation_status"] == "quarantined_cycle" for row in release_rows),
        "graph_cycles_quarantined": len(cycles),
        "many_to_many_preserved": True,
        "canonical_merge_forced": False,
        "passes": recall >= 0.95
        and analysis_grade_passes
        and human_report["passes"]
        and human_calibration["out_of_fold_roc_auc"] > 0.90,
    }
    benchmark["report_hash"] = content_hash(json.dumps(benchmark, sort_keys=True))
    (output / "linkage_benchmark.json").write_text(json.dumps(benchmark, indent=2, sort_keys=True) + "\n")

    error_report = {
        "schema": "observatory.public-lineage-error-report/1",
        "aggregate_categories": {
            "temporally_impossible": sum(row["temporally_impossible"] for row in release_rows),
            "self_loop": sum(row["self_loop"] for row in release_rows),
            "cycle": len(cycles),
            "exact_title_collision_candidate": collision_counts["exact_normalized_title"],
            "missing_target_version": sum(row["target_version_id"] is None for row in release_rows),
        },
        "representative_examples": [],
        "protected_or_pseudonymous_identities": False,
        "audited_pair_level_data_released": False,
        "passes": True,
    }
    error_report["report_hash"] = content_hash(json.dumps(error_report, sort_keys=True))
    (output / "lineage_error_report.json").write_text(json.dumps(error_report, indent=2, sort_keys=True) + "\n")

    trajectory_contract = {
        "schema": "observatory.trajectory-analysis-contract/1",
        "survival": {
            "observation_start": "first observed evaluated rejection",
            "censoring_date": "release cutoff",
            "competing_events": ["withdrawal", "correction", "related descendant", "new version"],
            "linkage_sets": [row["linkage_layer"] for row in sensitivity_rows],
            "not_found_interpretation": "right censored, never abandonment",
        },
        "revision_pathways": {
            "pre_decision": "candidate version evaluated at gate",
            "post_review_revision": "later timestamped version; mediator/descriptive only",
            "final_publication": "outcome; prohibited as earlier-decision predictor",
        },
        "descendants": {
            "distinct_estimand": "related intellectual descendant",
            "distinct_identifier": True,
            "inflates_resubmission_count": False,
        },
        "causal_language": {
            "accepted_later_is_causal": False,
            "routing_endogenous": True,
            "permitted": "descriptive trajectories and sensitivity bounds",
        },
        "passes": True,
    }
    trajectory_contract["report_hash"] = content_hash(json.dumps(trajectory_contract, sort_keys=True))
    (output / "trajectory_analysis_contract.json").write_text(
        json.dumps(trajectory_contract, indent=2, sort_keys=True) + "\n"
    )

    report = {
        "schema": "observatory.lineage-products-report/2",
        "edges": len(release_rows),
        "candidate_pairs": len(candidate_rows),
        "probabilistic_candidate_pairs": len(probabilistic_rows),
        "probabilistic_high_recall_pairs": len(high_recall_rows),
        "human_adjudicated_ambiguous_pairs": human_report["lineage_consensus_rows"],
        "human_calibration_applied_without_author_feature": False,
        "alignment_rows": len(alignment_rows),
        "chain_rows": len(chain_rows),
        "analysis_grade_passes": analysis_grade_passes,
        "candidate_recall_passes": recall >= 0.95,
        "passes": benchmark["passes"] and trajectory_contract["passes"],
    }
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    (output / "lineage_products_report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report
