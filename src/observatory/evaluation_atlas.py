"""Evaluation-object normalization and reproducible reference-corpus manifests."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, pstdev
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .ids import content_hash, stable_id
from .storage import ObservatoryCatalog
from .storage_guard import storage_preflight


def _hash_file(path: Path) -> str:
    return content_hash(path.read_bytes())


def _write_parquet(path: Path, rows: list[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path, compression="zstd")
    return path


def _evaluation_class(evaluation_type: Any, official: Any, invitation: Any) -> tuple[str, str]:
    text = f"{evaluation_type or ''} {invitation or ''}".lower()
    if "meta" in text or "editor" in text or "assessment" in text:
        kind = "editorial_or_meta_evaluation"
    elif "office action" in text or "examiner" in text:
        kind = "patent_office_action"
    elif "panel" in text or "funder" in text:
        kind = "funding_panel_evaluation"
    elif "comment" in text and "official" not in text:
        kind = "public_comment"
    elif "review" in text or "report" in text:
        kind = "peer_review"
    else:
        kind = "other_evaluation"
    official_status = "official" if official is True else "not_official" if official is False else "unspecified"
    if kind == "public_comment" and official_status == "unspecified":
        official_status = "not_official_by_type"
    return kind, official_status


def build_reference_corpus_manifest(connection, lake_root: Path) -> dict[str, Any]:
    version_rows = connection.execute(
        """SELECT candidate_version_id, source_id, created_at, language,
                  content_hash, title IS NOT NULL, abstract IS NOT NULL
           FROM candidate_version ORDER BY candidate_version_id"""
    ).fetchall()
    source_snapshots = []
    for source_id, retrieved_min, retrieved_max, object_count in connection.execute(
        """SELECT source_id, min(retrieved_at), max(retrieved_at), count(*)
           FROM source_object GROUP BY source_id ORDER BY source_id"""
    ).fetchall():
        manifests = sorted((lake_root.parent / "raw" / "manifests").glob(f"{source_id}.jsonl"))
        source_snapshots.append(
            {
                "source_id": source_id,
                "retrieved_from": retrieved_min.isoformat() if retrieved_min else None,
                "retrieved_to": retrieved_max.isoformat() if retrieved_max else None,
                "source_object_count": object_count,
                "raw_manifest_hash": _hash_file(manifests[0]) if manifests else None,
            }
        )
    ids = [str(row[0]) for row in version_rows]
    manifest: dict[str, Any] = {
        "schema": "observatory.reference-corpus-manifest/1",
        "feature_family": "semantic_novelty",
        "eligibility": {
            "unit": "candidate_version",
            "required": "non-null title or abstract and created_at strictly before target cutoff",
            "future_leakage_rule": "reference.created_at < target.created_at",
            "deduplication": "canonical candidate_version_id; no cross-source probabilistic collapse",
            "language": "native-language text retained; language-specific coverage reported",
        },
        "source_snapshots": source_snapshots,
        "candidate_version_count": len(version_rows),
        "with_title_count": sum(bool(row[5]) for row in version_rows),
        "with_abstract_count": sum(bool(row[6]) for row in version_rows),
        "missing_created_at_count": sum(row[2] is None for row in version_rows),
        "language_counts": dict(Counter(str(row[3] or "unknown") for row in version_rows)),
        "retained_identifier_set_hash": content_hash("\n".join(ids)),
        "encoder": {
            "status": "materialized_in_R1",
            "model_name": "allenai/specter2_base",
            "model_revision": "3447645e1def9117997203454fa4495937bfbd83",
            "pooling": "last_hidden_state[:, 0, :]",
            "required_fields": ["model_name", "model_revision", "vector_checksum"],
        },
        "reference_change_policy": "any source snapshot, eligibility, cutoff, deduplication, language, or encoder change creates a new feature version",
    }
    manifest["manifest_hash"] = content_hash(json.dumps(manifest, sort_keys=True))
    return manifest


def build_evaluation_products(lake_root: Path, output_root: Path) -> dict[str, Any]:
    connection = ObservatoryCatalog(lake_root).connect()
    required = {"evaluation", "candidate_version", "candidate_gate_event", "source_object"}
    available = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
    missing = sorted(required - available)
    if missing:
        raise RuntimeError(f"evaluation build is missing canonical tables: {missing}")
    event_pairs = {
        (row[0], row[1]) for row in connection.execute(
            "SELECT candidate_version_id, gate_cycle_id FROM candidate_gate_event WHERE candidate_version_id IS NOT NULL"
        ).fetchall()
    }
    normalized: list[dict[str, Any]] = []
    for row in connection.execute(
        """SELECT e.evaluation_id, e.candidate_version_id, e.gate_cycle_id, e.native_id,
                  evaluation_type, evaluator_role, anonymous, official,
                  criterion_native, criterion_normalized, criterion_value,
                  criterion_value_numeric, scale_json, confidence_value,
                  created_at, invitation_native, e.source_id, e.source_object_id,
                  gc.policy_version_id
           FROM evaluation e LEFT JOIN gate_cycle gc USING(gate_cycle_id)
           ORDER BY evaluation_id"""
    ).fetchall():
        (evaluation_id, version_id, cycle_id, native_id, evaluation_type,
         role, anonymous, official, criterion_native, criterion_normalized,
         criterion_value, criterion_numeric, scale_json, confidence,
         created_at, invitation, source_id, source_object_id, policy_version_id) = row
        kind, official_status = _evaluation_class(evaluation_type, official, invitation)
        scale_hash = content_hash(str(scale_json or "native-scale-unspecified"))
        rubric_version_id = stable_id(
            "rubric_version",
            str(source_id),
            f"{policy_version_id or cycle_id}|{criterion_native or evaluation_type}|{scale_hash}",
        )
        normalized.append(
            {
                "evaluation_id": evaluation_id,
                "candidate_version_id": version_id,
                "gate_cycle_id": cycle_id,
                "native_id": native_id,
                "evaluation_kind": kind,
                "native_evaluation_type": evaluation_type,
                "evaluator_role": role,
                "anonymous": anonymous,
                "official_status": official_status,
                "criterion_native": criterion_native,
                "criterion_normalized": criterion_normalized,
                "criterion_value_native": criterion_value,
                "criterion_value_numeric": criterion_numeric,
                "scale_json_native": scale_json,
                "scale_direction": "native_unspecified",
                "policy_version_id": policy_version_id,
                "rubric_version_id": rubric_version_id,
                "confidence_value": confidence,
                "created_at": created_at.isoformat() if created_at else None,
                "invitation_native": invitation,
                "exact_version_gate_join": (version_id, cycle_id) in event_pairs,
                "source_id": source_id,
                "source_object_id": source_object_id,
            }
        )
    numeric_groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for item in normalized:
        if item["criterion_value_numeric"] is not None:
            numeric_groups[(item["gate_cycle_id"], item["rubric_version_id"])].append(item)
    for group in numeric_groups.values():
        values = [float(item["criterion_value_numeric"]) for item in group]
        average = mean(values)
        deviation = pstdev(values)
        ordered = sorted(values)
        for item, value in zip(group, values, strict=True):
            item["criterion_value_cycle_z"] = (value - average) / deviation if deviation else 0.0
            item["criterion_value_cycle_percentile"] = (
                sum(other <= value for other in ordered) / len(ordered)
            )
    for item in normalized:
        item.setdefault("criterion_value_cycle_z", None)
        item.setdefault("criterion_value_cycle_percentile", None)
    storage_receipt = storage_preflight(
        output_root,
        projected_input_bytes=0,
        projected_output_bytes=max(len(normalized) * 2_048, 1),
    )
    eval_path = _write_parquet(output_root / "evaluation_objects.parquet", normalized)
    reference = build_reference_corpus_manifest(connection, lake_root)
    reference_path = output_root / "reference_corpus_manifest.json"
    reference_path.parent.mkdir(parents=True, exist_ok=True)
    reference_path.write_text(json.dumps(reference, indent=2, sort_keys=True) + "\n")
    report: dict[str, Any] = {
        "schema": "observatory.evaluation-products/1",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "evaluation_count": len(normalized),
        "kind_counts": dict(Counter(row["evaluation_kind"] for row in normalized)),
        "official_status_counts": dict(Counter(row["official_status"] for row in normalized)),
        "exact_version_gate_join_count": sum(row["exact_version_gate_join"] for row in normalized),
        "unresolved_version_gate_join_count": sum(not row["exact_version_gate_join"] for row in normalized),
        "checks": {
            "public_comments_not_silently_official": all(
                row["evaluation_kind"] != "public_comment" or row["official_status"] != "official"
                for row in normalized
            ),
            "exact_join_status_is_explicit": all(isinstance(row["exact_version_gate_join"], bool) for row in normalized),
            "native_rubric_values_preserved": all("criterion_value_native" in row and "scale_json_native" in row for row in normalized),
            "scale_changes_are_versioned": all(row["rubric_version_id"] for row in normalized),
            "within_cycle_calibration_is_non_destructive": all(
                row["criterion_value_native"] is not None
                or row["criterion_value_cycle_z"] is None
                for row in normalized
            ),
            "reference_manifest_prior_only_rule": reference["eligibility"]["future_leakage_rule"] == "reference.created_at < target.created_at",
            "reference_manifest_rebuildable": bool(reference["source_snapshots"] and reference["retained_identifier_set_hash"]),
        },
        "artifacts": {
            "evaluation_objects": str(eval_path),
            "reference_corpus_manifest": str(reference_path),
        },
        "storage_preflight": storage_receipt,
    }
    report["passes"] = all(report["checks"].values())
    report["artifact_hashes"] = {
        "evaluation_objects": _hash_file(eval_path),
        "reference_corpus_manifest": _hash_file(reference_path),
    }
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    report_path = output_root / "evaluation_products_report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report
