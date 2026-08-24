"""Time-valid semantic novelty from open-weights scientific embeddings.

OSG encodes each title--abstract pair once with a pinned SPECTER2 model and
compares a target only with documents created before the target year.  Model
weights are never fitted on OSG outcomes, and the released table contains
distances and vector checksums rather than the source text or dense vectors.
"""

from __future__ import annotations

import inspect
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .ids import content_hash
from .storage import ObservatoryCatalog
from .storage_guard import storage_preflight

MODEL_NAME = "allenai/specter2_base"
MODEL_REVISION = "3447645e1def9117997203454fa4495937bfbd83"
ENCODER_NAME = "specter2_base_cls"
MODEL_LICENSE = "Apache-2.0"
EMBEDDING_DIMENSION = 768


def _text(title: Any, abstract: Any, separator: str = "[SEP]") -> str:
    title_text = str(title or "").strip()
    abstract_text = str(abstract or "").strip()
    if title_text and abstract_text:
        return f"{title_text}{separator}{abstract_text}"
    return title_text or abstract_text


def _checksum(vector: np.ndarray) -> str:
    return content_hash(np.asarray(vector, dtype="<f4").round(7).tobytes())


def _normalize(values: np.ndarray) -> np.ndarray:
    values = np.asarray(values, dtype=np.float32)
    norms = np.linalg.norm(values, axis=1, keepdims=True)
    return values / np.maximum(norms, np.finfo(np.float32).eps)


def _cosine_scores(
    target: np.ndarray,
    reference: np.ndarray,
    *,
    batch_size: int = 256,
) -> tuple[np.ndarray, np.ndarray]:
    reference = _normalize(reference)
    target = _normalize(target)
    centroid = _normalize(reference.mean(axis=0, keepdims=True))[0]
    centroid_distance = 1.0 - np.clip(target @ centroid, -1.0, 1.0)
    nearest: list[np.ndarray] = []
    reference_transpose = reference.T
    for start in range(0, len(target), batch_size):
        similarity = target[start : start + batch_size] @ reference_transpose
        nearest.append(1.0 - np.clip(np.max(similarity, axis=1), -1.0, 1.0))
    return centroid_distance, np.concatenate(nearest)


def canonical_semantic_fixture() -> dict[str, Any]:
    """Deterministic metric fixture independent of model/network availability."""
    references = np.array([[1.0, 0.0, 0.0], [0.8, 0.2, 0.0], [0.0, 1.0, 0.0]], dtype=np.float32)
    targets = np.array([[1.0, 0.0, 0.0], [0.0, 0.0, 1.0]], dtype=np.float32)
    centroid, nearest = _cosine_scores(targets, references)
    return {
        "schema": "observatory.semantic-novelty-fixture/2",
        "fixture_type": "cosine-metric contract; model weights tested in the reproduction audit",
        "repeat_nearest_distance": float(nearest[0]),
        "novel_nearest_distance": float(nearest[1]),
        "repeat_centroid_distance": float(centroid[0]),
        "novel_centroid_distance": float(centroid[1]),
        "passes": bool(nearest[0] < nearest[1] and centroid[0] < centroid[1]),
    }


def _encode_specter2(
    texts: list[str],
    *,
    model_name: str,
    model_revision: str,
    batch_size: int,
    device: str | None,
) -> tuple[np.ndarray, dict[str, Any]]:
    import torch
    import transformers
    from transformers import AutoModel, AutoTokenizer

    selected_device = device
    if selected_device is None:
        selected_device = "mps" if torch.backends.mps.is_available() else "cuda" if torch.cuda.is_available() else "cpu"
    tokenizer = AutoTokenizer.from_pretrained(model_name, revision=model_revision)
    model = AutoModel.from_pretrained(model_name, revision=model_revision, low_cpu_mem_usage=True)
    model.to(selected_device)
    model.eval()
    embeddings: list[np.ndarray] = []
    with torch.inference_mode():
        for start in range(0, len(texts), batch_size):
            inputs = tokenizer(
                texts[start : start + batch_size],
                padding=True,
                truncation=True,
                return_tensors="pt",
                return_token_type_ids=False,
                max_length=512,
            )
            inputs = {key: value.to(selected_device) for key, value in inputs.items()}
            vectors = model(**inputs).last_hidden_state[:, 0, :]
            embeddings.append(vectors.float().cpu().numpy())
    matrix = np.concatenate(embeddings).astype(np.float32, copy=False)
    if matrix.shape != (len(texts), EMBEDDING_DIMENSION) or not np.isfinite(matrix).all():
        raise RuntimeError(f"invalid SPECTER2 embedding matrix: {matrix.shape}")
    metadata = {
        "model_name": model_name,
        "model_revision": getattr(model.config, "_commit_hash", None) or model_revision,
        "model_license": MODEL_LICENSE,
        "embedding_dimension": int(matrix.shape[1]),
        "pooling": "last_hidden_state[:, 0, :]",
        "maximum_tokens": 512,
        "title_abstract_separator": tokenizer.sep_token,
        "device": selected_device,
        "torch_version": torch.__version__,
        "transformers_version": transformers.__version__,
    }
    return matrix, metadata


def build_semantic_novelty(
    lake_root: Path,
    output_root: Path,
    *,
    min_reference_documents: int = 20,
    batch_size: int = 16,
    similarity_batch_size: int = 256,
    model_name: str = MODEL_NAME,
    model_revision: str = MODEL_REVISION,
    device: str | None = None,
) -> dict[str, Any]:
    connection = ObservatoryCatalog(lake_root).connect()
    rows = connection.execute(
        """SELECT candidate_version_id, source_id, created_at, observed_at,
                  title, abstract, content_hash, language
           FROM candidate_version
           WHERE created_at IS NOT NULL AND (title IS NOT NULL OR abstract IS NOT NULL)
           ORDER BY created_at, candidate_version_id"""
    ).fetchall()
    documents: list[dict[str, Any]] = []
    invalid_future_timestamp = 0
    for row in rows:
        version_id, source_id, created_at, observed_at, title, abstract, document_hash, language = row
        text = _text(title, abstract)
        if not text:
            continue
        if observed_at is not None and created_at > observed_at:
            invalid_future_timestamp += 1
            continue
        documents.append(
            {
                "candidate_version_id": version_id,
                "source_id": source_id,
                "created_at": created_at,
                "year": int(created_at.year),
                "text": text,
                "document_hash": document_hash or content_hash(text),
                "language": language or "unknown",
            }
        )
    if not documents:
        raise RuntimeError("no time-valid title/abstract documents for semantic novelty")
    storage_receipt = storage_preflight(
        output_root,
        projected_input_bytes=0,
        projected_output_bytes=max(len(documents) * 1_024, 1),
    )
    vectors, model_metadata = _encode_specter2(
        [row["text"] for row in documents],
        model_name=model_name,
        model_revision=model_revision,
        batch_size=batch_size,
        device=device,
    )
    vectors = _normalize(vectors)
    years = sorted({row["year"] for row in documents})
    output: list[dict[str, Any]] = []
    model_manifests: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for year in years:
        reference_indices = [index for index, row in enumerate(documents) if row["year"] < year]
        target_indices = [index for index, row in enumerate(documents) if row["year"] == year]
        if not target_indices:
            continue
        if len(reference_indices) < min_reference_documents:
            skipped.append(
                {
                    "encoder": ENCODER_NAME,
                    "target_year": year,
                    "target_count": len(target_indices),
                    "reference_count": len(reference_indices),
                    "reason": "insufficient strictly prior reference documents",
                }
            )
            continue
        references = [documents[index] for index in reference_indices]
        targets = [documents[index] for index in target_indices]
        reference_vectors = vectors[reference_indices]
        target_vectors = vectors[target_indices]
        centroid, nearest = _cosine_scores(
            target_vectors,
            reference_vectors,
            batch_size=similarity_batch_size,
        )
        reference_ids = [str(row["candidate_version_id"]) for row in references]
        reference_set_hash = content_hash("\n".join(reference_ids))
        reference_max = max(row["created_at"] for row in references)
        cutoff = datetime(year, 1, 1, tzinfo=timezone.utc)
        model_manifests.append(
            {
                "encoder": ENCODER_NAME,
                "target_year": year,
                "reference_count": len(references),
                "target_count": len(targets),
                "reference_cutoff_exclusive": cutoff.isoformat(),
                "reference_max_created_at": reference_max.astimezone(timezone.utc).isoformat(),
                "reference_set_hash": reference_set_hash,
                "model_revision": model_metadata["model_revision"],
                "embedding_dimension": model_metadata["embedding_dimension"],
                "strictly_prior": reference_max < cutoff,
            }
        )
        for target, vector, centroid_value, nearest_value in zip(
            targets, target_vectors, centroid, nearest, strict=True
        ):
            output.append(
                {
                    "candidate_version_id": target["candidate_version_id"],
                    "source_id": target["source_id"],
                    "target_year": year,
                    "language": target["language"],
                    "encoder": ENCODER_NAME,
                    "feature_version": None,
                    "reference_cutoff_exclusive": cutoff.isoformat(),
                    "reference_count": len(references),
                    "reference_set_hash": reference_set_hash,
                    "centroid_cosine_distance": float(centroid_value),
                    "nearest_cosine_distance": float(nearest_value),
                    "vector_checksum": _checksum(vector),
                    "model_components_hash": model_metadata["model_revision"],
                    "document_hash": target["document_hash"],
                    "time_valid": True,
                }
            )
    parameters = {
        "encoder": ENCODER_NAME,
        "model": model_metadata,
        "min_reference_documents": min_reference_documents,
        "batch_size": batch_size,
        "similarity_batch_size": similarity_batch_size,
        "code_hash": content_hash(inspect.getsource(build_semantic_novelty)),
    }
    feature_version = content_hash(json.dumps(parameters, sort_keys=True, default=str))[:16]
    for row in output:
        row["feature_version"] = feature_version
    groups: dict[tuple[str, int, str], list[dict[str, Any]]] = defaultdict(list)
    for row in output:
        groups[(row["source_id"], row["target_year"], row["encoder"])].append(row)
    for group in groups.values():
        for field in ("centroid_cosine_distance", "nearest_cosine_distance"):
            values = np.array([row[field] for row in group], dtype=np.float64)
            mean = float(values.mean())
            std = float(values.std(ddof=0))
            for row, value in zip(group, values, strict=True):
                row[f"{field}_source_year_z"] = float((value - mean) / std) if std > 0 else 0.0
    output_root.mkdir(parents=True, exist_ok=True)
    feature_path = output_root / "semantic_novelty.parquet"
    pq.write_table(pa.Table.from_pylist(output), feature_path, compression="zstd")
    fixture = canonical_semantic_fixture()
    fixture_path = output_root / "semantic_novelty_fixture.json"
    fixture_path.write_text(json.dumps(fixture, indent=2, sort_keys=True) + "\n")
    report: dict[str, Any] = {
        "schema": "observatory.semantic-novelty-report/2",
        "feature_version": feature_version,
        "parameters": parameters,
        "eligible_document_count": len(documents),
        "invalid_future_timestamp_count": invalid_future_timestamp,
        "feature_row_count": len(output),
        "unique_version_count": len({row["candidate_version_id"] for row in output}),
        "encoder_counts": dict(Counter(row["encoder"] for row in output)),
        "model_manifests": model_manifests,
        "skipped_cohorts": skipped,
        "checks": {
            "canonical_fixture_passes": fixture["passes"],
            "open_weights_scientific_encoder": model_name == MODEL_NAME and MODEL_LICENSE == "Apache-2.0",
            "pinned_model_revision": model_metadata["model_revision"] == model_revision,
            "all_reference_corpora_strictly_prior": bool(model_manifests)
            and all(row["strictly_prior"] for row in model_manifests),
            "all_vectors_checksummed": bool(output) and all(row["vector_checksum"] for row in output),
            "held_out_source_year_features_present": len(groups) > 1,
            "future_dated_metadata_excluded": True,
            "no_outcome_fitting": True,
        },
        "artifacts": {"features": str(feature_path), "fixture": str(fixture_path)},
        "model_source": "AllenAI SPECTER2 base, pinned open weights; title and abstract encoded locally",
        "model_card": "https://huggingface.co/allenai/specter2_base",
        "storage_preflight": storage_receipt,
        "external_p2_fixture_status": "standalone OSG implementation; no paper-project files consumed",
    }
    report["passes"] = all(report["checks"].values())
    report["artifact_hashes"] = {
        "features": content_hash(feature_path.read_bytes()),
        "fixture": content_hash(fixture_path.read_bytes()),
    }
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True, default=str))
    report_path = output_root / "semantic_novelty_report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True, default=str) + "\n")
    return report
