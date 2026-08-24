"""Modal-only reducers for large public OSG validity sources.

Raw HUPD metadata is downloaded to ephemeral storage, verified, reduced to a
privacy-minimal analytical panel, and discarded with the container.  Only the
derived panel, aggregate cells, and provenance report enter the durable Volume.
No credentials or paid APIs are used.
"""

from __future__ import annotations

import modal

APP_NAME = "open-selection-graph-validity"
VOLUME_NAME = "open-selection-graph"
HUPD_COMMIT = "f570a84b03663180b6034c1f7f4c15864f94385e"
HUPD_BYTES = 988_373_410
HUPD_URL = (
    "https://huggingface.co/datasets/HUPD/hupd/resolve/"
    f"{HUPD_COMMIT}/hupd_metadata_2022-02-22.feather"
)

app = modal.App(APP_NAME)
volume = modal.Volume.from_name(VOLUME_NAME, create_if_missing=True)
image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.1",
    "pyarrow>=16",
    "requests>=2.31",
)
embedding_image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "numpy>=2.0",
    "pandas>=2.2",
    "pyarrow>=16",
    "sentence-transformers>=5.0",
    "scikit-learn>=1.5",
    "torch>=2.6",
    "transformers>=4.51",
)


@app.function(
    image=image,
    cpu=4.0,
    memory=16_384,
    timeout=10_800,
    volumes={"/volume": volume},
)
def build_hupd_population(*, force: bool = False) -> dict:
    """Build a hash-only HUPD application population layer.

    The 988 MB source file remains ephemeral.  Examiner names, docket numbers,
    titles, abstracts, claims, and descriptions are not read or retained.
    """
    import hashlib
    import json
    import re
    import time
    from pathlib import Path

    import duckdb
    import pyarrow as pa
    import pyarrow.feather as feather
    import pyarrow.parquet as pq
    import requests

    started = time.time()
    root = Path("/volume/workspace")
    output = root / "results" / "observatory" / "validity"
    output.mkdir(parents=True, exist_ok=True)
    panel_path = output / "hupd_application_population.parquet"
    cells_path = output / "hupd_population_cells.parquet"
    report_path = output / "hupd_population_report.json"
    if panel_path.is_file() and cells_path.is_file() and report_path.is_file() and not force:
        report = json.loads(report_path.read_text())
        if report.get("passes"):
            return report

    raw_path = Path("/tmp/hupd_metadata_2022-02-22.feather")
    digest = hashlib.sha256()
    observed_bytes = 0
    with requests.get(HUPD_URL, stream=True, timeout=(30, 600)) as response:
        response.raise_for_status()
        with raw_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                if not chunk:
                    continue
                handle.write(chunk)
                digest.update(chunk)
                observed_bytes += len(chunk)
    if observed_bytes != HUPD_BYTES:
        raise RuntimeError(f"HUPD byte-count mismatch: {observed_bytes} != {HUPD_BYTES}")

    retained = [
        "application_number",
        "filing_date",
        "application_invention_type",
        "examiner_art_unit",
        "uspc_class",
        "appl_status_desc",
        "appl_status_date",
        "earliest_pgpub_number",
        "earliest_pgpub_date",
        "patent_number",
        "patent_issue_date",
        "small_entity_indicator",
        "aia_first_to_file",
        "publication_number",
        "date_application_published",
        "main_cpc_label",
        "main_ipcr_label",
        "foreign",
        "continuation",
        "decision",
        "decision_as_of_2020",
    ]
    table = feather.read_table(raw_path, columns=retained, memory_map=True)
    excluded_sensitive = {
        "examiner_full_name",
        "confirm_number",
        "atty_docket_number",
        "invention_title",
        "file_location",
    }
    if excluded_sensitive.intersection(table.column_names):
        raise RuntimeError("sensitive HUPD columns entered the retained table")

    digits = re.compile(r"[^0-9]")

    def hashed_identifier(value: object) -> str | None:
        if value is None:
            return None
        normalized = digits.sub("", str(value))
        if not normalized:
            return None
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    schema = pa.schema(
        [
            ("application_number_hash", pa.string()),
            ("publication_number_hash", pa.string()),
            ("patent_number_hash", pa.string()),
            ("filing_date", pa.timestamp("ns")),
            ("filing_year", pa.int16()),
            ("application_invention_type", pa.string()),
            ("examiner_art_unit", pa.string()),
            ("uspc_class", pa.string()),
            ("application_status", pa.string()),
            ("application_status_date", pa.timestamp("ns")),
            ("earliest_publication_date", pa.timestamp("ns")),
            ("patent_issue_date", pa.timestamp("ns")),
            ("date_application_published", pa.timestamp("ns")),
            ("small_entity_indicator", pa.string()),
            ("aia_first_to_file", pa.string()),
            ("main_cpc_label", pa.string()),
            ("main_ipcr_label", pa.string()),
            ("foreign", pa.bool_()),
            ("continuation", pa.int64()),
            ("decision", pa.string()),
            ("decision_as_of_2020", pa.string()),
            ("source_commit", pa.string()),
        ]
    )
    writer = pq.ParquetWriter(panel_path, schema, compression="zstd")
    try:
        for batch in table.to_batches(max_chunksize=100_000):
            frame = pa.Table.from_batches([batch]).to_pydict()
            filing_dates = frame["filing_date"]
            rows = {
                "application_number_hash": [hashed_identifier(v) for v in frame["application_number"]],
                "publication_number_hash": [hashed_identifier(v) for v in frame["publication_number"]],
                "patent_number_hash": [hashed_identifier(v) for v in frame["patent_number"]],
                "filing_date": filing_dates,
                "filing_year": [v.year if v is not None else None for v in filing_dates],
                "application_invention_type": frame["application_invention_type"],
                "examiner_art_unit": frame["examiner_art_unit"],
                "uspc_class": frame["uspc_class"],
                "application_status": frame["appl_status_desc"],
                "application_status_date": frame["appl_status_date"],
                "earliest_publication_date": frame["earliest_pgpub_date"],
                "patent_issue_date": frame["patent_issue_date"],
                "date_application_published": frame["date_application_published"],
                "small_entity_indicator": frame["small_entity_indicator"],
                "aia_first_to_file": frame["aia_first_to_file"],
                "main_cpc_label": frame["main_cpc_label"],
                "main_ipcr_label": frame["main_ipcr_label"],
                "foreign": frame["foreign"],
                "continuation": frame["continuation"],
                "decision": frame["decision"],
                "decision_as_of_2020": frame["decision_as_of_2020"],
                "source_commit": [HUPD_COMMIT] * len(filing_dates),
            }
            writer.write_table(pa.Table.from_pydict(rows, schema=schema))
    finally:
        writer.close()

    connection = duckdb.connect()
    quoted_panel = str(panel_path).replace("'", "''")
    quoted_cells = str(cells_path).replace("'", "''")
    connection.execute(
        f"""
        COPY (
          SELECT filing_year,
                 substr(coalesce(main_cpc_label, ''), 1, 1) AS cpc_section,
                 coalesce(decision_as_of_2020, decision, 'UNRESOLVED') AS decision_state,
                 count(*) AS application_count,
                 count(DISTINCT application_number_hash) AS distinct_application_count,
                 count(patent_issue_date) AS issued_patent_count,
                 count(date_application_published) AS published_application_count
          FROM read_parquet('{quoted_panel}')
          GROUP BY ALL
          ORDER BY filing_year, cpc_section, decision_state
        ) TO '{quoted_cells}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    summary = connection.execute(
        f"""
        SELECT count(*) AS rows,
               count(DISTINCT application_number_hash) AS distinct_applications,
               min(filing_year) AS first_filing_year,
               max(filing_year) AS last_filing_year,
               count(*) FILTER (WHERE decision_as_of_2020 = 'ACCEPTED') AS accepted,
               count(*) FILTER (WHERE decision_as_of_2020 = 'REJECTED') AS rejected,
               count(*) FILTER (WHERE coalesce(decision_as_of_2020, decision) IS NULL) AS unresolved,
               count(*) FILTER (WHERE date_application_published IS NOT NULL) AS public_applications,
               count(*) FILTER (WHERE patent_issue_date IS NOT NULL) AS issued_patents
        FROM read_parquet('{quoted_panel}')
        """
    ).fetchone()
    cell_count = connection.execute(
        f"SELECT count(*) FROM read_parquet('{quoted_cells}')"
    ).fetchone()[0]
    connection.close()

    report = {
        "schema": "observatory.hupd-population-report/1",
        "source": "Harvard USPTO Patent Dataset (HUPD) metadata",
        "source_url": HUPD_URL,
        "source_commit": HUPD_COMMIT,
        "source_bytes": observed_bytes,
        "source_sha256": digest.hexdigest(),
        "license": "CC-BY-NC-SA-4.0 (conservative reading of the project data card)",
        "rows": summary[0],
        "distinct_applications": summary[1],
        "first_filing_year": summary[2],
        "last_filing_year": summary[3],
        "accepted_as_of_2020": summary[4],
        "rejected_as_of_2020": summary[5],
        "unresolved_decision": summary[6],
        "published_application_rows": summary[7],
        "issued_patent_rows": summary[8],
        "population_cells": cell_count,
        "retained_identifier_policy": "SHA-256 of digits-only public application/publication/patent number",
        "person_fields_retained": [],
        "text_payload_fields_retained": [],
        "explicitly_excluded_fields": sorted(excluded_sensitive),
        "raw_input_persisted": False,
        "population_boundary": (
            "English-language US utility patent applications in HUPD, filed 2004-2018; "
            "not confidential, unpublished, non-utility, or post-2018 applications"
        ),
        "modal_resource_ceiling": {
            "cpu": 4.0,
            "memory_mib": 16_384,
            "ephemeral_disk": "Modal default (no paid large-disk override)",
            "timeout_seconds": 10_800,
            "hard_project_budget_usd": 30.0,
        },
        "elapsed_seconds": round(time.time() - started, 3),
    }
    report["passes"] = bool(
        observed_bytes == HUPD_BYTES
        and summary[0] == summary[1]
        and summary[2] == 2004
        and summary[3] == 2018
        and not report["person_fields_retained"]
        and not report["text_payload_fields_retained"]
        and not report["raw_input_persisted"]
    )
    report["report_hash"] = hashlib.sha256(
        json.dumps(report, sort_keys=True).encode("utf-8")
    ).hexdigest()
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    volume.commit()
    return report


@app.function(
    image=embedding_image,
    gpu="L4",
    cpu=4.0,
    memory=16_384,
    timeout=7_200,
    volumes={"/volume": volume},
)
def build_qwen3_semantic_novelty() -> dict:
    """Encode OSG title/abstracts with pinned Qwen3 and retain no text/vectors."""
    import hashlib
    import json
    import time
    from collections import defaultdict
    from pathlib import Path

    import numpy as np
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    import torch
    from sentence_transformers import SentenceTransformer

    model_name = "Qwen/Qwen3-Embedding-0.6B"
    model_revision = "97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3"
    started = time.time()
    root = Path("/volume/workspace")
    input_path = root / "staging" / "validity" / "semantic_ruler_input.parquet"
    input_report_path = input_path.with_suffix(".report.json")
    output = root / "results" / "observatory" / "validity"
    output.mkdir(parents=True, exist_ok=True)
    if not input_path.is_file() or not input_report_path.is_file():
        raise FileNotFoundError("upload semantic_ruler_input.parquet and its report to the Volume")
    input_report = json.loads(input_report_path.read_text())
    digest = hashlib.sha256(input_path.read_bytes()).hexdigest()
    if digest != input_report["input_sha256"]:
        raise RuntimeError("semantic ruler input hash mismatch")
    frame = pq.read_table(input_path).to_pandas()
    frame["created_at"] = pd.to_datetime(frame["created_at"], utc=True, errors="raise")
    frame["year"] = frame["created_at"].dt.year.astype(int)

    model = SentenceTransformer(
        model_name,
        revision=model_revision,
        model_kwargs={"torch_dtype": torch.bfloat16},
    )
    model.max_seq_length = 512
    vectors = model.encode(
        frame["text"].tolist(),
        batch_size=32,
        convert_to_numpy=True,
        normalize_embeddings=True,
        show_progress_bar=False,
    ).astype(np.float32, copy=False)
    if vectors.shape != (len(frame), 1024) or not np.isfinite(vectors).all():
        raise RuntimeError(f"unexpected Qwen3 embedding matrix {vectors.shape}")

    feature_rows = []
    manifests = []
    for year in sorted(frame["year"].unique()):
        reference_indices = np.flatnonzero(frame["year"].to_numpy() < year)
        target_indices = np.flatnonzero(frame["year"].to_numpy() == year)
        if len(reference_indices) < 20:
            continue
        reference = vectors[reference_indices]
        targets = vectors[target_indices]
        centroid = reference.mean(axis=0)
        centroid /= max(float(np.linalg.norm(centroid)), np.finfo(np.float32).eps)
        centroid_distance = 1.0 - np.clip(targets @ centroid, -1.0, 1.0)
        nearest_distance = np.empty(len(target_indices), dtype=np.float32)
        for start in range(0, len(target_indices), 256):
            similarities = targets[start : start + 256] @ reference.T
            nearest_distance[start : start + 256] = 1.0 - np.clip(
                similarities.max(axis=1), -1.0, 1.0
            )
        reference_ids = frame.iloc[reference_indices]["candidate_version_id"].astype(str).tolist()
        reference_hash = hashlib.sha256("\n".join(reference_ids).encode("utf-8")).hexdigest()
        manifests.append(
            {
                "target_year": int(year),
                "reference_count": len(reference_indices),
                "target_count": len(target_indices),
                "reference_set_hash": reference_hash,
                "strictly_prior": True,
            }
        )
        for position, frame_index in enumerate(target_indices):
            item = frame.iloc[frame_index]
            vector_hash = hashlib.sha256(
                np.asarray(vectors[frame_index], dtype="<f4").round(7).tobytes()
            ).hexdigest()
            feature_rows.append(
                {
                    "candidate_version_id": item["candidate_version_id"],
                    "source_id": item["source_id"],
                    "target_year": int(year),
                    "encoder": "qwen3_embedding_0_6b",
                    "model_revision": model_revision,
                    "reference_count": len(reference_indices),
                    "reference_set_hash": reference_hash,
                    "centroid_cosine_distance": float(centroid_distance[position]),
                    "nearest_cosine_distance": float(nearest_distance[position]),
                    "vector_checksum": vector_hash,
                    "time_valid": True,
                    "text_released": False,
                    "vector_released": False,
                    "outcome_fitted": False,
                }
            )
    groups = defaultdict(list)
    for row in feature_rows:
        groups[(row["source_id"], row["target_year"])].append(row)
    for group in groups.values():
        for field in ("centroid_cosine_distance", "nearest_cosine_distance"):
            values = np.asarray([row[field] for row in group], dtype=float)
            mean, std = float(values.mean()), float(values.std(ddof=0))
            for row, value in zip(group, values, strict=True):
                row[f"{field}_source_year_z"] = float((value - mean) / std) if std else 0.0
    feature_path = output / "qwen3_semantic_novelty.parquet"
    pq.write_table(pa.Table.from_pylist(feature_rows), feature_path, compression="zstd")
    feature_sha = hashlib.sha256(feature_path.read_bytes()).hexdigest()
    report = {
        "schema": "open-selection-graph.qwen3-semantic-novelty/1",
        "model": model_name,
        "model_revision": model_revision,
        "model_license": "Apache-2.0",
        "parameters": 595_776_512,
        "embedding_dimension": 1024,
        "maximum_tokens": 512,
        "eligible_documents": len(frame),
        "feature_rows": len(feature_rows),
        "input_sha256": digest,
        "feature_sha256": feature_sha,
        "model_manifests": manifests,
        "strictly_prior_references": all(row["strictly_prior"] for row in manifests),
        "input_text_persisted": False,
        "embedding_vectors_persisted": False,
        "outcomes_used": False,
        "gpu": "L4",
        "elapsed_seconds": round(time.time() - started, 3),
    }
    report["passes"] = bool(
        len(feature_rows) > 8_000
        and report["strictly_prior_references"]
        and not report["input_text_persisted"]
        and not report["embedding_vectors_persisted"]
        and not report["outcomes_used"]
    )
    report["report_hash"] = hashlib.sha256(
        json.dumps(report, sort_keys=True).encode("utf-8")
    ).hexdigest()
    (output / "qwen3_semantic_novelty_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    input_path.unlink()
    input_report_path.unlink()
    volume.commit()
    return report


@app.function(
    image=embedding_image,
    gpu="L4",
    cpu=4.0,
    memory=16_384,
    timeout=7_200,
    volumes={"/volume": volume},
)
def build_qwen3_review_benchmark() -> dict:
    """Run submission-grouped construct validation with frozen Qwen3 embeddings."""
    import hashlib
    import json
    import time
    from pathlib import Path

    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq
    import torch
    from sentence_transformers import SentenceTransformer
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import GroupKFold

    model_name = "Qwen/Qwen3-Embedding-0.6B"
    model_revision = "97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3"
    constructs = (
        "novelty_originality",
        "significance_impact",
        "soundness_evidence",
        "clarity_writing",
        "presentation_formatting",
    )
    started = time.time()
    root = Path("/volume/workspace")
    input_path = root / "staging" / "validity" / "qwen3_review_benchmark_input.parquet"
    input_report_path = input_path.with_suffix(".report.json")
    output = root / "results" / "observatory" / "validity"
    output.mkdir(parents=True, exist_ok=True)
    if not input_path.is_file() or not input_report_path.is_file():
        raise FileNotFoundError("upload Qwen3 review benchmark input and report to the Volume")
    input_report = json.loads(input_report_path.read_text())
    input_sha256 = hashlib.sha256(input_path.read_bytes()).hexdigest()
    if input_sha256 != input_report["input_sha256"]:
        raise RuntimeError("Qwen3 review benchmark input hash mismatch")
    frame = pq.read_table(input_path).to_pandas()
    if frame["span_id"].duplicated().any() or len(frame) != input_report["spans"]:
        raise RuntimeError("Qwen3 review benchmark input population mismatch")

    model = SentenceTransformer(
        model_name,
        revision=model_revision,
        model_kwargs={"torch_dtype": torch.bfloat16},
    )
    model.max_seq_length = 512
    vectors = model.encode(
        frame["text"].tolist(),
        batch_size=32,
        convert_to_numpy=True,
        normalize_embeddings=True,
        show_progress_bar=False,
    ).astype(np.float32, copy=False)
    if vectors.shape != (len(frame), 1024) or not np.isfinite(vectors).all():
        raise RuntimeError(f"unexpected Qwen3 review embedding matrix {vectors.shape}")

    splitter = GroupKFold(n_splits=5)
    groups = frame["submission_id"].astype(str).to_numpy()
    indices = np.arange(len(frame))
    output_rows = {"span_id": frame["span_id"].astype(str).tolist()}
    positive_counts = {}
    for construct in constructs:
        labels = frame[f"label_{construct}"].to_numpy(np.int8)
        probabilities = np.zeros(len(frame), dtype=np.float64)
        for train, test in splitter.split(indices, groups=groups):
            classifier = LogisticRegression(
                C=1.0,
                class_weight="balanced",
                max_iter=1_000,
                random_state=1729,
            )
            classifier.fit(vectors[train], labels[train])
            probabilities[test] = classifier.predict_proba(vectors[test])[:, 1]
        output_rows[f"probability_{construct}"] = probabilities.tolist()
        positive_counts[construct] = int(labels.sum())

    output_path = output / "qwen3_freeform_construct_oof.parquet"
    pq.write_table(pa.Table.from_pydict(output_rows), output_path, compression="zstd")
    report = {
        "schema": "open-selection-graph.qwen3-freeform-construct-oof/1",
        "model": model_name,
        "model_revision": model_revision,
        "model_license": "Apache-2.0",
        "parameters": 595_776_512,
        "embedding_dimension": 1024,
        "maximum_tokens": 512,
        "spans": len(frame),
        "submissions": int(frame["submission_id"].nunique()),
        "positive_counts": positive_counts,
        "grouped_cross_validation_folds": 5,
        "group_unit": "submission",
        "classifier": "class-balanced logistic regression",
        "input_sha256": input_sha256,
        "output_sha256": hashlib.sha256(output_path.read_bytes()).hexdigest(),
        "input_text_persisted": False,
        "embedding_vectors_persisted": False,
        "out_of_fold_predictions_only": True,
        "tfidf_or_bag_of_words_used": False,
        "gpu": "L4",
        "elapsed_seconds": round(time.time() - started, 3),
    }
    report["passes"] = bool(
        len(frame) > 10_000
        and report["submissions"] > 250
        and report["out_of_fold_predictions_only"]
        and not report["input_text_persisted"]
        and not report["embedding_vectors_persisted"]
        and not report["tfidf_or_bag_of_words_used"]
    )
    report["report_hash"] = hashlib.sha256(
        json.dumps(report, sort_keys=True).encode("utf-8")
    ).hexdigest()
    (output / "qwen3_freeform_construct_oof_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    input_path.unlink()
    input_report_path.unlink()
    volume.commit()
    return report


@app.function(
    image=embedding_image,
    gpu="L4",
    cpu=4.0,
    memory=16_384,
    timeout=7_200,
    volumes={"/volume": volume},
)
def build_qwen3_lineage_neighbors() -> dict:
    """Retrieve title neighbours with pinned Qwen3 without retaining text or vectors."""
    import hashlib
    import json
    import time
    from pathlib import Path

    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq
    import torch
    from sentence_transformers import SentenceTransformer

    model_name = "Qwen/Qwen3-Embedding-0.6B"
    model_revision = "97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3"
    started = time.time()
    root = Path("/volume/workspace")
    input_path = root / "staging" / "validity" / "qwen3_lineage_input.parquet"
    input_report_path = input_path.with_suffix(".report.json")
    output = root / "results" / "observatory" / "validity"
    output.mkdir(parents=True, exist_ok=True)
    if not input_path.is_file() or not input_report_path.is_file():
        raise FileNotFoundError("upload Qwen3 lineage input and report to the Volume")
    input_report = json.loads(input_report_path.read_text())
    input_sha256 = hashlib.sha256(input_path.read_bytes()).hexdigest()
    if input_sha256 != input_report["input_sha256"]:
        raise RuntimeError("Qwen3 lineage input hash mismatch")
    frame = pq.read_table(input_path).to_pandas()
    if frame["candidate_version_id"].duplicated().any() or len(frame) != input_report["versions"]:
        raise RuntimeError("Qwen3 lineage input population mismatch")

    model = SentenceTransformer(
        model_name,
        revision=model_revision,
        model_kwargs={"torch_dtype": torch.bfloat16},
    )
    model.max_seq_length = 512
    vectors = model.encode(
        frame["text"].tolist(),
        batch_size=32,
        convert_to_numpy=True,
        normalize_embeddings=True,
        show_progress_bar=False,
    ).astype(np.float32, copy=False)
    if vectors.shape != (len(frame), 1024) or not np.isfinite(vectors).all():
        raise RuntimeError(f"unexpected Qwen3 lineage embedding matrix {vectors.shape}")

    version_ids = frame["candidate_version_id"].astype(str).to_numpy()
    pair_scores: dict[tuple[str, str], float] = {}
    k = min(12, len(frame))
    for start in range(0, len(frame), 256):
        similarities = vectors[start : start + 256] @ vectors.T
        for local_index, row_scores in enumerate(similarities):
            source_index = start + local_index
            row_scores[source_index] = -np.inf
            neighbor_indices = np.argpartition(row_scores, -k)[-k:]
            neighbor_indices = neighbor_indices[np.argsort(row_scores[neighbor_indices])[::-1]][:11]
            for target_index in neighbor_indices:
                pair = tuple(sorted((version_ids[source_index], version_ids[target_index])))
                pair_scores[pair] = max(pair_scores.get(pair, -1.0), float(row_scores[target_index]))
    rows = [
        {
            "source_version_id": pair[0],
            "target_version_id": pair[1],
            "title_embedding_cosine": score,
            "candidate_generator_rank_limit": 11,
            "canonical_merge_forced": False,
        }
        for pair, score in sorted(pair_scores.items())
    ]
    output_path = output / "qwen3_lineage_title_neighbors.parquet"
    pq.write_table(pa.Table.from_pylist(rows), output_path, compression="zstd")
    report = {
        "schema": "open-selection-graph.qwen3-lineage-title-neighbors/1",
        "model": model_name,
        "model_revision": model_revision,
        "model_license": "Apache-2.0",
        "parameters": 595_776_512,
        "embedding_dimension": 1024,
        "versions": len(frame),
        "unique_neighbor_pairs": len(rows),
        "neighbors_per_version": 11,
        "input_sha256": input_sha256,
        "output_sha256": hashlib.sha256(output_path.read_bytes()).hexdigest(),
        "input_text_persisted": False,
        "embedding_vectors_persisted": False,
        "outcomes_used": False,
        "canonical_merge_forced": False,
        "tfidf_or_bag_of_words_used": False,
        "gpu": "L4",
        "elapsed_seconds": round(time.time() - started, 3),
    }
    report["passes"] = bool(
        len(frame) > 8_000
        and len(rows) > 40_000
        and not report["input_text_persisted"]
        and not report["embedding_vectors_persisted"]
        and not report["outcomes_used"]
        and not report["canonical_merge_forced"]
    )
    report["report_hash"] = hashlib.sha256(
        json.dumps(report, sort_keys=True).encode("utf-8")
    ).hexdigest()
    (output / "qwen3_lineage_title_neighbors_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    input_path.unlink()
    input_report_path.unlink()
    volume.commit()
    return report


@app.local_entrypoint()
def main(source: str = "hupd", force: bool = False):
    import json

    if source == "hupd":
        report = build_hupd_population.remote(force=force)
    elif source == "qwen3":
        report = build_qwen3_semantic_novelty.remote()
    elif source == "qwen3-review":
        report = build_qwen3_review_benchmark.remote()
    elif source == "qwen3-lineage":
        report = build_qwen3_lineage_neighbors.remote()
    else:
        raise ValueError(f"unsupported source: {source}")
    print(json.dumps(report, indent=2, sort_keys=True))
