"""Products that make the principal OSG validity boundaries estimable.

These builders do not convert missing populations into observed data.  They
publish partial-identification regions, transport diagnostics, lawful rebuild
contracts, and external human benchmarks so analyses can propagate rather
than merely mention the relevant uncertainty.
"""

from __future__ import annotations

import json
import math
import re
import zipfile
from collections import Counter
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    cohen_kappa_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold

from .ids import content_hash, stable_id
from .modern_novelty import build_modern_ruler_triangulation
from .storage import ObservatoryCatalog


def _write(rows: list[dict[str, Any]] | pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = rows.copy() if isinstance(rows, pd.DataFrame) else pd.DataFrame(rows)
    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = frame[column].map(
                lambda value: None
                if value is None
                or (
                    not isinstance(value, (dict, list, tuple, set, np.ndarray))
                    and bool(pd.isna(value))
                )
                else value
            )
    table = pa.Table.from_pandas(frame, preserve_index=False)
    pq.write_table(table, path, compression="zstd")


def _write_report(path: Path, body: dict[str, Any]) -> dict[str, Any]:
    body["report_hash"] = content_hash(json.dumps(body, sort_keys=True, default=str))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body, indent=2, sort_keys=True, default=str) + "\n")
    return body


def build_observability_bounds(workspace: Path, output: Path) -> dict[str, Any]:
    """Propagate hidden-screen uncertainty without inventing hidden candidates."""
    census_path = workspace / "results" / "observatory" / "r3" / "gate_cycle_observability_census.parquet"
    census = pd.read_parquet(census_path)
    fractions = np.asarray((0.10, 0.20, 0.33, 0.50, 0.67, 0.80, 1.00))
    bounds: list[dict[str, Any]] = []
    sensitivity: list[dict[str, Any]] = []
    for row in census.itertuples(index=False):
        observed = row.observable_count
        if pd.isna(observed):
            observed = row.provider_found_count
        observed = None if pd.isna(observed) else int(observed)
        entry_observed = row.received_count
        entry_observed = None if pd.isna(entry_observed) else int(entry_observed)
        grade = str(row.effective_observability_grade)
        entry_identified = grade == "A" and entry_observed is not None
        hidden_screen = grade in {"B", "C", "D", "U"}
        bounds.append(
            {
                "gate_cycle_id": row.gate_cycle_id,
                "source_id": row.source_id,
                "architecture": row.architecture,
                "cycle_start": row.cycle_start,
                "effective_observability_grade": grade,
                "earliest_observed_population": observed,
                "entry_population_point": entry_observed if entry_identified else None,
                "entry_population_lower": entry_observed if entry_identified else observed,
                "entry_population_upper": entry_observed if entry_identified else None,
                "hidden_candidate_lower": 0 if entry_identified else None,
                "hidden_candidate_upper": 0 if entry_identified else None,
                "entry_selection_probability_lower": (
                    row.selected_count / entry_observed
                    if entry_identified and entry_observed and not pd.isna(row.selected_count)
                    else 0.0
                ),
                "entry_selection_probability_upper": (
                    row.selected_count / entry_observed
                    if entry_identified and entry_observed and not pd.isna(row.selected_count)
                    else 1.0
                ),
                "stage_conditional_rate_identified": bool(row.selection_rate_eligible),
                "hidden_screen_conditioning": hidden_screen,
                "upper_bound_open": not entry_identified,
                "identification_basis": (
                    "observed_entry_population" if entry_identified else "Manski_no-assumption_bounds"
                ),
                "hidden_candidates_recovered": False,
            }
        )
        if not hidden_screen or observed is None:
            continue
        selected = 0 if pd.isna(row.selected_count) else int(row.selected_count)
        for fraction in fractions:
            inferred_entry = int(math.ceil(observed / float(fraction))) if observed else 0
            sensitivity.append(
                {
                    "gate_cycle_id": row.gate_cycle_id,
                    "source_id": row.source_id,
                    "assumed_public_stage_capture_fraction": float(fraction),
                    "observed_public_stage_count": observed,
                    "implied_entry_population": inferred_entry,
                    "implied_hidden_candidates": inferred_entry - observed,
                    "implied_entry_selection_rate": selected / inferred_entry if inferred_entry else None,
                    "assumption_not_observation": True,
                }
            )
    _write(bounds, output / "observability_partial_identification.parquet")
    _write(sensitivity, output / "hidden_screen_sensitivity.parquet")

    with ObservatoryCatalog(workspace / "data" / "observatory" / "normalized").connect() as connection:
        history = connection.execute(
            """
            SELECT coverage_observation_id, gate_cycle_id, source_id, object_type,
                   earliest_public_stage, observability_grade, expected_count,
                   found_count, expected_count_method, known_hidden_stages,
                   known_exclusions, audit_status, valid_from, valid_to, observed_at,
                   record_version
            FROM coverage_observation
            ORDER BY source_id, gate_cycle_id, object_type, observed_at, record_version
            """
        ).fetchdf()
    history["previous_grade"] = history.groupby(
        ["source_id", "gate_cycle_id", "object_type"], dropna=False
    )["observability_grade"].shift()
    history["grade_changed"] = history["previous_grade"].notna() & (
        history["previous_grade"] != history["observability_grade"]
    )
    history["time_semantics"] = "recorded observation interval; not imputed policy history"
    _write(history, output / "coverage_record_history.parquet")

    snapshot_registry = yaml.safe_load(
        (workspace / "configs" / "observatory" / "observability_snapshots.yaml").read_text()
    )
    public_snapshot_rows: list[dict[str, Any]] = []
    snapshot_summaries: list[dict[str, Any]] = []
    for record_version, snapshot in enumerate(snapshot_registry["snapshots"], 1):
        evidence_path = workspace / snapshot["evidence_path"]
        if not evidence_path.is_file():
            raise FileNotFoundError(f"public observability snapshot missing: {evidence_path}")
        audit = json.loads(evidence_path.read_text())
        declared_hash = audit.get("report_hash")
        hash_body = {key: value for key, value in audit.items() if key != "report_hash"}
        computed_hash = content_hash(json.dumps(hash_body, sort_keys=True, default=str))
        if declared_hash != computed_hash or not audit.get("passes"):
            raise RuntimeError(f"public observability snapshot failed integrity: {evidence_path}")
        observed_at = audit.get("observed_at") or snapshot["observed_at"]
        snapshot_summaries.append(
            {
                "snapshot_id": snapshot["snapshot_id"],
                "observed_at": observed_at,
                "cycles": len(audit["cycles"]),
                "report_hash": declared_hash,
                "evidence_path": snapshot["evidence_path"],
            }
        )
        for cycle in audit["cycles"]:
            public_snapshot_rows.append(
                {
                    "snapshot_id": snapshot["snapshot_id"],
                    "source_id": snapshot["source_id"],
                    "gate_cycle_native_id": cycle["venue_id"],
                    "object_type": "provider_public_note_state",
                    "observability_grade": cycle["observability_grade"],
                    "provider_observable_state_count": cycle[
                        "state_counts_sum_before_note_id_deduplication"
                    ],
                    "all_named_state_invitations_audited": cycle[
                        "all_named_state_invitations_audited"
                    ],
                    "submission_notes_explicitly_public": cycle[
                        "submission_notes_explicitly_public"
                    ],
                    "observed_at": observed_at,
                    "record_version": record_version,
                    "evidence_path": snapshot["evidence_path"],
                    "evidence_sha256": content_hash(evidence_path.read_bytes()),
                    "report_hash": declared_hash,
                    "time_semantics": "live provider count and public-reader audit at snapshot time",
                }
            )
    public_history = pd.DataFrame(public_snapshot_rows).sort_values(
        ["source_id", "gate_cycle_native_id", "record_version"]
    )
    public_history["previous_grade"] = public_history.groupby(
        ["source_id", "gate_cycle_native_id", "object_type"], dropna=False
    )["observability_grade"].shift()
    public_history["previous_provider_observable_state_count"] = public_history.groupby(
        ["source_id", "gate_cycle_native_id", "object_type"], dropna=False
    )["provider_observable_state_count"].shift()
    public_history["grade_changed"] = public_history["previous_grade"].notna() & (
        public_history["previous_grade"] != public_history["observability_grade"]
    )
    public_history["provider_count_changed"] = public_history[
        "previous_provider_observable_state_count"
    ].notna() & (
        public_history["previous_provider_observable_state_count"]
        != public_history["provider_observable_state_count"]
    )
    _write(public_history, output / "observability_snapshot_history.parquet")
    report = {
        "schema": "open-selection-graph.observability-bounds/2",
        "cycles": len(bounds),
        "cycles_with_open_entry_upper_bound": sum(row["upper_bound_open"] for row in bounds),
        "sensitivity_rows": len(sensitivity),
        "coverage_record_history_rows": len(history),
        "public_observability_snapshots": len(snapshot_summaries),
        "public_snapshot_summaries": snapshot_summaries,
        "snapshot_rows": len(public_history),
        "recorded_grade_changes": int(public_history["grade_changed"].sum()),
        "recorded_provider_count_changes": int(
            public_history["provider_count_changed"].sum()
        ),
        "temporal_change_coverage": len(snapshot_summaries) >= 2,
        "hidden_candidates_recovered": 0,
        "no_assumption_entry_probability_region": "[0,1] when entry population is hidden",
        "passes": bool(bounds)
        and bool(sensitivity)
        and len(snapshot_summaries) >= 2
        and public_history["record_version"].nunique() >= 2
        and not any(row["hidden_candidates_recovered"] for row in bounds),
    }
    return _write_report(output / "observability_bounds_report.json", report)


def _range_overlap(left: pd.Series, right: pd.Series) -> float | None:
    a = pd.to_numeric(left, errors="coerce").dropna()
    b = pd.to_numeric(right, errors="coerce").dropna()
    if len(a) < 2 or len(b) < 2:
        return None
    low = max(float(a.quantile(0.05)), float(b.quantile(0.05)))
    high = min(float(a.quantile(0.95)), float(b.quantile(0.95)))
    union_low = min(float(a.quantile(0.05)), float(b.quantile(0.05)))
    union_high = max(float(a.quantile(0.95)), float(b.quantile(0.95)))
    return max(0.0, high - low) / max(union_high - union_low, 1e-12)


def _standardized_difference(left: pd.Series, right: pd.Series) -> float | None:
    a = pd.to_numeric(left, errors="coerce").dropna()
    b = pd.to_numeric(right, errors="coerce").dropna()
    if len(a) < 2 or len(b) < 2:
        return None
    pooled = math.sqrt((float(a.var(ddof=1)) + float(b.var(ddof=1))) / 2)
    return abs(float(a.mean()) - float(b.mean())) / pooled if pooled else 0.0


def _clean_pointer_value(value: Any) -> str | None:
    """Normalize nullable dataframe scalars before truth-value validation."""
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def _genuine_sha256(value: Any) -> bool:
    """Accept a complete SHA-256 digest and reject obvious sentinel values."""
    digest = (_clean_pointer_value(value) or "").lower()
    return bool(_SHA256_PATTERN.fullmatch(digest)) and len(set(digest)) > 1


def _usable_https_target(value: Any) -> bool:
    """Require an absolute HTTPS retrieval target with a network location."""
    target = _clean_pointer_value(value)
    if target is None:
        return False
    parsed = urlparse(target)
    return parsed.scheme == "https" and bool(parsed.netloc)


def _pointer_contract_passes(rows: list[dict[str, Any]]) -> bool:
    """Require a public retrieval target and a payload-derived hash per pointer."""
    return bool(rows) and all(
        _usable_https_target(row.get("source_url"))
        and _usable_https_target(row.get("retrieval_target"))
        and bool(_clean_pointer_value(row.get("object_locator")))
        and bool(row.get("hash_verification_required"))
        and (
            _genuine_sha256(row.get("expected_byte_hash"))
            or _genuine_sha256(row.get("expected_normalized_text_hash"))
        )
        and not bool(row.get("automatic_redistribution_allowed"))
        for row in rows
    )


def audit_pointer_rebuild_registry(workspace: Path) -> dict[str, Any]:
    """Independently verify the released pointer registry and its report."""
    registry_path = (
        workspace / "results" / "observatory" / "validity" / "pointer_rebuild_registry.parquet"
    )
    report_path = registry_path.with_name("pointer_rebuild_registry_report.json")
    if not registry_path.is_file() or not report_path.is_file():
        return {
            "passes": False,
            "reason": "pointer registry or report is missing",
            "registry_path": str(registry_path.relative_to(workspace)),
            "report_path": str(report_path.relative_to(workspace)),
        }
    try:
        frame = pd.read_parquet(registry_path)
        rows = frame.to_dict(orient="records")
        report = json.loads(report_path.read_text())
    except (OSError, ValueError, json.JSONDecodeError) as error:
        return {
            "passes": False,
            "reason": f"pointer registry could not be read: {type(error).__name__}",
            "registry_path": str(registry_path.relative_to(workspace)),
            "report_path": str(report_path.relative_to(workspace)),
        }
    declared_hash = report.get("report_hash")
    report_body = {key: value for key, value in report.items() if key != "report_hash"}
    computed_hash = content_hash(json.dumps(report_body, sort_keys=True, default=str))
    invalid_ids = [
        str(row.get("content_artifact_id"))
        for row in rows
        if not _pointer_contract_passes([row])
    ]
    counts_match = bool(
        report.get("artifacts") == len(rows)
        and report.get("with_source_url") == len(rows)
        and report.get("with_locator") == len(rows)
        and report.get("with_verification_hash") == len(rows)
        and report.get("with_https_retrieval_target") == len(rows)
    )
    return {
        "passes": bool(
            rows
            and not invalid_ids
            and counts_match
            and report.get("passes") is True
            and declared_hash == computed_hash
        ),
        "artifacts": len(rows),
        "invalid_pointer_count": len(invalid_ids),
        "invalid_pointer_ids": invalid_ids[:20],
        "counts_match_report": counts_match,
        "report_hash_valid": declared_hash == computed_hash,
        "registry_path": str(registry_path.relative_to(workspace)),
        "report_path": str(report_path.relative_to(workspace)),
    }


def build_transportability_diagnostics(workspace: Path, output: Path) -> dict[str, Any]:
    """Create pairwise overlap gates; a common schema never implies pooling."""
    atlas = pd.read_parquet(
        workspace / "results" / "observatory" / "r3" / "gate_cycle_descriptive_atlas.parquet"
    )
    atlas["cycle_year"] = pd.to_datetime(atlas["cycle_start"], utc=True, errors="coerce").dt.year
    atlas["log_observable_count"] = np.log1p(pd.to_numeric(atlas["observable_count"], errors="coerce"))
    atlas["selected_share_observed"] = pd.to_numeric(atlas["selected_share"], errors="coerce")
    atlas["review_intensity"] = pd.to_numeric(
        atlas["official_reviews_per_observable_candidate"], errors="coerce"
    )
    features = ("cycle_year", "log_observable_count", "selected_share_observed", "review_intensity")
    groups = {
        str(source): frame.copy()
        for source, frame in atlas.groupby("source_id", dropna=False)
    }
    rows: list[dict[str, Any]] = []
    sources = sorted(groups)
    for i, left_name in enumerate(sources):
        for right_name in sources[i + 1 :]:
            left, right = groups[left_name], groups[right_name]
            overlaps = {feature: _range_overlap(left[feature], right[feature]) for feature in features}
            smds = {feature: _standardized_difference(left[feature], right[feature]) for feature in features}
            observed_overlaps = [value for value in overlaps.values() if value is not None]
            same_architecture = bool(
                set(left["architecture"].dropna()) & set(right["architecture"].dropna())
            )
            adequate_cells = len(left) >= 10 and len(right) >= 10
            positivity = adequate_cells and len(observed_overlaps) >= 2 and min(observed_overlaps) >= 0.10
            domain_coverage_left = float(left["field_of_study"].notna().mean())
            domain_coverage_right = float(right["field_of_study"].notna().mean())
            measured_modifiers = [
                "architecture",
                "calendar_time",
                "observable_population_size",
                "selection_share",
                "review_intensity",
            ]
            required_but_unmeasured = ["policy", "disclosure"]
            if domain_coverage_left < 1.0 or domain_coverage_right < 1.0:
                required_but_unmeasured.insert(0, "domain")
            if positivity:
                verdict = "conditional_modeling_candidate_but_key_modifiers_unmeasured"
            elif observed_overlaps:
                verdict = "descriptive_stratification_only"
            else:
                verdict = "comparison_not_supported"
            rows.append(
                {
                    "source_left": left_name,
                    "source_right": right_name,
                    "cycles_left": len(left),
                    "cycles_right": len(right),
                    "same_architecture_observed": same_architecture,
                    "positivity_supported": positivity,
                    **{f"overlap_{key}": value for key, value in overlaps.items()},
                    **{f"absolute_smd_{key}": value for key, value in smds.items()},
                    "pooling_verdict": verdict,
                    "direct_comparison_supported": False,
                    "universal_pooling_allowed": False,
                    "domain_coverage_left": domain_coverage_left,
                    "domain_coverage_right": domain_coverage_right,
                    "measured_support_dimensions": json.dumps(measured_modifiers),
                    "required_effect_modifiers": json.dumps(
                        ["architecture", "domain", "policy", "disclosure", "calendar_time"]
                    ),
                    "required_but_unmeasured_effect_modifiers": json.dumps(
                        required_but_unmeasured
                    ),
                }
            )
    _write(rows, output / "transportability_diagnostics.parquet")
    verdicts = Counter(row["pooling_verdict"] for row in rows)
    report = {
        "schema": "open-selection-graph.transportability-diagnostics/2",
        "source_pairs": len(rows),
        "verdict_counts": dict(verdicts),
        "direct_comparison_pairs": sum(
            row["direct_comparison_supported"] for row in rows
        ),
        "pairs_with_required_but_unmeasured_modifiers": sum(
            bool(json.loads(row["required_but_unmeasured_effect_modifiers"]))
            for row in rows
        ),
        "measured_support_dimensions": [
            "architecture",
            "calendar_time",
            "observable_population_size",
            "selection_share",
            "review_intensity",
        ],
        "partially_measured_modifier": "field_of_study/domain",
        "required_but_unmeasured_modifiers": ["domain", "policy", "disclosure"],
        "universal_pooling_allowed": False,
        "pooling_contract": (
            "The table is a support diagnostic, not a pooling licence. Domain is incomplete and "
            "joinable cycle-level policy and disclosure covariates are absent. Treat those as "
            "required-but-unmeasured effect modifiers; stratify or fit a sensitivity model."
        ),
        "passes": bool(rows)
        and not any(row["universal_pooling_allowed"] for row in rows)
        and not any(row["direct_comparison_supported"] for row in rows)
        and all(
            json.loads(row["required_but_unmeasured_effect_modifiers"])
            for row in rows
        ),
    }
    return _write_report(output / "transportability_diagnostics_report.json", report)


def build_pointer_rebuild_registry(workspace: Path, output: Path) -> dict[str, Any]:
    """Publish executable retrieval contracts without redistributing payloads."""
    with ObservatoryCatalog(workspace / "data" / "observatory" / "normalized").connect() as connection:
        artifacts = connection.execute(
            """
            SELECT a.content_artifact_id, a.source_id, a.object_type, a.media_type,
                   a.byte_hash, a.normalized_text_hash, a.source_url, a.local_pointer,
                   a.licence, a.release_class, a.observed_at, a.parser_version,
                   s.byte_hash AS source_object_byte_hash
            FROM content_artifact AS a
            LEFT JOIN source_object AS s
              ON s.source_object_id = a.source_object_id
            WHERE a.release_class IN ('pointer_hash', 'derived_only')
            ORDER BY a.content_artifact_id
            """
        ).fetchdf()
    rows = []
    for row in artifacts.itertuples(index=False):
        url = _clean_pointer_value(row.source_url)
        locator = _clean_pointer_value(row.local_pointer) or url
        byte_hash = _clean_pointer_value(row.byte_hash) or _clean_pointer_value(
            row.source_object_byte_hash
        )
        text_hash = _clean_pointer_value(row.normalized_text_hash)
        verification_hash_present = bool(byte_hash or text_hash)
        rows.append(
            {
                "content_artifact_id": row.content_artifact_id,
                "source_id": row.source_id,
                "object_type": row.object_type,
                "media_type": row.media_type,
                "source_url": url,
                "retrieval_target": url,
                "object_locator": locator,
                "expected_byte_hash": byte_hash,
                "expected_normalized_text_hash": text_hash,
                "licence_or_terms": row.licence,
                "release_class": row.release_class,
                "retrieval_method": (
                    "HTTPS GET from recorded source URL; when object_locator equals source_url, "
                    "the URL is the object-level locator"
                ),
                "terms_acceptance_required": True,
                "automatic_redistribution_allowed": False,
                "credential_persistence_allowed": False,
                "hash_verification_required": verification_hash_present,
                "source_url_is_https": bool(url and url.startswith("https://")),
                "observed_at": row.observed_at,
                "parser_version": row.parser_version,
            }
        )
    _write(rows, output / "pointer_rebuild_registry.parquet")
    report = {
        "schema": "open-selection-graph.pointer-rebuild-registry/1",
        "artifacts": len(rows),
        "with_source_url": sum(bool(row["source_url"]) for row in rows),
        "with_locator": sum(bool(row["object_locator"]) for row in rows),
        "with_verification_hash": sum(row["hash_verification_required"] for row in rows),
        "with_genuine_verification_hash": sum(
            _genuine_sha256(row["expected_byte_hash"])
            or _genuine_sha256(row["expected_normalized_text_hash"])
            for row in rows
        ),
        "with_https_retrieval_target": sum(
            bool(row["source_url_is_https"] and row["object_locator"]) for row in rows
        ),
        "payloads_redistributed": 0,
        "terms_acceptance_required": True,
        "passes": _pointer_contract_passes(rows),
    }
    return _write_report(output / "pointer_rebuild_registry_report.json", report)


_ANNOTATION = re.compile(r"\[\[(.*?)\]\]", re.S)
_TAG = re.compile(r"\b([A-Z]{3})(?:-(?:POS|NEG|NEU))?\b")
_CONSTRUCT_TAGS = {
    "novelty_originality": {"NOV"},
    "significance_impact": {"IMP"},
    "soundness_evidence": {"EMP"},
    "clarity_writing": {"CLA"},
    "presentation_formatting": {"PNF"},
}


def _review_benchmark_rows(
    path: Path,
) -> tuple[list[str], list[str], np.ndarray, list[str], int]:
    texts: list[str] = []
    groups: list[str] = []
    labels: list[list[int]] = []
    span_ids: list[str] = []
    review_files = 0
    with zipfile.ZipFile(path) as archive:
        for name in sorted(archive.namelist()):
            if "/Annotated/" not in name or not name.endswith(".txt") or name.startswith("__MACOSX"):
                continue
            review_files += 1
            body = archive.read(name).decode("utf-8", errors="replace")
            previous = 0
            group = name.split("/Annotated/", 1)[0]
            for match in _ANNOTATION.finditer(body):
                text = body[previous : match.start()].strip()
                tags = set(_TAG.findall(match.group(1)))
                previous = match.end()
                if not text or not tags:
                    continue
                texts.append(text)
                groups.append(group)
                span_ids.append(
                    stable_id(
                        "human-review-span",
                        "peer-review-analyze-1.0",
                        f"{name}|{match.start()}|{content_hash(text)}",
                    )
                )
                labels.append(
                    [int(bool(tags & _CONSTRUCT_TAGS[construct])) for construct in _CONSTRUCT_TAGS]
                )
    return texts, groups, np.asarray(labels, dtype=np.int8), span_ids, review_files


def export_modern_review_benchmark_input(workspace: Path, destination: Path) -> dict[str, Any]:
    """Export a temporary, hash-bound input for the Modal Qwen3 benchmark."""
    review_zip = (
        workspace
        / "data"
        / "observatory"
        / "external"
        / "benchmarks"
        / "Peer-Review-Analyze-1.0.zip"
    )
    texts, groups, labels, span_ids, review_file_count = _review_benchmark_rows(review_zip)
    destination.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(
        {
            "span_id": span_ids,
            "submission_id": groups,
            "text": texts,
            **{
                f"label_{construct}": labels[:, column]
                for column, construct in enumerate(_CONSTRUCT_TAGS)
            },
        }
    )
    _write(frame, destination)
    report = {
        "schema": "open-selection-graph.qwen3-review-benchmark-input/1",
        "source_sha256": content_hash(review_zip.read_bytes()),
        "review_files": review_file_count,
        "submissions": len(set(groups)),
        "spans": len(frame),
        "input_sha256": content_hash(destination.read_bytes()),
        "temporary_text_payload": True,
        "release_allowed": False,
    }
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    destination.with_suffix(".report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    return report


def build_human_benchmarks(workspace: Path, output: Path) -> dict[str, Any]:
    """Evaluate free-form constructs and ambiguous lineage on prior human labels."""
    external = workspace / "data" / "observatory" / "external" / "benchmarks"
    review_zip = external / "Peer-Review-Analyze-1.0.zip"
    texts, groups, labels, span_ids, review_file_count = _review_benchmark_rows(review_zip)
    qwen_path = output / "qwen3_freeform_construct_oof.parquet"
    qwen_report_path = output / "qwen3_freeform_construct_oof_report.json"
    if not qwen_path.is_file() or not qwen_report_path.is_file():
        raise FileNotFoundError(
            "Qwen3 review benchmark missing; export and run the Modal review encoder first"
        )
    qwen_report = json.loads(qwen_report_path.read_text())
    if not qwen_report.get("passes") or qwen_report.get("input_text_persisted"):
        raise RuntimeError("Qwen3 review benchmark provenance failed")
    embedded = pd.read_parquet(qwen_path)
    if embedded["span_id"].duplicated().any() or set(embedded["span_id"]) != set(span_ids):
        raise RuntimeError("Qwen3 review benchmark span population does not match source annotations")
    embedded = embedded.set_index("span_id").loc[span_ids]
    probabilities = np.column_stack(
        [embedded[f"probability_{construct}"].to_numpy(float) for construct in _CONSTRUCT_TAGS]
    )
    metric_rows = []
    for column, construct in enumerate(_CONSTRUCT_TAGS):
        truth = labels[:, column]
        predicted = probabilities[:, column] >= 0.5
        metric_rows.append(
            {
                "construct": construct,
                "human_positive_spans": int(truth.sum()),
                "human_negative_spans": int(len(truth) - truth.sum()),
                "grouped_cross_validation_folds": 5,
                "group_unit": "submission",
                "representation": "Qwen3-Embedding-0.6B frozen 1024-dimensional embedding",
                "classifier": "class-balanced logistic regression fitted within grouped folds",
                "roc_auc": float(roc_auc_score(truth, probabilities[:, column])),
                "average_precision": float(average_precision_score(truth, probabilities[:, column])),
                "precision_at_0_5": float(precision_score(truth, predicted, zero_division=0)),
                "recall_at_0_5": float(recall_score(truth, predicted, zero_division=0)),
                "f1_at_0_5": float(f1_score(truth, predicted, zero_division=0)),
                "review_text_released": False,
                "external_validation_only": True,
            }
        )
    _write(metric_rows, output / "freeform_construct_benchmark.parquet")

    gray_path = external / "PreprintToPaper_GrayZone.csv"
    gray = pd.read_csv(gray_path)
    left = gray["annotator1"].astype("string").str.upper()
    right = gray["annotator2"].astype("string").str.upper()
    eligible = left.isin(["TRUE", "FALSE"]) & right.isin(["TRUE", "FALSE"])
    agreed = eligible & (left == right)
    gray_rows = []
    for row, a, b, is_eligible, is_agreed in zip(
        gray.itertuples(index=False), left, right, eligible, agreed, strict=True
    ):
        pair_key = f"{row.biorxiv_doi}|{row.suspected_published_doi}"
        gray_rows.append(
            {
                "pair_id": stable_id("human-lineage-benchmark", "PreprintToPaper", pair_key),
                "pair_identifier_hash": content_hash(pair_key),
                "year": int(row.year),
                "title_similarity_design_point": 0.75,
                "author_match_score": float(row.author_match_score),
                "annotator_1_label": None if not is_eligible else a == "TRUE",
                "annotator_2_label": None if not is_eligible else b == "TRUE",
                "consensus_label": None if not is_agreed else a == "TRUE",
                "adjudication_status": (
                    "consensus" if is_agreed else "disagreement" if is_eligible else "at_least_one_NA"
                ),
                "new_human_subjects_collected_by_osg": False,
                "canonical_identity_collapse_allowed": False,
            }
        )
    annotator_left = (left[eligible] == "TRUE").astype(int)
    annotator_right = (right[eligible] == "TRUE").astype(int)
    consensus = [row for row in gray_rows if row["consensus_label"] is not None]
    consensus_indices = [
        index for index, row in enumerate(gray_rows) if row["consensus_label"] is not None
    ]
    consensus_labels = np.asarray(
        [int(gray_rows[index]["consensus_label"]) for index in consensus_indices], dtype=np.int8
    )
    consensus_features = np.asarray(
        [[gray_rows[index]["author_match_score"]] for index in consensus_indices], dtype=float
    )
    lineage_probabilities = np.zeros(len(consensus_indices), dtype=float)
    lineage_splitter = StratifiedKFold(n_splits=5, shuffle=True, random_state=1729)
    for train, test in lineage_splitter.split(consensus_features, consensus_labels):
        model = LogisticRegression(C=1.0, class_weight="balanced", max_iter=1_000, random_state=1729)
        model.fit(consensus_features[train], consensus_labels[train])
        lineage_probabilities[test] = model.predict_proba(consensus_features[test])[:, 1]
    for position, row_index in enumerate(consensus_indices):
        gray_rows[row_index]["human_calibrated_match_probability_oof"] = float(
            lineage_probabilities[position]
        )
        gray_rows[row_index]["human_calibration_scope"] = (
            "Gray Zone pairs near title similarity 0.75; author-match score only"
        )
    _write(gray_rows, output / "human_adjudicated_lineage_benchmark.parquet")
    lineage_calibration_metrics = {
        "folds": 5,
        "feature": "author_match_score",
        "title_similarity_design_point": 0.75,
        "out_of_fold_roc_auc": float(roc_auc_score(consensus_labels, lineage_probabilities)),
        "out_of_fold_average_precision": float(
            average_precision_score(consensus_labels, lineage_probabilities)
        ),
        "out_of_fold_brier_score": float(
            brier_score_loss(consensus_labels, lineage_probabilities)
        ),
        "automatic_identity_collapse_allowed": False,
        "transfer_without_author_feature_allowed": False,
    }
    strata = []
    for lower, upper in ((0.0, 0.25), (0.25, 0.50), (0.50, 0.75), (0.75, 1.01)):
        subset = [
            row for row in consensus if lower <= row["author_match_score"] < upper
        ]
        strata.append(
            {
                "author_match_interval": f"[{lower:.2f},{min(upper, 1.0):.2f}{']' if upper > 1 else ')'}",
                "pairs": len(subset),
                "consensus_match_share": (
                    sum(row["consensus_label"] for row in subset) / len(subset) if subset else None
                ),
            }
        )
    construct_min_auc = min(row["roc_auc"] for row in metric_rows)
    construct_max_auc = max(row["roc_auc"] for row in metric_rows)
    report = {
        "schema": "open-selection-graph.external-human-benchmarks/2",
        "review_dataset": "Peer Review Analyze 1.0 (ICLR 2018)",
        "review_licence": "MIT",
        "review_files": review_file_count,
        "reviewed_submissions": len(set(groups)),
        "human_annotated_spans": len(texts),
        "review_representation": {
            "model": qwen_report["model"],
            "model_revision": qwen_report["model_revision"],
            "embedding_dimension": qwen_report["embedding_dimension"],
            "out_of_fold_only": True,
            "bag_of_words_or_tfidf_used": False,
            "report_hash": qwen_report["report_hash"],
        },
        "construct_metrics": metric_rows,
        "construct_min_grouped_cv_auc": construct_min_auc,
        "construct_max_grouped_cv_auc": construct_max_auc,
        "lineage_dataset": "PreprintToPaper Gray Zone",
        "lineage_rows": len(gray_rows),
        "lineage_two_decisive_labels": int(eligible.sum()),
        "lineage_consensus_rows": len(consensus),
        "lineage_annotator_kappa": float(cohen_kappa_score(annotator_left, annotator_right)),
        "lineage_human_calibration": lineage_calibration_metrics,
        "lineage_author_score_strata": strata,
        "new_human_subjects": 0,
        "automatic_identity_collapses": 0,
        "limitations_after_benchmark": [
            "review benchmark is ICLR-2018 and transport to other sources must be tested",
            "lineage benchmark fixes title similarity at 0.75 and exposes author overlap unavailable in the current OSG tier",
        ],
        "passes": len(texts) > 10_000
        and len(consensus) > 100
        and construct_min_auc > 0.70
        and lineage_calibration_metrics["out_of_fold_roc_auc"] > 0.90
        and not any(row["review_text_released"] for row in metric_rows),
    }
    return _write_report(output / "external_human_benchmarks_report.json", report)


def build_limitation_products(workspace: Path, output: Path) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    reports = [
        build_observability_bounds(workspace, output),
        build_transportability_diagnostics(workspace, output),
        build_pointer_rebuild_registry(workspace, output),
        build_human_benchmarks(workspace, output),
        build_modern_ruler_triangulation(workspace, output),
    ]
    summary = {
        "schema": "open-selection-graph.validity-extension-build/2",
        "reports": [
            {"schema": report["schema"], "report_hash": report["report_hash"]}
            for report in reports
        ],
        "passes": all(report["passes"] for report in reports),
    }
    return _write_report(output / "validity_extension_build_report.json", summary)
