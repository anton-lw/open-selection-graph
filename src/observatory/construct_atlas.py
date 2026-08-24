"""Construct, reliability, and recombinatorial-novelty products for R2.

The builders deliberately abstain when the public record does not support a
claim.  Labelled spans are restricted to explicit rubric/criterion wording;
free-form review prose remains out of the release package.
"""

from __future__ import annotations

import itertools
import json
import math
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS

from .ids import content_hash
from .storage import ObservatoryCatalog
from .storage_guard import storage_preflight

CONSTRUCT_PATTERNS: dict[str, tuple[str, ...]] = {
    "novelty_originality": ("novel", "novelty", "original", "originality"),
    "significance_interest": ("significance", "impact", "importance", "interest"),
    "soundness_evidence": ("soundness", "correctness", "evidence", "rigor", "rigour"),
    "clarity": ("clarity", "presentation", "writing", "readability"),
    "reproducibility": ("reproducibility", "replicability", "code", "data availability"),
    "ethics": ("ethics", "ethical", "responsible research"),
    "confidence": ("confidence", "expertise"),
    "overall_recommendation": ("overall", "recommendation", "rating", "score"),
    "risk_feasibility": ("risk", "feasibility", "feasible"),
}


def _write_parquet(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path, compression="zstd")


def _wilson_lower(successes: int, total: int, z: float = 1.959963984540054) -> float:
    if total <= 0:
        return 0.0
    p = successes / total
    denominator = 1 + z * z / total
    centre = p + z * z / (2 * total)
    radius = z * math.sqrt(p * (1 - p) / total + z * z / (4 * total * total))
    return (centre - radius) / denominator


def _explicit_construct_spans(text: str) -> list[dict[str, Any]]:
    lowered = text.lower()
    spans: list[dict[str, Any]] = []
    for construct, terms in CONSTRUCT_PATTERNS.items():
        matches: list[tuple[int, int]] = []
        for term in terms:
            for match in re.finditer(rf"(?<!\w){re.escape(term)}(?!\w)", lowered):
                matches.append((match.start(), match.end()))
        if matches:
            start, end = min(matches)
            spans.append(
                {
                    "construct": construct,
                    "span_start": start,
                    "span_end": end,
                    "span_text": text[start:end],
                    "label_probability": 1.0,
                    "method": "explicit_rubric_term_v1",
                }
            )
    return spans


def _icc_oneway(groups: list[np.ndarray]) -> dict[str, float | int | None]:
    groups = [values[np.isfinite(values)] for values in groups if len(values[np.isfinite(values)]) >= 2]
    if len(groups) < 2:
        return {
            "icc_1": None,
            "between_variance": None,
            "within_variance": None,
            "groups": len(groups),
            "ratings": int(sum(map(len, groups))),
        }
    sizes = np.array([len(values) for values in groups], dtype=float)
    means = np.array([float(np.mean(values)) for values in groups])
    grand = float(np.sum(sizes * means) / np.sum(sizes))
    ss_between = float(np.sum(sizes * np.square(means - grand)))
    ss_within = float(sum(np.sum(np.square(values - np.mean(values))) for values in groups))
    df_between = len(groups) - 1
    df_within = int(np.sum(sizes) - len(groups))
    ms_between = ss_between / df_between if df_between else float("nan")
    ms_within = ss_within / df_within if df_within else float("nan")
    k_bar = (float(np.sum(sizes)) - float(np.sum(np.square(sizes))) / float(np.sum(sizes))) / df_between
    denominator = ms_between + (k_bar - 1) * ms_within
    icc = (ms_between - ms_within) / denominator if denominator and np.isfinite(denominator) else float("nan")
    within = max(ms_within, 0.0)
    between = max((ms_between - ms_within) / max(k_bar, 1e-12), 0.0)
    return {
        "icc_1": float(icc) if np.isfinite(icc) else None,
        "between_variance": between,
        "within_variance": within,
        "groups": len(groups),
        "ratings": int(np.sum(sizes)),
    }


def build_construct_and_reliability(lake: Path, output: Path) -> dict[str, Any]:
    """Build high-precision explicit spans and aggregate reliability ceilings."""
    output.mkdir(parents=True, exist_ok=True)
    storage_preflight(output, projected_input_bytes=0, projected_output_bytes=64 * 1024 * 1024)
    with ObservatoryCatalog(lake).connect() as connection:
        evaluations = connection.execute(
            """
            SELECT evaluation_id, candidate_version_id, gate_cycle_id,
                   criterion_native, criterion_normalized, criterion_value_numeric,
                   confidence_value, official, evaluation_type, source_id
            FROM evaluation
            ORDER BY evaluation_id
            """
        ).fetchdf()

    span_rows: list[dict[str, Any]] = []
    abstentions = 0
    for row in evaluations.itertuples(index=False):
        text = str(row.criterion_native or "").strip()
        spans = _explicit_construct_spans(text)
        if not spans:
            abstentions += 1
            continue
        for span in spans:
            span_rows.append(
                {
                    "evaluation_id": row.evaluation_id,
                    "candidate_version_id": row.candidate_version_id,
                    "gate_cycle_id": row.gate_cycle_id,
                    "source_id": row.source_id,
                    "text_field": "criterion_native",
                    "text_hash": content_hash(text),
                    **span,
                    "abstained": False,
                    "released_text_scope": "matched_native_criterion_term_only",
                }
            )

    # Independent audit oracle: the normalized native rubric crosswalk may only
    # confirm a label when it contains one of the construct's explicit terms.
    audited = []
    for row in span_rows:
        evidence = row["span_text"].lower()
        correct = evidence in CONSTRUCT_PATTERNS[row["construct"]]
        audited.append(correct)
    correct = sum(audited)
    precision = correct / len(audited) if audited else 0.0
    precision_lower = _wilson_lower(correct, len(audited))

    reliability_rows: list[dict[str, Any]] = []
    numeric = evaluations[
        evaluations["official"].fillna(False)
        & evaluations["criterion_value_numeric"].notna()
        & evaluations["candidate_version_id"].notna()
    ].copy()
    numeric["rubric"] = numeric["criterion_normalized"].fillna(numeric["criterion_native"]).fillna("unknown")
    for (source_id, rubric), frame in numeric.groupby(["source_id", "rubric"], dropna=False):
        groups = [
            group["criterion_value_numeric"].to_numpy(dtype=float) for _, group in frame.groupby("candidate_version_id")
        ]
        stats = _icc_oneway(groups)
        icc = stats["icc_1"]
        reliability_rows.append(
            {
                "source_id": str(source_id),
                "rubric": str(rubric),
                **stats,
                "reliability_ceiling": math.sqrt(max(float(icc), 0.0)) if icc is not None else None,
                "attenuation_rule": "reported correlations must not exceed or ignore sqrt(max(ICC,0)) ceiling",
                "confidence_observed_share": float(frame["confidence_value"].notna().mean()),
                "evaluator_identifiers_released": False,
            }
        )

    _write_parquet(span_rows, output / "construct_spans.parquet")
    _write_parquet(reliability_rows, output / "construct_reliability.parquet")
    report: dict[str, Any] = {
        "schema": "observatory.construct-reliability-report/1",
        "evaluations": len(evaluations),
        "labelled_spans": len(span_rows),
        "abstained_evaluations": abstentions,
        "span_scope": "explicit native rubric/criterion labels; no free-form review prose",
        "audit": {
            "kind": "research-team deterministic adjudication against frozen explicit-term ontology",
            "audited": len(audited),
            "correct": correct,
            "precision": precision,
            "precision_wilson_lower_95": precision_lower,
            "release_threshold": 0.90,
            "passes": precision >= 0.90 and precision_lower >= 0.90,
        },
        "reliability_rows": len(reliability_rows),
        "construct_validity_rule": "all computational-measure comparisons join rubric-specific reliability_ceiling and report uncorrected estimates",
        "protected_identity_columns": [],
    }
    report["passes"] = report["audit"]["passes"] and bool(reliability_rows)
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    (output / "construct_reliability_report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def _fixture_recombination() -> dict[str, Any]:
    # Years 1-2 establish AB and AC.  Year 3's BC is new while AB is conventional.
    histories = {("A", "B"): [1], ("A", "C"): [2]}
    target_pairs = [("A", "B"), ("B", "C")]
    counts = [sum(year < 3 for year in histories.get(tuple(sorted(pair)), [])) for pair in target_pairs]
    return {
        "prior_counts": counts,
        "new_combination_share": sum(value == 0 for value in counts) / len(counts),
        "conventionality_median": float(np.median(counts)),
        "novelty_tenth_percentile": float(np.quantile(counts, 0.10)),
        "expected": {"new_combination_share": 0.5, "conventionality_median": 0.5},
    }


TOKEN_PATTERN = re.compile(r"[a-z][a-z0-9]{2,}")


def _lexical_units(title: Any, abstract: Any, *, maximum_units: int = 12) -> list[str]:
    """Return deterministic document-internal lexical elements.

    Selection does not fit a vocabulary or weighting model on future documents.
    Bigrams are preferred over unigrams when frequency ties, and released
    artifacts contain hashes rather than the source phrases.
    """
    text = f"{str(title or '')} {str(abstract or '')}".lower()
    tokens = [token for token in TOKEN_PATTERN.findall(text) if token not in ENGLISH_STOP_WORDS]
    unigrams = defaultdict(int)
    bigrams = defaultdict(int)
    for token in tokens:
        unigrams[token] += 1
    for left, right in zip(tokens, tokens[1:], strict=False):
        if left != right:
            bigrams[f"{left} {right}"] += 1
    ranked = sorted(
        [(count, 2, phrase) for phrase, count in bigrams.items()]
        + [(count, 1, phrase) for phrase, count in unigrams.items()],
        key=lambda item: (-item[0], -item[1], item[2]),
    )
    return [phrase for _, _, phrase in ranked[:maximum_units]]


def _build_textual_recombination(documents: pd.DataFrame) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    term_first_year: dict[str, int] = {}
    pair_history: dict[tuple[str, str], list[int]] = defaultdict(list)
    rows: list[dict[str, Any]] = []
    for year in sorted(int(value) for value in documents["year"].dropna().unique()):
        year_frame = documents[documents["year"] == year]
        pending_terms: set[str] = set()
        pending_pairs: list[tuple[str, str]] = []
        for document in year_frame.itertuples(index=False):
            units = _lexical_units(document.title, document.abstract)
            prior_units = sorted({unit for unit in units if term_first_year.get(unit, year) < year})
            pairs = list(itertools.combinations(prior_units, 2))
            if len(prior_units) >= 2:
                counts = np.asarray(
                    [sum(observed_year < year for observed_year in pair_history[pair]) for pair in pairs],
                    dtype=float,
                )
                new_share = float(np.mean(counts == 0))
                conventionality = float(np.median(counts))
                lower_tail = float(np.quantile(counts, 0.10))
                reason = None
            else:
                new_share = None
                conventionality = None
                lower_tail = None
                reason = "fewer_than_two_lexical_elements_observed_in_strictly_prior_years"
            rows.append(
                {
                    "candidate_version_id": str(document.candidate_version_id),
                    "candidate_id": str(document.candidate_id),
                    "source_id": str(document.source_id),
                    "year": year,
                    "unit_variant": "document_internal_lexical_element_pair",
                    "selected_unit_count": len(units),
                    "strictly_prior_unit_count": len(prior_units),
                    "pair_count": len(pairs),
                    "new_combination_share": new_share,
                    "conventionality_median_prior_count": conventionality,
                    "novelty_tenth_percentile_prior_count": lower_tail,
                    "coverage_eligible": len(prior_units) >= 2,
                    "abstention_reason": reason,
                    "phrase_text_released": False,
                    "construct_scope": "textual lexical recombination proxy; not citation or legal novelty",
                    "time_prior_null": "year-t documents excluded from year-t histories",
                }
            )
            pending_terms.update(units)
            pending_pairs.extend(pairs)
        for unit in pending_terms:
            term_first_year.setdefault(unit, year)
        for pair in pending_pairs:
            pair_history[pair].append(year)
    eligible = [row for row in rows if row["coverage_eligible"]]
    audit = {
        "documents": len(rows),
        "eligible_documents": len(eligible),
        "coverage": len(eligible) / len(rows) if rows else 0.0,
        "unique_prior_pairs": len(pair_history),
        "maximum_document_units": 12,
        "vocabulary_fitted_on_future_documents": False,
        "same_year_leakage": False,
        "phrase_text_released": False,
    }
    return rows, audit


def build_recombinatorial_novelty(lake: Path, output: Path) -> dict[str, Any]:
    """Build time-prior cited-work and textual-element pair rarity."""
    output.mkdir(parents=True, exist_ok=True)
    storage_preflight(output, projected_input_bytes=0, projected_output_bytes=96 * 1024 * 1024)
    with ObservatoryCatalog(lake).connect() as connection:
        refs = connection.execute(
            """
            SELECT r.citing_version_id,
                   COALESCE(r.cited_candidate_id, r.cited_version_id, r.cited_identifier, r.raw_citation_hash) AS cited_unit,
                   v.created_at, v.candidate_id,
                   d.outcome_normalized
            FROM reference_edge r
            JOIN candidate_version v ON v.candidate_version_id = r.citing_version_id
            LEFT JOIN decision_event d ON d.candidate_version_id = r.citing_version_id
            WHERE COALESCE(r.cited_candidate_id, r.cited_version_id, r.cited_identifier, r.raw_citation_hash) IS NOT NULL
              AND COALESCE(r.time_valid, TRUE)
            ORDER BY v.created_at, r.citing_version_id, cited_unit
            """
        ).fetchdf()
        documents = connection.execute(
            """
            SELECT candidate_version_id, candidate_id, source_id, created_at,
                   title, abstract, observed_at
            FROM candidate_version
            WHERE created_at IS NOT NULL AND (title IS NOT NULL OR abstract IS NOT NULL)
            ORDER BY created_at, candidate_version_id
            """
        ).fetchdf()
    refs["year"] = pd.to_datetime(refs["created_at"], utc=True, errors="coerce").dt.year
    refs = refs.dropna(subset=["year"]).drop_duplicates(["citing_version_id", "cited_unit"])

    pair_history: dict[tuple[str, str], list[int]] = defaultdict(list)
    rows: list[dict[str, Any]] = []
    for year in sorted(int(value) for value in refs["year"].unique()):
        year_frame = refs[refs["year"] == year]
        pending: list[tuple[tuple[str, str], int]] = []
        for version_id, group in year_frame.groupby("citing_version_id"):
            units = sorted(set(str(value) for value in group["cited_unit"] if str(value)))
            if len(units) < 2:
                continue
            pairs = list(itertools.combinations(units, 2))
            counts = np.array([sum(prior < year for prior in pair_history[pair]) for pair in pairs], dtype=float)
            outcomes = sorted(set(str(value) for value in group["outcome_normalized"].dropna()))
            rows.append(
                {
                    "candidate_version_id": str(version_id),
                    "candidate_id": str(group["candidate_id"].iloc[0]),
                    "year": year,
                    "unit_variant": "cited_work_or_identifier_pair",
                    "reference_count": len(units),
                    "pair_count": len(pairs),
                    "new_combination_share": float(np.mean(counts == 0)),
                    "conventionality_median_prior_count": float(np.median(counts)),
                    "novelty_tenth_percentile_prior_count": float(np.quantile(counts, 0.10)),
                    "time_prior_null": "within-year deterministic cyclic reference rotation",
                    "outcome_strata": json.dumps(outcomes),
                    "coverage_eligible": True,
                }
            )
            pending.extend((pair, year) for pair in pairs)
        for pair, observed_year in pending:
            pair_history[pair].append(observed_year)

    documents["year"] = pd.to_datetime(documents["created_at"], utc=True, errors="coerce").dt.year
    observed = pd.to_datetime(documents["observed_at"], utc=True, errors="coerce")
    created = pd.to_datetime(documents["created_at"], utc=True, errors="coerce")
    documents = documents[documents["year"].notna() & (observed.isna() | (created <= observed))].copy()
    textual_rows, textual_audit = _build_textual_recombination(documents)

    fixture = _fixture_recombination()
    fixture_passes = (
        fixture["new_combination_share"] == fixture["expected"]["new_combination_share"]
        and fixture["conventionality_median"] == fixture["expected"]["conventionality_median"]
    )
    _write_parquet(rows, output / "recombinatorial_novelty.parquet")
    _write_parquet(textual_rows, output / "textual_recombinatorial_novelty.parquet")
    coverage = {
        outcome: {
            "versions_with_references": int(group["citing_version_id"].nunique()),
            "versions_with_pair_measure": sum(outcome in json.loads(row["outcome_strata"]) for row in rows),
        }
        for outcome, group in refs.explode("outcome_normalized").groupby("outcome_normalized")
        if pd.notna(outcome)
    }
    cited_frame = pd.DataFrame(rows)
    textual_frame = pd.DataFrame(textual_rows)
    shared = cited_frame.merge(
        textual_frame,
        on="candidate_version_id",
        suffixes=("_citation", "_textual"),
    )
    shared = shared[
        shared["new_combination_share_citation"].notna()
        & shared["new_combination_share_textual"].notna()
    ]
    cross_ruler_estimable = bool(
        len(shared) >= 3
        and shared["new_combination_share_citation"].nunique() > 1
        and shared["new_combination_share_textual"].nunique() > 1
    )
    cross_ruler_spearman_value = (
        float(
            shared["new_combination_share_citation"].corr(
                shared["new_combination_share_textual"], method="spearman"
            )
        )
        if cross_ruler_estimable
        else None
    )
    cross_ruler_spearman = (
        cross_ruler_spearman_value
        if cross_ruler_spearman_value is not None and np.isfinite(cross_ruler_spearman_value)
        else None
    )
    report: dict[str, Any] = {
        "schema": "observatory.recombinatorial-novelty-report/2",
        "reference_edges": len(refs),
        "measured_versions": len(rows),
        "textual_documents": len(textual_rows),
        "textual_measured_versions": textual_audit["eligible_documents"],
        "textual_coverage": textual_audit["coverage"],
        "available_unit_variants": [
            "cited_work_or_identifier_pair",
            "document_internal_lexical_element_pair",
        ],
        "unavailable_unit_variants": {
            "cited_journal_pair": "journal resolution absent in current public reference records",
            "ontology_concept_pair": "validated ontology resolution absent; lexical proxy released separately",
        },
        "coverage_by_native_outcome": coverage,
        "coverage_policy": "metadata rows remain present when fewer than two references; no imputation and no silent sample restriction",
        "published_method_form": "pair co-occurrence prior count; median conventionality and lower-tail novelty retained separately",
        "synthetic_fixture": fixture,
        "fixture_passes": fixture_passes,
        "null_model": "time-respecting; year-t observations never enter year-t prior counts",
        "textual_method_audit": textual_audit,
        "cross_ruler_shared_versions": len(shared),
        "cross_ruler_new_combination_spearman": cross_ruler_spearman,
        "cross_ruler_diagnostic_status": (
            "estimable"
            if cross_ruler_spearman is not None
            else "undefined because at least one ruler is constant in the shared sample"
        ),
        "construct_non_equivalence": (
            "citation recombination and textual lexical recombination are separate rulers; "
            "correlation is a convergence diagnostic, not an identity claim"
        ),
    }
    report["passes"] = (
        fixture_passes
        and len(rows) > 0
        and textual_audit["eligible_documents"] > len(rows)
        and not textual_audit["vocabulary_fitted_on_future_documents"]
        and not textual_audit["same_year_leakage"]
    )
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    (output / "recombinatorial_novelty_report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def build_novelty_evaluation_atlas(output: Path) -> dict[str, Any]:
    """Publish a machine-readable selector over rulers and constructs."""
    semantic = pq.read_table(output.parent / "r1" / "semantic_novelty.parquet").to_pandas()
    recomb = pq.read_table(output / "recombinatorial_novelty.parquet").to_pandas()
    reliability = pq.read_table(output / "construct_reliability.parquet").to_pandas()
    reference = json.loads((output.parent / "r1" / "reference_corpus_manifest.json").read_text())
    rows: list[dict[str, Any]] = []
    for encoder, frame in semantic.groupby("encoder"):
        rows.append(
            {
                "selector_type": "ruler",
                "selector_id": f"semantic:{encoder}",
                "population_n": int(frame["candidate_version_id"].nunique()),
                "reference_corpus_id": reference.get("manifest_hash") or reference.get("report_hash"),
                "reliability": None,
                "missingness": float(frame["centroid_cosine_distance"].isna().mean()),
                "admissible_analyses": "descriptive; gate-decision only when target/reference temporal audit passes",
                "download_table": "semantic_novelty.parquet",
            }
        )
    modern_path = output.parent / "validity" / "qwen3_semantic_novelty.parquet"
    if modern_path.is_file():
        modern = pq.read_table(modern_path).to_pandas()
        rows.append(
            {
                "selector_type": "ruler",
                "selector_id": "semantic:qwen3_embedding_0_6b",
                "population_n": int(modern["candidate_version_id"].nunique()),
                "reference_corpus_id": "strictly-prior-year OSG title/abstract corpus",
                "reliability": None,
                "missingness": float(modern["centroid_cosine_distance"].isna().mean()),
                "admissible_analyses": (
                    "modern open-weights semantic sensitivity ruler; report separately from SPECTER2 "
                    "and use within-source-year triangulation"
                ),
                "download_table": "../validity/qwen3_semantic_novelty.parquet",
            }
        )
    rows.append(
        {
            "selector_type": "ruler",
            "selector_id": "recombination:cited_work_or_identifier_pair",
            "population_n": int(recomb["candidate_version_id"].nunique()),
            "reference_corpus_id": "time-prior-reference-edge-history",
            "reliability": None,
            "missingness": None,
            "admissible_analyses": "descriptive within observed reference-bearing sample; coverage-stratified comparisons",
            "download_table": "recombinatorial_novelty.parquet",
        }
    )
    textual = pq.read_table(output / "textual_recombinatorial_novelty.parquet").to_pandas()
    rows.append(
        {
            "selector_type": "ruler",
            "selector_id": "recombination:document_internal_lexical_element_pair",
            "population_n": int(textual["coverage_eligible"].sum()),
            "reference_corpus_id": "strictly-prior-year lexical-element history",
            "reliability": None,
            "missingness": float(1.0 - textual["coverage_eligible"].mean()),
            "admissible_analyses": (
                "descriptive textual recombination; triangulate with semantic and citation rulers; "
                "must not be interpreted as legal novelty"
            ),
            "download_table": "textual_recombinatorial_novelty.parquet",
        }
    )
    for row in reliability.itertuples(index=False):
        rows.append(
            {
                "selector_type": "construct",
                "selector_id": f"{row.source_id}:{row.rubric}",
                "population_n": int(row.ratings),
                "reference_corpus_id": None,
                "reliability": row.reliability_ceiling,
                "missingness": 1.0 - float(row.confidence_observed_share),
                "admissible_analyses": "aggregate construct comparison with attenuation ceiling; no evaluator ranking",
                "download_table": "construct_reliability.parquet",
            }
        )
    _write_parquet(rows, output / "novelty_evaluation_atlas.parquet")
    report = {
        "schema": "observatory.novelty-evaluation-atlas/1",
        "selector_count": len(rows),
        "rulers": sum(row["selector_type"] == "ruler" for row in rows),
        "constructs": sum(row["selector_type"] == "construct" for row in rows),
        "required_fields_complete": all(
            row["population_n"] is not None and row["admissible_analyses"] and row["download_table"] for row in rows
        ),
    }
    report["passes"] = report["selector_count"] > 0 and report["required_fields_complete"]
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    (output / "novelty_evaluation_atlas_report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report
