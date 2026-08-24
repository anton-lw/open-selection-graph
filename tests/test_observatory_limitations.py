from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

from observatory.funding import _strict_ooxml_rows
from observatory.limitations import (
    _clean_pointer_value,
    _genuine_sha256,
    _pointer_contract_passes,
    _range_overlap,
    _standardized_difference,
    _usable_https_target,
)


def test_range_overlap_is_symmetric_and_bounded() -> None:
    left = pd.Series([0.0, 1.0, 2.0, 3.0])
    right = pd.Series([2.0, 3.0, 4.0, 5.0])
    forward = _range_overlap(left, right)
    reverse = _range_overlap(right, left)
    assert forward == reverse
    assert forward is not None and 0.0 <= forward <= 1.0


def test_standardized_difference_handles_equal_constants() -> None:
    assert _standardized_difference(pd.Series([2, 2]), pd.Series([2, 2])) == 0.0


def test_pointer_contract_treats_dataframe_nan_as_missing_and_fails_closed() -> None:
    assert _clean_pointer_value(float("nan")) is None
    valid = {
        "source_url": "https://example.org/object/1",
        "source_url_is_https": True,
        "retrieval_target": "https://example.org/object/1",
        "object_locator": "10.1234/example",
        "expected_byte_hash": "12" * 32,
        "expected_normalized_text_hash": None,
        "hash_verification_required": True,
        "automatic_redistribution_allowed": False,
    }
    assert _pointer_contract_passes([valid])
    for field in ("retrieval_target", "object_locator", "hash_verification_required"):
        invalid = {**valid, field: False if field == "hash_verification_required" else None}
        assert not _pointer_contract_passes([invalid])
    assert not _pointer_contract_passes(
        [{**valid, "expected_byte_hash": "0" * 64}]
    )
    assert not _pointer_contract_passes(
        [{**valid, "expected_byte_hash": "not-a-sha256"}]
    )
    assert not _pointer_contract_passes([{**valid, "automatic_redistribution_allowed": True}])
    assert _genuine_sha256("ab" * 32)
    assert not _genuine_sha256("f" * 64)
    assert _usable_https_target("https://example.org/object")
    assert not _usable_https_target("http://example.org/object")


@pytest.mark.release_assets
def test_external_benchmark_files_are_pinned() -> None:
    root = Path(__file__).resolve().parents[1]
    external = root / "data" / "observatory" / "external" / "benchmarks"
    assert (external / "Peer-Review-Analyze-1.0.zip").stat().st_size == 3_965_105
    gray = pd.read_csv(external / "PreprintToPaper_GrayZone.csv")
    assert len(gray) == 299
    assert {"annotator1", "annotator2", "author_match_score"} <= set(gray.columns)


@pytest.mark.release_assets
def test_snsf_strict_ooxml_panel_population_is_lossless() -> None:
    root = Path(__file__).resolve().parents[1]
    workbook = (
        root
        / "data"
        / "observatory"
        / "external"
        / "funding"
        / "SNSF_individual_votes_zenodo_4531160.xlsx"
    )
    sheets = list(_strict_ooxml_rows(workbook))
    assert [name for name, _ in sheets] == [
        "pm_stem",
        "pm_lsm",
        "pm_lsb",
        "pm_hsss",
        "pm_hssh",
        "mint_section1",
        "mint_section2",
        "mint_section3",
        "mint_section4",
    ]
    assert sum(len(rows) - 1 for _, rows in sheets) == 432
    assert sum("Fund" in rows[0].values() for _, rows in sheets) == 4


def test_osg_measurement_modules_do_not_depend_on_tfidf() -> None:
    root = Path(__file__).resolve().parents[1]
    for relative in (
        "src/observatory/limitations.py",
        "src/observatory/lineage.py",
        "src/observatory/modern_novelty.py",
    ):
        text = (root / relative).read_text()
        assert "TfidfVectorizer" not in text


@pytest.mark.release_assets
def test_qwen3_rulers_are_revision_pinned_and_nonreconstructive() -> None:
    root = Path(__file__).resolve().parents[1]
    report = json.loads(
        (root / "results/observatory/validity/qwen3_semantic_novelty_report.json").read_text()
    )
    assert report["model_revision"] == "97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3"
    assert report["embedding_dimension"] == 1024
    assert report["strictly_prior_references"]
    assert not report["input_text_persisted"]
    assert not report["embedding_vectors_persisted"]


@pytest.mark.release_assets
def test_full_hupd_census_and_panorama_reconciliation_are_release_complete() -> None:
    root = Path(__file__).resolve().parents[1]
    census = root / "results" / "observatory" / "validity" / "hupd_application_population.parquet"
    census_file = pq.ParquetFile(census)
    assert census_file.metadata.num_rows == 4_518_254
    assert "application_number_hash" in census_file.schema_arrow.names
    assert not {
        "application_number",
        "examiner_full_name",
        "invention_title",
        "abstract",
        "claims",
    } & set(census_file.schema_arrow.names)

    report = json.loads(
        (root / "results" / "observatory" / "r4" / "patent_population_report.json").read_text()
    )
    assert report["hupd_full_census_released"]
    assert report["panorama_cases"] == report["panorama_distinct_crosswalk_rows"] == 8_143
    assert report["panorama_perfect_row_accounting"]
    assert sum(report["panorama_reconciliation_counts"].values()) == 8_143
    assert report["panorama_full_hupd_match_possible"] is False
