from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from lxml import etree

from observatory.atlas import (
    build_source_atlas,
    count_coverage_grade,
    release_cycle_decision,
    write_lake_population_coverage,
)
from observatory.audit import audit_configuration
from observatory.cleanroom import rebuild_public_fixtures
from observatory.connectors.base import (
    Connector,
    ConnectorContext,
    CoverageEvidence,
    FetchBatch,
    NormalizedRecord,
    RawItem,
    SourceEstimate,
)
from observatory.connectors.checkpoint import CheckpointStore, query_hash
from observatory.connectors.delta import manifest_diff
from observatory.connectors.formats import parse_csv, parse_html, parse_jats, parse_oai
from observatory.connectors.http import NetworkPolicyError, PoliteSession
from observatory.connectors.runner import (
    RunOptions,
    _deduplicate_table_rows,
    run_connector,
)
from observatory.constitution import AnalysisClass, ObservabilityGrade, admissible
from observatory.copernicus_census import _audit_subtypes, _crossref_truth
from observatory.copernicus_outcomes import (
    _observe_or_fetch_chain,
    _RateGate,
    classify_provider_status,
    crawl_copernicus_outcomes,
)
from observatory.copernicus_relation_audit import (
    build_redirect_relation_audit,
    redirect_matches_parent,
)
from observatory.copernicus_relations import build_copernicus_chains
from observatory.crossref_profile import profile_crossref_peer_reviews
from observatory.evaluation_atlas import _evaluation_class
from observatory.external_reproduction import reproduce_p2_semantic_fixture
from observatory.fields import comparison_eligible, map_native_field_candidates
from observatory.fulltext import FullTextOrchestrator, TextJob
from observatory.funding import evaluate_funding_instrument
from observatory.hf_reconciliation import finalize_reconciliation_fields
from observatory.identity import Alias, AliasGraph, canonical_identifier
from observatory.ids import canonical_doi, content_hash, stable_id
from observatory.institutional import rubric_constructs
from observatory.integrity import verify_raw_manifests
from observatory.language import (
    benchmark_language_detection,
    detect_language,
    validate_translation_derivative,
)
from observatory.licensing import ReleaseClass, decide_release
from observatory.migrations import validate_migration
from observatory.openreview_invitation_audit import audit_openreview_invitations
from observatory.openreview_process import (
    _materialize_note_states,
    _merge_note_edit_state,
    build_forum_manifest,
    build_openreview_process_audit,
    build_passing_state_manifest,
    write_openreview_forum_count_sample,
    write_openreview_population_coverage,
)
from observatory.openreview_process import (
    _run_raw_rows as openreview_process_rows,
)
from observatory.operations import (
    BudgetLedger,
    estimate_feature_resources,
    governance_audit,
    network_policy_audit,
    reconcile_resource_estimate,
    release_field_catalogue,
)
from observatory.patents import _claim_map
from observatory.policy_audit import independently_extract_public_configuration
from observatory.provenance import ProvenanceIndex, make_event
from observatory.publication import _deterministic_tar
from observatory.quality import (
    independently_classify_outcome,
    validate_analysis_feature,
)
from observatory.references import benchmark_reference_matching, match_reference
from observatory.registry import evaluate_registered_estimand, source_cards, validate_all
from observatory.release_engineering import (
    build_release_package,
    validate_version_registry,
)
from observatory.release_validation import assert_release_packagable, evaluate_release_gate
from observatory.schema import TABLE_SCHEMAS, json_schema, validate_record
from observatory.selected_history import build_selected_history_audit
from observatory.semantic_novelty import canonical_semantic_fixture
from observatory.storage import NormalizedLake, ObservatoryCatalog, RawStore
from observatory.storage_guard import build_recoverability_manifest, storage_preflight
from observatory.ticket_evidence import audit_ticket_evidence
from observatory.views import install_views
from observatory.visibility import assert_feature_available, identity_visible_at

STAMP = "2026-08-10T00:00:00+00:00"


def test_openreview_partial_edits_infer_complete_note_order_independently() -> None:
    create = {
        "id": "edit-create",
        "invitation": "Venue/Submission1/-/Official_Review",
        "tcdate": 100,
        "readers": ["everyone"],
        "signatures": ["Venue/Reviewer_1"],
        "note": {
            "id": "review-1",
            "forum": "forum-1",
            "replyto": "forum-1",
            "readers": ["everyone"],
            "signatures": ["Venue/Reviewer_1"],
            "content": {"rating": {"value": 6}, "review": {"value": "Initial"}},
        },
    }
    revise = {
        "id": "edit-revise",
        "invitation": "Venue/Submission1/-/Official_Review",
        "tcdate": 200,
        "readers": ["everyone"],
        "signatures": ["Venue/Reviewer_1"],
        "note": {
            "id": "review-1",
            "content": {
                "rating": {"value": 8},
                "review": {"delete": True},
            },
        },
    }
    for edits in ((create, revise), (revise, create)):
        states: dict[str, dict[str, object]] = {}
        for edit in edits:
            _merge_note_edit_state(
                states,
                edit=edit,
                edit_id=edit["id"],
                venue_id="Venue",
            )
        row = list(_materialize_note_states(states))[0]["row"]
        assert row["forum"] == "forum-1"
        assert row["readers"] == ["everyone"]
        assert row["signatures"] == ["Venue/Reviewer_1"]
        assert row["content"] == {"rating": {"value": 8}}


def common() -> dict[str, object]:
    return {
        "source_id": "test",
        "source_object_id": "source-object",
        "provenance_event_id": "provenance-event",
        "observed_at": STAMP,
        "record_version": 1,
    }


def test_crossref_profile_isolates_latest_page_snapshot(tmp_path: Path) -> None:
    raw = RawStore(tmp_path / "raw")
    old = {
        "cursor": "*",
        "total_results": 1,
        "items": [{"DOI": "10.1/old", "type": "peer-review"}],
    }
    current_first = {
        "cursor": "*",
        "total_results": 1,
        "items": [{"DOI": "10.1/current-1", "type": "peer-review"}],
    }
    current_second = {
        "cursor": "next",
        "total_results": 2,
        "items": [{"DOI": "10.1/current-2", "type": "peer-review"}],
    }
    raw.put(
        source_id="crossref",
        native_id="page:proof",
        object_type="peer_review_metadata_page",
        payload=json.dumps(old),
        retrieved_at="2026-08-10T00:00:00+00:00",
    )
    raw.put(
        source_id="crossref",
        native_id="page:census-1",
        object_type="peer_review_metadata_page",
        payload=json.dumps(current_first),
        retrieved_at="2026-08-11T00:00:00+00:00",
    )
    raw.put(
        source_id="crossref",
        native_id="page:census-2",
        object_type="peer_review_metadata_page",
        payload=json.dumps(current_second),
        retrieved_at="2026-08-11T00:00:00+00:00",
    )

    report = profile_crossref_peer_reviews(raw.root)

    assert report["snapshot_objects"] == 2
    assert report["snapshot_manifest"]["provider_total_results"] == [1, 2]
    assert report["snapshot_manifest"]["provider_total_drift"] == 1
    assert report["snapshot_manifest"]["raw_page_bundle_count"] == 2
    assert report["snapshot_manifest"]["complete"] is True


def test_raw_integrity_hashes_duplicate_receipts_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    raw = RawStore(tmp_path / "raw")
    raw.put(source_id="test", native_id="one", object_type="note", payload="same")
    raw.put(source_id="test", native_id="two", object_type="note", payload="same")
    calls = []
    original = RawStore.verify

    def counted(store: RawStore, byte_hash: str) -> bool:
        calls.append(byte_hash)
        return original(store, byte_hash)

    monkeypatch.setattr(RawStore, "verify", counted)
    report = verify_raw_manifests(raw.root, source_ids={"test"})

    assert report["passes"] is True
    assert report["checked"] == 2
    assert report["unique_objects_checked"] == 1
    assert report["duplicate_receipts_skipped"] == 1
    assert len(calls) == 1


def test_constitution_firewall_and_registered_estimand() -> None:
    assert admissible(ObservabilityGrade.A, AnalysisClass.ENTRY_SELECTION)
    assert not admissible(ObservabilityGrade.B, AnalysisClass.ENTRY_SELECTION)
    verdict = evaluate_registered_estimand(
        "P4-ALLOCATION-EFFECT",
        grade="B",
        observed_fields={"application_id": "a", "eligible_pool_id": "p", "outcome": "funded"},
    )
    assert verdict.verdict == "not_identified"
    assert verdict.missing_fields == ("assignment_mechanism", "treatment_arm")
    bounded = evaluate_registered_estimand(
        "P4-RETURNER-LOWER-BOUND",
        grade="D",
        observed_fields={"application_id": "a", "round_id": "r"},
    )
    assert bounded.verdict == "partially_identified"


def test_funding_firewall_and_patent_claim_alignment_fail_closed() -> None:
    verdict = evaluate_funding_instrument(
        {
            "applications": "no",
            "passing_or_eligible_pool": "no",
            "assignment_arm": "no",
            "awards_or_outcome": "yes",
            "unfunded_candidate_text": "no",
            "decision_date_or_round": "yes",
        },
        "allocation_effect",
    )
    assert verdict["verdict"] == "not_identified"
    assert verdict["missing_disclosures"] == ["assignment_arm", "passing_or_eligible_pool"]
    assert verdict["manual_override_allowed"] is False

    claims, ambiguous = _claim_map(["1. A system", "2. A method", "2. Duplicate"])
    assert claims[1] == "1. A system"
    assert ambiguous == {2}


def test_release_atlas_freezes_inputs_and_rejects_weak_r1_cycles(tmp_path: Path) -> None:
    weak = {
        "observability_grade": "C",
        "expected_count": None,
        "found_count": 10,
        "audit_status": "unresolved",
        "earliest_public_stage": "selected publication",
        "known_hidden_stages": ["rejected candidates"],
    }
    assert not release_cycle_decision(weak, source_status="included", release="R1")["eligible"]
    assert release_cycle_decision(weak, source_status="included", release="R2")["eligible"]
    gated = release_cycle_decision(
        {**weak, "observability_grade": "B", "expected_count": 10, "audit_status": "provider_exact"},
        source_status="included",
        release="R1",
        required_ticket_statuses={"E6": "partial"},
    )
    assert not gated["eligible"]
    assert "ticket_gate=E6:partial" in gated["reasons"]
    downgraded = count_coverage_grade(
        {
            "observability_grade": "B",
            "expected_count": 100,
            "found_count": 94,
            "coverage_ratio": 0.94,
        }
    )
    assert downgraded["effective_grade"] == "U"
    assert downgraded["downgraded"]
    (tmp_path / "runs").mkdir()
    atlas = build_source_atlas(tmp_path)
    assert atlas["frozen"]
    assert atlas["release_snapshot_id"].startswith("obs-release-")
    assert atlas["atlas_hash"]


def test_release_atlas_requires_and_uses_uncapped_population_coverage(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    runs.mkdir()
    (runs / "copernicus-population.json").write_text(
        json.dumps(
            {
                "source_id": "copernicus",
                "status": "complete",
                "query_hash": "a" * 64,
                "coverage": [{"gate_cycle_id": "capped", "object_type": "record"}],
                "coverage_count": 101,
                "coverage_truncated_in_manifest": True,
            }
        )
    )
    incomplete = build_source_atlas(tmp_path)
    assert not incomplete["frozen"]
    assert incomplete["coverage_export_required_sources"] == ["copernicus"]

    export = {
        "schema": "observatory.population-coverage/1",
        "source_id": "copernicus",
        "row_count": 2,
        "rows": [
            {
                "gate_cycle_id": "cycle-1",
                "object_type": "discussion_preprint",
                "earliest_public_stage": "post-access public discussion",
                "observability_grade": "B",
                "expected_count": 2,
                "found_count": 2,
                "coverage_ratio": 1.0,
                "audit_status": "provider_exact",
                "known_hidden_stages": ["access review"],
            },
            {
                "gate_cycle_id": "cycle-2",
                "object_type": "discussion_preprint",
                "earliest_public_stage": "post-access public discussion",
                "observability_grade": "B",
                "expected_count": 3,
                "found_count": 3,
                "coverage_ratio": 1.0,
                "audit_status": "provider_exact",
                "known_hidden_stages": ["access review"],
            },
        ],
    }
    export["export_hash"] = content_hash(json.dumps(export, sort_keys=True))
    (tmp_path / "copernicus_population_coverage.json").write_text(json.dumps(export))

    complete = build_source_atlas(tmp_path)
    assert complete["frozen"]
    source = next(row for row in complete["sources"] if row["source_id"] == "copernicus")
    assert source["population_coverage_export"]["row_count"] == 2
    assert {row["gate_cycle_id"] for row in source["release_decisions"]} == {
        "cycle-1",
        "cycle-2",
    }


def test_lake_population_coverage_export_is_run_hash_scoped(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    qhash = "b" * 64
    partition = tmp_path / "lake" / "coverage_observation" / "source_id=copernicus"
    partition.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "coverage_observation_id": "coverage-1",
                    "gate_cycle_id": "cycle-1",
                    "object_type": "discussion_preprint",
                    "earliest_public_stage": "post-access public discussion",
                    "observability_grade": "B",
                    "expected_count": 1,
                    "found_count": 1,
                    "coverage_ratio": 1.0,
                    "expected_count_method": "provider",
                    "query_or_invitation": "query",
                    "known_hidden_stages": json.dumps(["access review"]),
                    "known_exclusions": json.dumps([]),
                    "missing_reason": None,
                    "audit_status": "provider_exact",
                    "source_id": "copernicus",
                }
            ]
        ),
        partition / f"run-{qhash[:16]}-b000.parquet",
    )
    output = write_lake_population_coverage(
        tmp_path / "lake",
        tmp_path / "copernicus_population_coverage.json",
        source_id="copernicus",
        query_hashes=[qhash],
    )

    export = json.loads(output.read_text())
    assert export["row_count"] == 1
    assert export["rows"][0]["known_hidden_stages"] == ["access review"]


def test_identifiers_are_canonical_and_stable() -> None:
    assert canonical_doi("https://doi.org/10.1234/ABC.1).") == "10.1234/abc.1"
    assert canonical_doi("not a doi") is None
    assert stable_id("candidate", "doi", "10.1234/x") == stable_id("candidate", "doi", "10.1234/x")
    assert canonical_identifier("arxiv", "arXiv:2401.00001v3") == "2401.00001"
    assert canonical_identifier("orcid", "https://orcid.org/0000-0002-1825-0097") == "0000-0002-1825-0097"


def test_copernicus_subtype_audit_uses_independent_crossref_labels() -> None:
    works = {
        "10.5194/acp-2025-1": {"type": "posted-content", "subtype": "preprint"},
        "10.5194/egusphere-egu25-1": {"type": "posted-content", "subtype": "other"},
        "10.5194/acp-25-1-2025": {"type": "journal-article"},
    }
    assert _crossref_truth(works["10.5194/acp-2025-1"], "10.5194/acp-2025-1") == "discussion_preprint"
    artifacts = [
        {
            "object_type": f"copernicus_{kind}_metadata",
            "source_url": f"https://doi.org/{doi}",
        }
        for doi, kind in (
            ("10.5194/acp-2025-1", "discussion_preprint"),
            ("10.5194/egusphere-egu25-1", "conference_abstract"),
            ("10.5194/acp-25-1-2025", "final_article"),
        )
    ]
    report = _audit_subtypes(artifacts, works.get, target_per_stratum=1, maximum_attempts_per_stratum=1)
    assert report["micro_precision"] == 1.0
    assert all(row["precision"] == 1.0 for row in report["strata"].values())


def test_copernicus_outcome_requires_affirmative_provider_status() -> None:
    assert (
        classify_provider_status("Status: this preprint was under review for ACP but the revision was not accepted.")
        == "affirmative_rejected_after_public_discussion"
    )
    assert (
        classify_provider_status("Status: this preprint is under review for ACP.")
        == "provider_visible_review_ongoing_censored"
    )
    assert (
        classify_provider_status(
            "This discussion paper has been under review. The manuscript was not "
            "accepted for further review after discussion."
        )
        == "affirmative_rejected_after_public_discussion"
    )
    assert (
        classify_provider_status("Status: this preprint has been withdrawn by the authors.")
        == "affirmative_author_withdrawal_after_public_discussion"
    )
    assert (
        classify_provider_status("Status: this preprint was under review. A final paper is not foreseen.")
        == "affirmative_discontinued_after_public_discussion"
    )
    assert classify_provider_status(None) == "public_discussion_outcome_unresolved_censored"


def test_copernicus_published_relation_skips_unneeded_page_fetch() -> None:
    result = _observe_or_fetch_chain(
        {
            "discussion_doi": "10.5194/acp-2025-1",
            "final_article_dois": ["10.5194/acp-25-1-2025"],
        },
        audit_sample={},
        gate=_RateGate(0),
    )
    assert result["provider_status"] == "published_final_observed_from_relation"
    assert result["acquisition_status"] == "http_not_required_final_relation_observed"


def test_copernicus_resume_repairs_transient_error_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    (staging / "batch-000000.json").write_text(
        json.dumps(
            [
                {
                    "discussion_doi": "10.5194/acp-2025-1",
                    "http_status": None,
                    "resolved_url": None,
                    "page_byte_hash": None,
                    "notification": None,
                    "provider_status": "source_page_unavailable_censored",
                    "error_class": "ConnectionError",
                    "audit_comment_presence": {},
                    "acquisition_status": "provider_landing_page_fetched",
                }
            ]
        )
    )

    def repaired(*_args, **_kwargs):
        return {
            "discussion_doi": "10.5194/acp-2025-1",
            "http_status": 200,
            "resolved_url": "https://example.test/acp-2025-1",
            "page_byte_hash": "hash",
            "notification": "under review",
            "provider_status": "provider_visible_review_ongoing_censored",
            "error_class": None,
            "audit_comment_presence": {},
            "acquisition_status": "provider_landing_page_fetched",
        }

    monkeypatch.setattr("observatory.copernicus_outcomes._observe_or_fetch_chain", repaired)
    output = tmp_path / "report.json"
    crawl_copernicus_outcomes(
        {
            "records": [
                {
                    "discussion_doi": "10.5194/acp-2025-1",
                    "final_article_dois": [],
                    "comments": [],
                }
            ]
        },
        staging_dir=staging,
        output=output,
        workers=1,
    )
    staged = json.loads((staging / "batch-000000.json").read_text())
    assert staged[0]["http_status"] == 200
    assert staged[0]["error_class"] is None


def test_copernicus_resume_can_retain_transient_errors_as_censoring(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    original = [
        {
            "discussion_doi": "10.5194/acp-2025-1",
            "http_status": None,
            "resolved_url": None,
            "page_byte_hash": None,
            "notification": None,
            "provider_status": "source_page_unavailable_censored",
            "error_class": "ConnectionError",
            "audit_comment_presence": {},
            "acquisition_status": "provider_landing_page_fetched",
        }
    ]
    (staging / "batch-000000.json").write_text(json.dumps(original))
    monkeypatch.setattr(
        "observatory.copernicus_outcomes._observe_or_fetch_chain",
        lambda *_args, **_kwargs: pytest.fail("censored row must not be retried"),
    )

    output = tmp_path / "report.json"
    crawl_copernicus_outcomes(
        {
            "records": [
                {
                    "discussion_doi": "10.5194/acp-2025-1",
                    "final_article_dois": [],
                    "comments": [],
                }
            ]
        },
        staging_dir=staging,
        output=output,
        workers=1,
        repair_transient_errors=False,
    )

    report = json.loads(output.read_text())
    assert report["outcome_states"] == {"source_page_unavailable_censored": 1}
    assert report["provider_page_fetch_scope"]["transient_failures_retained_as_censored"] == 1
    assert report["absence_of_final_relation_means_rejection"] is False


def test_copernicus_relation_redirect_audit_validates_parent_namespace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Redirect:
        status_code = 302
        headers = {"Location": ("https://acp.copernicus.org/preprints/acp-2022-242/acp-2022-242-AC2.pdf")}

    monkeypatch.setattr(
        "observatory.copernicus_relation_audit._DOI_SESSION.get",
        lambda *_args, **_kwargs: Redirect(),
    )
    source_row = {
        "discussion_doi": "10.5194/acp-2022-242",
        "comment_doi": "10.5194/acp-2022-242-ac2",
    }
    report = build_redirect_relation_audit({"relation_audit": {"rows": [source_row] * 200}}, workers=1)

    assert redirect_matches_parent(source_row["discussion_doi"], Redirect.headers["Location"])
    assert report["checked"] == 200
    assert report["precision"] == 1.0
    assert report["passes"] is True


def test_copernicus_chains_include_posted_content_relations_without_duplication(
    tmp_path: Path,
) -> None:
    raw = RawStore(tmp_path / "raw")
    preprint = {
        "DOI": "10.5194/acp-2025-1",
        "type": "posted-content",
        "subtype": "preprint",
    }
    comment = {
        "DOI": "10.5194/acp-2025-1-rc1",
        "type": "posted-content",
        "subtype": "other",
        "created": {"date-time": "2025-01-03T00:00:00Z"},
        "relation": {"is-review-of": [{"id": "10.5194/acp-2025-1", "id-type": "doi"}]},
    }
    raw.put(
        source_id="copernicus_crossref",
        native_id="page",
        object_type="copernicus_posted_content_page",
        payload=json.dumps({"items": [preprint, comment]}),
    )
    report = build_copernicus_chains(raw.root)
    assert report["crossref_posted_preprint_population"] == 1
    assert report["discussion_chains"] == 1
    assert report["comments_linked"] == 1
    assert report["records"][0]["roles"] == {"referee_comment": 1}


def test_copernicus_chain_year_prefers_deposit_when_doi_has_no_year(
    tmp_path: Path,
) -> None:
    raw = RawStore(tmp_path / "raw")
    preprint = {
        "DOI": "10.5194/discussion-alpha",
        "type": "posted-content",
        "subtype": "preprint",
        "published": {"date-parts": [[2024, 7, 2]]},
    }
    comment = {
        "DOI": "10.5194/discussion-alpha-rc1",
        "type": "posted-content",
        "subtype": "other",
        "relation": {"is-review-of": [{"id": "10.5194/discussion-alpha", "id-type": "doi"}]},
    }
    raw.put(
        source_id="copernicus_crossref",
        native_id="page",
        object_type="copernicus_posted_content_page",
        payload=json.dumps({"items": [preprint, comment]}),
    )
    report = build_copernicus_chains(raw.root)
    assert report["records"][0]["year"] == 2024


def test_provider_alias_conflict_preserves_both_entity_mappings() -> None:
    base = {
        "identifier_alias_id": "provider-keyed-alias",
        "entity_kind": "candidate",
        "scheme": "doi",
        "value": "10.1101/shared",
        "canonical_value": "10.1101/shared",
        "relation": "source_declared",
        "confidence": 1.0,
        "conflict_status": "none",
        **common(),
    }
    rows = _deduplicate_table_rows(
        "identifier_alias",
        [{**base, "entity_id": "candidate-a"}, {**base, "entity_id": "candidate-b"}],
    )
    assert len(rows) == 2
    assert len({row["identifier_alias_id"] for row in rows}) == 2
    assert {row["conflict_status"] for row in rows} == {"provider_alias_maps_multiple_entities"}


def test_duplicate_metadata_wrappers_keep_richer_raw_pointer_record() -> None:
    base = {
        "content_artifact_id": "artifact",
        "object_type": "provider_metadata",
        "media_type": "application/xml",
        "byte_hash": None,
        "normalized_text_hash": None,
        "source_url": "https://doi.org/10.1234/x",
        "local_pointer": None,
        "licence": "CC-BY-4.0",
        "release_class": "redistribute",
        "language": "eng",
        "parser_version": "1",
        **common(),
    }
    rows = _deduplicate_table_rows(
        "content_artifact",
        [
            {**base, "size_bytes": 100, "source_object_id": "smaller"},
            {**base, "size_bytes": 200, "source_object_id": "richer"},
        ],
    )
    assert len(rows) == 1
    assert rows[0]["size_bytes"] == 200
    assert rows[0]["source_object_id"] == "richer"

    same_page = _deduplicate_table_rows(
        "content_artifact",
        [
            {**base, "size_bytes": 200, "local_pointer": "bundle.records[3]"},
            {**base, "size_bytes": 200, "local_pointer": "bundle.records[7]"},
        ],
    )
    assert len(same_page) == 1
    assert same_page[0]["local_pointer"] == "bundle.records[7]"


@pytest.mark.release_assets
def test_registries_schema_and_secret_audit() -> None:
    result = validate_all()
    assert result["source_cards"] >= 30
    assert result["estimands"] >= 20
    assert all(card.cost_class == "free" for card in source_cards())
    for table in TABLE_SCHEMAS:
        Draft202012Validator.check_schema(json_schema(table))
    root = Path(__file__).resolve().parents[1]
    audit = audit_configuration(root)
    assert audit["possible_secrets"] == []
    assert audit["operations"]["passes"] is True
    assert audit["outbound_configuration"]["passes"] is True
    assert audit["git_secret_surfaces"]["staged_findings"] == []
    # History findings deliberately expose only commit ids and pattern classes;
    # unresolved rotation/history remediation remains an N7 acceptance gap.
    assert all(
        set(row) <= {"surface", "commit", "pattern_class", "error"}
        for row in audit["git_secret_surfaces"]["history_findings"]
    )


def test_governance_resolves_every_table_and_field() -> None:
    catalogue = release_field_catalogue()
    assert len(catalogue) == sum(len(fields) for fields in TABLE_SCHEMAS.values())
    assert {(row["table"], row["field"]) for row in catalogue} == {
        (table, field.name) for table, fields in TABLE_SCHEMAS.items() for field in fields
    }
    restricted = {
        (row["table"], row["field"])
        for row in catalogue
        if row["release_tier"] == "restricted"
    }
    assert ("evaluation", "evaluator_protected_id") in restricted
    assert ("authorship_observation", "protected_person_id") in restricted
    assert governance_audit()["passes"]
    assert network_policy_audit()["passes"]


def test_resource_forecast_requires_reforecast_above_25_percent() -> None:
    estimate = estimate_feature_resources(
        source_id="fixture",
        feature_family="embedding",
        fixture_objects=100,
        fixture_raw_bytes=1_000_000,
        fixture_compressed_bytes=250_000,
        fixture_normalized_bytes=500_000,
        fixture_parse_seconds=100,
        fixture_tokens=25_000,
        provider_objects=10_000,
    )
    assert estimate.expected_requests == 50
    assert estimate.embedding_documents == 10_000
    close = reconcile_resource_estimate(
        estimate,
        {
            "expected_objects": 10_000,
            "expected_requests": 50,
            "raw_bytes": 105_000_000,
            "compressed_bytes": 25_000_000,
            "normalized_bytes": 50_000_000,
            "parsing_hours": estimate.parsing_hours,
            "embedding_documents": 10_000,
            "embedding_tokens": 2_500_000,
            "peak_memory_bytes": estimate.peak_memory_bytes,
            "modal_upper_cost_usd": estimate.modal_upper_cost_usd,
        },
    )
    assert close["passes"]
    far = reconcile_resource_estimate(estimate, {"raw_bytes": 140_000_000})
    assert far["reforecast_required"]
    assert far["offending_metrics"] == ["raw_bytes"]


def test_modal_budget_ledger_enforces_total_envelope_job_and_retry_caps(tmp_path: Path) -> None:
    ledger = BudgetLedger(tmp_path / "budget.jsonl")
    assert ledger.preflight(job_id="pilot-1", envelope="pilots", projected_cost_usd=2.0)["passes"]
    ledger.append_preflight(job_id="pilot-1", envelope="pilots", projected_cost_usd=2.0)
    actual = ledger.record_actual(
        job_id="pilot-1",
        envelope="pilots",
        actual_cost_usd=2.0,
        provider_receipt="modal dashboard export row 1",
    )
    assert actual["actual_cumulative_usd"] == 2.0
    assert not ledger.preflight(job_id="pilot-2", envelope="pilots", projected_cost_usd=2.0)["passes"]
    assert not ledger.preflight(
        job_id="retry", envelope="reference_linkage", projected_cost_usd=1.0, retry_number=3
    )["passes"]
    assert not ledger.preflight(
        job_id="contingency", envelope="contingency", projected_cost_usd=1.0
    )["passes"]


def test_storage_preflight_fails_before_reserve_and_recoverability_is_exact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Usage:
        total = 10_000
        used = 5_000
        free = 5_000

    monkeypatch.setattr("observatory.storage_guard.shutil.disk_usage", lambda _path: Usage())
    assert storage_preflight(
        tmp_path, projected_input_bytes=1_000, projected_output_bytes=1_000, reserve_bytes=2_000
    )["passes"]
    with pytest.raises(RuntimeError, match="unsafe disk pressure"):
        storage_preflight(
            tmp_path, projected_input_bytes=2_000, projected_output_bytes=2_000, reserve_bytes=2_000
        )
    manifest_dir = tmp_path / "raw" / "manifests"
    manifest_dir.mkdir(parents=True)
    (manifest_dir / "openreview_api.jsonl").write_text('{"byte_hash":"a"}\n')
    recoverability = build_recoverability_manifest(tmp_path / "raw")
    assert recoverability["source_count"] == 1
    assert recoverability["sources"][0]["refetchable"] is True
    assert recoverability["sources"][0]["manifest_sha256"]


def test_release_validation_blocks_p0_and_permits_only_documented_scope_reduction() -> None:
    requirements = [
        {"id": "A1", "priority": "P0"},
        {"id": "A2", "priority": "P1"},
    ]
    evidence = [
        {"id": "A1", "status": "partial", "gap": "component is not ready"},
        {"id": "A2", "status": "complete"},
    ]
    blocked = evaluate_release_gate(
        release_id="candidate",
        requirements=requirements,
        evidence=evidence,
        ticket_structure_passes=True,
        policy_passes=True,
        governance_passes=True,
    )
    assert not blocked["packaging_allowed"]
    with pytest.raises(RuntimeError, match="A1"):
        assert_release_packagable(blocked)
    narrowed = evaluate_release_gate(
        release_id="candidate-without-a1-component",
        requirements=requirements,
        evidence=evidence,
        ticket_structure_passes=True,
        policy_passes=True,
        governance_passes=True,
        scope_reductions={
            "A1": {
                "component": "component-a1",
                "reason": "P0 acceptance gap",
                "affected_release": "candidate-without-a1-component",
                "rollback_or_narrowing_path": "restore only after A1 passes",
            }
        },
    )
    assert narrowed["packaging_allowed"]
    assert_release_packagable(narrowed)
    invalid_waiver = evaluate_release_gate(
        release_id="candidate",
        requirements=requirements,
        evidence=evidence,
        ticket_structure_passes=True,
        policy_passes=True,
        governance_passes=True,
        scope_reductions={
            "A1": {
                "component": "component-a1",
                "reason": "skip",
                "affected_release": "candidate",
                "rollback_or_narrowing_path": "none",
                "waiver": "approved",
            }
        },
    )
    assert not invalid_waiver["packaging_allowed"]
    invalid_pointer_registry = evaluate_release_gate(
        release_id="candidate",
        requirements=[{"id": "A1", "priority": "P0"}],
        evidence=[{"id": "A1", "status": "complete"}],
        ticket_structure_passes=True,
        policy_passes=True,
        governance_passes=True,
        pointer_registry_passes=False,
    )
    assert not invalid_pointer_registry["packaging_allowed"]
    assert not invalid_pointer_registry["checks"]["pointer_rebuild_registry"]


def test_schema_rejects_missing_and_unknown_fields() -> None:
    row = {
        "candidate_id": "c",
        "first_observed_at": STAMP,
        "domain": None,
        "candidate_type": "manuscript",
        "canonical_title": "A",
        "status": "visible",
        **common(),
    }
    validate_record("candidate", row)
    with pytest.raises(ValueError, match="missing non-null"):
        validate_record("candidate", {k: v for k, v in row.items() if k != "candidate_id"})
    with pytest.raises(ValueError, match="unknown fields"):
        validate_record("candidate", {**row, "surprise": 1})


def test_raw_store_is_content_addressed_and_verified(tmp_path: Path) -> None:
    store = RawStore(tmp_path / "raw")
    one = store.put(source_id="s", native_id="a", object_type="json", payload=b"same")
    two = store.put(source_id="s", native_id="b", object_type="json", payload=b"same")
    assert one.byte_hash == two.byte_hash
    assert one.created is True and two.created is False
    assert store.get(one.byte_hash) == b"same"
    assert store.verify(one.byte_hash)
    assert len((tmp_path / "raw" / "manifests" / "s.jsonl").read_text().splitlines()) == 2


def test_raw_store_pack_preserves_manifests_and_hash_access(tmp_path: Path) -> None:
    raw = RawStore(tmp_path / "raw")
    first = raw.put(source_id="inactive", native_id="one", object_type="note", payload=b"one")
    second = raw.put(source_id="inactive", native_id="two", object_type="note", payload=b"two")
    raw.put(source_id="inactive", native_id="one-again", object_type="note", payload=b"one")
    manifest_before = (raw.manifests / "inactive.jsonl").read_bytes()
    result = raw.pack_source("inactive")
    assert result["passes"]
    assert result["packed_loose_objects"] == 2
    assert (raw.manifests / "inactive.jsonl").read_bytes() == manifest_before
    assert raw.get(first.byte_hash) == b"one"
    assert raw.get(second.byte_hash) == b"two"
    assert raw.verify(first.byte_hash)
    duplicate = raw.put(source_id="inactive", native_id="packed", object_type="note", payload=b"one")
    assert duplicate.created is False
    assert "#" in duplicate.raw_pointer
    resumed = RawStore(tmp_path / "raw").pack_source("inactive")
    assert resumed["passes"]
    assert resumed["packed_loose_objects"] == 0
    assert resumed["verified_members"] == 2


def test_normalized_lake_and_constitution_views(tmp_path: Path) -> None:
    lake = NormalizedLake(tmp_path / "lake")
    coverage = {
        "coverage_observation_id": "cov",
        "gate_cycle_id": "cycle",
        "object_type": "submission",
        "earliest_public_stage": "submitted",
        "observability_grade": "A",
        "expected_count": 1,
        "found_count": 1,
        "coverage_ratio": 1.0,
        "expected_count_method": "fixture",
        "query_or_invitation": "fixture",
        "known_hidden_stages": [],
        "known_exclusions": [],
        "missing_reason": None,
        "audit_status": "verified",
        "valid_from": STAMP,
        "valid_to": None,
        **common(),
    }
    event = {
        "candidate_gate_event_id": "event",
        "candidate_id": "candidate",
        "candidate_version_id": "version",
        "gate_cycle_id": "cycle",
        "native_id": "native",
        "submitted_at": STAMP,
        "earliest_observed_stage": "submitted",
        "final_observed_stage": "selected",
        "coverage_observation_id": "cov",
        **common(),
    }
    lake.write("coverage_observation", [coverage])
    lake.write("candidate_gate_event", [event])
    assert lake.read("candidate_gate_event").num_rows == 1
    con = ObservatoryCatalog(lake.root).connect()
    install_views(con)
    assert con.execute("SELECT count(*) FROM analysis_entry_selection").fetchone()[0] == 1


def test_nullable_year_zero_provider_sentinel_is_not_fabricated(tmp_path: Path) -> None:
    lake = NormalizedLake(tmp_path / "lake")
    rows = []
    for index, sentinel in enumerate(("0000-01-01T00:00:00+00:00", "0-01-01T00:00:00+00:00")):
        rows.append(
            {
                "candidate_id": f"candidate-{index}",
                "first_observed_at": sentinel,
                "domain": None,
                "candidate_type": "manuscript",
                "canonical_title": "Provider record with unknown date",
                "status": "visible",
                **common(),
            }
        )
    lake.write("candidate", rows)
    assert all(row["first_observed_at"] is None for row in lake.read("candidate").to_pylist())


def test_openreview_population_audit_is_run_scoped_and_exact(tmp_path: Path) -> None:
    run_root = tmp_path / "results"
    checkpoint_root = run_root / "checkpoints"
    checkpoint_root.mkdir(parents=True)
    raw = RawStore(tmp_path / "raw")
    state_hash = "a" * 64
    forum_hash = "b" * 64
    note_hash = "c" * 64
    state_stamp = "2026-08-10T01:00:00+00:00"
    forum_stamp = "2026-08-10T02:00:00+00:00"
    note_stamp = "2026-08-10T03:00:00+00:00"
    for qhash, stamp in (
        (state_hash, state_stamp),
        (forum_hash, forum_stamp),
        (note_hash, note_stamp),
    ):
        (checkpoint_root / f"openreview_api-{qhash[:12]}.json").write_text(
            json.dumps(
                {
                    "complete": True,
                    "started_at": stamp,
                }
            )
        )

    audit = tmp_path / "invitation_audit.json"
    audit.write_text(
        json.dumps(
            {
                "report_hash": "audit",
                "cycles": [{"venue_id": "Venue/2026", "observability_grade": "B"}],
                "invitations": [
                    {
                        "api_version": "v2",
                        "invitation": "Venue/2026/-/Submission",
                        "kind": "submission",
                        "venue_id": "Venue/2026",
                        "provider_note_count": 1,
                        "error_class": None,
                    }
                ],
            }
        )
    )
    state_manifest = build_passing_state_manifest(audit, tmp_path / "state_manifest.json")
    root_note = {
        "id": "forum-1",
        "forum": "forum-1",
        "invitations": ["Venue/2026/-/Submission"],
        "readers": ["everyone"],
        "signatures": ["~Author1"],
        "tcdate": 1_700_000_000_000,
        "content": {"title": {"value": "Paper"}},
    }
    state_bundle = {
        "api_version": "v2",
        "invitation": "Venue/2026/-/Submission",
        "provider_note_count": 1,
        "items": [
            {
                "native_id": "v2:note:forum-1",
                "object_type": "note",
                "payload": root_note,
                "metadata": {"invitation_query": "Venue/2026/-/Submission"},
            }
        ],
    }
    raw.put(
        source_id="openreview_api",
        native_id="state-page",
        object_type="notes_edits_page_bundle",
        payload=json.dumps(state_bundle),
        retrieved_at=state_stamp,
    )
    forum_manifest = build_forum_manifest(
        raw.root,
        tmp_path / "forum_manifest.json",
        run_root=run_root,
        state_query_hash=state_hash,
        state_manifest_path=state_manifest,
    )
    assert json.loads(forum_manifest.read_text())["forum_count"] == 1

    review = {
        "id": "review-1",
        "forum": "forum-1",
        "replyto": "forum-1",
        "invitations": ["Venue/2026/-/Official_Review"],
        "readers": ["everyone"],
        "signatures": ["Venue/2026/Reviewer_1"],
        "tcdate": 1_700_000_100_000,
        "content": {"review": {"value": "Sound."}},
    }
    forum_bundle = {
        "api_version": "v2",
        "domain": "Venue/2026",
        "venue_id": "Venue/2026",
        "provider_edit_count": 2,
        "items": [
            {
                "native_id": "v2:edit:edit-root",
                "object_type": "note_edit",
                    "payload": {
                        "id": "edit-root",
                        "invitation": "Venue/2026/-/Submission",
                        "readers": ["everyone"],
                    "tcdate": 1_700_000_000_000,
                    "note": root_note,
                },
                "metadata": {"invitation_query": "Venue/2026/-/Submission"},
            },
            {
                "native_id": "v2:edit:edit-review",
                "object_type": "note_edit",
                    "payload": {
                        "id": "edit-review",
                        "invitation": "Venue/2026/-/Official_Review",
                        "readers": ["everyone"],
                    "tcdate": 1_700_000_100_000,
                    "note": review,
                },
                "metadata": {"invitation_query": "Venue/2026/-/Official_Review"},
            },
        ],
    }
    raw.put(
        source_id="openreview_api",
        native_id="forum-page",
        object_type="notes_edits_page_bundle",
        payload=json.dumps(forum_bundle),
        retrieved_at=forum_stamp,
    )
    raw.put(
        source_id="openreview_api",
        native_id="domain-note-page",
        object_type="notes_edits_page_bundle",
        payload=json.dumps(
            {
                "api_version": "v2",
                "forum_batch_id": "batch-1",
                "forum_batch": [
                    {
                        "api_version": "v2",
                        "forum": "forum-1",
                        "venue_id": "",
                    }
                ],
                "provider_forum_batch_note_count": 2,
                "items": [
                    {
                        "native_id": "v2:note:forum-1",
                        "object_type": "note",
                        "payload": root_note,
                    },
                    {
                        "native_id": "v2:note:review-1",
                        "object_type": "note",
                        "payload": review,
                    },
                ],
            }
        ),
        retrieved_at=note_stamp,
    )
    forum_sample = tmp_path / "forum_sample.json"
    forum_sample.write_text(
        json.dumps(
            {
                "passes": True,
                "rows": [
                    {
                        "forum": "forum-1",
                        "api_version": "v2",
                        "venue_id": "Venue/2026",
                        "provider_note_count": 2,
                        "error_class": None,
                    }
                ],
            }
        )
    )
    report_path = build_openreview_process_audit(
        raw.root,
        tmp_path / "process_audit.json",
        run_root=run_root,
        state_manifest_path=state_manifest,
        state_query_hash=state_hash,
        forum_query_hash=forum_hash,
        note_query_hash=note_hash,
        forum_count_sample_path=forum_sample,
    )
    report = json.loads(report_path.read_text())
    assert report["passes"]
    assert report["domain_edit_count_reconciliation_passes"]
    assert report["current_note_count_reconciliation_passes"]
    assert report["current_note_forum_partition_passes"]
    assert report["current_note_requested_forum_count"] == 1
    assert report["current_note_zero_count_forum_count"] == 0
    assert report["state_root_current_note_overlap_ratio"] == 1.0
    assert report["current_expected_note_count"] == 2
    assert report["domain_expected_edit_count"] == 2
    assert report["state_cycles_at_or_above_95_percent"] == 1
    assert report["role_counts"] == {"official_review": 1, "submission": 1}
    assert report["cycle_graph_quality"] == [
        {
            "venue_id": "Venue/2026",
            "object_count": 2,
            "orphan_reply_count": 0,
            "orphan_reply_rate": 0.0,
            "duplicate_object_count": 0,
            "duplicate_object_rate": 0.0,
        }
    ]
    coverage_path = write_openreview_population_coverage(
        report_path,
        tmp_path / "openreview_api_population_coverage.json",
    )
    coverage = json.loads(coverage_path.read_text())
    assert coverage["cycle_count"] == 1
    assert coverage["row_count"] == 3
    assert {
        (row["venue_id"], row["object_type"])
        for row in coverage["rows"]
    } == {
        ("Venue/2026", "candidate_state"),
        ("Venue/2026", "note_edit_history"),
        ("Venue/2026", "current_note_graph"),
    }


def test_openreview_population_audit_unions_disjoint_complete_runs(
    tmp_path: Path,
) -> None:
    run_root = tmp_path / "results"
    checkpoint_root = run_root / "checkpoints"
    checkpoint_root.mkdir(parents=True)
    raw = RawStore(tmp_path / "raw")
    hashes = ("a" * 64, "b" * 64)
    stamps = ("2026-08-10T01:00:00+00:00", "2026-08-10T02:00:00+00:00")
    for qhash, stamp in zip(hashes, stamps):
        (checkpoint_root / f"openreview_api-{qhash[:12]}.json").write_text(
            json.dumps({"complete": True, "started_at": stamp})
        )
    for index, stamp in enumerate(stamps):
        raw.put(
            source_id="openreview_api",
            native_id=f"page-{index}",
            object_type="notes_edits_page_bundle",
            payload=json.dumps(
                {
                    "domain": f"Venue/{index}",
                    "provider_edit_count": 1,
                    "items": [
                        {
                            "native_id": f"v2:edit:{index}",
                            "object_type": "note_edit",
                            "payload": {
                                "id": f"edit-{index}",
                                "note": {"id": f"note-{index}"},
                            },
                        }
                    ],
                }
            ),
            retrieved_at=stamp,
        )
    rows = list(
        openreview_process_rows(
            raw.root,
            run_root=run_root,
            query_hash=hashes,
        )
    )
    assert len(rows) == 2
    with pytest.raises(ValueError, match="unique"):
        list(
            openreview_process_rows(
                raw.root,
                run_root=run_root,
                query_hash=(hashes[0], hashes[0]),
            )
        )


def test_openreview_run_reader_prefers_run_scoped_source_object_shards(
    tmp_path: Path,
) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    run_root = tmp_path / "results"
    checkpoints = run_root / "checkpoints"
    checkpoints.mkdir(parents=True)
    qhash = "d" * 64
    stamp = "2026-08-10T02:30:00+00:00"
    (checkpoints / f"openreview_api-{qhash[:12]}.json").write_text(
        json.dumps({"complete": True, "started_at": stamp})
    )
    raw = RawStore(tmp_path / "data" / "raw")
    receipts = [
        raw.put(
            source_id="openreview_api",
            native_id=f"page-{index}",
            object_type="notes_edits_page_bundle",
            payload=json.dumps({"domain": f"Venue/{index}", "items": []}),
            retrieved_at=stamp,
        )
        for index in range(2)
    ]
    # Model a lost concurrent append in the shared raw JSONL. Both immutable
    # blobs and both run-scoped source_object rows remain authoritative.
    raw_manifest = raw.manifests / "openreview_api.jsonl"
    raw_manifest.write_text(raw_manifest.read_text().splitlines()[0] + "\n")
    partition = (
        tmp_path
        / "data"
        / "normalized"
        / "source_object"
        / "source_id=openreview_api"
    )
    partition.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "source_object_id": receipt.source_object_id,
                    "native_id": f"page-{index}",
                    "object_type": "notes_edits_page_bundle",
                    "byte_hash": receipt.byte_hash,
                    "retrieved_at": stamp,
                }
                for index, receipt in enumerate(receipts)
            ]
        ),
        partition / f"run-{qhash[:16]}-b000.parquet",
    )

    rows = list(
        openreview_process_rows(
            raw.root,
            run_root=run_root,
            query_hash=qhash,
        )
    )

    assert {row[1]["domain"] for row in rows} == {"Venue/0", "Venue/1"}


def test_openreview_run_reader_resolves_packed_raw_objects(tmp_path: Path) -> None:
    run_root = tmp_path / "results"
    checkpoint_root = run_root / "checkpoints"
    checkpoint_root.mkdir(parents=True)
    qhash = "c" * 64
    stamp = "2026-08-10T03:00:00+00:00"
    (checkpoint_root / f"openreview_api-{qhash[:12]}.json").write_text(
        json.dumps({"complete": True, "started_at": stamp})
    )
    raw = RawStore(tmp_path / "raw")
    receipt = raw.put(
        source_id="openreview_api",
        native_id="page",
        object_type="notes_edits_page_bundle",
        payload=json.dumps({"domain": "Venue", "items": []}),
        retrieved_at=stamp,
    )
    loose_path = Path(receipt.raw_pointer)
    assert loose_path.exists()
    assert raw.pack_source("openreview_api")["passes"]
    assert not loose_path.exists()
    rows = list(openreview_process_rows(raw.root, run_root=run_root, query_hash=qhash))
    assert rows[0][1]["domain"] == "Venue"


def test_openreview_forum_sample_batches_and_regroups_exact_counts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = tmp_path / "forums.json"
    manifest.write_text(
        json.dumps(
            {
                "manifest_hash": "frozen",
                "forums": [
                    {"api_version": "v2", "forum": "forum-a", "venue_id": "Venue/A"},
                    {"api_version": "v2", "forum": "forum-b", "venue_id": "Venue/B"},
                ],
            }
        )
    )
    calls: list[dict[str, object]] = []

    def fake_get_json(self, context, version, path, params):
        calls.append(dict(params))
        return {
            "count": 3,
            "notes": [
                {"id": "a1", "forum": "forum-a"},
                {"id": "a2", "forum": "forum-a"},
                {"id": "b1", "forum": "forum-b"},
            ],
        }

    monkeypatch.setattr(
        "observatory.adapters.openreview_api.OpenReviewAPINotesConnector._get_json",
        fake_get_json,
    )
    output = write_openreview_forum_count_sample(object(), manifest, tmp_path / "sample.json")
    report = json.loads(output.read_text())
    assert report["passes"]
    assert len(calls) == 1
    assert calls[0]["forum"] == ["forum-a", "forum-b"]
    assert [row["provider_note_count"] for row in report["rows"]] == [2, 1]


def test_openreview_invitation_audit_uses_surface_flag_and_runtime_readers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = tmp_path / "invitations.json"
    manifest.write_text(
        json.dumps(
            {
                "invitations": [
                    {
                        "api_version": "v2",
                        "invitation": "Venue/2026/-/Submission",
                        "kind": "submission",
                        "venue_id": "Venue/2026",
                        "public_configuration_flag": True,
                    }
                ]
            }
        )
    )
    calls = []

    def fake_get_json(self, context, version, path, params):
        calls.append((version, path, params))
        return {
            "count": 1,
            "notes": [{"id": "forum-1", "readers": ["everyone"]}],
        }

    monkeypatch.setattr(
        "observatory.adapters.openreview_api.OpenReviewAPINotesConnector._get_json",
        fake_get_json,
    )
    context = ConnectorContext(tmp_path, tmp_path / "fixtures", tmp_path / "cache")
    report = audit_openreview_invitations(
        context,
        manifest_path=manifest,
        checkpoint_path=tmp_path / "checkpoint.json",
    )
    assert report["passes"]
    assert report["grade_counts"] == {"B": 1}
    assert calls[0][1] == "/notes"
    assert report["invitations"][0]["runtime_reader_sample_public_count"] == 1


def test_selected_history_layer_invokes_pointer_downgrade_and_firewall() -> None:
    report = build_selected_history_audit(Path("tests/fixtures/observatory"))
    assert report["passes"]
    assert report["kill_downgrade_invoked"] is True
    assert set(report["providers"]) == {
        "peerj",
        "plos_review_history",
        "embo_transparent_review",
        "royal_society_review",
        "bmc_open_review",
    }
    assert all(
        row["observability_grade"] == "C"
        and row["text_release_decision"] == "pointer_hash_only"
        and not row["entry_selection_admissible"]
        for row in report["providers"].values()
    )


def test_openreview_derived_reconciliation_covers_all_named_corpora() -> None:
    report = finalize_reconciliation_fields(
        json.loads(Path("results/observatory/openreview_derived_corpora_reconciliation.json").read_text())
    )
    assert report["passes"]
    assert set(report["required_corpus_groups"]) == {
        "ReviewArena",
        "Re2",
        "NLPeer",
        "ResearchArcade",
        "PeerRead",
        "MOPRD",
        "ARR-consent",
    }
    assert report["raw_text_duplicated"] is False


def test_provenance_trace(tmp_path: Path) -> None:
    index = ProvenanceIndex(tmp_path / "provenance.jsonl")
    root = make_event(source_id="s", source_object_id="o", event_type="retrieve")
    child = make_event(
        source_id="s",
        source_object_id="o",
        event_type="normalize",
        parent_event_ids=[root.provenance_event_id],
    )
    index.append(root)
    index.append(child)
    assert {row["event_type"] for row in index.trace(child.provenance_event_id)} == {"retrieve", "normalize"}


def test_checkpoint_refuses_query_drift(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "checkpoint.json")
    first = store.load(source_id="s", expected_query_hash=query_hash({"q": 1}))
    first.cursor = "next"
    store.save(first)
    assert store.load(source_id="s", expected_query_hash=query_hash({"q": 1})).cursor == "next"
    with pytest.raises(ValueError, match="query changed"):
        store.load(source_id="s", expected_query_hash=query_hash({"q": 2}))


def test_manifest_diff() -> None:
    # Exercise absent manifests and exact identity.
    assert manifest_diff(Path("/definitely/absent-a"), Path("/definitely/absent-b"))["after_count"] == 0


def test_parsers_cover_core_formats() -> None:
    assert parse_csv("a,b\n1,2\n") == [{"a": "1", "b": "2"}]
    assert parse_html("<html><head><title>T</title></head><body>X</body></html>")["title"] == "T"
    jats = b"""<article><front><article-meta><title-group><article-title>T</article-title></title-group>
      <article-id pub-id-type='doi'>10.1234/x</article-id><abstract><p>A</p></abstract></article-meta></front>
      <body><sec><p>Body</p></sec></body><back><ref-list><ref id='r1'><element-citation>
      <pub-id pub-id-type='doi'>10.9999/y</pub-id></element-citation></ref></ref-list></back></article>"""
    parsed = parse_jats(jats)
    assert parsed["doi"] == "10.1234/x"
    assert parsed["references"][0]["doi"] == "10.9999/y"
    oai = b"""<OAI-PMH xmlns='http://www.openarchives.org/OAI/2.0/'><ListRecords><record>
      <header><identifier>oai:test:1</identifier><datestamp>2026-01-01</datestamp></header>
      <metadata><x xmlns='urn:x'><title>T</title></x></metadata></record>
      <resumptionToken>next</resumptionToken></ListRecords></OAI-PMH>"""
    rows, token = parse_oai(oai)
    assert rows[0]["identifier"] == "oai:test:1" and token == "next"


def test_oai_recovery_is_explicit_opt_in() -> None:
    malformed = b"""<OAI-PMH xmlns='http://www.openarchives.org/OAI/2.0/'>
      <ListRecords><record><header><identifier>oai:test:1</identifier></header>
      <metadata><x xmlns='urn:x'><title>A & B</title></x></metadata>
      </record></ListRecords></OAI-PMH>"""
    with pytest.raises(etree.XMLSyntaxError):
        parse_oai(malformed)
    rows, token = parse_oai(malformed, recover=True)
    assert token is None
    assert rows[0]["identifier"] == "oai:test:1"


def test_network_policy_is_deny_by_default(tmp_path: Path) -> None:
    session = PoliteSession(cache_dir=tmp_path, allowed_hosts={"api.crossref.org"})
    with pytest.raises(NetworkPolicyError):
        session.get("https://api.openai.com/v1/models")


def test_licence_identity_reference_visibility_and_language() -> None:
    assert (
        decide_release(object_type="review", licence="CC-BY-4.0", source_allows_redistribution=True).release_class
        == ReleaseClass.REDISTRIBUTE
    )
    assert (
        decide_release(object_type="article", licence=None, source_allows_redistribution=None).release_class
        == ReleaseClass.DERIVED_ONLY
    )
    graph = AliasGraph()
    assert graph.add(Alias("a", "doi", "https://doi.org/10.1234/X", evidence="source:a")) == "ok"
    assert graph.add(Alias("b", "doi", "10.1234/x", evidence="source:b")) == "conflict"
    assert graph.conflicts()
    explanation = graph.explain("doi", "10.1234/x")
    assert explanation.status == "quarantined_conflict"
    assert explanation.evidence_paths == (("doi:10.1234/x", "source:a", "a"), ("doi:10.1234/x", "source:b", "b"))
    with pytest.raises(ValueError, match="conflicting doi alias"):
        graph.require_unique("doi", "10.1234/x")
    match = match_reference({"text": "Example doi:10.1234/X"}, by_doi={"10.1234/x": "a"})
    assert match.candidate_id == "a" and match.confidence == 1.0
    ambiguous = match_reference(
        {"title": "Nearly identical paper", "year": 2025, "text": "citation"},
        by_doi={},
        candidates=[
            {"candidate_id": "x", "title": "Nearly identical paper", "year": 2025},
            {"candidate_id": "y", "title": "Nearly identical papers", "year": 2025},
        ],
    )
    assert ambiguous.candidate_id is None and ambiguous.method == "ambiguous_bibliographic_hash"
    reference_benchmark = benchmark_reference_matching(
        [
            {"reference": {"doi": "10.1234/x", "text": "structured"}, "expected_candidate_id": "a"},
            {"reference": {"text": "unresolvable reference"}, "expected_candidate_id": None},
        ],
        by_doi={"10.1234/x": "a"},
    )
    assert reference_benchmark["precision"] == 1.0
    assert reference_benchmark["recall"] == 0.5
    visibility = {"audience": "reviewer", "visible_from": "2026-01-01T00:00:00+00:00", "visible_to": None}
    assert identity_visible_at(visibility, "2026-02-01T00:00:00+00:00", audience="reviewer")
    with pytest.raises(ValueError, match="temporal leakage"):
        assert_feature_available("2026-03-01T00:00:00+00:00", "2026-02-01T00:00:00+00:00")
    assert detect_language("the method and the evidence in the article with the model " * 4).language == "en"
    benchmark = benchmark_language_detection(
        [
            {"language": "en", "text": "the method and the evidence in the article with the model " * 4},
            {"language": "de", "text": "der Ansatz und die Evidenz in der Studie mit der Methode " * 4},
            {"language": "fr", "text": "le modèle et la preuve de la méthode avec le résultat " * 4},
            {"language": "es", "text": "el modelo y la evidencia de la investigación con el método " * 4},
        ]
    )
    assert benchmark["accuracy_when_decided"] == 1.0
    derivative = validate_translation_derivative(
        {
            "source_language": "de",
            "target_language": "en",
            "model_name": "open-model",
            "model_version": "1",
            "source_text_hash": "source",
            "translated_text_hash": "translation",
        }
    )
    assert derivative.primary_construct_eligible is False
    with pytest.raises(ValueError, match="lacks provenance"):
        validate_translation_derivative({"source_language": "de"})
    field_rows = map_native_field_candidates("provider-track", "Statistics and machine learning")
    assert {row.normalized_label for row in field_rows} == {"computer science", "mathematics"}
    assert all(row.score == 0.5 for row in field_rows)
    assert comparison_eligible(field_rows) == ()


def test_fulltext_benchmark_is_shardable_and_resumable(tmp_path: Path) -> None:
    xml = tmp_path / "one.xml"
    html = tmp_path / "two.html"
    unknown = tmp_path / "three.bin"
    xml.write_text("""<article><front><article-meta><title-group><article-title>T</article-title>
      </title-group></article-meta></front><body><sec><p>Body</p></sec></body>
      <back><ref-list><ref><mixed-citation>Reference</mixed-citation></ref></ref-list></back></article>""")
    html.write_text("<html><head><title>H</title></head><body>Text</body></html>")
    unknown.write_bytes(b"unknown")
    jobs = [
        TextJob("xml", "application/jats+xml", str(xml), expected_reference_count=1),
        TextJob("html", "text/html", str(html)),
        TextJob("unknown", "application/octet-stream", str(unknown)),
    ]
    orchestrator = FullTextOrchestrator(tmp_path / "derived")
    output = tmp_path / "benchmark.json"
    first = orchestrator.benchmark(jobs, output)
    second = orchestrator.benchmark(jobs, output)
    assert first["document_count"] == 3 and first["success_count"] == 2
    assert first["reference_recall_proxy_mean"] == 1.0
    assert first["failure_taxonomy"] == {"ValueError": 1}
    assert second["resumed_count"] == 3
    shard = orchestrator.benchmark(jobs, tmp_path / "shard.json", shard_index=1, shard_count=2)
    assert shard["document_count"] == 1 and shard["results"][0]["native_id"] == "html"


@pytest.mark.release_assets
def test_full_ticket_evidence_is_structurally_valid() -> None:
    workspace = Path(__file__).resolve().parents[1]
    audit = audit_ticket_evidence(workspace)
    assert audit["expected_ticket_count"] == 164
    assert audit["configured_ticket_count"] == 164
    assert audit["requirements_ticket_count"] == 164
    assert audit["requirements_match_ticketbook"] is True
    assert audit["release_wave_ticket_count"] == 164
    assert not audit["release_wave_duplicates"]
    assert not audit["release_wave_missing"]
    assert set(audit["status_by_release_wave"]) == {"R0", "R1", "R2", "R3", "R4", "R5"}
    assert audit["passes_structure"] is True


class FakeConnector(Connector):
    source_id = "fake"

    def discover(self, context):
        return [{"fixture": True}]

    def count(self, context):
        return SourceEstimate("fake", 1, method="fixture", confidence="exact")

    def fetch(self, context, *, cursor=None, limit=None):
        yield FetchBatch(
            (RawItem("1", "fixture", '{"title":"T"}', release_class="redistribute"),), None, True, "fixture", 1
        )

    def normalize(self, item, *, source_object_id, provenance_event_id):
        yield NormalizedRecord(
            "candidate",
            {
                "candidate_id": stable_id("candidate", "fake", "1"),
                "first_observed_at": STAMP,
                "domain": None,
                "candidate_type": "fixture",
                "canonical_title": "T",
                "status": "visible",
                "source_id": "fake",
                "source_object_id": source_object_id,
                "provenance_event_id": provenance_event_id,
                "observed_at": STAMP,
                "record_version": 1,
            },
        )

    def validate_fixture(self, context):
        return {"passes": True}

    def emit_coverage(self, context, *, found_count):
        yield CoverageEvidence(
            "cycle", "fixture", "public", "D", 1, found_count, "fixture", "fixture", audit_status="verified"
        )


def test_runner_end_to_end(tmp_path: Path) -> None:
    context = ConnectorContext(tmp_path, tmp_path / "fixtures", tmp_path / "cache")
    connector = FakeConnector()
    result = run_connector(
        connector,
        context,
        raw_store=RawStore(tmp_path / "raw"),
        lake=NormalizedLake(tmp_path / "lake"),
        run_root=tmp_path / "runs-root",
        options=RunOptions(),
    )
    assert result["storage_preflight"]["passes"]
    assert result["connector_code_hash"]
    assert result["output_hashes"]["candidate"]
    assert result["run_manifest_hash"]
    assert result["status"] == "complete"
    assert result["found_count"] == 1
    assert result["tables"]["candidate"] == 1
    assert content_hash((tmp_path / "raw" / "objects").glob("**/*.gz").__next__().read_bytes())

    def unavailable_count(_context):
        raise AssertionError("completed run must not re-query the provider")

    connector.count = unavailable_count  # type: ignore[method-assign]
    frozen = run_connector(
        connector,
        context,
        raw_store=RawStore(tmp_path / "raw"),
        lake=NormalizedLake(tmp_path / "lake"),
        run_root=tmp_path / "runs-root",
        options=RunOptions(),
    )
    assert frozen == result


def test_catalog_exposes_append_only_history_and_one_current_record(tmp_path: Path) -> None:
    lake = NormalizedLake(tmp_path / "lake")
    candidate_id = stable_id("candidate", "fixture", "same")
    common = {
        "candidate_id": candidate_id,
        "first_observed_at": STAMP,
        "domain": None,
        "candidate_type": "fixture",
        "canonical_title": "Earlier",
        "status": "visible",
        "source_id": "fixture",
        "source_object_id": stable_id("source_object", "fixture", "one"),
        "provenance_event_id": stable_id("provenance", "fixture", "one"),
        "observed_at": STAMP,
        "record_version": 1,
    }
    lake.write("candidate", [common], partition={"source_id": "fixture"}, shard_name="one.parquet")
    later = {
        **common,
        "canonical_title": "Later",
        "source_object_id": stable_id("source_object", "fixture", "two"),
        "provenance_event_id": stable_id("provenance", "fixture", "two"),
        "observed_at": "2026-08-11T00:00:00+00:00",
        "record_version": 2,
    }
    lake.write("candidate", [later], partition={"source_id": "fixture"}, shard_name="two.parquet")

    connection = ObservatoryCatalog(lake.root).connect()

    assert connection.execute("SELECT count(*) FROM candidate_history").fetchone()[0] == 2
    assert connection.execute("SELECT count(*) FROM candidate").fetchone()[0] == 1
    assert connection.execute("SELECT canonical_title FROM candidate").fetchone()[0] == "Later"


def test_rubric_crosswalk_can_multimap_and_public_comments_default_nonofficial() -> None:
    assert rubric_constructs("overall novelty and significance") == [
        "novelty_originality",
        "overall_recommendation",
        "significance_interest",
    ]
    assert _evaluation_class("public comment", None, None) == (
        "public_comment",
        "not_official_by_type",
    )


def test_policy_audit_extraction_is_lossless_and_drops_direct_contacts() -> None:
    group = {
        "content": {
            "review_rating": {"value": [1, 2, 3]},
            "review_confidence": {"value": "high"},
            "submission_deadline": {"value": 100},
            "contact_email": {"value": "excluded@example.invalid"},
        }
    }
    assert independently_extract_public_configuration(group) == {
        "review_rating": [1, 2, 3],
        "review_confidence": "high",
        "submission_deadline": 100,
    }


def test_outcome_and_temporal_contracts_fail_closed() -> None:
    assert independently_classify_outcome("ICLR 2021 Spotlight") == "accepted"
    assert independently_classify_outcome("Publish") == "selected"
    assert independently_classify_outcome("unclear provider state") is None
    with pytest.raises(ValueError, match="temporal leakage"):
        validate_analysis_feature(
            "entry_selection",
            feature_available_at="2026-03-01T00:00:00+00:00",
            decision_at="2026-02-01T00:00:00+00:00",
            feature_role="decision_time_feature",
        )
    with pytest.raises(ValueError, match="final_version"):
        validate_analysis_feature(
            "stage_selection",
            feature_available_at="2026-01-01T00:00:00+00:00",
            decision_at="2026-02-01T00:00:00+00:00",
            feature_role="final_version",
        )


def test_standalone_semantic_fixture_and_version_registry() -> None:
    fixture = canonical_semantic_fixture()
    assert fixture["passes"]
    assert fixture["repeat_nearest_distance"] < fixture["novel_nearest_distance"]
    registry = {
        "release": {
            "release_version": "1.0.0-r1",
            "immutable_cutoff": STAMP,
            "schema_version": "1.0.0",
            "source_snapshot_version": "2026-08-20",
            "normalized_data_version": "1.0.0",
            "linkage_model_version": "0.0.0-not-released",
            "feature_versions": {"semantic": "1.0.0"},
            "release_package_version": "1.0.0",
        },
        "row_version_columns": [
            "schema_version",
            "source_snapshot_version",
            "normalized_data_version",
            "linkage_model_version",
            "feature_version",
            "release_package_version",
        ],
    }
    assert validate_version_registry(registry)["passes"]


@pytest.mark.release_assets
def test_external_semantic_reproduction_is_read_only(tmp_path: Path) -> None:
    workspace = Path(__file__).resolve().parents[1]
    report = reproduce_p2_semantic_fixture(
        workspace,
        tmp_path / "p2_semantic_reproduction_audit.json",
    )
    assert report["passes"] is True
    assert report["paper_project_modified"] is False
    assert report["row_count"] == 3000


@pytest.mark.release_assets
def test_cleanroom_rebuild_and_release_package_load_without_raw_lake(tmp_path: Path) -> None:
    workspace = Path(__file__).resolve().parents[1]
    cleanroom = rebuild_public_fixtures(
        workspace / "tests" / "fixtures" / "observatory",
        tmp_path / "cleanroom.json",
    )
    assert cleanroom["passes"]
    assert cleanroom["network_calls"] == cleanroom["paid_api_calls"] == 0
    release_root = tmp_path / "release"
    try:
        package = build_release_package(
            workspace,
            release_root,
            validation_report={
                "packaging_allowed": True,
                "validation_hash": "fixture-validation",
                "p0_failures": [],
            },
        )
        assert package["passes"]
        assert package["privacy"]["passes"]
        assert package["duckdb_table_counts"]["semantic_novelty"] > 0
        assert package["external_parquet_table_counts"]["hupd_application_population"] == 4_518_254
        assert "hupd_application_population" not in package["duckdb_table_counts"]
        assert not list(release_root.rglob("raw"))
    finally:
        shutil.rmtree(release_root, ignore_errors=True)


def test_runner_streaming_compile_closes_and_reads_bucket_writers(
    tmp_path: Path,
) -> None:
    class StreamingFake(FakeConnector):
        source_id = "streaming_fake"
        force_streaming = True

    context = ConnectorContext(tmp_path, tmp_path / "fixtures", tmp_path / "cache")
    result = run_connector(
        StreamingFake(),
        context,
        raw_store=RawStore(tmp_path / "raw"),
        lake=NormalizedLake(tmp_path / "lake"),
        run_root=tmp_path / "runs-root",
        options=RunOptions(),
    )
    assert result["streaming_compile"] is True
    assert result["local_stage_cache_bytes"] > 0
    assert result["tables"]["candidate"] == 1
    assert result["tables"]["source_object"] == 2
    assert list((tmp_path / "lake" / "candidate").glob("**/*.parquet"))


def test_publication_tar_is_deterministic_and_scoped(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    source = workspace / "docs" / "observatory" / "card.md"
    source.parent.mkdir(parents=True)
    source.write_text("public fixture\n")
    cache = workspace / "docs" / "observatory" / "__pycache__" / "card.pyc"
    cache.parent.mkdir()
    cache.write_bytes(b"compiled fixture")
    build_log = workspace / "docs" / "observatory" / "build" / "paper.log"
    build_log.parent.mkdir()
    build_log.write_text("transient fixture\n")
    first = _deterministic_tar(workspace, [source], workspace / "first.tar")
    second = _deterministic_tar(workspace, [source], workspace / "second.tar")
    assert first["sha256"] == second["sha256"]
    assert first["member_count"] == second["member_count"] == 1

    directory_tar = _deterministic_tar(
        workspace,
        [workspace / "docs" / "observatory"],
        workspace / "directory.tar",
    )
    assert directory_tar["member_count"] == 1


def test_release_migration_rejects_ambiguous_or_mutating_id_changes() -> None:
    report = validate_migration(
        [
            {
                "change_class": "id_merges",
                "reason": "fixture",
                "evidence": "public fixture",
                "old_ids": ["old-a", "old-b"],
                "new_ids": [],
                "mutates_frozen_release": True,
            }
        ]
    )
    reasons = {row["reason"] for row in report["failures"]}
    assert report["passes"] is False
    assert reasons == {"ambiguous_id_mapping", "frozen_release_mutation"}
