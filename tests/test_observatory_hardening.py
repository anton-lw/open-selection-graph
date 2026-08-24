from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from observatory.adapters.openreview_api import (
    OpenReviewBatchedForumNotesConnector,
    OpenReviewDomainEditsConnector,
)
from observatory.audit import scan_secrets
from observatory.connectors.base import (
    Connector,
    ConnectorContext,
    CoverageEvidence,
    FetchBatch,
    NormalizedRecord,
    RawItem,
    SourceEstimate,
)
from observatory.connectors.delta import coverage_grade_diff, manifest_diff
from observatory.connectors.formats import safe_parse
from observatory.connectors.http import NetworkPolicyError, PoliteSession, RatePolicy
from observatory.connectors.runner import (
    RunOptions,
    _deduplicate_table_rows,
    run_connector,
)
from observatory.f1000_family import (
    _rows as f1000_report_rows,
)
from observatory.f1000_family import (
    build_f1000_family_report,
)
from observatory.f1000_shards import migrate_f1000_prefix_checkpoint
from observatory.ids import content_hash, stable_id
from observatory.integrity import trace_field
from observatory.license_matrix import object_license_matrix, validate_release_bundle
from observatory.mappings import MappingRegistry
from observatory.r5 import build_publication_readiness
from observatory.registry import source_cards
from observatory.storage import NormalizedLake, RawStore
from observatory.temporal import as_of_sql

STAMP = "2026-08-10T00:00:00+00:00"


def test_data_card_catalogues_every_registered_source() -> None:
    data_card = Path("docs/observatory/DATA_CARD.md").read_text()
    source_block = data_card.split("<!-- source-card-order", 1)[1].split("-->", 1)[0]
    documented_ids = [line.strip() for line in source_block.splitlines() if line.strip()]
    cards = source_cards()
    registered_ids = [card.source_id for card in cards]

    assert len(documented_ids) == len(set(documented_ids))
    assert set(documented_ids) == set(registered_ids)

    catalogue = data_card.split("<details>", 1)[1].split("</details>", 1)[0]
    source_rows = []
    for line in catalogue.splitlines():
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if not line.strip().startswith("|") or len(cells) != 3:
            continue
        if cells[0] == "Source" or all(set(cell) <= set("-:") for cell in cells):
            continue
        source_rows.append(cells)

    assert len(source_rows) == len(registered_ids)

    expected_states = {
        "included": "Released",
        "pointer_only": "Pointer",
        "quarantined": "Catalogued",
    }
    cards_by_id = {card.source_id: card for card in cards}
    for source_id, row in zip(documented_ids, source_rows, strict=True):
        assert row[2] == expected_states[cards_by_id[source_id].status]


def test_data_card_quantitative_claims_match_the_frozen_ledger() -> None:
    data_card = Path("docs/observatory/DATA_CARD.md").read_text()
    ledger = json.loads(
        Path("results/observatory/r5/quantitative_claim_ledger.json").read_text()
    )
    values = {row["claim_id"]: row["value"] for row in ledger["claims"]}

    expected_phrases = {
        f"{values['observability_census_cycles']:,} cycle records",
        f"{values['publication_gate_cycles']:,} cycles",
        f"{values['verified_openreview_candidates']:,} candidates and "
        f"{values['verified_openreview_reviews']:,} reviews",
        f"{values['funding_panel_application_events']:,} UKRI events and "
        f"{values['snsf_individual_vote_cells']:,} SNSF vote cells",
        f"{values['patent_pilot_applications']:,} application trajectories",
        f"{values['hupd_public_applications']:,} applications",
        f"{values['panorama_exact_hupd_matches']:,} cases match HUPD exactly",
        f"{values['panorama_outside_hupd_year_boundary']:,} fall outside HUPD's "
        "filing-year boundary",
        f"{values['panorama_within_hupd_year_nonmatches']:,} fall within the boundary "
        "but are absent from the frozen HUPD population",
        f"{values['semantic_ruler_shared_rows']:,} shared time-valid document versions",
        f"{values['admissible_rate_cycles']:,} Grade A or B process cycles",
        f"including {values['populated_admissible_cycles']:,} with an observed "
        "candidate population",
        "Sixty-one cycles support observed selection rates",
        f"{values['review_rate_cycles']:,} support review-incidence measures",
        f"all {values['verified_openreview_cycles']:,} audited cycles",
        f"checks {values['release_parquet_tables']:,} Parquet tables",
    }

    assert values["selection_rate_cycles"] == 61
    assert all(phrase in data_card for phrase in expected_phrases)


def test_quantitative_claim_macros_match_the_frozen_ledger() -> None:
    ledger = json.loads(
        Path("results/observatory/r5/quantitative_claim_ledger.json").read_text()
    )
    paper = Path("docs/observatory/DATA_METHODS_PAPER.tex").read_text()

    assert ledger["passes_quantitative_reproduction"]
    assert ledger["generated_macro_values_match_ledger"]
    assert not ledger["macro_value_mismatches"]
    assert not ledger["paper_claims_without_ledger_row"]
    assert not ledger["comment_only_registration_present"]
    assert "% QCL:" not in paper


def test_publication_readiness_recognises_verified_private_staging(
    tmp_path: Path,
) -> None:
    receipt = (
        tmp_path
        / "results"
        / "observatory"
        / "publication"
        / "HUGGINGFACE_PRIVATE_DEPOSIT_RECEIPT.json"
    )
    receipt.parent.mkdir(parents=True)
    receipt.write_text(
        json.dumps(
            {
                "visibility": "private",
                "passes_private_staging_deposit": True,
                "remote_checksum_verification": {"passes": True},
            }
        )
    )

    report = build_publication_readiness(tmp_path, tmp_path / "r5")

    assert report["private_staging_paths"] == 1
    assert report["paths"][1]["status"] == (
        "private_staging_verified_public_visibility_pending"
    )
    assert not report["passes"]


def test_publication_readiness_counts_each_verified_private_provider(
    tmp_path: Path,
) -> None:
    publication = tmp_path / "results" / "observatory" / "publication"
    publication.mkdir(parents=True)
    common = {
        "passes_private_staging_deposit": True,
        "remote_checksum_verification": {"passes": True},
    }
    (publication / "HUGGINGFACE_PRIVATE_DEPOSIT_RECEIPT.json").write_text(
        json.dumps({**common, "visibility": "private"})
    )
    (publication / "ZENODO_PRIVATE_DEPOSIT_RECEIPT.json").write_text(
        json.dumps(
            {
                **common,
                "visibility": "private_draft",
                "reserved_doi": "10.5281/zenodo.123456",
            }
        )
    )
    (publication / "GITHUB_PRIVATE_DEPOSIT_RECEIPT.json").write_text(
        json.dumps({**common, "visibility": "private"})
    )

    report = build_publication_readiness(tmp_path, tmp_path / "r5")

    assert report["private_staging_paths"] == 3
    assert all(
        row["status"] == "private_staging_verified_public_visibility_pending"
        for row in report["paths"]
    )
    assert report["paths"][1]["reserved_identifier"] == "10.5281/zenodo.123456"
    assert len(report["private_deposit_receipts"]) == 3
    assert not report["passes"]


def test_publication_readiness_accepts_two_verified_public_paths(
    tmp_path: Path,
) -> None:
    publication = tmp_path / "results" / "observatory" / "publication"
    publication.mkdir(parents=True)
    common = {
        "visibility": "public",
        "passes_private_staging_deposit": True,
        "passes_public_release_gate": True,
        "remote_checksum_verification": {"passes": True},
    }
    (publication / "GITHUB_PRIVATE_DEPOSIT_RECEIPT.json").write_text(
        json.dumps(
            {
                **common,
                "persistent_public_identifier": "https://github.com/example/osg",
            }
        )
    )
    (publication / "HUGGINGFACE_PRIVATE_DEPOSIT_RECEIPT.json").write_text(
        json.dumps(
            {
                **common,
                "persistent_public_identifier": "https://huggingface.co/datasets/example/osg",
            }
        )
    )

    report = build_publication_readiness(tmp_path, tmp_path / "r5")

    assert report["passes"]
    assert report["independent_live_paths"] == 2
    assert report["public_release_paths"] == 2
    assert report["private_staging_paths"] == 0
    assert all(row["status"] == "public_release_verified" for row in report["paths"])


def test_secret_scan_allows_only_an_explicitly_released_contact(tmp_path: Path) -> None:
    metadata = tmp_path / "publication.yaml"
    metadata.write_text(
        "email: author@institute.org\n"
        "backup_email: private@institute.org\n"
        'token_secret: "credential-shaped-value"\n'
    )
    findings = scan_secrets(
        [metadata], released_contacts={"author@institute.org"}
    )
    assert [finding["line"] for finding in findings] == [2, 3]


def test_f1000_report_separates_exact_acquisition_from_normalization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    platforms = ("f1000research", "wellcome", "gates", "nihr")
    gates = [{"gate_id": f"g-{platform}", "native_id": platform} for platform in platforms]
    cycles = [{"gate_cycle_id": f"cycle-{platform}", "gate_id": f"g-{platform}"} for platform in platforms]
    events = []
    versions = []
    for platform in platforms:
        count = 99 if platform == "wellcome" else 100
        for index in range(count):
            candidate_id = (
                f"{platform}-{max(index - 1, 0)}"
                if platform == "f1000research" and index == 1
                else f"{platform}-{index}"
            )
            version_number = (
                2 if ((platform == "f1000research" and index == 1) or (platform == "wellcome" and index == 0)) else 1
            )
            versions.append(
                {
                    "candidate_id": candidate_id,
                    "version_number": version_number,
                }
            )
            events.append(
                {
                    "gate_cycle_id": f"cycle-{platform}",
                    "earliest_observed_stage": "publication_after_editorial_screen",
                    "final_observed_stage": "published",
                }
            )
    policies = [
        {
            "gate_id": f"g-{platform}",
            "native_id": f"policy-{platform}",
            "policy_url": "https://example.org/policy",
            "rubric_json": "{}",
            "stage_rules_json": '{"not_approved_is_rejection": false}',
        }
        for platform in platforms
    ]
    tables = {
        "gate": gates,
        "gate_cycle": cycles,
        "candidate_version": versions,
        "candidate_gate_event": events,
        "evaluation": [
            {"evaluation_type": "referee-report", "criterion_normalized": None},
            {"evaluation_type": "response", "criterion_normalized": None},
        ],
        "lineage_edge": [{"native_id": "observed-lineage"}, {"native_id": "unresolved-predecessor"}],
        "policy_version": policies,
        "decision_event": [],
        "content_artifact": [],
    }
    monkeypatch.setattr(
        "observatory.f1000_family._rows",
        lambda _root, table, **_kwargs: tables[table],
    )
    census = {platform: {"included": True, "expected_version_count": 100} for platform in platforms}
    report = build_f1000_family_report(
        tmp_path,
        query_hash="union",
        provider_expected_objects=400,
        found_count=400,
        platform_census=census,
        acquired_by_platform={platform: 100 for platform in platforms},
    )
    assert report["completed_acquisition_census"]
    assert report["normalized_version_count"] == 399
    assert report["unresolved_after_acquisition"] == 1
    assert report["version_gap_slot_count"] == 1
    assert report["leading_version_gaps_only"]
    assert report["platform_reconciliation"]["wellcome"]["normalization_ratio"] == 0.99
    assert report["passes"]


def _response(status: int, payload: bytes = b"{}") -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response.url = "https://example.org/data"
    response._content = payload
    response.headers["Content-Type"] = "application/json"
    return response


def test_openreview_domain_connector_can_raise_politeness_floor(tmp_path: Path) -> None:
    connector = OpenReviewDomainEditsConnector(
        page_size=100,
        min_interval_seconds=10,
    )
    context = ConnectorContext(
        workspace=tmp_path,
        fixture_dir=tmp_path,
        cache_dir=tmp_path / "cache",
    )
    session, _ = connector._session(context, "v2")
    assert session.policy.min_interval_seconds == 10


def test_openreview_domain_connector_selects_explicit_disjoint_indices() -> None:
    connector = OpenReviewDomainEditsConnector()
    rows = [{"venue_id": str(index)} for index in range(5)]
    context = ConnectorContext(
        workspace=Path("."),
        fixture_dir=Path("."),
        cache_dir=Path("."),
        parameters={"domain_indices": [4, 1, 4]},
    )
    assert connector._selected_rows(context, rows) == [rows[1], rows[4]]
    context.parameters["domain_indices"] = [5]
    with pytest.raises(ValueError, match="out-of-range"):
        connector._selected_rows(context, rows)


def test_openreview_batched_forum_notes_connector_exhausts_exact_union(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = tmp_path / "forums.json"
    manifest.write_text(
        json.dumps(
            {
                "forums": [
                    {"api_version": "v2", "forum": "forum-a", "venue_id": "Venue/A"},
                    {"api_version": "v2", "forum": "forum-b", "venue_id": "Venue/B"},
                ]
            }
        )
    )
    connector = OpenReviewBatchedForumNotesConnector(
        page_size=1,
        forums_per_query=2,
    )
    connector._sessions["v2"] = (None, True)  # type: ignore[assignment]
    responses = iter(
        [
            {
                "count": 2,
                "notes": [
                    {
                        "id": "note-a",
                        "forum": "forum-a",
                        "invitations": ["Venue/A/-/Submission"],
                        "content": {},
                    }
                ],
            },
            {
                "notes": [
                    {
                        "id": "note-b",
                        "forum": "forum-b",
                        "invitations": ["Venue/B/-/Official_Review"],
                        "content": {},
                    }
                ],
            },
        ]
    )
    monkeypatch.setattr(connector, "_get_json", lambda *args, **kwargs: next(responses))
    context = ConnectorContext(
        workspace=tmp_path,
        fixture_dir=tmp_path,
        cache_dir=tmp_path / "cache",
        parameters={
            "forum_files": [str(manifest)],
            "forum_start": 0,
            "forum_stop": 2,
        },
    )

    batches = list(connector.fetch(context))

    assert len(batches) == 2 and batches[-1].done
    payloads = [json.loads(batch.items[0].payload) for batch in batches]
    assert [payload["provider_forum_batch_note_count"] for payload in payloads] == [2, 2]
    assert {row["forum"] for row in payloads[0]["forum_batch"]} == {
        "forum-a",
        "forum-b",
    }
    assert [payload["items"][0]["payload"]["id"] for payload in payloads] == [
        "note-a",
        "note-b",
    ]


def test_http_retries_429_and_enforces_persistent_daily_ceiling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    policy = RatePolicy(
        min_interval_seconds=0,
        max_retries=2,
        max_backoff_seconds=0,
        daily_request_ceiling=2,
    )
    session = PoliteSession(cache_dir=tmp_path, allowed_hosts={"api.crossref.org"}, policy=policy)
    responses = iter((_response(429), _response(200, b'{"ok":true}')))
    monkeypatch.setattr(session.session, "get", lambda *args, **kwargs: next(responses))
    monkeypatch.setattr("observatory.connectors.http.time.sleep", lambda _: None)
    assert session.get("https://api.crossref.org/data", use_cache=False).json() == {"ok": True}
    with pytest.raises(NetworkPolicyError, match="daily request ceiling"):
        session.get("https://api.crossref.org/data", use_cache=False)


def test_http_can_return_declared_censoring_status_without_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session = PoliteSession(
        cache_dir=tmp_path,
        allowed_hosts={"api.crossref.org"},
        policy=RatePolicy(min_interval_seconds=0, max_retries=3),
    )
    calls = []

    def respond(*args, **kwargs):
        calls.append((args, kwargs))
        return _response(403, b"public page unavailable from this egress")

    monkeypatch.setattr(session.session, "get", respond)
    response = session.get("https://api.crossref.org/data", use_cache=False, accepted_statuses={403})
    assert response.status_code == 403
    assert len(calls) == 1


def test_http_honors_absolute_provider_rate_limit_reset(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    session = PoliteSession(
        cache_dir=tmp_path,
        allowed_hosts={"api.crossref.org"},
        policy=RatePolicy(max_backoff_seconds=3_700),
    )
    response = _response(429)
    response.headers["X-RateLimit-Reset"] = "5000"
    monkeypatch.setattr("observatory.connectors.http.time.time", lambda: 1400)
    assert session._retry_wait(response, 0) == 3600


def test_safe_parse_quarantines_malformed_and_blocks_xxe(tmp_path: Path) -> None:
    malformed = safe_parse("jats", b"<article><broken>", quarantine_root=tmp_path, native_id="x")
    assert not malformed.success and Path(malformed.quarantine_path or "").exists()
    xxe = safe_parse(
        "jats",
        b'<!DOCTYPE x [<!ENTITY leak SYSTEM "file:///etc/passwd">]><article>&leak;</article>',
        quarantine_root=tmp_path,
        native_id="xxe",
    )
    assert not xxe.success or "root:" not in json.dumps(xxe.value)


def test_delta_native_identity_modification_removal_and_grade_change(tmp_path: Path) -> None:
    before, after = tmp_path / "before.jsonl", tmp_path / "after.jsonl"
    before.write_text(
        "\n".join(
            (
                json.dumps({"source_id": "s", "native_id": "a", "byte_hash": "1", "source_object_id": "o1"}),
                json.dumps({"source_id": "s", "native_id": "gone", "byte_hash": "2", "source_object_id": "o2"}),
            )
        )
        + "\n"
    )
    after.write_text(
        "\n".join(
            (
                json.dumps({"source_id": "s", "native_id": "a", "byte_hash": "3", "source_object_id": "o3"}),
                json.dumps({"source_id": "s", "native_id": "new", "byte_hash": "4", "source_object_id": "o4"}),
            )
        )
        + "\n"
    )
    diff = manifest_diff(before, after)
    assert diff["modified"] == ["s|a"]
    assert diff["removed"] == ["s|gone"] and diff["added"] == ["s|new"]
    grades = coverage_grade_diff(
        [{"gate_cycle_id": "c", "object_type": "submission", "observability_grade": "U"}],
        [{"gate_cycle_id": "c", "object_type": "submission", "observability_grade": "B"}],
    )
    assert grades[0]["before"] == "U" and grades[0]["after"] == "B"


def test_temporal_truth_and_source_tombstone() -> None:
    con = duckdb.connect()
    con.execute("""
      CREATE TABLE source_object(
        source_id VARCHAR, native_id VARCHAR, retrieved_at TIMESTAMPTZ,
        deleted_at TIMESTAMPTZ, value VARCHAR
      )
    """)
    con.execute("""
      INSERT INTO source_object VALUES
      ('s','a','2026-01-01',NULL,'old'),
      ('s','a','2026-02-01','2026-03-01','new')
    """)
    feb = con.execute(as_of_sql("source_object", "2026-02-15T00:00:00+00:00")).fetchall()
    apr = con.execute(as_of_sql("source_object", "2026-04-01T00:00:00+00:00")).fetchall()
    assert feb[0][-1] == "new" and apr == []


def test_mapping_unmapped_is_explicit() -> None:
    root = Path(__file__).resolve().parents[1]
    registry = MappingRegistry(root / "configs" / "observatory" / "mappings.yaml")
    result = registry.map("openreview", "decision", "definitely-new-native-label")
    assert result.native_value == "definitely-new-native-label"
    assert not result.mapped and result.normalized_value is None


def test_object_licence_matrix_cannot_trust_unrecognized_open_claim() -> None:
    matrix = object_license_matrix(
        [
            {
                "source_object_id": "o",
                "source_id": "s",
                "object_type": "review",
                "licence": "unknown-private-terms",
                "release_class": "redistribute",
            }
        ]
    )
    assert matrix[0]["release_class"] == "pointer_hash"
    with pytest.raises(ValueError, match="not redistributable"):
        validate_release_bundle(matrix, include_content=True)


def test_field_trace_returns_raw_and_transform_metadata() -> None:
    con = duckdb.connect()
    con.execute("""
      CREATE TABLE field_provenance(
        field_provenance_id VARCHAR, table_name VARCHAR, record_id VARCHAR,
        field_name VARCHAR, source_object_id VARCHAR, provenance_event_id VARCHAR,
        source_selector VARCHAR, confidence DOUBLE, override_reason VARCHAR,
        observed_at TIMESTAMPTZ
      );
      CREATE TABLE source_object(
        source_object_id VARCHAR, byte_hash VARCHAR, raw_pointer VARCHAR, source_url VARCHAR
      );
      CREATE TABLE provenance_event(
        provenance_event_id VARCHAR, parser_name VARCHAR, parser_version VARCHAR,
        code_hash VARCHAR, input_hash VARCHAR, output_hash VARCHAR
      );
      INSERT INTO field_provenance VALUES
        ('f','candidate','c','canonical_title','o','p','$.title',1,NULL,'2026-01-01');
      INSERT INTO source_object VALUES ('o','abc','/raw/abc.gz','https://example.org/a');
      INSERT INTO provenance_event VALUES ('p','Parser','1','code','abc','out');
    """)
    rows = trace_field(con, "candidate", "c", "canonical_title")
    assert rows[0]["raw_pointer"] == "/raw/abc.gz" and rows[0]["parser_name"] == "Parser"


class InterruptibleConnector(Connector):
    source_id = "interruptible"

    def __init__(self, interrupt: bool = False):
        self.interrupt = interrupt

    def discover(self, context):
        return [{"fixture": True}]

    def count(self, context):
        return SourceEstimate(self.source_id, 2, method="fixture", confidence="exact")

    def fetch(self, context, *, cursor=None, limit=None):
        start = int(cursor or 0)
        if start == 0:
            yield FetchBatch((RawItem("1", "fixture", "one"),), "1", False, "page:0", 2)
            if self.interrupt:
                raise RuntimeError("forced interruption")
            start = 1
        if start == 1:
            yield FetchBatch((RawItem("2", "fixture", "two"),), None, True, "page:1", 2)

    def normalize(self, item, *, source_object_id, provenance_event_id):
        yield NormalizedRecord(
            "candidate",
            {
                "candidate_id": stable_id("candidate", self.source_id, item.native_id),
                "first_observed_at": STAMP,
                "domain": None,
                "candidate_type": "fixture",
                "canonical_title": str(item.payload),
                "status": "visible",
                "source_id": self.source_id,
                "source_object_id": source_object_id,
                "provenance_event_id": provenance_event_id,
                "observed_at": STAMP,
                "record_version": 1,
            },
        )

    def validate_fixture(self, context):
        return {"passes": True}

    def emit_coverage(self, context, *, found_count):
        yield CoverageEvidence("c", "candidate", "public", "D", 2, found_count, "fixture", "fixture")


def test_interruption_restart_does_not_skip_or_duplicate(tmp_path: Path) -> None:
    context = ConnectorContext(tmp_path, tmp_path / "fixtures", tmp_path / "cache")
    kwargs = {
        "context": context,
        "raw_store": RawStore(tmp_path / "raw"),
        "lake": NormalizedLake(tmp_path / "lake"),
        "run_root": tmp_path / "runs",
        "options": RunOptions(limit=30_000),
    }
    with pytest.raises(RuntimeError, match="forced interruption"):
        run_connector(InterruptibleConnector(interrupt=True), **kwargs)
    result = run_connector(InterruptibleConnector(), **kwargs)
    assert result["found_count"] == 2 and result["tables"]["candidate"] == 2
    assert result["streaming_compile"] is True
    assert result["coverage"][0]["found_count"] == 2
    table = NormalizedLake(tmp_path / "lake").read("candidate")
    assert sorted(table.column("canonical_title").to_pylist()) == ["one", "two"]
    assert NormalizedLake(tmp_path / "lake").verify()["passes"]


def test_f1000_checkpoint_migration_proves_platform_prefix(tmp_path: Path) -> None:
    f1000_urls = [
        "https://f1000research.com/articles/1-1/v1/xml",
        "https://f1000research.com/articles/1-2/v1/xml",
        "https://f1000research.com/articles/1-3/v1/xml",
    ]
    snapshot = {
        "schema": "observatory.f1000-enumeration-snapshot/1",
        "connector_version": "6",
        "urls": sorted(
            [
                *f1000_urls,
                "https://wellcomeopenresearch.org/articles/1-1/v1/xml",
            ]
        ),
        "platform_access": {},
    }
    snapshot["snapshot_hash"] = content_hash(json.dumps(snapshot, sort_keys=True))
    snapshot_path = tmp_path / "enumeration.json"
    snapshot_path.write_text(json.dumps(snapshot))

    old_hash = "a" * 64
    checkpoints = tmp_path / "checkpoints"
    checkpoints.mkdir()
    (checkpoints / f"f1000_process-{old_hash[:12]}.json").write_text(
        json.dumps(
            {
                "source_id": "f1000_process",
                "query_hash": old_hash,
                "cursor": "2",
                "batch_count": 2,
                "found_count": 2,
                "last_native_id": f1000_urls[1],
                "complete": False,
                "fetch_complete": False,
            }
        )
    )
    old_stage = tmp_path / "staging" / "f1000_process" / old_hash[:16]
    old_stage.mkdir(parents=True)
    for index in (1, 2):
        (old_stage / f"batch-{index:08d}.json.gz").write_bytes(b"fixture")
    parameters = {
        "enumeration_snapshot": str(snapshot_path),
        "platform_ids": ["f1000research"],
        "recovery_manifest": "/opt/recovery.json",
    }
    receipt = migrate_f1000_prefix_checkpoint(
        tmp_path,
        enumeration_snapshot=snapshot_path,
        old_query_hash=old_hash,
        new_parameters=parameters,
        platform_id="f1000research",
    )
    assert receipt["passes"] and receipt["cursor"] == 2
    assert receipt["selected_platform_expected_count"] == 3
    target = tmp_path / "checkpoints" / f"f1000_process-{receipt['new_query_hash'][:12]}.json"
    assert json.loads(target.read_text())["query_hash"] == receipt["new_query_hash"]
    assert (
        len(list((tmp_path / "staging" / "f1000_process" / receipt["new_query_hash"][:16]).glob("batch-*.json.gz")))
        == 2
    )
    assert old_stage.exists()


def test_f1000_union_report_reads_every_explicit_shard_hash(tmp_path: Path) -> None:
    partition = tmp_path / "candidate_version" / "source_id=f1000_process"
    partition.mkdir(parents=True)
    shard_hashes = ("a" * 64, "b" * 64)
    for index, query_hash in enumerate((*shard_hashes, "c" * 64), start=1):
        pq.write_table(
            pa.Table.from_pylist([{"native_id": f"version-{index}"}]),
            partition / f"run-{query_hash[:16]}-part-00000.parquet",
        )

    rows = f1000_report_rows(
        tmp_path,
        "candidate_version",
        source_id="f1000_process",
        query_hashes=shard_hashes,
    )

    assert [row["native_id"] for row in rows] == ["version-1", "version-2"]


def test_candidate_dedup_uses_earliest_version_observation() -> None:
    shared = {
        "candidate_id": "candidate-1",
        "source_id": "fixture",
        "domain": None,
        "candidate_type": "published_article",
        "canonical_title": "One article",
        "status": "published_before_peer_review",
        "record_version": 1,
    }
    rows = [
        {
            **shared,
            "first_observed_at": "2018-12-03T00:00:00+00:00",
            "observed_at": "2018-12-03T00:00:00+00:00",
            "source_object_id": "version-2",
            "provenance_event_id": "event-2",
        },
        {
            **shared,
            "first_observed_at": "2018-06-04T00:00:00+00:00",
            "observed_at": "2018-06-04T00:00:00+00:00",
            "source_object_id": "version-1",
            "provenance_event_id": "event-1",
        },
    ]

    deduplicated = _deduplicate_table_rows("candidate", rows)

    assert len(deduplicated) == 1
    assert deduplicated[0]["first_observed_at"] == "2018-06-04T00:00:00+00:00"
    assert deduplicated[0]["source_object_id"] == "version-1"


def test_candidate_dedup_merges_mutable_provider_snapshots() -> None:
    shared = {
        "candidate_id": "candidate-1",
        "source_id": "openreview_api",
        "domain": "machine learning",
        "candidate_type": "manuscript",
        "record_version": 1,
    }
    rows = [
        {
            **shared,
            "first_observed_at": "2026-01-01T00:00:00+00:00",
            "canonical_title": "Initial title",
            "status": "submitted",
            "observed_at": "2026-01-01T00:00:00+00:00",
            "source_object_id": "initial-edit",
            "provenance_event_id": "initial-event",
        },
        {
            **shared,
            "first_observed_at": "2026-02-01T00:00:00+00:00",
            "canonical_title": "Revised title",
            "status": "observed",
            "observed_at": "2026-02-01T00:00:00+00:00",
            "source_object_id": "revision-edit",
            "provenance_event_id": "revision-event",
        },
    ]

    deduplicated = _deduplicate_table_rows("candidate", rows)

    assert deduplicated == [
        {
            **rows[1],
            "first_observed_at": "2026-01-01T00:00:00+00:00",
        }
    ]


def test_content_artifact_dedup_uses_earliest_embedded_version() -> None:
    shared = {
        "content_artifact_id": "artifact-1",
        "source_id": "fixture",
        "object_type": "reviewer-report",
        "media_type": "text/xml",
        "byte_hash": None,
        "normalized_text_hash": "same-text",
        "licence": "CC-BY-4.0",
        "release_class": "redistribute",
        "size_bytes": 570,
        "language": "en",
        "parser_version": "6",
        "record_version": 1,
    }
    rows = [
        {
            **shared,
            "source_url": "https://example.test/v3/xml",
            "local_pointer": "sub-article[3]",
            "observed_at": "2018-12-03T00:00:00+00:00",
            "source_object_id": "version-3",
            "provenance_event_id": "event-3",
        },
        {
            **shared,
            "source_url": "https://example.test/v2/xml",
            "local_pointer": "sub-article[2]",
            "observed_at": "2018-07-11T00:00:00+00:00",
            "source_object_id": "version-2",
            "provenance_event_id": "event-2",
        },
    ]

    deduplicated = _deduplicate_table_rows("content_artifact", rows)

    assert len(deduplicated) == 1
    assert deduplicated[0]["source_url"].endswith("/v2/xml")


def test_evaluation_dedup_uses_earliest_embedding_wrapper() -> None:
    shared = {
        "evaluation_id": "evaluation-1",
        "source_id": "fixture",
        "candidate_version_id": "target-version-2",
        "gate_cycle_id": "cycle",
        "native_id": "review-doi",
        "evaluation_type": "reviewer-report",
        "reply_to_native_id": "target-version-2",
        "text_artifact_id": "artifact-1",
        "record_version": 1,
    }
    rows = [
        {
            **shared,
            "forum_native_id": "wrapper-version-3",
            "observed_at": "2018-12-03T00:00:00+00:00",
            "source_object_id": "version-3",
            "provenance_event_id": "event-3",
        },
        {
            **shared,
            "forum_native_id": "wrapper-version-2",
            "observed_at": "2018-07-11T00:00:00+00:00",
            "source_object_id": "version-2",
            "provenance_event_id": "event-2",
        },
    ]

    deduplicated = _deduplicate_table_rows("evaluation", rows)

    assert len(deduplicated) == 1
    assert deduplicated[0]["forum_native_id"] == "wrapper-version-2"
