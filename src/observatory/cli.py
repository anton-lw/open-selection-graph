"""Command line interface for schemas, audits, fixtures, and connector runs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from .adapters import (
    ArxivOAIConnector,
    CopernicusCrossrefPostedConnector,
    CopernicusOAIConnector,
    CrossrefPeerReviewConnector,
    CrossrefProviderConnector,
    ELifeProcessConnector,
    EuropePMCConnector,
    F1000ProcessConnector,
    OpenAlexSingletonConnector,
    OpenReviewAPINotesConnector,
    OpenReviewLocalConnector,
    OpenReviewSurfaceConnector,
    SciPostProcessConnector,
)
from .atlas import write_source_atlas
from .audit import audit_configuration, audit_no_paid_api_policy
from .cleanroom import rebuild_public_fixtures
from .connectors import ConnectorContext, RunOptions, run_connector
from .construct_atlas import (
    build_construct_and_reliability,
    build_novelty_evaluation_atlas,
    build_recombinatorial_novelty,
)
from .construct_model import build_construct_model
from .content_claims import build_content_claim_features
from .evaluation_atlas import build_evaluation_products
from .external_reproduction import reproduce_p2_semantic_fixture
from .funding import build_funding_products
from .governance import execute_claim_ledger
from .institutional import build_institutional_products
from .limitations import build_limitation_products, export_modern_review_benchmark_input
from .lineage import build_lineage_products, export_modern_lineage_input
from .migrations import build_initial_release_diff
from .modern_novelty import export_modern_ruler_input
from .notebooks import build_and_execute_notebooks
from .outcomes import build_afterlife_products
from .patents import build_patent_pilot
from .policy_audit import build_policy_extraction_audit
from .policy_history import build_policy_history
from .publication import build_publication_bundles
from .quality import audit_stage_outcomes, audit_temporal_leakage
from .r2_assurance import build_r2_assurance
from .r3_validation import (
    build_adversarial_referee_suite,
    build_analysis_contracts,
    build_descriptive_atlas,
    build_determinism_audit,
)
from .r5 import build_r5_products
from .registry import CONFIG, load_yaml, source_cards, validate_all
from .release_engineering import build_release_package, release_version_metadata
from .release_validation import write_release_validation
from .schema import write_schema_artifacts
from .semantic_novelty import build_semantic_novelty
from .storage import NormalizedLake, ObservatoryCatalog, RawStore
from .storage_guard import write_recoverability_manifest
from .strain import build_strain_and_reform_products
from .ticket_evidence import write_ticket_evidence_audit
from .views import install_views

ROOT = Path(__file__).resolve().parents[2]


PROVIDERS = {
    "elife": dict(
        provider="eLife",
        crossref_filter="prefix:10.7554",
        architecture="publish_review_curate",
        earliest_stage="visible reviewed-preprint/article",
        grade="B",
    ),
    "f1000research": dict(
        provider="F1000Research",
        crossref_filter="prefix:10.12688",
        architecture="post_publication_review",
        earliest_stage="published after editorial screen",
        grade="B",
    ),
    "scipost": dict(
        provider="SciPost",
        crossref_filter="prefix:10.21468",
        architecture="access_public_discussion",
        earliest_stage="public submission/article deposit",
        grade="U",
    ),
    "peerj": dict(
        provider="PeerJ",
        crossref_filter="prefix:10.7717",
        architecture="rolling_threshold",
        earliest_stage="published/selected work",
        grade="C",
    ),
    "plos_review_history": dict(
        provider="PLOS",
        crossref_filter="prefix:10.1371",
        architecture="rolling_threshold",
        earliest_stage="accepted opt-in history / published work",
        grade="C",
    ),
    "embo_transparent_review": dict(
        provider="EMBO Press",
        crossref_filter="prefix:10.15252",
        architecture="rolling_threshold",
        earliest_stage="selected published process file",
        grade="C",
    ),
    "royal_society_review": dict(
        provider="The Royal Society",
        crossref_filter="prefix:10.1098",
        architecture="rolling_threshold",
        earliest_stage="selected published transparent-review history",
        grade="C",
    ),
    "bmc_open_review": dict(
        provider="BMC/Springer Nature",
        crossref_filter="prefix:10.1186",
        architecture="rolling_threshold",
        earliest_stage="published open-review journal article",
        grade="C",
    ),
    "qeios": dict(
        provider="Qeios",
        crossref_filter="prefix:10.32388",
        architecture="post_publication_review",
        earliest_stage="public preprint",
        grade="U",
    ),
}


def _adapter(name: str, args):
    if name == "openreview-local":
        return OpenReviewLocalConnector()
    if name == "openreview-api":
        return OpenReviewAPINotesConnector(page_size=args.page_size)
    if name == "openreview-surface":
        return OpenReviewSurfaceConnector(batch_size=args.page_size)
    if name == "crossref-peer-review":
        return CrossrefPeerReviewConnector(rows=args.page_size)
    if name == "copernicus-oai":
        return CopernicusOAIConnector(metadata_prefix=args.metadata_prefix)
    if name == "copernicus-crossref":
        return CopernicusCrossrefPostedConnector(rows=args.page_size)
    if name == "arxiv-oai":
        return ArxivOAIConnector(metadata_prefix=args.metadata_prefix)
    if name == "europe-pmc":
        return EuropePMCConnector(query=args.query or "OPEN_ACCESS:Y", page_size=args.page_size)
    if name == "elife-process":
        return ELifeProcessConnector(page_size=args.page_size)
    if name == "f1000-process":
        return F1000ProcessConnector(page_size=min(args.page_size, 100))
    if name == "scipost-process":
        return SciPostProcessConnector(page_size=min(args.page_size, 50))
    if name == "openalex-singleton":
        return OpenAlexSingletonConnector(args.identifier)
    if name.startswith("provider:"):
        source_id = name.split(":", 1)[1]
        if source_id not in PROVIDERS:
            raise SystemExit(f"unknown provider adapter: {source_id}")
        return CrossrefProviderConnector(source_id=source_id, **PROVIDERS[source_id], rows=args.page_size)
    raise SystemExit(f"unknown adapter: {name}")


def _context(args) -> ConnectorContext:
    parameters = {}
    if args.query:
        parameters["query"] = args.query
    if args.set_spec:
        parameters["set"] = args.set_spec
    if args.include_fulltext:
        parameters["include_fulltext"] = True
    if args.file:
        parameters["files"] = args.file
    if args.identifier:
        parameters["identifiers"] = args.identifier
    return ConnectorContext(
        workspace=ROOT,
        fixture_dir=ROOT / "tests" / "fixtures" / "observatory",
        cache_dir=ROOT / "data" / "observatory" / "cache",
        no_text=args.no_text,
        since=args.since,
        until=args.until,
        parameters=parameters,
    )


def _connector_parser(subparsers) -> None:
    p = subparsers.add_parser("connector", help="dry-run or execute a source connector")
    p.add_argument("adapter", help="adapter name or provider:<source_id>")
    p.add_argument("--limit", type=int)
    p.add_argument("--page-size", type=int, default=200)
    p.add_argument("--metadata-prefix", default="oai_dc")
    p.add_argument("--query")
    p.add_argument("--set-spec")
    p.add_argument("--since")
    p.add_argument("--until")
    p.add_argument("--identifier", action="append", default=[])
    p.add_argument("--file", action="append", default=[])
    p.add_argument("--include-fulltext", action="store_true")
    p.add_argument("--no-text", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--fixture", action="store_true")
    p.add_argument("--restart", action="store_true")
    p.add_argument("--estimate-storage", action="store_true")
    p.add_argument("--estimate-modal-cost", action="store_true")


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Open Selection Graph (OSG)")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init", help="generate schemas and validate registries")
    sub.add_parser("validate", help="audit configuration and secret/cost constraints")
    sub.add_parser("policy", help="enforce the deny-by-default no-paid-API policy")
    sub.add_parser("sources", help="list source cards")
    sub.add_parser("atlas", help="build source/coverage atlas from run manifests")
    sub.add_parser("claims", help="execute the public claim ledger against analysis views")
    sub.add_parser("tickets", help="audit scoped ticketbook acceptance evidence")
    sub.add_parser("recoverability", help="write exact raw-source refetch manifest")
    release_parser = sub.add_parser("release-validate", help="write the fail-closed full release report")
    release_parser.add_argument("--release-id", default="open-selection-graph-2.0.0")
    sub.add_parser("r1-build", help="build all standalone R1 dataset products and audits")
    r2_parser = sub.add_parser("r2-build", help="build publication-gate R2 products and audits")
    r2_parser.add_argument("--refresh-public-metadata", action="store_true")
    sub.add_parser("r3-build", help="build lineage, construct, strain, and trajectory products")
    sub.add_parser("r4-build", help="build bounded public funding and patent extensions")
    sub.add_parser("r5-build", help="build governance, operations, explorer, benchmarks, and analysis products")
    sub.add_parser("validity-build", help="build partial-identification, transport, pointer, and human-benchmark products")
    sub.add_parser("modern-ruler-export", help="export the temporary input for the pinned Modal Qwen3 ruler")
    sub.add_parser(
        "modern-review-export",
        help="export the temporary input for the pinned Modal Qwen3 human benchmark",
    )
    sub.add_parser(
        "modern-lineage-export",
        help="export temporary titles for pinned Modal Qwen3 lineage-neighbour retrieval",
    )
    sub.add_parser("publication-bundles", help="build deterministic licence-separated external deposit bundles")
    package_parser = sub.add_parser("release-build", help="build a validated immutable release package")
    package_parser.add_argument("destination")
    package_parser.add_argument(
        "--validation",
        default="results/observatory/release_validation.json",
    )
    sub.add_parser("version-audit", help="validate and print the semantic version registry")
    _connector_parser(sub)
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.command == "init":
        written = write_schema_artifacts(ROOT / "schemas" / "observatory")
        for path in (
            ROOT / "data" / "observatory" / "raw",
            ROOT / "data" / "observatory" / "normalized",
            ROOT / "data" / "observatory" / "derived",
            ROOT / "results" / "observatory" / "runs",
        ):
            path.mkdir(parents=True, exist_ok=True)
        print(json.dumps({"registries": validate_all(), "schema_artifacts": [str(p) for p in written]}, indent=2))
        return 0
    if args.command == "validate":
        result = audit_configuration(ROOT)
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
        return 0 if result["passes"] else 1
    if args.command == "policy":
        result = audit_no_paid_api_policy(ROOT)
        output = ROOT / "results" / "observatory" / "no_paid_api_audit.json"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, indent=2, sort_keys=True, default=str) + "\n")
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
        return 0 if result["passes"] else 1
    if args.command == "sources":
        print(json.dumps([card.__dict__ for card in source_cards()], indent=2, default=list, sort_keys=True))
        return 0
    if args.command == "atlas":
        output = write_source_atlas(
            ROOT / "results" / "observatory",
            ROOT / "results" / "observatory" / "source_coverage_atlas.json",
        )
        print(output)
        return 0
    if args.command == "claims":
        connection = ObservatoryCatalog(ROOT / "data" / "observatory" / "normalized").connect()
        install_views(connection)
        claims = load_yaml(CONFIG / "claims.yaml").get("claims") or []
        report = execute_claim_ledger(connection, claims, dataset_version="observatory-r0")
        output = ROOT / "results" / "observatory" / "executed_claim_ledger.json"
        output.write_text(json.dumps(report, indent=2, sort_keys=True, default=str) + "\n")
        print(output)
        return 0 if report["passes"] else 1
    if args.command == "tickets":
        output = write_ticket_evidence_audit(ROOT, ROOT / "results" / "observatory" / "ticket_evidence_audit.json")
        report = json.loads(output.read_text())
        print(output)
        return 0 if report["passes_structure"] else 1
    if args.command == "recoverability":
        output = write_recoverability_manifest(
            ROOT / "data" / "observatory" / "raw",
            ROOT / "results" / "observatory" / "recoverability_manifest.json",
        )
        print(output)
        return 0
    if args.command == "release-validate":
        output = write_release_validation(
            ROOT,
            ROOT / "results" / "observatory" / "release_validation.json",
            release_id=args.release_id,
        )
        report = json.loads(output.read_text())
        print(output)
        return 0 if report["packaging_allowed"] else 1
    if args.command == "r1-build":
        lake = ROOT / "data" / "observatory" / "normalized"
        output = ROOT / "results" / "observatory" / "r1"
        reports = [
            build_institutional_products(lake, output),
            build_evaluation_products(lake, output),
            build_policy_extraction_audit(lake, output / "policy_extraction_audit.json"),
            build_semantic_novelty(lake, output),
            audit_stage_outcomes(lake, output / "stage_outcome_audit.json"),
            audit_temporal_leakage(output / "temporal_leakage_audit.json"),
            rebuild_public_fixtures(
                ROOT / "tests" / "fixtures" / "observatory",
                output / "cleanroom_fixture_rebuild.json",
            ),
            reproduce_p2_semantic_fixture(
                ROOT,
                output / "p2_semantic_reproduction_audit.json",
            ),
        ]
        summary = {
            "passes": all(report.get("passes") for report in reports),
            "reports": [report.get("schema") for report in reports],
        }
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0 if summary["passes"] else 1
    if args.command == "r2-build":
        lake = ROOT / "data" / "observatory" / "normalized"
        output = ROOT / "results" / "observatory" / "r2"
        reports = [
            build_policy_history(lake, output),
            build_construct_and_reliability(lake, output),
            build_recombinatorial_novelty(lake, output),
        ]
        reports.append(build_novelty_evaluation_atlas(output))
        reports.append(
            build_afterlife_products(
                lake,
                output,
                refresh_public_metadata=args.refresh_public_metadata,
            )
        )
        reports.append(build_r2_assurance(ROOT, lake, output))
        reports.append(build_and_execute_notebooks(ROOT, output))
        summary = {
            "schema": "observatory.r2-build/1",
            "passes": all(report.get("passes") for report in reports),
            "reports": [report.get("schema") for report in reports],
        }
        (output / "r2_build_report.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0 if summary["passes"] else 1
    if args.command == "r3-build":
        lake = ROOT / "data" / "observatory" / "normalized"
        output = ROOT / "results" / "observatory" / "r3"
        reports = [
            build_lineage_products(lake, output),
            build_strain_and_reform_products(lake, output),
            build_content_claim_features(lake, output),
            build_construct_model(ROOT, lake, output),
            build_descriptive_atlas(output),
            build_determinism_audit(output),
            build_adversarial_referee_suite(output),
            build_analysis_contracts(output),
        ]
        summary = {
            "schema": "observatory.r3-build/1",
            "passes": all(report.get("passes") for report in reports),
            "reports": [report.get("schema") for report in reports],
        }
        output.mkdir(parents=True, exist_ok=True)
        (output / "r3_build_report.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0 if summary["passes"] else 1
    if args.command == "r4-build":
        output = ROOT / "results" / "observatory" / "r4"
        reports = [
            build_funding_products(ROOT, output),
            build_patent_pilot(ROOT, output),
        ]
        summary = {
            "schema": "observatory.r4-build/1",
            "passes": all(report.get("passes") for report in reports),
            "reports": [report.get("schema") for report in reports],
        }
        (output / "r4_build_report.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0 if summary["passes"] else 1
    if args.command == "r5-build":
        output = ROOT / "results" / "observatory" / "r5"
        # The release diff is generated from the same frozen version registry
        # as the remaining R5 products.
        build_initial_release_diff(
            ROOT,
            output / "release_diff_0.1.0-r1_to_1.0.0.json",
        )
        summary = build_r5_products(ROOT, output)
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0 if summary["passes"] else 1
    if args.command == "validity-build":
        summary = build_limitation_products(
            ROOT,
            ROOT / "results" / "observatory" / "validity",
        )
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0 if summary["passes"] else 1
    if args.command == "modern-ruler-export":
        report = export_modern_ruler_input(
            ROOT / "data" / "observatory" / "normalized",
            ROOT / "results" / "observatory" / "staging" / "validity" / "semantic_ruler_input.parquet",
        )
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if report["passes"] else 1
    if args.command == "modern-review-export":
        report = export_modern_review_benchmark_input(
            ROOT,
            ROOT
            / "results"
            / "observatory"
            / "staging"
            / "validity"
            / "qwen3_review_benchmark_input.parquet",
        )
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    if args.command == "modern-lineage-export":
        report = export_modern_lineage_input(
            ROOT / "data" / "observatory" / "normalized",
            ROOT
            / "results"
            / "observatory"
            / "staging"
            / "validity"
            / "qwen3_lineage_input.parquet",
        )
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if report["passes"] else 1
    if args.command == "publication-bundles":
        result = build_publication_bundles(
            ROOT,
            ROOT / "results" / "observatory" / "publication",
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["passes"] else 1
    if args.command == "release-build":
        validation = ROOT / args.validation
        result = build_release_package(
            ROOT,
            Path(args.destination).resolve(),
            validation_report=validation,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["passes"] else 1
    if args.command == "version-audit":
        print(json.dumps(release_version_metadata(ROOT), indent=2, sort_keys=True))
        return 0
    if args.command == "connector":
        connector = _adapter(args.adapter, args)
        context = _context(args)
        result = run_connector(
            connector,
            context,
            raw_store=RawStore(ROOT / "data" / "observatory" / "raw"),
            lake=NormalizedLake(ROOT / "data" / "observatory" / "normalized"),
            run_root=ROOT / "results" / "observatory",
            options=RunOptions(
                limit=args.limit,
                fixture=args.fixture,
                dry_run=args.dry_run,
                no_text=args.no_text,
                restart=args.restart,
                estimate_storage=args.estimate_storage,
                estimate_modal_cost=args.estimate_modal_cost,
            ),
        )
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
        return 0
    return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
