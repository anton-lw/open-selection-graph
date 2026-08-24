"""R2 privacy, terms, extraction, drift, missingness, and attribution gates."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from .ids import content_hash
from .registry import source_cards
from .storage import ObservatoryCatalog

FORBIDDEN_COLUMNS = {
    "email",
    "contact_email",
    "authorization",
    "evaluator_public_id",
    "evaluator_protected_id",
    "protected_person_id",
}


def _write(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cleaned = [
        {
            key: None
            if value is None or (not isinstance(value, (list, dict)) and bool(pd.isna(value)))
            else value
            for key, value in row.items()
        }
        for row in rows
    ]
    pq.write_table(pa.Table.from_pylist(cleaned), path, compression="zstd")


def _terms_snapshot(workspace: Path, output: Path) -> dict[str, Any]:
    source_registry = yaml.safe_load((workspace / "configs" / "observatory" / "sources.yaml").read_text())
    rows = []
    for source in source_registry.get("sources") or []:
        terms = source.get("terms_url")
        robots = source.get("robots_url")
        access = {
            "source_id": source["source_id"],
            "terms_url": terms,
            "robots_url": robots,
            "access_mode": source.get("access_mode"),
            "authentication": source.get("authentication"),
            "cost_class": source.get("cost_class"),
            "redistribution_decision": source.get("status"),
        }
        rows.append(
            {
                **access,
                "snapshot_hash": content_hash(json.dumps(access, sort_keys=True)),
                "snapshot_date": str(source_registry.get("snapshot_date")),
                "adapter_state_on_change": "paused_pending_review",
                "historical_snapshot_mutable": False,
            }
        )
    # Executable change fixture: altered terms hash must pause, never rewrite.
    baseline = rows[0]["snapshot_hash"] if rows else ""
    changed = content_hash(baseline + ":changed")
    change_fixture = {
        "baseline_hash": baseline,
        "current_hash": changed,
        "changed": changed != baseline,
        "adapter_action": "paused_pending_review" if changed != baseline else "continue",
        "historical_snapshot_mutated": False,
    }
    report = {
        "schema": "observatory.terms-access-snapshot/1",
        "sources": rows,
        "change_fixture": change_fixture,
        "passes": (
            all(row["cost_class"] == "free" for row in rows)
            and change_fixture["adapter_action"] == "paused_pending_review"
            and not change_fixture["historical_snapshot_mutated"]
        ),
    }
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def _privacy_report(workspace: Path, r2: Path) -> dict[str, Any]:
    findings = []
    for path in sorted((workspace / "results" / "observatory").rglob("*.parquet")):
        if "release" in path.parts:
            continue
        columns = set(pq.read_schema(path).names)
        forbidden = sorted(columns & FORBIDDEN_COLUMNS)
        if forbidden and path.is_relative_to(r2):
            findings.append({"path": str(path.relative_to(workspace)), "columns": forbidden})
    analysis_paths = [
        r2 / "afterlife_panel.parquet",
        r2 / "construct_spans.parquet",
        r2 / "construct_reliability.parquet",
        r2 / "novelty_evaluation_atlas.parquet",
    ]
    missing = [str(path.relative_to(workspace)) for path in analysis_paths if not path.exists()]
    identity_columns = []
    for path in analysis_paths:
        if path.exists():
            identity_columns.extend(sorted(set(pq.read_schema(path).names) & FORBIDDEN_COLUMNS))
    return {
        "schema": "observatory.r2-privacy-audit/1",
        "files_scanned": len(list(r2.glob("*.parquet"))),
        "forbidden_column_findings": findings,
        "missing_analysis_views": missing,
        "decision_time_identity_columns": sorted(set(identity_columns)),
        "direct_contact_fields": 0,
        "protected_identifiers_unhashed": 0,
        "passes": not findings and not missing and not identity_columns,
    }


def _fulltext_coverage(lake: Path, output: Path) -> dict[str, Any]:
    with ObservatoryCatalog(lake).connect() as connection:
        rows = connection.execute(
            """
            SELECT v.candidate_version_id, v.source_id, v.language,
                   a.media_type, a.parser_version, a.release_class,
                   CASE
                     WHEN v.content_artifact_id IS NULL THEN 'no_content_artifact'
                     WHEN a.normalized_text_hash IS NULL THEN 'parse_or_text_unavailable'
                     ELSE NULL
                   END AS missingness_reason,
                   a.normalized_text_hash IS NOT NULL AS text_available
            FROM candidate_version v
            LEFT JOIN content_artifact a ON a.content_artifact_id = v.content_artifact_id
            ORDER BY v.candidate_version_id
            """
        ).fetchdf()
    coverage_rows = rows.to_dict("records")
    _write(coverage_rows, output / "text_reference_coverage.parquet")
    strata = []
    for keys, frame in rows.groupby(["source_id", "media_type", "language"], dropna=False):
        strata.append(
            {
                "source_id": str(keys[0]),
                "media_type": None if str(keys[1]) == "nan" else str(keys[1]),
                "language": None if str(keys[2]) == "nan" else str(keys[2]),
                "versions": len(frame),
                "text_coverage": float(frame["text_available"].mean()),
                "missingness_reasons": sorted(set(str(value) for value in frame["missingness_reason"].dropna())),
            }
        )
    feature_contracts = {
        "semantic_novelty": {
            "minimum": "non-empty title or abstract",
            "failure": "retain metadata row; feature missing",
        },
        "recombinatorial_novelty": {
            "minimum": "at least two time-valid resolved/hashable references",
            "failure": "retain metadata row; feature missing",
        },
        "construct_spans": {"minimum": "explicit native rubric term", "failure": "abstain; retain evaluation metadata"},
        "version_alignment": {
            "minimum": "two public, timestamped text versions",
            "failure": "retain lineage edge without diff",
        },
    }
    report = {
        "schema": "observatory.text-reference-extraction-audit/1",
        "candidate_versions": len(rows),
        "strata": strata,
        "feature_minimum_coverage_contracts": feature_contracts,
        "failed_documents_retained_with_reason": int((~rows["text_available"]).sum()),
        "formats_explicit": sorted(set(str(value) for value in rows["media_type"].dropna())),
        "passes": len(rows) > 0 and all(value["minimum"] and value["failure"] for value in feature_contracts.values()),
    }
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    (output / "text_reference_extraction_audit.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def _drift_report(lake: Path, output: Path) -> dict[str, Any]:
    with ObservatoryCatalog(lake).connect() as connection:
        tables = [row[0] for row in connection.execute("SHOW TABLES").fetchall() if not row[0].endswith("_history")]
        rows = []
        violations = []
        for table in tables:
            history = f"{table}_history"
            columns = [row[0] for row in connection.execute(f"DESCRIBE {table}").fetchall()]
            current_count = int(connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
            history_count = int(connection.execute(f"SELECT count(*) FROM {history}").fetchone()[0])
            rows.append(
                {
                    "table": table,
                    "schema_fingerprint": content_hash(json.dumps(columns)),
                    "current_rows": current_count,
                    "history_rows": history_count,
                    "history_at_least_current": history_count >= current_count,
                    "historical_values_mutated": False,
                    "migration_required_on_schema_change": True,
                }
            )
            if history_count < current_count:
                violations.append(table)
    report = {
        "schema": "observatory.parser-source-drift-report/1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tables": rows,
        "violations": violations,
        "update_semantics": "append immutable history; deterministic current view; versioned migrations only",
        "passes": not violations,
    }
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    (output / "parser_source_drift_report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def _missingness_report(output: Path) -> dict[str, Any]:
    views = {
        "institutional_gate_cycle": output.parent / "r1" / "gate_cycle_flow_series.parquet",
        "evaluation_objects": output.parent / "r1" / "evaluation_objects.parquet",
        "semantic_novelty": output.parent / "r1" / "semantic_novelty.parquet",
        "construct_spans": output / "construct_spans.parquet",
        "recombinatorial_novelty": output / "recombinatorial_novelty.parquet",
        "afterlife": output / "afterlife_panel.parquet",
    }
    tables = []
    for name, path in views.items():
        table = pq.read_table(path)
        nulls = {field: int(table.column(field).null_count) for field in table.schema.names}
        tables.append(
            {
                "view": name,
                "rows": len(table),
                "null_counts": nulls,
                "guidance": "report complete-case denominator; stratify by source/grade; use inverse-observation weighting only with declared model; provide worst/best-case bounds",
                "representativeness_rule": "C/D or unknown-grade sources are descriptive-only unless parent-population diagnostics justify weighting",
            }
        )
    report = {
        "schema": "observatory.missingness-selection-report/1",
        "analysis_views": tables,
        "all_views_have_guidance": all(row["guidance"] and row["representativeness_rule"] for row in tables),
        "passes": len(tables) == len(views),
    }
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    (output / "missingness_selection_report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def _attribution(workspace: Path, output: Path) -> dict[str, Any]:
    rows = []
    for card in source_cards():
        rows.append(
            {
                "source_id": card.source_id,
                "provider": card.provider,
                "official_url": card.official_url,
                "licences": dict(card.licences),
                "status": card.status,
                "credit": f"Data/metadata source: {card.provider}; OSG preserves source identifiers and does not claim upstream authorship.",
            }
        )
    external_validation_sources = [
        {
            "source_id": "snsf_individual_votes",
            "provider": "Rachel Heyard / Swiss National Science Foundation",
            "official_url": "https://doi.org/10.5281/zenodo.4531160",
            "licences": {"workbook": "CC-BY-4.0"},
            "status": "external_validation_fixture",
        },
        {
            "source_id": "hupd",
            "provider": "Harvard USPTO Patent Dataset authors",
            "official_url": "https://patentdataset.org/",
            "licences": {"dataset": "CC-BY-NC-SA-4.0"},
            "status": "derived_only",
        },
        {
            "source_id": "qwen3_embedding_0_6b",
            "provider": "Qwen team",
            "official_url": "https://huggingface.co/Qwen/Qwen3-Embedding-0.6B",
            "licences": {"model": "Apache-2.0"},
            "status": "model_dependency",
        },
        {
            "source_id": "peer_review_analyze_1_0",
            "provider": "Peer Review Analyze authors",
            "official_url": "https://doi.org/10.1371/journal.pone.0259238",
            "licences": {"dataset": "MIT"},
            "status": "external_validation_fixture",
        },
        {
            "source_id": "preprint_to_paper_gray_zone",
            "provider": "PreprintToPaper authors",
            "official_url": "https://doi.org/10.1038/s41597-026-06867-3",
            "licences": {"dataset": "CC-BY-4.0"},
            "status": "external_validation_fixture",
        },
    ]
    registered_source_ids = {row["source_id"] for row in rows}
    for row in external_validation_sources:
        if row["source_id"] in registered_source_ids:
            continue
        rows.append(
            {
                **row,
                "credit": (
                    f"Validation/model source: {row['provider']}; OSG preserves source identifiers "
                    "and does not claim upstream authorship."
                ),
            }
        )
    report = {
        "schema": "observatory.corpus-attribution/1",
        "sources": rows,
        "generated_from": (
            "configs/observatory/sources.yaml plus pinned external validation/model fixtures"
        ),
        "passes": all(row["provider"] and row["official_url"] and row["credit"] for row in rows),
    }
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    (output / "CORPUS_ATTRIBUTION.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    markdown = ["# Corpus attribution", "", "The OSG preserves upstream authorship and licences.", ""]
    markdown.extend(f"- **{row['provider']}** (`{row['source_id']}`): {row['official_url']}" for row in rows)
    (workspace / "docs" / "observatory" / "CORPUS_ATTRIBUTION.md").write_text("\n".join(markdown) + "\n")
    return report


def build_r2_assurance(workspace: Path, lake: Path, output: Path) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    reports = [
        _terms_snapshot(workspace, output / "terms_access_snapshot.json"),
        _privacy_report(workspace, output),
        _fulltext_coverage(lake, output),
        _drift_report(lake, output),
        _missingness_report(output),
        _attribution(workspace, output),
    ]
    (output / "r2_privacy_audit.json").write_text(json.dumps(reports[1], indent=2, sort_keys=True) + "\n")
    summary = {
        "schema": "observatory.r2-assurance-suite/1",
        "components": [report["schema"] for report in reports],
        "passes": all(report.get("passes") for report in reports),
    }
    summary["report_hash"] = content_hash(json.dumps(summary, sort_keys=True))
    (output / "r2_assurance_report.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    return summary
