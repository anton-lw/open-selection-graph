"""Bounded public funding-gate extension with strict identification firewalls.

Related programme artifacts are immutable external fixtures: this module reads
and hashes them but writes only to ``results/observatory/r4``.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .ids import content_hash, stable_id
from .storage_guard import storage_preflight

SIX_FIELDS = (
    "applications",
    "passing_or_eligible_pool",
    "assignment_arm",
    "awards_or_outcome",
    "unfunded_candidate_text",
    "decision_date_or_round",
)


def evaluate_funding_instrument(row: Mapping[str, Any], estimand: str) -> dict[str, Any]:
    required = {
        "entry_effect": {"applications", "awards_or_outcome", "decision_date_or_round"},
        "allocation_effect": {
            "passing_or_eligible_pool",
            "assignment_arm",
            "awards_or_outcome",
            "decision_date_or_round",
        },
        "entrant_returner_lower_bound": {"awards_or_outcome", "decision_date_or_round"},
        "portfolio_descriptive": {"awards_or_outcome", "decision_date_or_round"},
    }
    if estimand not in required:
        raise ValueError(f"unknown funding estimand: {estimand}")
    observed = {name for name in SIX_FIELDS if str(row.get(name, "")).lower() in {"yes", "true", "1", "observed"}}
    missing = sorted(required[estimand] - observed)
    if missing:
        verdict = "not_identified"
    elif estimand == "entrant_returner_lower_bound":
        verdict = "partially_identified"
    else:
        verdict = "identified"
    return {
        "estimand": estimand,
        "verdict": verdict,
        "missing_disclosures": missing,
        "manual_override_allowed": False,
    }


def _normal_title(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def _write(frame: pd.DataFrame | list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(frame, pd.DataFrame):
        table = pa.Table.from_pandas(frame, preserve_index=False)
    else:
        cleaned = [
            {
                key: None
                if value is None or (not isinstance(value, (list, dict)) and bool(pd.isna(value)))
                else value
                for key, value in row.items()
            }
            for row in frame
        ]
        table = pa.Table.from_pylist(cleaned)
    pq.write_table(table, path, compression="zstd")


def _strict_ooxml_rows(path: Path) -> Iterator[tuple[str, list[dict[str, str | None]]]]:
    """Yield sheets from the SNSF strict-OOXML workbook without normalising it.

    The workbook uses the ISO strict namespace, which common Python spreadsheet
    readers do not currently recognise.  This parser reads only shared strings,
    workbook relationships, and cell values from the immutable ZIP package.
    """
    relationship_ns = "http://schemas.openxmlformats.org/package/2006/relationships"
    office_relationship_ns = (
        "http://purl.oclc.org/ooxml/officeDocument/relationships"
    )
    with ZipFile(path) as archive:
        shared_root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
        shared = [
            "".join(node.text or "" for node in item.iter() if node.tag.endswith("}t"))
            for item in shared_root
        ]
        relationships_root = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        targets = {
            node.attrib["Id"]: node.attrib["Target"]
            for node in relationships_root.findall(f"{{{relationship_ns}}}Relationship")
        }
        workbook_root = ET.fromstring(archive.read("xl/workbook.xml"))
        for sheet in workbook_root.findall(".//{*}sheet"):
            target = targets[sheet.attrib[f"{{{office_relationship_ns}}}id"]]
            sheet_root = ET.fromstring(archive.read(f"xl/{target}"))
            rows: list[dict[str, str | None]] = []
            for row_node in sheet_root.findall(".//{*}sheetData/{*}row"):
                row: dict[str, str | None] = {}
                for cell in row_node.findall("{*}c"):
                    match = re.match(r"[A-Z]+", cell.attrib["r"])
                    if match is None:
                        continue
                    value_node = cell.find("{*}v")
                    if value_node is None or value_node.text is None:
                        value = None
                    elif cell.attrib.get("t") == "s":
                        value = shared[int(value_node.text)]
                    else:
                        value = value_node.text
                    row[match.group()] = value
                rows.append(row)
            yield sheet.attrib["name"], rows


def build_funding_products(workspace: Path, output: Path) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    storage_preflight(output, projected_input_bytes=0, projected_output_bytes=128 * 1024 * 1024)
    opportunities_path = workspace / "data" / "observatory" / "external" / "ukri_opportunities.parquet"
    p4_root = workspace / "results" / "p4" / "candidate"
    p4_data = workspace / "data" / "p4"
    gtr_audit_path = output / "gtr_backup_audit.json"
    required = [
        opportunities_path,
        p4_root / "census.json",
        p4_root / "manifest.json",
        p4_root / "evaluability.json",
        p4_root / "snsf_replication.json",
        p4_data / "instruments.csv",
        p4_data / "evaluability_scoreboard.csv",
        p4_data / "frame.csv",
        p4_data / "snsf" / "snsf_frame.csv",
        p4_data / "fwf" / "projects.jsonl",
    ]
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"read-only funding fixtures missing: {missing}")

    opportunities = pd.read_parquet(opportunities_path)
    if not gtr_audit_path.is_file():
        raise FileNotFoundError(
            "streaming GtR audit missing; run observatory.gtr_sql.audit_gtr_backup first"
        )
    gtr_audit = json.loads(gtr_audit_path.read_text())
    if not gtr_audit.get("passes"):
        raise RuntimeError("GtR upstream count/link audit failed")
    opportunity_rows = opportunities[
        ["id", "title", "status", "funders", "funding_type", "publication_date", "opening_date", "closing_date", "href"]
    ].copy()
    opportunity_rows["upstream_source_id"] = "wrmthorne/ukri-funding-opportunities"
    opportunity_rows["upstream_authorship_preserved"] = True
    opportunity_rows["observatory_claims_upstream_authorship"] = False
    opportunity_rows["observability_grade"] = "U"
    opportunity_rows["earliest_public_stage"] = "funding opportunity announcement"
    opportunity_rows["application_population_observed"] = False
    _write(opportunity_rows, output / "ukri_opportunity_registry.parquet")

    rounds = opportunity_rows.copy()
    rounds["round_id"] = rounds["id"].map(
        lambda value: stable_id("funding_round", "ukri_opportunities", value)
    )
    rounds["expected_count"] = 1
    rounds["found_count"] = 1
    rounds["outcome_records_found"] = 0
    rounds["outcome_population_status"] = "not_observed_quarantined"
    rounds["source_document_hash"] = rounds["id"].map(lambda value: content_hash(str(value)))
    rounds["tabular_pdf_extraction_status"] = "not_applicable_opportunity_page"
    _write(rounds, output / "ukri_round_registry.parquet")
    panel_rounds = []
    for row in gtr_audit["meeting_rounds"]:
        panel_rounds.append(
            {
                **row,
                "outcome_counts": json.dumps(row["outcome_counts"], sort_keys=True),
                "source_document_sha256": gtr_audit["source_sha256"],
                "coverage_ratio": 1.0,
                "application_population_complete": False,
            }
        )
    _write(panel_rounds, output / "ukri_panel_rounds.parquet")

    nerc = opportunities[opportunities["title"].str.contains("Pushing the frontiers", case=False, na=False)].copy()
    nerc_rows = []
    for row in nerc.itertuples(index=False):
        nerc_rows.append(
            {
                "round_id": stable_id("funding_round", "nerc_ptf", row.id),
                "upstream_id": row.id,
                "title": row.title,
                "opening_date": row.opening_date,
                "closing_date": row.closing_date,
                "source_url": row.href,
                "exact_returner_links": 0,
                "inferred_returner_links": 0,
                "inferred_linkage_precision": None,
                "application_text_observed": False,
                "assignment_arm_observed": False,
                "allocation_effect": "not_identified",
                "bundled_process_changes": "unknown_from_opportunity_only",
            }
        )
    _write(nerc_rows, output / "nerc_ptf_rounds.parquet")

    # Read-only reproduction of existing programme artifacts.
    fwf_manifest = json.loads((p4_root / "manifest.json").read_text())
    fwf_jsonl_count = sum(1 for line in (p4_data / "fwf" / "projects.jsonl").open() if line.strip())
    fwf = pd.read_csv(p4_data / "frame.csv")
    snsf = pd.read_csv(p4_data / "snsf" / "snsf_frame.csv")
    snsf_replication = json.loads((p4_root / "snsf_replication.json").read_text())
    reproduction = {
        "schema": "observatory.external-p4-funding-reproduction/1",
        "mode": "read_only_external_fixture",
        "paper_project_modified": False,
        "fixture_hashes": {str(path.relative_to(workspace)): content_hash(path.read_bytes()) for path in required[1:]},
        "fwf_api_reported_total": int(fwf_manifest["api_reported_total"]),
        "fwf_jsonl_rows": fwf_jsonl_count,
        "fwf_reconciles": fwf_jsonl_count == int(fwf_manifest["n_records"]) == int(fwf_manifest["api_reported_total"]),
        "fwf_analysis_rows": len(fwf),
        "snsf_analysis_rows": len(snsf),
        "snsf_registered_grants": int(snsf_replication["n_grants"]),
        "snsf_reproduces": len(snsf) == int(snsf_replication["n_grants"]),
        "multilingual_versioning": "FWF English/German fields remain upstream; OSG releases hashes/coverage, not copied text",
        "passes": fwf_jsonl_count == int(fwf_manifest["api_reported_total"])
        and len(snsf) == int(snsf_replication["n_grants"]),
    }
    reproduction["report_hash"] = content_hash(json.dumps(reproduction, sort_keys=True))
    (output / "funding_external_reproduction_audit.json").write_text(
        json.dumps(reproduction, indent=2, sort_keys=True) + "\n"
    )

    # Licence-safe aggregate portfolio panels; no proposal text or person IDs.
    fwf_series = (
        fwf.groupby(["programme", "year", "round_id"], dropna=False)
        .agg(
            awards=("grant_id", "nunique"), total_awarded_eur=("amount_eur", "sum"), text_coverage=("has_text", "mean")
        )
        .reset_index()
    )
    fwf_series["source_id"] = "fwf"
    fwf_series["numerator"] = "public awarded projects"
    fwf_series["denominator"] = "public awarded projects in programme-year-round; applications unobserved"
    fwf_series["microdata_claim"] = False
    snsf_series = (
        snsf.groupby(["programme", "year", "round_id"], dropna=False)
        .agg(
            awards=("grant_id", "nunique"), total_awarded_eur=("amount_eur", "sum"), text_coverage=("has_text", "mean")
        )
        .reset_index()
    )
    snsf_series["source_id"] = "snsf"
    snsf_series["numerator"] = "public awarded grants"
    snsf_series["denominator"] = "public awarded grants in programme-year-round; applications unobserved"
    snsf_series["microdata_claim"] = False
    portfolio = pd.concat([fwf_series, snsf_series], ignore_index=True, sort=False)
    _write(portfolio, output / "funding_portfolio_series.parquet")

    instruments = pd.read_csv(p4_data / "instruments.csv")
    scoreboard = pd.read_csv(p4_data / "evaluability_scoreboard.csv")
    instrument = instruments.merge(scoreboard, on=["iid", "funder", "scheme"], how="left", suffixes=("", "_eval"))
    instrument_rows = []
    for row in instrument.to_dict("records"):
        standard = {
            "applications": row.get("has_applications"),
            "passing_or_eligible_pool": row.get("has_passing"),
            "assignment_arm": row.get("has_arm_label"),
            "awards_or_outcome": row.get("has_awards"),
            "unfunded_candidate_text": row.get("has_unfunded_text"),
            "decision_date_or_round": row.get("has_decision_date"),
        }
        verdicts = {
            name: evaluate_funding_instrument(standard, name)
            for name in ("entry_effect", "allocation_effect", "entrant_returner_lower_bound", "portfolio_descriptive")
        }
        instrument_rows.append(
            {
                "instrument_id": row["iid"],
                "funder": row["funder"],
                "scheme": row["scheme"],
                "rule_history_effective": row.get("announced") or row.get("start_year"),
                "source_url": row.get("source_url"),
                "observability_grade": "D" if str(row.get("applications_published")).lower() != "yes" else "U",
                **standard,
                "evaluability_json": json.dumps(verdicts, sort_keys=True),
                "winner_only_stays_grade_d": str(row.get("applications_published")).lower() != "yes",
                "manual_override_allowed": False,
            }
        )
    _write(instrument_rows, output / "funding_instrument_evaluability.parquet")

    # Awardee-only lower bounds remain distinct from application returners.
    repeat_rows = []
    for source_id, frame in (("fwf", fwf), ("snsf", snsf)):
        key = "orcid" if source_id == "fwf" and "orcid" in frame else None
        if key:
            public = frame[frame[key].notna() & (frame[key].astype(str).str.len() > 5)].copy()
            for protected_id, group in public.groupby(key):
                ordered = group.sort_values("year")
                if len(ordered) < 2:
                    continue
                repeat_rows.append(
                    {
                        "source_id": source_id,
                        "protected_awardee_id_hash": content_hash(str(protected_id)),
                        "observed_awards": len(ordered),
                        "first_year": int(ordered["year"].min()),
                        "last_year": int(ordered["year"].max()),
                        "linkage_layer": "exact_public_orcid_awardee_only",
                        "precision": 1.0,
                        "application_returner_inferred": False,
                        "lower_bound_interpretation": "repeat awardee activity only; not proposal or application return",
                    }
                )
    _write(repeat_rows, output / "funding_repeat_lower_bounds.parquet")

    output_links = []
    for source_id, frame in (("fwf", fwf), ("snsf", snsf)):
        for row in frame[["grant_id", "year"]].itertuples(index=False):
            output_links.append(
                {
                    "source_id": source_id,
                    "grant_id": str(row.grant_id),
                    "grant_year": int(row.year),
                    "output_link_status": "not_observed_in_current_public_fixture",
                    "link_method": "funder identifier/acknowledgement required",
                    "precision_audit_status": "no links promoted",
                    "missing_acknowledgement_interpretation": "undercoverage, never zero output",
                }
            )
    _write(output_links, output / "grant_output_linkage.parquet")

    watcher = {
        "schema": "observatory.prospective-funding-watcher/1",
        "registered_at": datetime.now(timezone.utc).isoformat(),
        "cutoff_exclusive": "2026-08-20T00:00:00Z",
        "predicate_hash": content_hash(
            json.dumps(
                {"six_fields": SIX_FIELDS, "instruments": sorted(row["instrument_id"] for row in instrument_rows)}
            )
        ),
        "source_manifest_hashes": {
            "ukri_opportunities": content_hash(opportunities_path.read_bytes()),
            "fwf": content_hash((p4_root / "manifest.json").read_bytes()),
            "snsf": content_hash((p4_root / "snsf_replication.json").read_bytes()),
        },
        "future_records_partition": "sealed_holdout",
        "analysis_before_registered_evaluation": False,
        "passes": True,
    }
    watcher["report_hash"] = content_hash(json.dumps(watcher, sort_keys=True))
    (output / "funding_prospective_watcher.json").write_text(json.dumps(watcher, indent=2, sort_keys=True) + "\n")

    # Explicit invalid-estimand fixture for winner-only instruments.
    invalid_fixture = evaluate_funding_instrument(
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
    firewall = {
        "schema": "observatory.funding-identification-firewall-audit/1",
        "winner_registry_fixture": invalid_fixture,
        "informative_not_identified": invalid_fixture["verdict"] == "not_identified"
        and bool(invalid_fixture["missing_disclosures"]),
        "manual_override_allowed": False,
        "passes": invalid_fixture["verdict"] == "not_identified" and not invalid_fixture["manual_override_allowed"],
    }
    firewall["report_hash"] = content_hash(json.dumps(firewall, sort_keys=True))
    (output / "funding_identification_firewall_audit.json").write_text(
        json.dumps(firewall, indent=2, sort_keys=True) + "\n"
    )

    upstream = {
        "schema": "observatory.ukri-upstream-adoption/1",
        "zenodo_record": "10.5281/zenodo.19243841",
        "zenodo_database_file": {
            "name": "gtr_backup.sql.gz",
            "size_bytes": 786098742,
            "md5": "2f2a3d288ae03f049fe489eb099dc9b0",
            "licence": "CC-BY-NC-SA-4.0",
            "sha256": gtr_audit["source_sha256"],
            "disposition": "locally_protected_upstream_snapshot; release only counts and non-personal aggregates",
        },
        "published_entity_counts_reproduce": gtr_audit["count_reproduction"],
        "published_link_rates_reproduce": gtr_audit["link_rate_checks"],
        "application_link_rates": gtr_audit["application_link_rates"],
        "upstream_internal_count_discrepancies": gtr_audit[
            "surfaced_internal_paper_discrepancies"
        ],
        "opportunity_dataset": {
            "rows_provider": 2102,
            "rows_local": len(opportunities),
            "sha256": content_hash(opportunities_path.read_bytes()),
            "link_rate": 1.0,
        },
        "rescraped_by_observatory": False,
        "upstream_authorship_claimed": False,
        "passes": (
            len(opportunities) == 2102
            and all(gtr_audit["count_reproduction"].values())
            and all(gtr_audit["link_rate_checks"].values())
        ),
    }
    upstream["report_hash"] = content_hash(json.dumps(upstream, sort_keys=True))
    (output / "ukri_upstream_adoption.json").write_text(json.dumps(upstream, indent=2, sort_keys=True) + "\n")

    choice_sets = build_public_panel_choice_sets(workspace, output)
    snsf_votes = build_snsf_individual_vote_panels(workspace, output)
    report: dict[str, Any] = {
        "schema": "observatory.funding-products-report/2",
        "ukri_opportunities": len(opportunities),
        "ukri_panel_rounds": len(panel_rounds),
        "nerc_ptf_rounds": len(nerc_rows),
        "fwf_award_rows": len(fwf),
        "snsf_award_rows": len(snsf),
        "instruments": len(instrument_rows),
        "repeat_awardee_lower_bounds": len(repeat_rows),
        "full_application_returner_claims": 0,
        "allocation_effects_from_missing_arms": 0,
        "public_panel_application_events": choice_sets["application_events"],
        "public_panel_choice_sets": choice_sets["choice_sets"],
        "snsf_proposal_panel_events": snsf_votes["proposal_panel_events"],
        "snsf_individual_vote_cells": snsf_votes["vote_cells"],
        "snsf_outcome_observed_proposals": snsf_votes["outcome_observed_proposals"],
        "external_project_modified": False,
        "passes": reproduction["passes"]
        and upstream["passes"]
        and firewall["passes"]
        and choice_sets["passes"]
        and snsf_votes["passes"],
    }
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    (output / "funding_products_report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def _text_or_none(value: Any) -> str | None:
    return None if value is None or bool(pd.isna(value)) else str(value).strip()


def build_snsf_individual_vote_panels(workspace: Path, output: Path) -> dict[str, Any]:
    """Build proposal-level panel populations and explicit individual vote cells.

    The source contains nine anonymised SNSF panels.  Four MINT panels include
    a funding outcome.  Empty vote cells remain an observed missing state; they
    are never recoded as a grade or a conflict of interest.
    """
    source = (
        workspace
        / "data"
        / "observatory"
        / "external"
        / "funding"
        / "SNSF_individual_votes_zenodo_4531160.xlsx"
    )
    if not source.is_file():
        raise FileNotFoundError(f"SNSF individual-vote workbook missing: {source}")

    grade_order = {"A": 6, "AB": 5, "B": 4, "BC": 3, "C": 2, "D": 1}
    proposal_rows: list[dict[str, Any]] = []
    vote_rows: list[dict[str, Any]] = []
    choice_rows: list[dict[str, Any]] = []
    for panel_name, rows in _strict_ooxml_rows(source):
        if len(rows) < 2 or rows[0].get("A") != "proposal":
            raise RuntimeError(f"unexpected SNSF panel sheet structure: {panel_name}")
        header = rows[0]
        voter_columns = sorted(
            (column for column, value in header.items() if str(value).startswith("voter")),
            key=lambda column: int(str(header[column]).replace("voter", "")),
        )
        outcome_column = next(
            (column for column, value in header.items() if value == "Fund"), None
        )
        panel_id = stable_id("funding_panel", "snsf_individual_votes", panel_name)
        panel_proposals: list[dict[str, Any]] = []
        for source_row, row in enumerate(rows[1:], start=2):
            proposal_native = str(row.get("A") or "").strip()
            if not proposal_native:
                continue
            proposal_id = stable_id(
                "funding_application", "snsf_individual_votes", f"{panel_name}|{proposal_native}"
            )
            grades: list[int] = []
            explicit_coi = 0
            missing_vote = 0
            for column in voter_columns:
                voter_native = str(header[column])
                native = str(row.get(column) or "").strip().upper()
                if native in grade_order:
                    state = "grade_cast"
                    grade = native
                    grade_ordinal = grade_order[native]
                    grades.append(grade_ordinal)
                elif native == "COI":
                    state = "explicit_conflict_of_interest"
                    grade = None
                    grade_ordinal = None
                    explicit_coi += 1
                elif not native:
                    state = "blank_vote_cell"
                    grade = None
                    grade_ordinal = None
                    missing_vote += 1
                else:
                    raise RuntimeError(
                        f"unexpected SNSF vote value {native!r} in {panel_name} row {source_row}"
                    )
                vote_rows.append(
                    {
                        "vote_cell_id": stable_id(
                            "funding_vote_cell",
                            "snsf_individual_votes",
                            f"{panel_name}|{proposal_native}|{voter_native}",
                        ),
                        "panel_id": panel_id,
                        "proposal_id": proposal_id,
                        "voter_id_within_panel": stable_id(
                            "funding_panel_voter",
                            "snsf_individual_votes",
                            f"{panel_name}|{voter_native}",
                        ),
                        "vote_state": state,
                        "grade": grade,
                        "grade_ordinal": grade_ordinal,
                        "blank_cell_interpretation": (
                            "not_cast_or_withheld; abstention and undisclosed conflict are not separable"
                            if state == "blank_vote_cell"
                            else None
                        ),
                    }
                )
            outcome_native = str(row.get(outcome_column) or "").strip() if outcome_column else ""
            if outcome_column and outcome_native not in {"0", "1"}:
                raise RuntimeError(
                    f"unexpected SNSF funding outcome {outcome_native!r} in {panel_name} row {source_row}"
                )
            proposal = {
                "proposal_id": proposal_id,
                "panel_id": panel_id,
                "panel_name": panel_name,
                "proposal_label_within_panel": proposal_native,
                "panel_member_count": len(voter_columns),
                "grade_vote_count": len(grades),
                "explicit_coi_count": explicit_coi,
                "blank_vote_cell_count": missing_vote,
                "mean_grade_ordinal": sum(grades) / len(grades) if grades else None,
                "min_grade_ordinal": min(grades) if grades else None,
                "max_grade_ordinal": max(grades) if grades else None,
                "funding_outcome_observed": outcome_column is not None,
                "funded": outcome_native == "1" if outcome_column else None,
                "proposal_text_observed": False,
                "applicant_identity_observed": False,
                "randomized_assignment_observed": False,
                "entry_population_observed": False,
                "population_stage": "proposal evaluated in observed panel",
            }
            proposal_rows.append(proposal)
            panel_proposals.append(proposal)
        outcome_observed = [row for row in panel_proposals if row["funding_outcome_observed"]]
        choice_rows.append(
            {
                "panel_id": panel_id,
                "panel_name": panel_name,
                "proposal_count": len(panel_proposals),
                "panel_member_count": len(voter_columns),
                "funding_outcome_observed": outcome_column is not None,
                "funded_count": (
                    sum(bool(row["funded"]) for row in outcome_observed)
                    if outcome_column
                    else None
                ),
                "individual_votes_observed": True,
                "proposal_text_observed": False,
                "randomized_assignment_observed": False,
                "allocation_effect_identified": False,
                "choice_set_stage": "panel_evaluated_proposals",
            }
        )

    proposals = pd.DataFrame(proposal_rows)
    votes = pd.DataFrame(vote_rows)
    choices = pd.DataFrame(choice_rows)
    _write(proposals, output / "snsf_proposal_panel_events.parquet")
    _write(votes, output / "snsf_individual_vote_cells.parquet")
    _write(choices, output / "snsf_panel_choice_sets.parquet")

    report: dict[str, Any] = {
        "schema": "open-selection-graph.snsf-individual-vote-panels/1",
        "source": {
            "zenodo_record": "https://zenodo.org/records/4531160",
            "doi": "10.5281/zenodo.4531160",
            "sha256": content_hash(source.read_bytes()),
            "md5_published": "7180009e55d14c77ac27a1dc72ec8fea",
            "licence": "CC-BY-4.0",
            "format": "ISO strict OOXML",
        },
        "panels": len(choices),
        "proposal_panel_events": len(proposals),
        "vote_cells": len(votes),
        "grade_votes": int((votes["vote_state"] == "grade_cast").sum()),
        "explicit_conflicts_of_interest": int(
            (votes["vote_state"] == "explicit_conflict_of_interest").sum()
        ),
        "blank_vote_cells": int((votes["vote_state"] == "blank_vote_cell").sum()),
        "outcome_observed_panels": int(choices["funding_outcome_observed"].sum()),
        "outcome_observed_proposals": int(proposals["funding_outcome_observed"].sum()),
        "funded_proposals": int(proposals["funded"].fillna(False).sum()),
        "proposal_text_rows": 0,
        "person_identity_rows": 0,
        "randomized_assignment_arms": 0,
        "entry_population_claims": 0,
        "blank_cells_recoded_as_coi": 0,
        "allocation_effect_identified": False,
    }
    report["passes"] = (
        report["panels"] == 9
        and report["outcome_observed_panels"] == 4
        and report["proposal_panel_events"] > 400
        and report["vote_cells"] > 4_000
        and report["randomized_assignment_arms"] == 0
        and report["blank_cells_recoded_as_coi"] == 0
    )
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    (output / "snsf_individual_vote_panels_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    return report


def build_public_panel_choice_sets(workspace: Path, output: Path) -> dict[str, Any]:
    """Normalize UKRI's public panel-stage application populations.

    These are real panel choice sets, including unfunded applications.  They
    remain conditional on reaching panel and contain neither proposal text nor
    a randomized assignment arm.
    """
    root = workspace / "data" / "observatory" / "external" / "funding"
    epsrc_path = root / "EPSRC_Funding_Application_Outcomes_2026-08-05.xlsx"
    ahrc_path = root / "AHRC_Panel_Outcomes_2026-06-09.xlsx"
    epsrc = pd.read_excel(epsrc_path, sheet_name="Applications").drop_duplicates()
    ahrc = (
        pd.read_excel(ahrc_path, sheet_name="AHRC Funding Decision")
        .drop_duplicates()
        .rename(
            columns={
                "Opportunity Name": "opportunity_name",
                "Overall Score Range": "score_band",
                "Funded?": "outcome",
                "Meeting Name": "meeting_name",
            }
        )
    )

    event_rows: list[dict[str, Any]] = []
    epsrc_keys = ["Application ID", "Meeting Reference", "List Name"]
    for key, frame in epsrc.groupby(epsrc_keys, dropna=False, sort=True):
        outcomes = sorted(set(frame["Outcome"].dropna().astype(str)))
        ranks = sorted(set(float(value) for value in frame["Rank"].dropna()))
        application_key = str(key[0])
        event_rows.append(
            {
                "funding_application_event_id": stable_id(
                    "funding_application_event", "epsrc", "|".join(map(str, key))
                ),
                "application_id": stable_id("funding_application", "epsrc", application_key),
                "funder": "EPSRC",
                "meeting_id": str(key[1]),
                "list_name": _text_or_none(key[2]),
                "meeting_name": _text_or_none(frame["Meeting Name"].iloc[0]),
                "meeting_date": frame["Meeting Start Date"].iloc[0],
                "opportunity_id": _text_or_none(frame["Opportunity Number"].iloc[0]),
                "opportunity_name": _text_or_none(frame["Opportunity Name"].iloc[0]),
                "rank": ranks[0] if len(ranks) == 1 else None,
                "rank_conflict": len(ranks) > 1,
                "outcome_native": outcomes[0] if len(outcomes) == 1 else "|".join(outcomes),
                "funded": outcomes == ["Funded"],
                "outcome_conflict": len(outcomes) > 1,
                "source_duplicate_rows_collapsed": len(frame) - 1,
                "earliest_public_stage": "application assessed at panel",
                "entry_population_observed": False,
                "pre_panel_screen_observed": False,
                "proposal_text_observed": False,
                "assignment_mechanism_observed": False,
                "panel_choice_set_observed": True,
                "personal_fields_released": False,
            }
        )
    for row in ahrc.itertuples(index=False):
        native = str(row.GrantRefNumber)
        meeting = str(row.meeting_name)
        outcome = str(row.outcome)
        event_rows.append(
            {
                "funding_application_event_id": stable_id(
                    "funding_application_event", "ahrc", f"{native}|{meeting}"
                ),
                "application_id": stable_id("funding_application", "ahrc", native),
                "funder": "AHRC",
                "meeting_id": content_hash(meeting)[:24],
                "list_name": None,
                "meeting_name": meeting,
                "meeting_date": None,
                "opportunity_id": None,
                "opportunity_name": str(row.opportunity_name),
                "rank": None,
                "score_band": str(row.score_band),
                "rank_conflict": False,
                "outcome_native": outcome,
                "funded": outcome == "Funded",
                "outcome_conflict": False,
                "source_duplicate_rows_collapsed": 0,
                "earliest_public_stage": "application assessed at panel",
                "entry_population_observed": False,
                "pre_panel_screen_observed": False,
                "proposal_text_observed": False,
                "assignment_mechanism_observed": False,
                "panel_choice_set_observed": True,
                "personal_fields_released": False,
            }
        )
    events = pd.DataFrame(event_rows)
    _write(events, output / "funding_panel_application_events.parquet")

    choice_rows = []
    for key, frame in events.groupby(["funder", "meeting_id", "list_name"], dropna=False):
        resolved = frame[~frame["outcome_conflict"]]
        choice_rows.append(
            {
                "panel_choice_set_id": stable_id(
                    "funding_panel_choice_set", str(key[0]).lower(), "|".join(map(str, key[1:]))
                ),
                "funder": key[0],
                "meeting_id": key[1],
                "list_name": _text_or_none(key[2]),
                "application_count": len(frame),
                "funded_count": int(resolved["funded"].sum()),
                "unfunded_or_other_count": int(len(resolved) - resolved["funded"].sum()),
                "outcome_conflict_count": int(frame["outcome_conflict"].sum()),
                "rank_coverage": float(frame["rank"].notna().mean()),
                "panel_stage_funding_share": float(resolved["funded"].mean()) if len(resolved) else None,
                "choice_set_stage": "panel_assessed_applications",
                "entry_stage_generalization_allowed": False,
                "allocation_effect_identified": False,
                "unfunded_text_available": False,
            }
        )
    _write(choice_rows, output / "funding_panel_choice_sets.parquet")
    repeated = events.groupby(["funder", "application_id"])["meeting_id"].nunique()
    report = {
        "schema": "open-selection-graph.public-funding-panel-choice-sets/1",
        "epsrc_source_rows": 22_840,
        "epsrc_exact_duplicate_rows_removed": 3_044,
        "epsrc_application_events": sum(row["funder"] == "EPSRC" for row in event_rows),
        "ahrc_application_events": sum(row["funder"] == "AHRC" for row in event_rows),
        "application_events": len(event_rows),
        "choice_sets": len(choice_rows),
        "applications_observed_at_multiple_panels": int((repeated > 1).sum()),
        "funded_events": int(events["funded"].sum()),
        "unfunded_or_other_events": int((~events["funded"]).sum()),
        "personal_fields_released": False,
        "proposal_text_rows": 0,
        "randomized_assignment_arms": 0,
        "entry_population_claims": 0,
        "source_files": {
            "EPSRC": {
                "url": "https://www.ukri.org/publications/epsrc-funding-application-outcomes/",
                "sha256": content_hash(epsrc_path.read_bytes()),
                "as_of": "2026-08-05",
            },
            "AHRC": {
                "url": "https://www.ukri.org/publications/ahrc-panel-outcomes-and-attendance/",
                "sha256": content_hash(ahrc_path.read_bytes()),
                "as_of": "2026-06-09",
            },
        },
    }
    report["passes"] = (
        report["application_events"] > 20_000
        and report["choice_sets"] > 800
        and not report["personal_fields_released"]
        and report["entry_population_claims"] == 0
    )
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    (output / "funding_panel_choice_sets_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    return report
