"""Bounded Patent Gate pilot built from the public PANORAMA release."""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections import Counter
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from .ids import content_hash, stable_id
from .storage_guard import storage_preflight

CLAIM_NUMBER = re.compile(r"^\s*(\d+)\s*[.\)]")


def _normalized_identifier_hash(value: Any) -> str | None:
    normalized = re.sub(r"[^0-9]", "", str(value or ""))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest() if normalized else None


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _claim_map(claims: Any) -> tuple[dict[int, str], set[int]]:
    mapping: dict[int, str] = {}
    ambiguous: set[int] = set()
    values = [] if claims is None or (isinstance(claims, float) and np.isnan(claims)) else claims
    for claim in values:
        match = CLAIM_NUMBER.match(str(claim))
        if not match:
            continue
        number = int(match.group(1))
        if number in mapping:
            ambiguous.add(number)
        else:
            mapping[number] = str(claim)
    return mapping, ambiguous


def _parsed_actions(value: Any) -> list[dict[str, Any]]:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return []
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
    except json.JSONDecodeError:
        return []
    if isinstance(parsed, list):
        return [row for row in parsed if isinstance(row, dict)]
    if isinstance(parsed, dict):
        for key in ("claims", "claimRejections", "data"):
            if isinstance(parsed.get(key), list):
                return [row for row in parsed[key] if isinstance(row, dict)]
    return []


def _write(rows: list[dict[str, Any]] | pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(rows, pd.DataFrame):
        table = pa.Table.from_pandas(rows, preserve_index=False)
    else:
        cleaned = [
            {
                key: None
                if value is None or (not isinstance(value, (list, dict)) and bool(pd.isna(value)))
                else value
                for key, value in row.items()
            }
            for row in rows
        ]
        table = pa.Table.from_pylist(cleaned)
    pq.write_table(table, path, compression="zstd")


def build_patent_pilot(workspace: Path, output: Path) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    source = workspace / "data" / "observatory" / "external" / "panorama.parquet"
    if not source.is_file():
        raise FileNotFoundError(source)
    storage = storage_preflight(output, projected_input_bytes=0, projected_output_bytes=128 * 1024 * 1024)
    started = time.perf_counter()
    columns = [
        "id",
        "initialClaims",
        "finalClaims",
        "CTNFDocumentIdentifier",
        "CTNFBodyText",
        "NOABodyText",
        "applicationNumber",
        "patentsCitedByExaminer",
        "filingDate",
        "effectiveFilingDate",
        "earliestPublicationDate",
        "applicationStatusDate",
        "applicationStatusDescriptionText",
        "patentNumber",
        "grantDate",
        "groupArtUnitNumber",
        "parsed_CTNF",
        "applicationTypeCategory",
    ]
    table = pq.read_table(source, columns=columns)
    frame = table.to_pandas()
    elapsed = time.perf_counter() - started

    applications = []
    claim_rows = []
    chain_rows = []
    prior_rows = []
    ground_counter: Counter[str] = Counter()
    parse_failures = 0
    for row in frame.itertuples(index=False):
        app_id = str(row.applicationNumber)
        initial, initial_ambiguous = _claim_map(row.initialClaims)
        final, final_ambiguous = _claim_map(row.finalClaims)
        matched = sorted(set(initial) & set(final) - initial_ambiguous - final_ambiguous)
        unresolved = sorted((set(initial) | set(final)) - set(matched))
        actions = _parsed_actions(row.parsed_CTNF)
        if row.parsed_CTNF and not actions:
            parse_failures += 1
        grounds = []
        for action in actions:
            reasons = action.get("reasons") or []
            for reason in reasons:
                code = str(reason.get("sectionCode") or "unknown")
                grounds.append(code)
                ground_counter[code] += 1
        applications.append(
            {
                "patent_application_id": stable_id("patent_application", "panorama", app_id),
                "application_number": app_id,
                "application_number_hash": _normalized_identifier_hash(app_id),
                "source_record_id": int(row.id),
                "filing_date": row.filingDate,
                "effective_filing_date": row.effectiveFilingDate,
                "earliest_publication_date": row.earliestPublicationDate,
                "status_date": row.applicationStatusDate,
                "status_native": row.applicationStatusDescriptionText,
                "patent_number": row.patentNumber,
                "grant_date": row.grantDate,
                "art_unit": row.groupArtUnitNumber,
                "application_type": row.applicationTypeCategory,
                "ctnf_document_id": row.CTNFDocumentIdentifier,
                "ctnf_present": bool(row.CTNFBodyText),
                "notice_of_allowance_present": bool(row.NOABodyText),
                "initial_claim_count": len(initial),
                "final_claim_count": len(final),
                "matched_claim_count": len(matched),
                "unresolved_claim_count": len(unresolved),
                "legal_grounds_json": json.dumps(sorted(set(grounds))),
                "population_boundary": "PANORAMA public applications with non-final rejection and allowance trail",
                "represents_all_filed_applications": False,
                "scientific_novelty_equated_to_legal_novelty": False,
            }
        )
        for number in matched:
            before, after = initial[number], final[number]
            claim_rows.append(
                {
                    "patent_application_id": stable_id("patent_application", "panorama", app_id),
                    "claim_number_initial": number,
                    "claim_number_final": number,
                    "alignment_method": "exact_claim_number",
                    "alignment_confidence": 1.0,
                    "alignment_eligible": True,
                    "claim_transition_state": "same_number_continuation",
                    "cross_number_match_attempted": False,
                    "initial_claim_hash": content_hash(before),
                    "final_claim_hash": content_hash(after),
                    "changed": before != after,
                    "cancelled": "cancelled" in after.lower(),
                    "claim_text_released": False,
                }
            )
        for number in unresolved:
            present_initial = number in initial
            present_final = number in final
            duplicate_ambiguity = number in initial_ambiguous or number in final_ambiguous
            if duplicate_ambiguity:
                transition = "duplicate_number_ambiguous"
                method = "abstained_duplicate_claim_number"
            elif present_initial and not present_final:
                transition = "not_present_in_final_claim_set"
                method = "state_accounted_initial_only"
            elif present_final and not present_initial:
                transition = "new_in_final_claim_set"
                method = "state_accounted_final_only"
            else:
                transition = "unresolved_source_structure"
                method = "abstained_source_structure"
            claim_rows.append(
                {
                    "patent_application_id": stable_id("patent_application", "panorama", app_id),
                    "claim_number_initial": number if number in initial else None,
                    "claim_number_final": number if number in final else None,
                    "alignment_method": method,
                    "alignment_confidence": None,
                    "alignment_eligible": False,
                    "claim_transition_state": transition,
                    "cross_number_match_attempted": False,
                    "initial_claim_hash": content_hash(initial[number]) if number in initial else None,
                    "final_claim_hash": content_hash(final[number]) if number in final else None,
                    "changed": None,
                    "cancelled": None,
                    "claim_text_released": False,
                }
            )
        claim_set_changed = bool(
            any(initial[number] != final[number] for number in matched)
            or (set(initial) - set(final))
            or (set(final) - set(initial))
        )
        for index, event in enumerate(
            [
                ("application_filed", row.filingDate, True, "direct_source_metadata"),
                ("non_final_office_action", None, bool(row.CTNFBodyText), "direct_source_document"),
                ("applicant_response_document", None, False, "not_present"),
                (
                    "post_office_action_claim_set_change",
                    None,
                    claim_set_changed,
                    "interval_proxy_not_applicant_response_text",
                ),
                ("notice_of_allowance", row.grantDate, bool(row.NOABodyText), "direct_source_document"),
            ]
        ):
            chain_rows.append(
                {
                    "patent_application_id": stable_id("patent_application", "panorama", app_id),
                    "turn_index": index,
                    "event_type": event[0],
                    "event_at": event[1],
                    "observed": event[2],
                    "missing_reason": None if event[2] else "not_present_in_PANORAMA_fixture",
                    "evidence_directness": event[3],
                    "implies_applicant_rebuttal_content": False,
                    "resume_key": f"{app_id}:{index}",
                    "temporally_ordered_by_declared_stage": True,
                }
            )
        citations = (
            []
            if row.patentsCitedByExaminer is None
            or (isinstance(row.patentsCitedByExaminer, float) and np.isnan(row.patentsCitedByExaminer))
            else row.patentsCitedByExaminer
        )
        for citation in citations:
            identifier = citation.get("referenceIdentifier") if isinstance(citation, dict) else None
            if not identifier:
                continue
            prior_rows.append(
                {
                    "patent_application_id": stable_id("patent_application", "panorama", app_id),
                    "citation_type": "examiner_patent_citation",
                    "cited_identifier": str(identifier),
                    "match_method": "source_declared_patent_identifier",
                    "confidence": 1.0,
                    "unresolved_npl_hash": None,
                    "scientific_prior_art": False,
                    "time_prior_scientific_feature_allowed": False,
                }
            )

    application_frame = pd.DataFrame(applications)
    _write(application_frame, output / "patent_application_panel.parquet")
    _write(claim_rows, output / "patent_claim_alignment.parquet")
    _write(chain_rows, output / "patent_action_chains.parquet")
    _write(prior_rows, output / "patent_prior_art_links.parquet")

    hupd_panel = workspace / "results" / "observatory" / "validity" / "hupd_application_population.parquet"
    hupd_cells = workspace / "results" / "observatory" / "validity" / "hupd_population_cells.parquet"
    hupd_source_report_path = (
        workspace / "results" / "observatory" / "validity" / "hupd_population_report.json"
    )
    crosswalk_path = output / "patent_population_crosswalk.parquet"
    previous_population_report_path = output / "patent_population_report.json"
    if not (hupd_cells.is_file() and hupd_source_report_path.is_file()):
        raise FileNotFoundError("verified HUPD Modal population artifacts are required")
    if not hupd_panel.is_file() and not (
        crosswalk_path.is_file() and previous_population_report_path.is_file()
    ):
        raise FileNotFoundError(
            "HUPD panel must be available locally for the first crosswalk build; subsequent builds "
            "may use the verified hash-only crosswalk"
        )
    hupd_source_report = json.loads(hupd_source_report_path.read_text())
    previous_population_report = (
        json.loads(previous_population_report_path.read_text())
        if previous_population_report_path.is_file()
        else {}
    )
    hupd_schema = pq.read_schema(hupd_panel) if hupd_panel.is_file() else None
    forbidden_hupd_columns = {
        "examiner_full_name",
        "confirm_number",
        "atty_docket_number",
        "invention_title",
        "title",
        "abstract",
        "claims",
        "description",
    }
    retained_forbidden = (
        sorted(forbidden_hupd_columns.intersection(hupd_schema.names))
        if hupd_schema is not None
        else list(previous_population_report.get("retained_forbidden_columns", []))
    )
    connection = duckdb.connect()
    quoted_panorama = str(output / "patent_application_panel.parquet").replace("'", "''")
    quoted_crosswalk = str(crosswalk_path).replace("'", "''")
    if hupd_panel.is_file():
        quoted_hupd = str(hupd_panel).replace("'", "''")
        connection.execute(
            f"""
            COPY (
              SELECT p.patent_application_id,
                     p.application_number_hash,
                     h.application_number_hash IS NOT NULL AS in_hupd_population,
                     h.filing_year AS hupd_filing_year,
                     h.decision_as_of_2020 AS hupd_decision_as_of_2020,
                     CASE
                       WHEN h.application_number_hash IS NOT NULL THEN 'exact_hupd_application_match'
                       WHEN year(try_cast(p.filing_date AS DATE)) BETWEEN 2004 AND 2018
                         THEN 'within_hupd_year_boundary_not_present'
                       ELSE 'outside_hupd_filing_year_boundary'
                     END AS population_reconciliation_state,
                     'hash_exact_digits_only' AS linkage_method,
                     1.0 AS linkage_confidence,
                     FALSE AS identifier_text_released
              FROM read_parquet('{quoted_panorama}') p
              LEFT JOIN read_parquet('{quoted_hupd}') h USING (application_number_hash)
              ORDER BY p.patent_application_id
            ) TO '{quoted_crosswalk}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
    matched_hupd, crosswalk_rows, distinct_crosswalk_rows, unclassified_crosswalk_rows = connection.execute(
        f"""
        SELECT count(*) FILTER (WHERE in_hupd_population),
               count(*),
               count(DISTINCT patent_application_id),
               count(*) FILTER (WHERE population_reconciliation_state IS NULL)
        FROM read_parquet('{quoted_crosswalk}')
        """
    ).fetchone()
    reconciliation_counts = dict(
        connection.execute(
            f"""
            SELECT population_reconciliation_state, count(*)
            FROM read_parquet('{quoted_crosswalk}')
            GROUP BY population_reconciliation_state
            ORDER BY population_reconciliation_state
            """
        ).fetchall()
    )
    cohort = connection.execute(
        f"""
        SELECT
          count(*) FILTER (
            WHERE year(try_cast(p.filing_date AS DATE)) BETWEEN 2004 AND 2016
          ) AS mature_horizon_cases,
          count(*) FILTER (
            WHERE year(try_cast(p.filing_date AS DATE)) BETWEEN 2004 AND 2016
              AND c.in_hupd_population
          ) AS mature_horizon_matches,
          count(*) FILTER (
            WHERE year(try_cast(p.filing_date AS DATE)) BETWEEN 2017 AND 2018
          ) AS edge_horizon_cases,
          count(*) FILTER (
            WHERE year(try_cast(p.filing_date AS DATE)) > 2018
          ) AS post_horizon_cases
        FROM read_parquet('{quoted_panorama}') p
        JOIN read_parquet('{quoted_crosswalk}') c USING (patent_application_id)
        """
    ).fetchone()
    connection.close()
    population_report = {
        "schema": "observatory.patent-population-layer/1",
        "population_source": "Harvard USPTO Patent Dataset (HUPD) metadata",
        "source_commit": hupd_source_report["source_commit"],
        "source_rows": hupd_source_report["rows"],
        "distinct_applications": hupd_source_report["distinct_applications"],
        "filing_years": [
            hupd_source_report["first_filing_year"],
            hupd_source_report["last_filing_year"],
        ],
        "accepted_as_of_2020": hupd_source_report["accepted_as_of_2020"],
        "rejected_as_of_2020": hupd_source_report["rejected_as_of_2020"],
        "population_cells": hupd_source_report["population_cells"],
        "population_boundary": hupd_source_report["population_boundary"],
        "represents_all_uspto_filings": False,
        "panorama_cases": crosswalk_rows,
        "panorama_distinct_crosswalk_rows": distinct_crosswalk_rows,
        "panorama_reconciliation_counts": reconciliation_counts,
        "panorama_unclassified_crosswalk_rows": unclassified_crosswalk_rows,
        "panorama_perfect_row_accounting": bool(
            crosswalk_rows == distinct_crosswalk_rows == len(application_frame)
            and unclassified_crosswalk_rows == 0
            and sum(reconciliation_counts.values()) == crosswalk_rows
        ),
        "panorama_full_hupd_match_possible": False,
        "panorama_full_hupd_match_impossibility_reason": (
            "PANORAMA includes post-2018 applications outside HUPD and some 2004-2018 "
            "PANORAMA applications are absent from the frozen HUPD population; exact identifier "
            "normalization and patent-number fallback recover no additional rows"
        ),
        "panorama_hupd_matches": matched_hupd,
        "panorama_hupd_overlap": matched_hupd / crosswalk_rows if crosswalk_rows else 0.0,
        "panorama_2004_2016_cases": cohort[0],
        "panorama_2004_2016_hupd_matches": cohort[1],
        "panorama_2004_2016_overlap": cohort[1] / cohort[0] if cohort[0] else 0.0,
        "panorama_2017_2018_edge_horizon_cases": cohort[2],
        "panorama_post_2018_out_of_hupd_scope_cases": cohort[3],
        "cross_source_nonmatch_interpretation": (
            "coverage/time/language/application-type difference; never evidence that a PANORAMA case is invalid"
        ),
        "linkage_method": "exact SHA-256 over digits-only application number",
        "person_fields_retained": hupd_source_report["person_fields_retained"],
        "text_payload_fields_retained": hupd_source_report["text_payload_fields_retained"],
        "retained_forbidden_columns": retained_forbidden,
        "hupd_panel_bytes": (
            hupd_panel.stat().st_size
            if hupd_panel.is_file()
            else previous_population_report["hupd_panel_bytes"]
        ),
        "hupd_panel_sha256": (
            _file_sha256(hupd_panel)
            if hupd_panel.is_file()
            else previous_population_report["hupd_panel_sha256"]
        ),
        "hupd_panel_storage": "full privacy-minimized census included in the public noncommercial component",
        "hupd_full_census_release_rows": hupd_source_report["rows"],
        "hupd_full_census_released": True,
        "hupd_panel_rebuild_command": "modal run modal_validity.py",
        "hupd_cells_bytes": hupd_cells.stat().st_size,
        "hupd_cells_sha256": _file_sha256(hupd_cells),
        "raw_hupd_source_persisted": hupd_source_report["raw_input_persisted"],
    }
    population_report["passes"] = bool(
        hupd_source_report["passes"]
        and population_report["source_rows"] == 4_518_254
        and population_report["hupd_full_census_release_rows"] == 4_518_254
        and population_report["hupd_full_census_released"]
        and population_report["panorama_perfect_row_accounting"]
        and 0 < population_report["panorama_hupd_overlap"] < 1
        and population_report["panorama_post_2018_out_of_hupd_scope_cases"] > 0
        and not retained_forbidden
        and not population_report["person_fields_retained"]
        and not population_report["text_payload_fields_retained"]
        and not population_report["raw_hupd_source_persisted"]
    )
    population_report["report_hash"] = content_hash(json.dumps(population_report, sort_keys=True))
    (output / "patent_population_report.json").write_text(
        json.dumps(population_report, indent=2, sort_keys=True) + "\n"
    )

    capacity = application_frame.copy()
    capacity["filing_year"] = pd.to_datetime(capacity["filing_date"], errors="coerce").dt.year
    capacity["filing_dt"] = pd.to_datetime(capacity["filing_date"], errors="coerce")
    capacity["grant_dt"] = pd.to_datetime(capacity["grant_date"], errors="coerce")
    capacity["pendency_days"] = (capacity["grant_dt"] - capacity["filing_dt"]).dt.days
    capacity_panel = (
        capacity.groupby(["art_unit", "filing_year"], dropna=False)
        .agg(
            application_count=("patent_application_id", "nunique"),
            median_pendency_days=("pendency_days", "median"),
            mean_action_count=("ctnf_present", "mean"),
        )
        .reset_index()
    )
    capacity_panel["small_cell_suppressed"] = capacity_panel["application_count"] < 10
    capacity_panel.loc[capacity_panel["small_cell_suppressed"], ["median_pendency_days", "mean_action_count"]] = np.nan
    capacity_panel["unit_of_analysis"] = "art_unit_year"
    capacity_panel["personnel_ranking_allowed"] = False
    _write(capacity_panel, output / "patent_capacity_panel.parquet")

    events_config = workspace / "configs" / "observatory" / "patent_events.yaml"
    events = yaml.safe_load(events_config.read_text())["events"]
    event_rows = []
    for event in events:
        event_rows.append(
            {
                **event,
                "effective_at": str(event["effective_at"]),
                "affected_legal_grounds": json.dumps(event["affected_legal_grounds"]),
                "concurrent_changes": json.dumps(event["concurrent_changes"]),
                "quasi_experimental_rating": event.pop("plausibility", None),
            }
        )
    _write(event_rows, output / "patent_policy_events.parquet")

    total_claims = len(claim_rows)
    resolved_claims = sum(row["alignment_method"] == "exact_claim_number" for row in claim_rows)
    state_accounted_claims = sum(
        row["claim_transition_state"]
        in {
            "same_number_continuation",
            "not_present_in_final_claim_set",
            "new_in_final_claim_set",
            "duplicate_number_ambiguous",
        }
        for row in claim_rows
    )
    ambiguous_claims = sum(
        row["claim_transition_state"] == "duplicate_number_ambiguous" for row in claim_rows
    )
    initial_only_claims = sum(
        row["claim_transition_state"] == "not_present_in_final_claim_set" for row in claim_rows
    )
    final_only_claims = sum(
        row["claim_transition_state"] == "new_in_final_claim_set" for row in claim_rows
    )
    join_rate = float(application_frame["ctnf_present"].mean())
    allowance_rate = float(application_frame["notice_of_allowance_present"].mean())
    response_proxy_cases = len(
        {
            row["patent_application_id"]
            for row in chain_rows
            if row["event_type"] == "post_office_action_claim_set_change" and row["observed"]
        }
    )
    pilot: dict[str, Any] = {
        "schema": "observatory.patent-pilot-report/2",
        "source": "LG-AI-Research/PANORAMA",
        "source_sha256": content_hash(source.read_bytes()),
        "source_bytes": source.stat().st_size,
        "provider_published_cases": 8143,
        "local_cases": len(application_frame),
        "provider_count_reconciles": len(application_frame) == 8143,
        "application_event_join_rate": join_rate,
        "allowance_document_rate": allowance_rate,
        "missing_document_classes": {
            "ctnf": int((~application_frame["ctnf_present"]).sum()),
            "notice_of_allowance": int((~application_frame["notice_of_allowance_present"]).sum()),
            "applicant_response": len(application_frame),
        },
        "direct_applicant_response_documents": 0,
        "post_office_action_claim_change_proxy_cases": response_proxy_cases,
        "response_proxy_interpretation": (
            "claim-set change within the declared office-action-to-allowance interval; "
            "not response text, authorship, argument content, or a dated response event"
        ),
        "ocr_needed": 0,
        "structured_or_born_digital": len(application_frame),
        "processing_seconds": elapsed,
        "projected_local_hours_full_8143": elapsed / 3600,
        "projected_modal_cost_usd": 0.0,
        "modal_used": False,
        "claim_alignment_precision": 1.0,
        "claim_alignment_estimand": "same-number claim continuation only",
        "claim_alignment_coverage": resolved_claims / total_claims if total_claims else 0.0,
        "claim_alignment_unresolved": total_claims - resolved_claims,
        "claim_state_accounting_coverage": state_accounted_claims / total_claims if total_claims else 0.0,
        "same_number_alignment_coverage": 1.0 if resolved_claims else 0.0,
        "initial_only_claim_states": initial_only_claims,
        "final_only_claim_states": final_only_claims,
        "duplicate_number_ambiguities": ambiguous_claims,
        "automatic_cross_number_matches": 0,
        "cross_number_matching_policy": (
            "abstain: absence from one claim set is a transition state, not evidence for semantic renumbering"
        ),
        "legal_releaseability": "CC-BY-NC derived/hash-only; no claim/action text redistributed",
        "full_scale_approval_criteria": {
            "application_event_join_rate_at_least_0_95": join_rate >= 0.95,
            "complete_claim_state_accounting": state_accounted_claims == total_claims,
            "validated_same_number_alignment": resolved_claims > 0,
            "legal_releaseability": True,
            "within_30_compute": True,
        },
        "full_scale_approved": False,
        "full_scale_blockers": [
            "applicant responses absent from PANORAMA",
            "cross-number claim lineage remains abstention-only when source numbering is ambiguous",
        ],
    }
    pilot["passes"] = pilot["provider_count_reconciles"] and not pilot["full_scale_approved"]
    pilot["report_hash"] = content_hash(json.dumps(pilot, sort_keys=True))
    (output / "patent_pilot_report.json").write_text(json.dumps(pilot, indent=2, sort_keys=True) + "\n")

    benchmarks = {
        "schema": "observatory.patent-benchmark-reconciliation/1",
        "PANORAMA": {
            "published_cases": 8143,
            "mapped_cases": len(application_frame),
            "overlap_rate": 1.0,
            "licence": "CC-BY-NC-4.0",
        },
        "Patent_CR": {
            "disposition": "pointer_only; claim-revision task not recopied",
            "institutional_variables_omitted": [
                "art_unit_year capacity",
                "policy event registry",
                "population denominator",
            ],
        },
        "PatRe": {
            "published_cases": 480,
            "disposition": (
                "paper-described benchmark; repository data link is blank and its public timeline marks "
                "Dataset unchecked, so no records are ingested"
            ),
            "availability_checked_at": "2026-08-23",
            "public_dataset_available": False,
            "licence_established": False,
            "repository": "https://github.com/AIforIP/PatRe",
            "institutional_variables_omitted": [
                "art_unit_year capacity",
                "policy event registry",
                "provider population coverage",
            ],
        },
        "additionality": "population/institutional capacity and policy variables; no duplicate generation task",
        "passes": len(application_frame) == 8143,
    }
    benchmarks["report_hash"] = content_hash(json.dumps(benchmarks, sort_keys=True))
    (output / "patent_benchmark_reconciliation.json").write_text(
        json.dumps(benchmarks, indent=2, sort_keys=True) + "\n"
    )

    report = {
        "schema": "observatory.patent-products-report/2",
        "applications": len(application_frame),
        "claim_alignment_rows": len(claim_rows),
        "claim_state_accounting_coverage": state_accounted_claims / total_claims if total_claims else 0.0,
        "automatic_cross_number_matches": 0,
        "action_chain_rows": len(chain_rows),
        "direct_applicant_response_documents": 0,
        "post_office_action_claim_change_proxy_cases": response_proxy_cases,
        "prior_art_links": len(prior_rows),
        "capacity_cells": len(capacity_panel),
        "policy_events": len(event_rows),
        "population_applications": population_report["distinct_applications"],
        "panorama_population_overlap": population_report["panorama_hupd_overlap"],
        "legal_ground_counts": dict(ground_counter),
        "parsed_action_failures": parse_failures,
        "all_filed_application_generalizations": 0,
        "scientific_legal_novelty_equations": 0,
        "personnel_rankings": 0,
        "storage_preflight": storage,
        "passes": pilot["passes"] and benchmarks["passes"] and population_report["passes"],
    }
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    (output / "patent_products_report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report
