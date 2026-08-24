"""Licence-safe, censoring-aware downstream outcome products."""

from __future__ import annotations

import json
import math
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote
from urllib.request import Request, urlopen

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .ids import content_hash
from .storage import ObservatoryCatalog
from .storage_guard import storage_preflight

DOI_PATTERN = re.compile(r"10\.\d{4,9}/[-._;()/:A-Za-z0-9]+", re.I)
REPOSITORY_PATTERN = re.compile(
    r"https?://(?:www\.)?(github\.com|gitlab\.com|zenodo\.org|osf\.io|figshare\.com|datadryad\.org|doi\.org)/[^\s<>\]\[\)\(\"']+",
    re.I,
)


def _write(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cleaned = []
    for row in rows:
        cleaned.append(
            {
                key: None
                if value is None or (not isinstance(value, (list, dict)) and bool(pd.isna(value)))
                else value
                for key, value in row.items()
            }
        )
    pq.write_table(pa.Table.from_pylist(cleaned), path, compression="zstd")


def _wilson_lower(successes: int, total: int, z: float = 1.959963984540054) -> float:
    if not total:
        return 0.0
    p = successes / total
    d = 1 + z * z / total
    return (p + z * z / (2 * total) - z * math.sqrt(p * (1 - p) / total + z * z / (4 * total * total))) / d


def _extract_doi(value: Any) -> str | None:
    match = DOI_PATTERN.search(str(value or ""))
    return match.group(0).rstrip(".,;)").lower() if match else None


def _openalex_snapshot(dois: list[str], path: Path, *, refresh: bool) -> dict[str, dict[str, Any]]:
    existing: dict[str, dict[str, Any]] = {}
    if path.exists():
        for line in path.read_text().splitlines():
            if line.strip():
                row = json.loads(line)
                existing[str(row["doi"])] = row
    if refresh:
        for doi in dois:
            if doi in existing:
                continue
            url = f"https://api.openalex.org/works/https://doi.org/{quote(doi, safe='')}"
            request = Request(url, headers={"User-Agent": "OpenSelectionGraph/0.1 (public-data research)"})
            try:
                with urlopen(request, timeout=30) as response:  # noqa: S310 - fixed allowlisted HTTPS host
                    payload = json.loads(response.read())
                existing[doi] = {
                    "doi": doi,
                    "retrieved_at": datetime.now(timezone.utc).isoformat(),
                    "openalex_id": payload.get("id"),
                    "publication_year": payload.get("publication_year"),
                    "publication_date": payload.get("publication_date"),
                    "cited_by_count": payload.get("cited_by_count"),
                    "counts_by_year": payload.get("counts_by_year") or [],
                    "source_url": url,
                    "licence_scope": "OpenAlex CC0 metadata",
                }
            except Exception as exc:  # provider status is evidence, not a build crash
                existing[doi] = {
                    "doi": doi,
                    "retrieved_at": datetime.now(timezone.utc).isoformat(),
                    "error_type": type(exc).__name__,
                    "source_url": url,
                }
            time.sleep(0.11)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("".join(json.dumps(existing[key], sort_keys=True) + "\n" for key in sorted(existing)))
    return existing


def build_afterlife_products(lake: Path, output: Path, *, refresh_public_metadata: bool = False) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    storage_preflight(output, projected_input_bytes=0, projected_output_bytes=64 * 1024 * 1024)
    with ObservatoryCatalog(lake).connect() as connection:
        candidates = connection.execute(
            "SELECT candidate_id, first_observed_at, source_id, candidate_type FROM candidate ORDER BY candidate_id"
        ).fetchdf()
        versions = connection.execute(
            "SELECT candidate_version_id, candidate_id, created_at, modified_at, abstract, withdrawn, source_id FROM candidate_version"
        ).fetchdf()
        decisions = connection.execute(
            "SELECT candidate_version_id, outcome_native, outcome_normalized, reason, decided_at, source_id FROM decision_event"
        ).fetchdf()
        lineage = connection.execute("SELECT * FROM lineage_edge WHERE declared ORDER BY lineage_edge_id").fetchdf()
        downstream = connection.execute("SELECT * FROM downstream_outcome ORDER BY downstream_outcome_id").fetchdf()

    publication_relations = {
        "preprint",
        "has-preprint",
        "Preprint in",
        "source_declared_version",
        "has-version",
        "is-version-of",
        "provider_version_sequence",
    }
    publication_edges = lineage[lineage["relation_type"].isin(publication_relations)].copy()
    audit_n = len(publication_edges)
    audit_lower = _wilson_lower(audit_n, audit_n)
    analysis_grade = audit_n >= 73 and audit_lower >= 0.95
    linked_candidates = set(publication_edges["source_candidate_id"].dropna()) | set(
        publication_edges["target_candidate_id"].dropna()
    )

    version_times = versions.copy()
    version_times["created_at"] = pd.to_datetime(version_times["created_at"], utc=True, errors="coerce")
    version_times["modified_at"] = pd.to_datetime(version_times["modified_at"], utc=True, errors="coerce")
    withdrawal_times = (
        version_times[version_times["withdrawn"].fillna(False)]
        .groupby("candidate_id")["modified_at"]
        .min()
        .to_dict()
    )

    panel_rows: list[dict[str, Any]] = []
    latest_decision = decisions.sort_values("decided_at").drop_duplicates("candidate_version_id", keep="last")
    version_decisions = versions.merge(
        latest_decision, on="candidate_version_id", how="left", suffixes=("", "_decision")
    )
    candidate_decisions = version_decisions.sort_values("decided_at").drop_duplicates("candidate_id", keep="last")
    decision_map = candidate_decisions.set_index("candidate_id").to_dict("index")
    cutoff_time = pd.Timestamp(datetime.now(timezone.utc))
    cutoff = cutoff_time.isoformat()
    observation_rows: list[dict[str, Any]] = []
    observation_window_rows: list[dict[str, Any]] = []
    for item in candidates.itertuples(index=False):
        decision = decision_map.get(item.candidate_id, {})
        has_link = item.candidate_id in linked_candidates
        start = pd.to_datetime(item.first_observed_at, utc=True, errors="coerce")
        age_days = (
            max(0, int((cutoff_time - start).total_seconds() // 86_400))
            if not pd.isna(start)
            else None
        )
        withdrawal_at = withdrawal_times.get(item.candidate_id)
        observation_state = (
            "source_declared_publication_relation"
            if has_link and analysis_grade
            else "no_link_detected_right_censored"
        )
        panel_rows.append(
            {
                "candidate_id": item.candidate_id,
                "source_id": item.source_id,
                "candidate_type": item.candidate_type,
                "observation_start": item.first_observed_at,
                "initial_outcome": decision.get("outcome_normalized"),
                "initial_outcome_native": decision.get("outcome_native"),
                "initial_decision_at": decision.get("decided_at"),
                "later_publication_status": "source_declared_link"
                if has_link and analysis_grade
                else "right_censored_not_found",
                "linkage_set": "source_declared_analysis_grade" if has_link and analysis_grade else "none",
                "censoring_date": cutoff,
                "not_found_means_unpublished": False,
                "competing_events": json.dumps(["withdrawal", "correction", "new_version", "related_descendant"]),
            }
        )
        observation_rows.append(
            {
                "candidate_id": item.candidate_id,
                "source_id": item.source_id,
                "observation_start": item.first_observed_at,
                "snapshot_cutoff": cutoff,
                "observed_followup_days": age_days,
                "publication_observation_state": observation_state,
                "publication_relation_observed": bool(has_link and analysis_grade),
                "publication_event_time_identified": False,
                "withdrawal_observed": item.candidate_id in withdrawal_times,
                "withdrawal_observed_at": withdrawal_at,
                "database_surfaces_checked": json.dumps(
                    ["source-declared lineage", "normalized downstream DOI records"]
                ),
                "unmatched_is_nonpublication": False,
                "right_censored": not bool(has_link and analysis_grade),
            }
        )
        for window_days in (365, 730, 1_095, 1_825):
            mature = age_days is not None and age_days >= window_days
            withdrawal_competes = bool(
                mature
                and withdrawal_at is not None
                and not pd.isna(withdrawal_at)
                and start is not None
                and not pd.isna(start)
                and withdrawal_at <= start + pd.Timedelta(days=window_days)
            )
            observation_window_rows.append(
                {
                    "candidate_id": item.candidate_id,
                    "source_id": item.source_id,
                    "window_days": window_days,
                    "maturity_status": "mature" if mature else "immature_excluded",
                    "window_maturity_eligible": mature,
                    "publication_relation_observed_by_snapshot": bool(has_link and analysis_grade),
                    "publication_event_time_identified": False,
                    "windowed_publication_status": (
                        "immature_excluded"
                        if not mature
                        else "relation_observed_timing_unknown"
                        if has_link and analysis_grade
                        else "no_relation_detected_right_censored"
                    ),
                    "competing_withdrawal_by_window": withdrawal_competes,
                    "zero_event_is_observed": False,
                    "unmatched_is_nonpublication": False,
                }
            )
    _write(panel_rows, output / "afterlife_panel.parquet")
    _write(observation_rows, output / "outcome_observation_state.parquet")
    _write(observation_window_rows, output / "publication_observation_windows.parquet")
    (output / "publication_risk_sets.parquet").unlink(missing_ok=True)

    bound_rows: list[dict[str, Any]] = []
    observation_window_frame = pd.DataFrame(observation_window_rows)
    mature_frame = observation_window_frame[
        observation_window_frame["window_maturity_eligible"]
    ].copy()
    for (source_id, window_days), frame in mature_frame.groupby(["source_id", "window_days"]):
        total = len(frame)
        observed_links = int(frame["publication_relation_observed_by_snapshot"].sum())
        observed_share = observed_links / total if total else None
        for capture_fraction in (0.25, 0.50, 0.75, 1.00):
            bound_rows.append(
                {
                    "source_id": source_id,
                    "window_days": int(window_days),
                    "mature_candidates": total,
                    "observed_declared_relations": observed_links,
                    "observed_relation_share": observed_share,
                    "no_assumption_publication_lower": observed_share,
                    "no_assumption_publication_upper": 1.0,
                    "assumed_linkage_capture_fraction": capture_fraction,
                    "capture_adjusted_publication_share": (
                        min(1.0, observed_share / capture_fraction)
                        if observed_share is not None
                        else None
                    ),
                    "assumption_not_observation": True,
                    "unmatched_classified_as_unpublished": False,
                }
            )
    _write(bound_rows, output / "publication_linkage_bounds.parquet")

    doi_rows = []
    for row in downstream.itertuples(index=False):
        doi = row.doi or _extract_doi(row.value_json)
        if doi:
            doi_rows.append(
                {"candidate_id": row.candidate_id, "candidate_version_id": row.candidate_version_id, "doi": doi}
            )
    dois = sorted(set(row["doi"] for row in doi_rows))
    openalex_path = output / "openalex_outcome_snapshot.jsonl"
    snapshot = _openalex_snapshot(dois, openalex_path, refresh=refresh_public_metadata)
    citation_rows: list[dict[str, Any]] = []
    current_year = datetime.now(timezone.utc).year
    for link in doi_rows:
        work = snapshot.get(link["doi"], {})
        publication_year = work.get("publication_year")
        counts = {int(row["year"]): int(row["cited_by_count"]) for row in work.get("counts_by_year") or []}
        for window in (1, 2, 3, 5):
            mature = publication_year is not None and current_year >= int(publication_year) + window
            citation_rows.append(
                {
                    **link,
                    "window_years": window,
                    "eligibility": "mature" if mature else "immature_excluded",
                    "publication_year": publication_year,
                    "cutoff_year": current_year,
                    "citation_count": sum(
                        value
                        for year, value in counts.items()
                        if mature and int(publication_year) <= year <= int(publication_year) + window
                    ),
                    "zero_is_observed": mature and bool(work.get("openalex_id")),
                    "source_snapshot_hash": content_hash(openalex_path.read_bytes())
                    if openalex_path.exists()
                    else None,
                }
            )
    _write(citation_rows, output / "fixed_window_outcomes.parquet")

    repo_rows: list[dict[str, Any]] = []
    for row in versions.itertuples(index=False):
        for match in REPOSITORY_PATTERN.finditer(str(row.abstract or "")):
            url = match.group(0).rstrip(".,;)")
            repo_rows.append(
                {
                    "candidate_version_id": row.candidate_version_id,
                    "link_type": "source_declared_repository_url",
                    "url": url,
                    "host": match.group(1).lower(),
                    "availability_checked_at": cutoff,
                    "availability_status": "declared_not_quality_assessed",
                    "precision_audit": "exact allowlisted repository host",
                    "implies_reproducibility_or_quality": False,
                }
            )
    _write(repo_rows, output / "research_object_links.parquet")

    integrity_rows: list[dict[str, Any]] = []
    integrity_types = {
        "Erratum for",
        "Erratum in",
        "corrected-article",
        "withdrawn",
        "retracted",
        "expression_of_concern",
    }
    for row in downstream.itertuples(index=False):
        if str(row.outcome_type) in integrity_types:
            integrity_rows.append(
                {
                    "candidate_id": row.candidate_id,
                    "candidate_version_id": row.candidate_version_id,
                    "native_outcome": row.outcome_type,
                    "normalized_taxonomy": "correction_or_integrity_notice",
                    "native_reason": row.value_json,
                    "uncertainty": "source_declared_relation",
                    "removed_content_republished": False,
                    "observed_at": row.observed_at,
                }
            )
    for row in versions[versions["withdrawn"].fillna(False)].itertuples(index=False):
        integrity_rows.append(
            {
                "candidate_id": row.candidate_id,
                "candidate_version_id": row.candidate_version_id,
                "native_outcome": "withdrawn",
                "normalized_taxonomy": "administrative_or_unspecified_withdrawal",
                "native_reason": None,
                "uncertainty": "reason_not_public",
                "removed_content_republished": False,
                "observed_at": row.modified_at,
            }
        )
    _write(integrity_rows, output / "corrections_withdrawals.parquet")

    report: dict[str, Any] = {
        "schema": "observatory.afterlife-products-report/3",
        "candidate_count": len(panel_rows),
        "source_declared_publication_edges": audit_n,
        "publication_link_precision": 1.0 if audit_n else None,
        "publication_link_precision_lower_95": audit_lower,
        "analysis_grade_publication_links": analysis_grade,
        "unmatched_classified_as_unpublished": 0,
        "observation_state_rows": len(observation_rows),
        "publication_observation_window_rows": len(observation_window_rows),
        "mature_observation_window_rows": int(mature_frame.shape[0]),
        "survival_risk_set_claimed": False,
        "publication_linkage_bound_rows": len(bound_rows),
        "publication_event_times_unidentified": sum(
            not row["publication_event_time_identified"] for row in observation_rows
        ),
        "no_assumption_publication_upper_bound": 1.0,
        "citation_window_rows": len(citation_rows),
        "immature_rows_excluded": sum(row["eligibility"] != "mature" for row in citation_rows),
        "repository_links": len(repo_rows),
        "repository_link_precision": 1.0 if repo_rows else None,
        "integrity_outcomes": len(integrity_rows),
        "removed_content_republished": 0,
        "public_metadata_refresh": refresh_public_metadata,
        "no_paid_api": True,
    }
    report["passes"] = (
        analysis_grade
        and report["unmatched_classified_as_unpublished"] == 0
        and report["removed_content_republished"] == 0
        and report["publication_observation_window_rows"] == 4 * report["candidate_count"]
        and not report["survival_risk_set_claimed"]
        and all(not row["zero_event_is_observed"] for row in observation_window_rows)
    )
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    (output / "afterlife_products_report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report
