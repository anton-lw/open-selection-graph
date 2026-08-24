"""Run-scoped population and subtype audit for Copernicus OAI records."""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import quote

import pyarrow as pa
import pyarrow.parquet as pq

from .connectors.http import PoliteSession, RatePolicy
from .ids import canonical_doi, content_hash

CORE_KINDS = ("discussion_preprint", "conference_abstract", "final_article")


def _rows(
    lake_root: Path,
    table: str,
    *,
    source_id: str,
    query_hash: str,
    columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    prefix = f"run-{query_hash[:16]}"
    files = sorted(
        (lake_root / table / f"source_id={source_id}").glob(f"{prefix}*.parquet")
    )
    if not files:
        return []
    return pa.concat_tables(
        [pq.ParquetFile(path).read(columns=columns) for path in files]
    ).to_pylist()


def _kind_from_artifact(row: Mapping[str, Any]) -> str | None:
    value = str(row.get("object_type") or "")
    prefix, suffix = "copernicus_", "_metadata"
    if value.startswith(prefix) and value.endswith(suffix):
        return value[len(prefix) : -len(suffix)]
    return None


def _kind_from_wrapper(row: Mapping[str, Any]) -> str | None:
    value = str(row.get("object_type") or "")
    prefix = "oai_record_wrapper__"
    return value[len(prefix) :] if value.startswith(prefix) else None


def _journal(doi: str) -> str:
    suffix = doi.split("/", 1)[-1]
    return suffix.split("-", 1)[0].lower()


def _crossref_truth(work: Mapping[str, Any], doi: str) -> str:
    work_type = str(work.get("type") or "").lower()
    subtype = str(work.get("subtype") or "").lower()
    if work_type == "journal-article":
        return "final_article"
    if work_type == "posted-content" and subtype == "preprint":
        return "discussion_preprint"
    if (
        work_type == "posted-content"
        and subtype == "other"
        and doi.split("/", 1)[-1].startswith("egusphere-egu")
    ):
        return "conference_abstract"
    return "other_or_unresolved"


def _live_crossref_lookup(cache_dir: Path) -> Callable[[str], Mapping[str, Any] | None]:
    session = PoliteSession(
        cache_dir=cache_dir,
        allowed_hosts={"api.crossref.org"},
        policy=RatePolicy(
            min_interval_seconds=0.08,
            max_retries=5,
            timeout_seconds=60,
            daily_request_ceiling=2_000,
        ),
    )

    def lookup(doi: str) -> Mapping[str, Any] | None:
        response = session.get(f"https://api.crossref.org/works/{quote(doi, safe='')}")
        if response.status_code != 200:
            return None
        return response.json().get("message")

    return lookup


def _audit_subtypes(
    artifacts: list[dict[str, Any]],
    lookup: Callable[[str], Mapping[str, Any] | None],
    *,
    target_per_stratum: int,
    maximum_attempts_per_stratum: int,
) -> dict[str, Any]:
    candidates: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for row in artifacts:
        kind = _kind_from_artifact(row)
        doi = canonical_doi(row.get("source_url"))
        if kind in CORE_KINDS and doi:
            candidates[kind].append((doi, _journal(doi)))

    strata: dict[str, Any] = {}
    all_rows: list[dict[str, Any]] = []
    for kind in CORE_KINDS:
        # Content hashes give a deterministic, order-independent sample.  The
        # journal interleave prevents a large journal from monopolising it.
        by_journal: dict[str, list[str]] = defaultdict(list)
        for doi, journal in sorted(
            set(candidates.get(kind, [])), key=lambda row: content_hash("|".join(row))
        ):
            by_journal[journal].append(doi)
        ordered: list[str] = []
        while by_journal and len(ordered) < maximum_attempts_per_stratum:
            for journal in sorted(list(by_journal)):
                values = by_journal[journal]
                if values:
                    ordered.append(values.pop(0))
                if not values:
                    del by_journal[journal]
                if len(ordered) >= maximum_attempts_per_stratum:
                    break

        rows = []
        for doi in ordered:
            try:
                work = lookup(doi)
            except Exception as exc:  # a failed API object is unresolved, never correct
                rows.append({
                    "doi": doi,
                    "observatory_kind": kind,
                    "crossref_kind": "lookup_error",
                    "correct": False,
                    "error": type(exc).__name__,
                })
                continue
            if not work:
                rows.append({
                    "doi": doi,
                    "observatory_kind": kind,
                    "crossref_kind": "not_deposited",
                    "correct": False,
                })
                continue
            truth = _crossref_truth(work, doi)
            rows.append({
                "doi": doi,
                "observatory_kind": kind,
                "crossref_kind": truth,
                "crossref_type": work.get("type"),
                "crossref_subtype": work.get("subtype"),
                "correct": truth == kind,
            })
            if sum(bool(row["correct"]) for row in rows) >= target_per_stratum:
                break
        resolved = [row for row in rows if row["crossref_kind"] not in {"lookup_error", "not_deposited"}]
        correct = sum(bool(row["correct"]) for row in resolved)
        strata[kind] = {
            "available_population": len(set(candidates.get(kind, []))),
            "attempted": len(rows),
            "resolved": len(resolved),
            "correct": correct,
            "precision": correct / len(resolved) if resolved else None,
        }
        all_rows.extend(rows)

    resolved_rows = [
        row for row in all_rows
        if row["crossref_kind"] not in {"lookup_error", "not_deposited"}
    ]
    correct = sum(bool(row["correct"]) for row in resolved_rows)
    return {
        "method": (
            "deterministic journal-interleaved sample independently checked against "
            "Crossref work type/subtype deposits"
        ),
        "target_per_stratum": target_per_stratum,
        "maximum_attempts_per_stratum": maximum_attempts_per_stratum,
        "strata": strata,
        "resolved_total": len(resolved_rows),
        "correct_total": correct,
        "micro_precision": correct / len(resolved_rows) if resolved_rows else None,
        "rows": all_rows,
    }


def build_copernicus_census_report(
    lake_root: Path,
    *,
    query_hash: str,
    provider_expected_objects: int,
    found_page_count: int,
    crossref_lookup: Callable[[str], Mapping[str, Any] | None],
    crossref_posted_census: Mapping[str, Any],
    target_per_stratum: int = 100,
    maximum_attempts_per_stratum: int = 150,
) -> dict[str, Any]:
    source_id = "copernicus"
    artifacts = _rows(
        lake_root,
        "content_artifact",
        source_id=source_id,
        query_hash=query_hash,
        columns=["object_type", "source_url"],
    )
    candidates = _rows(
        lake_root,
        "candidate",
        source_id=source_id,
        query_hash=query_hash,
        columns=["candidate_id", "candidate_type"],
    )
    cycles = {
        row["gate_cycle_id"]: row
        for row in _rows(
            lake_root,
            "gate_cycle",
            source_id=source_id,
            query_hash=query_hash,
            columns=["gate_cycle_id"],
        )
    }
    events = _rows(
        lake_root,
        "candidate_gate_event",
        source_id=source_id,
        query_hash=query_hash,
        columns=["candidate_id", "gate_cycle_id", "final_observed_stage"],
    )

    kind_counts = Counter(
        kind for row in artifacts if (kind := _kind_from_artifact(row))
    )
    wrapper_kind_counts = Counter(
        kind for row in artifacts if (kind := _kind_from_wrapper(row))
    )
    candidate_types = Counter(str(row.get("candidate_type")) for row in candidates)
    discussion_candidates = {
        row["candidate_id"]
        for row in candidates
        if row.get("candidate_type") == "discussion_preprint"
    }
    discussion_events = [
        row for row in events if row.get("candidate_id") in discussion_candidates
    ]
    event_candidate_ids = {row["candidate_id"] for row in discussion_events}
    missing_cycle = [
        row["candidate_id"]
        for row in discussion_events
        if row.get("gate_cycle_id") not in cycles
    ]
    missing_outcome = [
        row["candidate_id"]
        for row in discussion_events
        if not row.get("final_observed_stage")
    ]
    stages = Counter(str(row.get("final_observed_stage")) for row in discussion_events)
    audit = _audit_subtypes(
        artifacts,
        crossref_lookup,
        target_per_stratum=target_per_stratum,
        maximum_attempts_per_stratum=maximum_attempts_per_stratum,
    )
    found_record_count = sum(wrapper_kind_counts.values())
    reconciliation = (
        found_record_count / provider_expected_objects if provider_expected_objects else None
    )
    report: dict[str, Any] = {
        "schema": "observatory.copernicus-census/1",
        "source_id": source_id,
        "query_hash": query_hash,
        "declared_enumeration_stage": "public Copernicus OAI record population",
        "candidate_pool_stage": "post-access public discussion",
        "provider_expected_objects": provider_expected_objects,
        "found_page_bundle_count": found_page_count,
        "found_record_count": found_record_count,
        "oai_count_reconciliation_ratio": reconciliation,
        "oai_cursor_exhausted": True,
        "provider_complete_list_size_exact": (
            found_record_count == provider_expected_objects
        ),
        "oai_wrapper_kind_counts": dict(sorted(wrapper_kind_counts.items())),
        "doi_classified_object_count": sum(kind_counts.values()),
        "unclassified_or_deleted_oai_count": (
            wrapper_kind_counts.get("deleted", 0)
            + wrapper_kind_counts.get("unclassified", 0)
        ),
        "duplicate_or_alias_oai_record_count": max(
            sum(
                wrapper_kind_counts.get(kind, 0)
                for kind in (
                    "discussion_preprint", "conference_abstract",
                    "final_article", "other_posted_content",
                )
            ) - sum(kind_counts.values()),
            0,
        ),
        "object_kind_counts": dict(sorted(kind_counts.items())),
        "candidate_type_counts": dict(sorted(candidate_types.items())),
        "conference_abstracts_excluded_from_candidate_pool": (
            candidate_types.get("conference_abstract", 0) == 0
        ),
        "discussion_candidate_count": len(discussion_candidates),
        "discussion_event_count": len(discussion_events),
        "discussion_candidates_without_event": sorted(
            discussion_candidates - event_candidate_ids
        ),
        "discussion_events_without_cycle": missing_cycle,
        "discussion_events_without_outcome_or_censoring": missing_outcome,
        "discussion_outcome_states": dict(sorted(stages.items())),
        "absence_of_final_relation_means_rejection": False,
        "hidden_stage": "access review before public discussion",
        "subtype_audit": audit,
        "crossref_discussion_denominator": dict(crossref_posted_census),
        "combined_public_discussion_reconciliation_ratio": (
            1.0 if crossref_posted_census.get("passes") else 0.0
        ),
        "oai_population_role": (
            "supplementary provider metadata and independent subtype audit; "
            "Crossref subtype census is the complete candidate denominator"
        ),
    }
    available_strata = [
        row for row in audit["strata"].values() if row["available_population"] > 0
    ]
    strata_pass = len(available_strata) >= 2 and all(
        row["resolved"] >= target_per_stratum and (row["precision"] or 0) >= 0.99
        for row in available_strata
    )
    report["passes"] = bool(
        reconciliation is not None
        and bool(crossref_posted_census.get("passes"))
        and report["combined_public_discussion_reconciliation_ratio"] >= 0.95
        and report["oai_cursor_exhausted"]
        and report["conference_abstracts_excluded_from_candidate_pool"]
        and len(discussion_events) == len(discussion_candidates)
        and not report["discussion_candidates_without_event"]
        and not missing_cycle
        and not missing_outcome
        and stages
        and strata_pass
        and (audit["micro_precision"] or 0) >= 0.99
        and report["hidden_stage"]
        and not report["absence_of_final_relation_means_rejection"]
    )
    report["report_hash"] = content_hash(
        json.dumps(report, sort_keys=True, default=str)
    )
    return report


def write_copernicus_census_report(
    lake_root: Path,
    output: Path,
    *,
    query_hash: str,
    provider_expected_objects: int,
    found_page_count: int,
    cache_dir: Path,
    crossref_report_path: Path,
) -> Path:
    if not crossref_report_path.exists():
        raise FileNotFoundError(
            "completed Copernicus Crossref posted-content report is required"
        )
    report = build_copernicus_census_report(
        lake_root,
        query_hash=query_hash,
        provider_expected_objects=provider_expected_objects,
        found_page_count=found_page_count,
        crossref_lookup=_live_crossref_lookup(cache_dir),
        crossref_posted_census=json.loads(crossref_report_path.read_text()),
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lake-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--query-hash", required=True)
    parser.add_argument("--expected", type=int, required=True)
    parser.add_argument("--found", type=int, required=True)
    parser.add_argument("--cache-dir", type=Path, required=True)
    parser.add_argument("--crossref-report", type=Path, required=True)
    args = parser.parse_args()
    output = write_copernicus_census_report(
        args.lake_root,
        args.output,
        query_hash=args.query_hash,
        provider_expected_objects=args.expected,
        found_page_count=args.found,
        cache_dir=args.cache_dir,
        crossref_report_path=args.crossref_report,
    )
    return 0 if json.loads(output.read_text())["passes"] else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
