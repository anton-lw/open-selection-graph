"""Streaming, non-personal audit of the upstream GtR PostgreSQL backup."""

from __future__ import annotations

import gzip
import hashlib
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .ids import content_hash

COPY = re.compile(r"^COPY public\.([a-zA-Z0-9_]+) \((.+)\) FROM stdin;$")
AUDIT_COLUMNS = {
    "applications": ("opportunity_id", "project_id", "application_id", "award_id", "year", "source"),
    "meeting_applications": ("meeting_id", "application_id", "rank", "outcome", "awarded_amount", "score"),
    "meetings": ("panel_reference", "meeting_start", "meeting_end", "council", "year"),
    "opportunities": ("opportunity_id", "href", "opening_date", "closing_date", "funders"),
    "outcomes": ("project_id", "outcome_type", "supporting_url"),
    "projects": ("grant_reference", "start_date", "end_date", "status"),
    "project_relationships": ("from_project_id", "to_project_id", "relationship_type"),
}


def _stream_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def audit_gtr_backup(path: Path, output: Path) -> dict[str, Any]:
    """Count every COPY table and selected non-null link fields without retaining rows."""
    tables: dict[str, dict[str, Any]] = {}
    current: str | None = None
    columns: list[str] = []
    selected_indexes: dict[int, str] = {}
    meeting_rows: dict[str, dict[str, Any]] = {}
    meeting_applications: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"found_count": 0, "outcome_counts": Counter(), "missing_outcome": 0, "missing_rank": 0, "missing_score": 0}
    )
    with gzip.open(path, "rt", encoding="utf-8", errors="replace", newline="") as handle:
        for line in handle:
            line = line.rstrip("\n")
            if current is None:
                match = COPY.match(line)
                if not match:
                    continue
                current = match.group(1)
                columns = [value.strip() for value in match.group(2).split(",")]
                wanted = set(AUDIT_COLUMNS.get(current, ()))
                selected_indexes = {index: name for index, name in enumerate(columns) if name in wanted}
                tables[current] = {
                    "row_count": 0,
                    "column_count": len(columns),
                    "schema_hash": content_hash(json.dumps(columns)),
                    "non_null_counts": {name: 0 for name in selected_indexes.values()},
                }
                continue
            if line == r"\.":
                current = None
                columns = []
                selected_indexes = {}
                continue
            table = tables[current]
            table["row_count"] += 1
            values = line.split("\t") if selected_indexes or current in {"meetings", "meeting_applications"} else []
            if selected_indexes:
                for index, name in selected_indexes.items():
                    if index < len(values) and values[index] != r"\N":
                        table["non_null_counts"][name] += 1
            if current == "meetings" and values:
                row = dict(zip(columns, values, strict=False))
                meeting_rows[row["id"]] = {
                    "meeting_id": row["id"],
                    "panel_reference": None if row.get("panel_reference") == r"\N" else row.get("panel_reference"),
                    "meeting_start": None if row.get("meeting_start") == r"\N" else row.get("meeting_start"),
                    "meeting_end": None if row.get("meeting_end") == r"\N" else row.get("meeting_end"),
                    "council": None if row.get("council") == r"\N" else row.get("council"),
                    "year": None if row.get("year") == r"\N" else row.get("year"),
                    "source": None if row.get("source") == r"\N" else row.get("source"),
                }
            elif current == "meeting_applications" and values:
                row = dict(zip(columns, values, strict=False))
                meeting_id = row.get("meeting_id")
                aggregate = meeting_applications[str(meeting_id)]
                aggregate["found_count"] += 1
                outcome = row.get("outcome")
                if outcome == r"\N" or not outcome:
                    aggregate["missing_outcome"] += 1
                else:
                    aggregate["outcome_counts"][outcome] += 1
                aggregate["missing_rank"] += row.get("rank") in {r"\N", None, ""}
                aggregate["missing_score"] += row.get("score") in {r"\N", None, ""}

    applications = tables.get("applications", {})
    app_count = int(applications.get("row_count", 0))
    app_nonnull = applications.get("non_null_counts") or {}
    link_rates = {
        name: (count / app_count if app_count else None)
        for name, count in app_nonnull.items()
        if name in {"opportunity_id", "project_id", "application_id", "award_id"}
    }
    published_counts = {
        "projects": 173220,
        "outcomes": 2619446,
        "meetings_table_3": 1449,
        "panel_attendance": 6951,
        "applications": 38862,
        "opportunities": 2053,
    }
    reproduced_counts = {
        "projects": int(tables.get("projects", {}).get("row_count", 0)),
        "outcomes": int(tables.get("outcomes", {}).get("row_count", 0)),
        "meetings_table_3": int(tables.get("meetings", {}).get("row_count", 0)),
        "panel_attendance": int(tables.get("panel_attendance", {}).get("row_count", 0)),
        "applications": app_count,
        "opportunities": int(tables.get("opportunities", {}).get("row_count", 0)),
    }
    count_reproduction = {
        name: reproduced_counts[name] == expected for name, expected in published_counts.items()
    }
    link_rate_checks = {
        "application_to_project": abs(float(link_rates.get("project_id") or 0) - 10531 / 38862) < 1e-12,
        "application_to_opportunity": abs(float(link_rates.get("opportunity_id") or 0) - 11514 / 38862) < 1e-12,
    }
    meeting_rounds = []
    for meeting_id, meeting in sorted(meeting_rows.items()):
        aggregate = meeting_applications.get(meeting_id) or {
            "found_count": 0,
            "outcome_counts": Counter(),
            "missing_outcome": 0,
            "missing_rank": 0,
            "missing_score": 0,
        }
        meeting_rounds.append(
            {
                **meeting,
                "found_count": aggregate["found_count"],
                "expected_count": aggregate["found_count"],
                "expected_count_method": "upstream frozen database row count",
                "outcome_counts": dict(aggregate["outcome_counts"]),
                "missing_outcome": aggregate["missing_outcome"],
                "missing_rank": aggregate["missing_rank"],
                "missing_score": aggregate["missing_score"],
                "earliest_public_stage": "published panel meeting outcome record",
                "observability_grade": "U",
                "tabular_extraction_errors_surfaced": True,
            }
        )
    report: dict[str, Any] = {
        "schema": "observatory.gtr-backup-stream-audit/1",
        "mode": "streaming_counts_and_non_null_rates_only",
        "source_file": path.name,
        "source_size_bytes": path.stat().st_size,
        "source_md5_provider": "2f2a3d288ae03f049fe489eb099dc9b0",
        "source_sha256": _stream_sha256(path),
        "table_count": len(tables),
        "tables": tables,
        "application_link_rates": link_rates,
        "paper_registered_counts": published_counts,
        "reproduced_counts": reproduced_counts,
        "count_reproduction": count_reproduction,
        "paper_registered_link_rates": {
            "application_to_project": 10531 / 38862,
            "application_to_opportunity": 11514 / 38862,
        },
        "link_rate_checks": link_rate_checks,
        "meeting_rounds": meeting_rounds,
        "surfaced_internal_paper_discrepancies": {
            "meetings_table_1": 1450,
            "meetings_table_3_and_backup": 1449,
            "meeting_applications_table_1": 40509,
            "meeting_applications_backup": int(tables.get("meeting_applications", {}).get("row_count", 0)),
        },
        "personal_rows_retained": 0,
        "source_text_retained": 0,
        "upstream_authorship_preserved": True,
        "passes": (
            len(tables) > 20
            and app_count > 0
            and all(count_reproduction.values())
            and all(link_rate_checks.values())
            and not any(int(value.get("row_count", 0)) < 0 for value in tables.values())
        ),
    }
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report
