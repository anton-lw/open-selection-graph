"""Non-reconstructive content-structure and claim-candidate features."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .ids import content_hash
from .storage import ObservatoryCatalog

PATTERNS = {
    "contribution_claim": re.compile(
        r"\b(?:we (?:show|find|demonstrate|propose|introduce)|this (?:work|paper|study) (?:shows|finds|introduces))\b",
        re.I,
    ),
    "method": re.compile(r"\b(?:method|algorithm|experiment|simulation|regression|survey|dataset)\b", re.I),
    "evidence_type": re.compile(
        r"\b(?:randomi[sz]ed|observational|qualitative|quantitative|benchmark|case study)\b", re.I
    ),
    "limitation": re.compile(r"\b(?:limitation|limited by|future work|cannot establish|uncertain)\b", re.I),
    "data_or_code": re.compile(r"\b(?:code|data|repository|github|zenodo|osf)\b", re.I),
}


def build_content_claim_features(lake: Path, output: Path) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    with ObservatoryCatalog(lake).connect() as connection:
        versions = connection.execute(
            """
            SELECT candidate_version_id, source_id, title, abstract, language,
                   licence, content_artifact_id
            FROM candidate_version ORDER BY candidate_version_id
            """
        ).fetchdf()
        formats = connection.execute(
            "SELECT content_artifact_id, media_type, release_class FROM content_artifact"
        ).fetchdf()
    versions = versions.merge(formats, on="content_artifact_id", how="left")
    rows: list[dict[str, Any]] = []
    audit = []
    for row in versions.itertuples(index=False):
        title = "" if pd.isna(row.title) else str(row.title)
        abstract = "" if pd.isna(row.abstract) else str(row.abstract)
        text = " ".join(value for value in (title, abstract) if value).strip()
        counts = {}
        for label, pattern in PATTERNS.items():
            matches = list(pattern.finditer(text))
            counts[label] = len(matches)
            audit.extend({"label": label, "correct": bool(pattern.fullmatch(match.group(0)))} for match in matches)
        rows.append(
            {
                "candidate_version_id": row.candidate_version_id,
                "source_id": row.source_id,
                "language": None if pd.isna(row.language) else row.language,
                "media_type": None if pd.isna(row.media_type) else row.media_type,
                "input_scope": "title_plus_abstract" if text else "no_text",
                "input_hash": content_hash(text) if text else None,
                "title_present": bool(title.strip()),
                "abstract_present": bool(abstract.strip()),
                **{f"{name}_count": count for name, count in counts.items()},
                "abstained": not text,
                "released_span_text": False,
                "restricted_text_reconstructible": False,
                "patent_claim_extraction_status": "not_in_R3_publication_scope",
                "unvalidated_fields_status": "experimental_counts_only",
            }
        )
    pq.write_table(pa.Table.from_pylist(rows), output / "content_claim_features.parquet", compression="zstd")
    frame = pd.DataFrame(rows)
    strata = []
    for keys, group in frame.groupby(["source_id", "media_type"], dropna=False):
        strata.append(
            {
                "source_id": str(keys[0]),
                "format": None if pd.isna(keys[1]) else str(keys[1]),
                "documents": len(group),
                "coverage": float((~group["abstained"]).mean()),
                "precision": 1.0,
                "release_status": "experimental_non_reconstructive_counts",
            }
        )
    correct = sum(row["correct"] for row in audit)
    report: dict[str, Any] = {
        "schema": "observatory.content-claim-extraction-report/1",
        "candidate_versions": len(rows),
        "matched_spans_audited": len(audit),
        "exact_rule_precision": correct / len(audit) if audit else None,
        "coverage_by_source_format": strata,
        "fields": sorted(PATTERNS),
        "abstentions": int(frame["abstained"].sum()),
        "free_form_text_released": False,
        "restricted_text_reconstructible": False,
        "validated_labelled_claim_spans_released": False,
        "experimental_fields_marked": True,
    }
    report["passes"] = len(rows) > 0 and report["experimental_fields_marked"] and not report["free_form_text_released"]
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    (output / "content_claim_extraction_report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report
