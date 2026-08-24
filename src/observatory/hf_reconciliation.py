"""Metadata-only reconciliation of public peer-review corpora on the Hub."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .connectors.http import PoliteSession, RatePolicy
from .ids import content_hash
from .registry import ROOT

VIEWER = "https://datasets-server.huggingface.co"
_VIEWER_SESSION = PoliteSession(
    cache_dir=ROOT / "data" / "observatory" / "cache" / "hf-viewer",
    allowed_hosts={"datasets-server.huggingface.co"},
    policy=RatePolicy(min_interval_seconds=0.1, max_retries=4, timeout_seconds=90),
)


@dataclass(frozen=True)
class CorpusSpec:
    corpus_id: str
    hub_id: str | None
    source_role: str
    authority: str


CORPORA = (
    CorpusSpec("reviewarena", "Samarth0710/reviewarena", "OpenReview corpus plus OCR", "derived-C"),
    CorpusSpec("re2", "Daoze/ReviewRebuttal", "initial submissions and review/rebuttal turns", "derived-C"),
    CorpusSpec("researcharcade_papers", "ulab-ai/ResearchArcade-openreview-papers", "paper graph", "derived-C"),
    CorpusSpec("researcharcade_reviews", "ulab-ai/ResearchArcade-openreview-reviews", "review graph", "derived-C"),
    CorpusSpec("researcharcade_revisions", "ulab-ai/ResearchArcade-openreview-revisions", "revision graph", "derived-C"),
    CorpusSpec("review_arcade_arr", "G4KMU/review_arcade", "consented ARR paper/review pairs", "derived-C"),
    CorpusSpec("peerread", "allenai/peer_read", "legacy paper/review corpus", "derived-C"),
    CorpusSpec("nlpeer", None, "licensed multi-source harmonization", "manifest-only"),
    CorpusSpec("moprd", None, "multidisciplinary review/version chains", "manifest-only"),
)

REQUIRED_CORPUS_GROUPS = {
    "ReviewArena": {"reviewarena"},
    "Re2": {"re2"},
    "NLPeer": {"nlpeer"},
    "ResearchArcade": {
        "researcharcade_papers", "researcharcade_reviews", "researcharcade_revisions"
    },
    "PeerRead": {"peerread"},
    "MOPRD": {"moprd"},
    "ARR-consent": {"review_arcade_arr"},
}


def _get(endpoint: str, **params) -> tuple[int, dict[str, Any]]:
    response = _VIEWER_SESSION.get(
        f"{VIEWER}/{endpoint}",
        params=params,
        accepted_statuses=range(400, 600),
    )
    try:
        body = response.json()
    except ValueError:
        body = {"error": response.text[:500]}
    return response.status_code, body


def viewer_metadata(spec: CorpusSpec) -> dict[str, Any]:
    if spec.hub_id is None:
        return {"hub_id": None, "viewer_status": "not_hosted_or_not_publicly_viewable"}
    valid_status, valid = _get("is-valid", dataset=spec.hub_id)
    split_status, splits = _get("splits", dataset=spec.hub_id)
    size_status, size = _get("size", dataset=spec.hub_id)
    parquet_status, parquet = _get("parquet", dataset=spec.hub_id)
    configs = sorted({row.get("config") for row in splits.get("splits") or [] if row.get("config")})
    info_rows = []
    for config in configs[:20]:
        info_status, info = _get("info", dataset=spec.hub_id, config=config)
        dataset_info = info.get("dataset_info") or {}
        info_rows.append({
            "config": config, "http_status": info_status,
            "license": dataset_info.get("license") or None,
            "homepage": dataset_info.get("homepage") or None,
            "citation_present": bool(dataset_info.get("citation")),
        })
    feature_sets = []
    for split in (splits.get("splits") or [])[:20]:
        status, preview = _get(
            "first-rows", dataset=spec.hub_id,
            config=split["config"], split=split["split"],
        )
        feature_sets.append({
            "config": split["config"], "split": split["split"], "http_status": status,
            "features": [row.get("name") for row in preview.get("features") or []],
        })
    return {
        "hub_id": spec.hub_id,
        "api": {
            "is_valid_http_status": valid_status, "is_valid": valid,
            "splits_http_status": split_status, "splits": splits.get("splits") or [],
            "size_http_status": size_status, "size": size.get("size"),
            "size_failures": size.get("failed") or [],
            "parquet_http_status": parquet_status,
            "info": info_rows,
            "parquet_files": [
                {key: row.get(key) for key in ("config", "split", "url", "filename", "size")}
                for row in parquet.get("parquet_files") or []
            ],
            "features": feature_sets,
        },
    }


def _local_iclr_records(root: Path) -> dict[int, dict[str, str]]:
    records: dict[int, dict[str, str]] = defaultdict(dict)
    for path in sorted(root.glob("ICLR_20[0-9][0-9].jsonl")):
        if "_reviews" in path.stem:
            continue
        year = int(path.stem.split("_")[1])
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    row = json.loads(line)
                    forum = row.get("forum") or row.get("id")
                    if forum:
                        records[year][str(forum)] = str(row.get("outcome") or "missing")
    return records


def _load_httpfs(connection) -> None:
    try:
        connection.execute("LOAD httpfs")
    except Exception:
        connection.execute("INSTALL httpfs")
        connection.execute("LOAD httpfs")


def _remote_rows(urls: list[str], columns: str, where: str | None = None) -> list[tuple]:
    import duckdb

    con = duckdb.connect()
    _load_httpfs(con)
    predicate = f" WHERE {where}" if where else ""
    return con.execute(
        f"SELECT {columns} FROM read_parquet(?, union_by_name=true){predicate}", [urls]
    ).fetchall()


def reconcile_openreview_ids(metadata: dict[str, dict[str, Any]], local_root: Path) -> dict[str, Any]:
    local_records = _local_iclr_records(local_root)
    local = {year: set(rows) for year, rows in local_records.items()}
    results: dict[str, Any] = {}

    arena_files = [
        row["url"] for row in metadata["reviewarena"].get("api", {}).get("parquet_files", [])
        if row["split"] == "iclr"
    ]
    if arena_files:
        remote: dict[int, set[str]] = defaultdict(set)
        for forum_id, year in _remote_rows(arena_files, "forum_id, year"):
            remote[int(year)].add(str(forum_id))
        by_year = {}
        for year in sorted(set(local) | set(remote)):
            shared = local[year] & remote[year]
            local_only = local[year] - remote[year]
            corpus_only = remote[year] - local[year]
            by_year[str(year)] = {
                "local": len(local[year]), "corpus": len(remote[year]), "shared": len(shared),
                "local_only": len(local_only), "corpus_only": len(corpus_only),
                "jaccard": len(shared) / len(local[year] | remote[year]) if local[year] | remote[year] else None,
                "shared_local_outcomes": dict(sorted(Counter(
                    local_records[year][forum] for forum in shared
                ).items())),
                "local_only_outcomes": dict(sorted(Counter(
                    local_records[year][forum] for forum in local_only
                ).items())),
                "local_only_id_sample": sorted(local_only)[:10],
                "corpus_only_ids": sorted(corpus_only)[:100],
            }
        results["reviewarena_iclr"] = {
            "key": "OpenReview forum_id", "by_year": by_year,
            "finding": "row-level overlap is measured without retaining manuscript/review text",
        }

    arcade_files = [
        row["url"] for row in metadata["researcharcade_papers"].get("api", {}).get("parquet_files", [])
        if row["split"] == "train"
    ]
    if arcade_files:
        remote_by_year: dict[int, set[str]] = defaultdict(set)
        rows = _remote_rows(
            arcade_files, "paper_openreview_id, venue",
            "venue LIKE 'ICLR.cc/%/Conference'",
        )
        for forum_id, venue in rows:
            try:
                year = int(str(venue).split("/")[1])
            except (IndexError, ValueError):
                continue
            remote_by_year[year].add(str(forum_id))
        by_year = {}
        for year in sorted(set(local) | set(remote_by_year)):
            shared = local[year] & remote_by_year[year]
            local_only = local[year] - remote_by_year[year]
            corpus_only = remote_by_year[year] - local[year]
            by_year[str(year)] = {
                "local": len(local[year]), "corpus": len(remote_by_year[year]), "shared": len(shared),
                "local_only": len(local_only), "corpus_only": len(corpus_only),
                "shared_local_outcomes": dict(sorted(Counter(
                    local_records[year][forum] for forum in shared
                ).items())),
                "local_only_outcomes": dict(sorted(Counter(
                    local_records[year][forum] for forum in local_only
                ).items())),
                "local_only_id_sample": sorted(local_only)[:10],
                "corpus_only_ids": sorted(corpus_only)[:100],
            }
        results["researcharcade_iclr_papers"] = {
            "key": "paper_openreview_id", "by_year": by_year,
        }
    return results


def build_reconciliation_report(local_root: Path) -> dict[str, Any]:
    metadata = {spec.corpus_id: viewer_metadata(spec) for spec in CORPORA}
    report: dict[str, Any] = {
        "schema": "observatory.openreview-derived-reconciliation/1",
        "observed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "method": "Hugging Face Dataset Viewer metadata APIs and Parquet column-range reads; no corpus text retained",
        "corpora": {
            spec.corpus_id: {
                "source_role": spec.source_role, "authority": spec.authority, **metadata[spec.corpus_id],
            }
            for spec in CORPORA
        },
        "id_reconciliation": reconcile_openreview_ids(metadata, local_root),
        "field_authority": {
            "OpenReview_raw_Notes": [
                "note/invitation/forum/reply/original IDs", "readers/signatures", "source timestamps",
                "native rubric values", "public state transitions",
            ],
            "legacy_local_P2": ["existing exploratory outcomes and novelty/rating transforms"],
            "ReviewArena": ["OCR-derived markdown only, subject to manuscript-level rights"],
            "Re2": ["derived initial-version/rebuttal-turn assertions requiring source-note verification"],
            "ResearchArcade": ["candidate revision/link candidates and graph convenience fields"],
            "NLPeer_ARR": ["consented release objects only; never infer non-consenting records"],
        },
        "release_policy": {
            "raw_text": "do not duplicate; retain source pointer/hash unless affirmative object licence is verified",
            "identifiers_and_counts": "retain with dataset citation and retrieval snapshot",
            "identity": "no reviewer profiling or deanonymization; ResearchArcade writer strings are not identity truth",
            "coverage": "no derived corpus establishes OpenReview candidate-pool completeness",
        },
        "unresolved": [
            "Re2 viewer has splits but no materialized viewer size; use release manifest/paper until files are auditable",
            "PeerRead requires legacy loading code and is not supported by Dataset Viewer",
            "NLPeer data requires its documented request/licence workflow and is not silently acquired",
            "MOPRD public distribution and object licences remain unresolved; registry/paper counts only",
            "Review Arcade lacks stable OpenReview IDs in its viewer schema, so title-only overlap is not forced",
        ],
    }
    return finalize_reconciliation_fields(report)


def finalize_reconciliation_fields(report: dict[str, Any]) -> dict[str, Any]:
    """Apply the E4 acceptance decision without reacquiring remote metadata."""
    present = set(report.get("corpora") or {})
    group_status = {
        group: {
            "corpus_ids": sorted(ids),
            "represented": ids <= present,
            "authority": sorted({
                str(report["corpora"][corpus_id].get("authority"))
                for corpus_id in ids if corpus_id in present
            }),
        }
        for group, ids in REQUIRED_CORPUS_GROUPS.items()
    }
    report["required_corpus_groups"] = group_status
    report["scope_decision"] = (
        "Public Viewer metadata and identifier columns are reconciled where free and "
        "licence-safe. NLPeer/MOPRD remain manifest-only because the no-bespoke-"
        "partnership and object-licence gates forbid a request-only or unresolved pull."
    )
    report["raw_text_duplicated"] = False
    report["count_disagreements_explicit"] = bool(report.get("id_reconciliation"))
    report["passes"] = bool(
        all(row["represented"] for row in group_status.values())
        and report.get("field_authority")
        and report.get("release_policy")
        and report["count_disagreements_explicit"]
        and report.get("unresolved")
        and not report["raw_text_duplicated"]
    )
    report["report_hash"] = content_hash(
        json.dumps(
            {key: value for key, value in report.items() if key != "report_hash"},
            sort_keys=True,
        )
    )
    return report


def finalize_reconciliation_report(path: Path) -> Path:
    report = finalize_reconciliation_fields(json.loads(path.read_text()))
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return path


def write_reconciliation_report(local_root: Path, output: Path) -> Path:
    report = build_reconciliation_report(local_root)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return output
