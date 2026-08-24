"""Reconstruct Copernicus public-review chains from Crossref relations."""

from __future__ import annotations

import gzip
import json
import re
from collections import Counter, defaultdict
from itertools import chain
from pathlib import Path
from typing import Any

from .adapters.copernicus import _object_kind
from .crossref_raw import iter_crossref_peer_reviews
from .ids import canonical_doi, content_hash


def _iter_copernicus_posted_content(raw_root: Path):
    manifest = raw_root / "manifests" / "copernicus_crossref.jsonl"
    if not manifest.exists():
        return
    latest: dict[str, dict[str, Any]] = {}
    for line in manifest.read_text().splitlines():
        row = json.loads(line)
        if row.get("object_type") == "copernicus_posted_content_page":
            latest[row["source_object_id"]] = row
    for receipt in latest.values():
        page = json.loads(gzip.decompress(Path(receipt["raw_pointer"]).read_bytes()))
        for index, work in enumerate(page.get("items") or []):
            yield {"receipt": receipt, "work": work, "item_index": index}


def comment_role(doi: str, title: object = None, subtype: object = None) -> str:
    suffix = doi.lower().rsplit("-", 1)[-1]
    if re.fullmatch(r"rc\d+", suffix):
        return "referee_comment"
    if re.fullmatch(r"ac\d+", suffix):
        return "author_comment"
    if re.fullmatch(r"cc\d+", suffix):
        return "community_comment"
    if re.fullmatch(r"ec\d+", suffix):
        return "editor_comment"
    if re.fullmatch(r"sc\d+", suffix):
        return "short_comment"
    text = f"{title or ''} {subtype or ''}".lower()
    for needle, role in (
        ("referee", "referee_comment"), ("author", "author_comment"),
        ("editor", "editor_comment"), ("community", "community_comment"),
    ):
        if needle in text:
            return role
    return "comment_unresolved"


def _deposited_year(work: dict[str, Any]) -> int | None:
    for key in ("published", "published-online", "published-print", "issued"):
        parts = (work.get(key) or {}).get("date-parts") or []
        if parts and parts[0]:
            try:
                return int(parts[0][0])
            except (TypeError, ValueError, IndexError):
                pass
    created = str((work.get("created") or {}).get("date-time") or "")
    match = re.match(r"^(\d{4})-", created)
    return int(match.group(1)) if match else None


def build_copernicus_chains(raw_root: Path) -> dict[str, Any]:
    chains: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"comments": [], "final_article_dois": set(), "relation_types": set()}
    )
    roles: Counter[str] = Counter()
    relation_types: Counter[str] = Counter()
    unresolved_target_comments = 0
    objects_scanned = 0
    copernicus_comments = 0
    posted_preprints: set[str] = set()
    posted_years: dict[str, int | None] = {}
    for bundled in _iter_copernicus_posted_content(raw_root) or ():
        work = bundled["work"]
        doi = canonical_doi(work.get("DOI"))
        if doi and str(work.get("subtype") or "").lower() == "preprint":
            posted_preprints.add(doi)
            posted_years[doi] = _deposited_year(work)
    seen_comment_targets: set[tuple[str, str]] = set()
    for bundled in chain(
        iter_crossref_peer_reviews(raw_root),
        _iter_copernicus_posted_content(raw_root) or (),
    ):
        receipt = bundled["receipt"]
        objects_scanned += 1
        work = bundled["work"]
        review_doi = canonical_doi(work.get("DOI"))
        if not review_doi or not (
            review_doi.startswith("10.5194/")
            or "copernicus" in str(work.get("publisher") or "").lower()
        ):
            continue
        if str(work.get("subtype") or "").lower() == "preprint":
            continue
        copernicus_comments += 1
        role = comment_role(review_doi, work.get("title"), work.get("subtype"))
        roles[role] += 1
        targets: list[tuple[str, str]] = []
        for relation_type, values in (work.get("relation") or {}).items():
            for value in values or []:
                target = canonical_doi(value.get("id"))
                if target:
                    targets.append((str(relation_type), target))
                    relation_types[str(relation_type)] += 1
        discussions = sorted({
            target for _, target in targets
            if target in posted_preprints
            or _object_kind(target, []) == "discussion_preprint"
        })
        finals = sorted({
            target for _, target in targets if _object_kind(target, []) == "other_posted_content"
            and target.startswith("10.5194/")
        })
        # A final Copernicus DOI has journal-volume-page-year and therefore is
        # conservatively classified here only when it is not a comment DOI.
        finals = [target for target in finals if not re.search(r"-(?:rc|ac|cc|ec|sc)\d+$", target)]
        if not discussions:
            unresolved_target_comments += 1
            continue
        comment = {
            "comment_doi": review_doi, "role": role,
            "created_at": (work.get("created") or {}).get("date-time"),
            "stage_native": work.get("stage"), "recommendation_native": work.get("recommendation"),
            "source_url": (work.get("resource") or {}).get("primary", {}).get("URL") or work.get("URL"),
            "relation_types": sorted({relation_type for relation_type, _ in targets}),
            "source_object_id": receipt["source_object_id"],
            "raw_bundle_byte_hash": receipt["byte_hash"],
            "raw_bundle_item_index": bundled["item_index"],
        }
        for discussion in discussions:
            comment_target = (review_doi, discussion)
            if comment_target in seen_comment_targets:
                continue
            seen_comment_targets.add(comment_target)
            chains[discussion]["comments"].append(comment)
            chains[discussion]["final_article_dois"].update(finals)
            chains[discussion]["relation_types"].update(comment["relation_types"])
    records = []
    for discussion, chain_row in sorted(chains.items()):
        comments = sorted(
            chain_row["comments"],
            key=lambda row: (row.get("created_at") or "", row["comment_doi"]),
        )
        finals = sorted(chain_row["final_article_dois"])
        doi_year = re.search(r"-(\d{4})-", discussion)
        year = posted_years.get(discussion)
        if year is None and doi_year:
            year = int(doi_year.group(1))
        records.append({
            "discussion_doi": discussion,
            "journal": discussion.split("/", 1)[1].split("-", 1)[0],
            "year": year,
            "comments": comments, "comment_count": len(comments),
            "roles": dict(Counter(row["role"] for row in comments)),
            "final_article_dois": finals,
            "outcome_state": "published_final_observed" if finals else "public_discussion_censored",
            "relation_types": sorted(chain_row["relation_types"]),
        })
    report: dict[str, Any] = {
        "schema": "observatory.copernicus-public-review-chains/1",
        "crossref_posted_preprint_population": len(posted_preprints),
        "objects_scanned": objects_scanned, "copernicus_comment_objects": copernicus_comments,
        "discussion_chains": len(records), "comments_linked": sum(row["comment_count"] for row in records),
        "comments_without_discussion_target": unresolved_target_comments,
        "roles": dict(sorted(roles.items())), "relation_types": dict(sorted(relation_types.items())),
        "published_final_chains": sum(row["outcome_state"] == "published_final_observed" for row in records),
        "censored_chains": sum(row["outcome_state"] == "public_discussion_censored" for row in records),
        "important_scope_rule": (
            "absence of a final-article relation is censoring, not rejection; rejected status requires "
            "an affirmative provider-visible outcome"
        ),
        "relation_audit": {
            "status": "pending_stratified_manual_audit",
            "required_precision": 0.98,
            "sample_frame": "role x journal x year x relation-type strata",
        },
        "records": records,
    }
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    return report


def write_copernicus_chains(raw_root: Path, output: Path) -> Path:
    report = build_copernicus_chains(raw_root)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return output
