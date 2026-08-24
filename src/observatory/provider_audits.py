"""Provider-specific audits over Crossref discovery snapshots.

These reports deliberately describe deposited public objects, not submission
denominators.  They make the architecture-specific gaps explicit before any
provider can be promoted from discovery/pointer status.
"""

from __future__ import annotations

import gzip
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

from .ids import canonical_doi, content_hash


def _latest_objects(raw_root: Path, source_id: str) -> Iterable[dict[str, Any]]:
    manifest = raw_root / "manifests" / f"{source_id}.jsonl"
    latest: dict[str, dict[str, Any]] = {}
    if not manifest.exists():
        return ()
    for line in manifest.read_text().splitlines():
        if not line.strip():
            continue
        receipt = json.loads(line)
        if receipt.get("object_type") == "work_metadata":
            latest[str(receipt["native_id"])] = receipt
    rows = []
    for receipt in latest.values():
        rows.append(json.loads(gzip.decompress(Path(receipt["raw_pointer"]).read_bytes())))
    return rows


def _year(work: dict[str, Any]) -> int | None:
    for key in ("published-online", "published-print", "published", "issued", "created"):
        value = work.get(key) or {}
        parts = value.get("date-parts") or []
        if parts and parts[0] and parts[0][0] is not None:
            return int(parts[0][0])
        if value.get("date-time"):
            return int(str(value["date-time"])[:4])
    return None


def _f1000_platform(work: dict[str, Any]) -> str:
    doi = canonical_doi(work.get("DOI")) or ""
    stem = doi.split("/", 1)[-1].split(".", 1)[0].lower()
    known = {
        "f1000research": "F1000Research", "wellcomeopenres": "Wellcome Open Research",
        "gatesopenres": "Gates Open Research", "hrbopenres": "HRB Open Research",
        "nihropenres": "NIHR Open Research", "amrcopenres": "AMRC Open Research",
        "emeraldopenres": "Emerald Open Research", "mniopenres": "MNI Open Research",
    }
    if stem in known:
        return known[stem]
    container = " ".join(work.get("container-title") or [])
    return container or "unresolved-platform"


def build_provider_process_audit(raw_root: Path) -> dict[str, Any]:
    report: dict[str, Any] = {
        "schema": "observatory.provider-process-audit/1",
        "scope_rule": (
            "Crossref deposit visibility is discovery evidence only. It does not establish a candidate "
            "pool, sent-for-review denominator, or complete public process graph."
        ),
        "providers": {},
    }
    specifications = {
        "elife": {
            "architecture": "publish_review_curate",
            "entry_stage": "sent_for_review",
            "grade": "B_pending_provider_reconciliation",
            "policy_cohorts": {
                "legacy_editorial_decision_model": "publication date before 2023-01-31",
                "reviewed_preprint_model": "publication date on/after 2023-01-31",
            },
            "required_missing_objects": [
                "public reviews", "author responses", "eLife Assessments",
                "assessment vocabulary policy version", "sent-for-review denominator",
            ],
        },
        "f1000research": {
            "architecture": "post_publication_review",
            "entry_stage": "publication_after_editorial_screen",
            "grade": "B_pending_platform_reconciliation",
            "required_missing_objects": [
                "version-specific review status", "review reports and responses",
                "platform-specific editorial-screen denominator",
            ],
        },
        "scipost": {
            "architecture": "access_public_discussion",
            "entry_stage": "public_submission_after_editorial_vetting",
            "grade": "U_pending_submission_enumeration",
            "required_missing_objects": [
                "submission page", "reports/replies", "editorial recommendation/decision",
                "invited-versus-contributed report status", "journal-year denominator",
            ],
        },
        "peerj": {
            "architecture": "selected_only_transparent_review",
            "entry_stage": "accepted_published_article_with_history",
            "grade": "C",
            "required_missing_objects": ["rejected candidates", "non-opt-in histories"],
        },
        "plos_review_history": {
            "architecture": "selected_only_opt_in_transparent_review",
            "entry_stage": "accepted_opt_in_review_history",
            "grade": "C",
            "required_missing_objects": ["rejected candidates", "non-opt-in accepted histories"],
        },
        "embo_transparent_review": {
            "architecture": "selected_only_transparent_review",
            "entry_stage": "selected_published_process_file",
            "grade": "C",
            "required_missing_objects": ["rejected candidates", "unpublished process files"],
        },
        "royal_society_review": {
            "architecture": "selected_only_transparent_review",
            "entry_stage": "selected_published_transparent_history",
            "grade": "C",
            "required_missing_objects": ["rejected candidates", "non-transparent histories"],
        },
        "bmc_open_review": {
            "architecture": "selected_only_open_review",
            "entry_stage": "published_open_review_article",
            "grade": "C",
            "required_missing_objects": ["rejected candidates", "closed-review journals"],
        },
        "qeios": {
            "architecture": "post_publication_review",
            "entry_stage": "public_preprint",
            "grade": "U_pending_site_denominator",
            "required_missing_objects": ["site denominator", "complete review/revision chain"],
        },
    }
    for source_id, specification in specifications.items():
        works = list(_latest_objects(raw_root, source_id))
        years = Counter(str(_year(work) or "unknown") for work in works)
        types = Counter(str(work.get("type") or "unknown") for work in works)
        subtypes = Counter(str(work.get("subtype") or "unknown") for work in works)
        relations: Counter[str] = Counter()
        relation_targets = 0
        for work in works:
            for relation_type, values in (work.get("relation") or {}).items():
                relations[str(relation_type)] += len(values or [])
                relation_targets += sum(bool(canonical_doi(value.get("id"))) for value in values or [])
        row: dict[str, Any] = {
            **specification, "objects_profiled": len(works),
            "snapshot_scope": "latest locally retained proof/full snapshot",
            "years": dict(sorted(years.items())), "types": dict(types.most_common()),
            "subtypes": dict(subtypes.most_common()), "relations": dict(relations.most_common()),
            "doi_relation_targets": relation_targets,
            "selection_view_eligible": specification["grade"] == "A",
        }
        if source_id == "elife":
            cohorts = Counter(
                "reviewed_preprint_model" if (_year(work) or 0) >= 2023 else "legacy_editorial_decision_model"
                for work in works
            )
            row["provisional_crossref_cohorts"] = dict(cohorts)
            row["cohort_warning"] = (
                "Year is only a discovery stratifier; exact 2023-01-31 effective-date assignment "
                "requires provider publication dates and policy-version reconciliation."
            )
        if source_id == "f1000research":
            row["platforms"] = dict(Counter(_f1000_platform(work) for work in works).most_common())
            row["editorial_screen_boundary"] = "explicit_hidden_pre_publication_stage"
        if source_id in {
            "peerj", "plos_review_history", "embo_transparent_review",
            "royal_society_review", "bmc_open_review",
        }:
            row["candidate_pool_firewall"] = (
                "grade C; excluded from entry-selection and denominator views regardless of review text"
            )
        report["providers"][source_id] = row
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    return report


def write_provider_process_audit(raw_root: Path, output: Path) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(build_provider_process_audit(raw_root), indent=2, sort_keys=True) + "\n"
    )
    return output
