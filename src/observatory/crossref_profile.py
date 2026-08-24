"""Crossref peer-review depositor/year/relation coverage profiling."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .crossref_raw import iter_crossref_peer_reviews
from .ids import canonical_doi, content_hash


def profile_crossref_peer_reviews(raw_root: Path) -> dict[str, Any]:
    depositor_year: Counter[tuple[str, str]] = Counter()
    publisher_year: Counter[tuple[str, str]] = Counter()
    relation_year: Counter[tuple[str, str]] = Counter()
    subtype_year: Counter[tuple[str, str]] = Counter()
    target_prefixes: Counter[str] = Counter()
    items_with_relation = 0
    items_with_multiple_targets = 0
    invalid_json = 0
    raw_bundle_ids: set[str] = set()
    provider_totals: set[int] = set()
    retrieval_times = []
    observed_initial_cursor = False
    snapshot_objects = 0
    for bundled in iter_crossref_peer_reviews(raw_root):
        snapshot_objects += 1
        receipt = bundled["receipt"]
        raw_bundle_ids.add(str(receipt["source_object_id"]))
        if receipt.get("retrieved_at"):
            retrieval_times.append(str(receipt["retrieved_at"]))
        if bundled.get("bundle_total_results") is not None:
            provider_totals.add(int(bundled["bundle_total_results"]))
        observed_initial_cursor = observed_initial_cursor or bundled.get("bundle_cursor") == "*"
        try:
            work = bundled["work"]
        except (KeyError, TypeError):
            invalid_json += 1
            continue
        year = str(((work.get("created") or {}).get("date-parts") or [["unknown"]])[0][0])
        member = str(work.get("member") or "unknown")
        publisher = str(work.get("publisher") or "unknown")
        depositor_year[(member, year)] += 1
        publisher_year[(publisher, year)] += 1
        subtype_year[(str(work.get("subtype") or work.get("type") or "unknown"), year)] += 1
        targets = set()
        for relation_type, values in (work.get("relation") or {}).items():
            relation_year[(str(relation_type), year)] += len(values or [])
            for value in values or []:
                target = canonical_doi(value.get("id"))
                if target:
                    targets.add(target)
                    target_prefixes[target.split("/", 1)[0]] += 1
        items_with_relation += bool(targets)
        items_with_multiple_targets += len(targets) > 1

    def nested(counter: Counter[tuple[str, str]]) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, int]] = defaultdict(dict)
        for (name, year), count in counter.items():
            grouped[name][year] = count
        return [
            {"name": name, "total": sum(years.values()), "years": dict(sorted(years.items()))}
            for name, years in sorted(grouped.items(), key=lambda item: (-sum(item[1].values()), item[0]))
        ]

    provider_total_min = min(provider_totals) if provider_totals else None
    provider_total_max = max(provider_totals) if provider_totals else None

    report: dict[str, Any] = {
        "schema": "observatory.crossref-peer-review-profile/1",
        "snapshot_objects": snapshot_objects, "invalid_json": invalid_json,
        "snapshot_manifest": {
            "endpoint": "https://api.crossref.org/types/peer-review/works",
            "api_filter": "type=peer-review",
            "cursor_initial": "*",
            "initial_cursor_observed": observed_initial_cursor,
            "provider_total_results": sorted(provider_totals),
            "provider_total_results_min": provider_total_min,
            "provider_total_results_max": provider_total_max,
            "provider_total_drift": (
                provider_total_max - provider_total_min
                if provider_total_min is not None and provider_total_max is not None
                else None
            ),
            "provider_total_stable_during_cursor": len(provider_totals) == 1,
            "raw_page_bundle_count": len(raw_bundle_ids),
            "retrieved_at_min": min(retrieval_times) if retrieval_times else None,
            "retrieved_at_max": max(retrieval_times) if retrieval_times else None,
            "complete": (
                provider_total_max is not None
                and snapshot_objects == provider_total_max
                and observed_initial_cursor
            ),
            "completeness_rule": (
                "initial cursor observed and harvested objects equal the maximum "
                "provider total reported during the dated cursor traversal; total drift is retained"
            ),
            "storage_layout": "immutable lossless API-page bundles with per-work hashes",
        },
        "items_with_resolvable_relation": items_with_relation,
        "relation_item_rate": items_with_relation / snapshot_objects if snapshot_objects else None,
        "items_with_multiple_targets": items_with_multiple_targets,
        "depositors_by_year": nested(depositor_year),
        "publishers_by_year": nested(publisher_year),
        "relation_types_by_year": nested(relation_year),
        "subtypes_by_year": nested(subtype_year),
        "target_prefixes": [
            {"prefix": prefix, "count": count} for prefix, count in target_prefixes.most_common()
        ],
        "scope_warning": (
            "Crossref deposition proves discoverability of deposited review relations, not candidate-pool "
            "or stage completeness at any publisher."
        ),
    }
    report["profile_hash"] = content_hash(json.dumps(report, sort_keys=True))
    return report


def write_crossref_profile(raw_root: Path, output: Path) -> Path:
    report = profile_crossref_peer_reviews(raw_root)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return output
