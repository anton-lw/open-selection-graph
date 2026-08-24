"""Validation and reporting for publication, funding, and patent censuses."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .registry import CONFIG, load_yaml, source_cards


def validate_censuses(root: Path | None = None) -> dict[str, Any]:
    workspace = root or CONFIG.parents[1]
    cards = {card.source_id: card for card in source_cards()}
    publication = load_yaml(CONFIG / "publication_census.yaml")
    required = {
        "candidate_id", "provider", "source_card_id", "architecture", "grade", "disposition",
        "earliest_public_stage", "size_probe", "adapter",
    }
    errors = []
    for row in publication.get("candidates") or []:
        missing = required - set(row)
        if missing:
            errors.append(f"publication {row.get('candidate_id')} missing {sorted(missing)}")
        if row.get("source_card_id") not in cards:
            errors.append(f"publication {row.get('candidate_id')} has unknown source card")
        if not (row.get("size_probe") or {}).get("method"):
            errors.append(f"publication {row.get('candidate_id')} lacks size method")

    funding = load_yaml(CONFIG / "funding_census.yaml")
    existing = workspace / str(funding["authoritative_existing_census"])
    if not existing.exists():
        errors.append("funding authoritative census absent")
        funding_rows = []
    else:
        funding_rows = json.loads(existing.read_text()).get("rows") or []
    six = set(funding["six_field_standard"])
    field_map = {
        "applications": "has_applications", "passing_or_eligible_pool": "has_passing",
        "assignment_arm": "has_arm_label", "awards_or_outcome": "has_awards",
        "unfunded_candidate_text": "has_unfunded_text", "decision_date_or_round": "has_decision_date",
    }
    if six != set(field_map):
        errors.append("funding six-field standard changed without migration")
    for row in funding_rows:
        missing = [native for native in field_map.values() if native not in row]
        if missing:
            errors.append(f"funding {row.get('iid')} missing {missing}")
    for card in funding.get("source_completeness_cards") or []:
        if card.get("source_card_id") not in cards:
            errors.append(f"funding completeness card has unknown source {card.get('source_card_id')}")

    patent = load_yaml(CONFIG / "patent_source_matrix.yaml")
    for field, row in (patent.get("fields") or {}).items():
        if row.get("authoritative") not in cards:
            errors.append(f"patent {field} has unknown authoritative source")
        for source in row.get("corroborating") or []:
            if source not in cards:
                errors.append(f"patent {field} has unknown corroborating source {source}")
    return {
        "passes": not errors, "errors": errors,
        "publication_candidates": len(publication.get("candidates") or []),
        "funding_instruments": len(funding_rows),
        "patent_fields": len(patent.get("fields") or {}),
    }
