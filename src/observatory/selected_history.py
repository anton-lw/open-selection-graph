"""Acceptance audit for selected-only transparent-review pointer layers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .constitution import AnalysisClass, ObservabilityGrade, admissible
from .ids import content_hash
from .registry import source_by_id

PROVIDERS: dict[str, dict[str, Any]] = {
    "peerj": {
        "selection_mechanism": "published/accepted article history; public-review participation is not an entry pool",
        "known_missing": ["rejected submissions", "histories not exposed by provider"],
        "policy_evidence": "https://peerj.com/about/policies-and-procedures/",
        "pointer_routes": ["Crossref DOI", "provider article review-history page"],
    },
    "plos_review_history": {
        "selection_mechanism": "author opt-in after acceptance for eligible published articles",
        "known_missing": ["rejected submissions", "accepted articles without opt-in"],
        "policy_evidence": "https://plos.org/resource/open-peer-review/",
        "pointer_routes": ["Crossref DOI/sub-DOI", "PLOS published peer-review history"],
    },
    "embo_transparent_review": {
        "selection_mechanism": "transparent process file attached to selected published output",
        "known_missing": ["rejected submissions", "process files not made public"],
        "policy_evidence": "https://www.embopress.org/page/journal/14602075/authorguide",
        "pointer_routes": ["Crossref DOI", "publisher process-file pointer"],
    },
    "royal_society_review": {
        "selection_mechanism": "transparent review material attached to selected published output",
        "known_missing": ["rejected submissions", "published articles without transparent material"],
        "policy_evidence": "https://royalsociety.org/journals/ethics-policies/editorial-standards/",
        "pointer_routes": ["Crossref DOI/component", "publisher supplementary-history pointer"],
    },
    "bmc_open_review": {
        "selection_mechanism": "published articles in journals operating an open-review workflow",
        "known_missing": ["rejected submissions", "closed-review journals and articles"],
        "policy_evidence": "https://www.biomedcentral.com/getpublished/peer-review-process",
        "pointer_routes": ["Crossref DOI", "Europe PMC/PMCID", "provider review page"],
    },
}


def build_selected_history_audit(fixture_root: Path) -> dict[str, Any]:
    rows = {}
    for source_id, specification in PROVIDERS.items():
        card = source_by_id(source_id)
        proof_path = fixture_root / source_id / "probe_manifest.json"
        proof = json.loads(proof_path.read_text()) if proof_path.exists() else None
        row = {
            "source_id": source_id,
            "provider": card.provider,
            "observability_grade": card.provisional_grade,
            "source_status": card.status,
            "earliest_public_stage": card.earliest_public_stage,
            "selection_mechanism": specification["selection_mechanism"],
            "known_missing": specification["known_missing"],
            "policy_evidence": specification["policy_evidence"],
            "pointer_routes": specification["pointer_routes"],
            "text_release_decision": "pointer_hash_only",
            "decision_reason": (
                "provider review-history text has no corpus-wide affirmative redistribution "
                "licence and/or stable document route proven; E10 kill/downgrade invoked"
            ),
            "crossref_pointer_adapter": f"provider:{source_id}",
            "proof_manifest": str(proof_path),
            "proof_passes": bool(proof and proof.get("passes")),
            "entry_selection_admissible": admissible(
                ObservabilityGrade.C, AnalysisClass.ENTRY_SELECTION
            ),
            "evaluation_description_admissible": admissible(
                ObservabilityGrade.C, AnalysisClass.EVALUATION_DESCRIPTION
            ),
        }
        row["passes"] = bool(
            row["observability_grade"] == "C"
            and row["source_status"] == "pointer_only"
            and row["selection_mechanism"]
            and row["known_missing"]
            and row["text_release_decision"] == "pointer_hash_only"
            and not row["entry_selection_admissible"]
            and row["evaluation_description_admissible"]
        )
        rows[source_id] = row
    report: dict[str, Any] = {
        "schema": "observatory.selected-history-pointer-layer/1",
        "providers": rows,
        "provider_count": len(rows),
        "entry_selection_firewall": (
            "analysis_entry_selection SQL admits grade A only; every provider here is grade C"
        ),
        "release_scope": (
            "Crossref/source identifiers, source pointers, object hashes, and "
            "non-reconstructive metadata; no review-history text redistribution"
        ),
        "kill_downgrade_invoked": True,
        "passes": len(rows) == 5 and all(row["passes"] for row in rows.values()),
    }
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    return report


def write_selected_history_audit(fixture_root: Path, output: Path) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(build_selected_history_audit(fixture_root), indent=2, sort_keys=True)
        + "\n"
    )
    return output
