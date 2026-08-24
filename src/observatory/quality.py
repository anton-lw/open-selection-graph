"""Release-facing normalization and temporal-leakage audits."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .ids import content_hash
from .storage import ObservatoryCatalog
from .views import VIEW_SQL
from .visibility import assert_feature_available


def independently_classify_outcome(native: Any) -> str | None:
    value = str(native or "").strip().lower()
    if not value:
        return None
    if value in {"publish", "published", "selected"}:
        return "selected"
    if "accept" in value or any(token in value for token in ("poster", "spotlight", "oral")):
        return "accepted"
    if "reject" in value or "decline" in value:
        return "rejected"
    if "withdraw" in value:
        return "withdrawn"
    return None


VIEW_TEMPORAL_CONTRACTS: dict[str, dict[str, Any]] = {
    "entry_selection": {"class": "decision_time", "post_decision_predictors": False},
    "stage_selection": {"class": "decision_time", "post_decision_predictors": False},
    "evaluation_descriptive": {"class": "evaluation_time", "post_decision_predictors": False},
    "portfolio_descriptive": {"class": "descriptive_population", "post_decision_predictors": False},
    "stage_transitions": {"class": "event_history", "post_decision_predictors": False},
    "lineage": {"class": "time_ordered_lineage", "post_decision_predictors": False},
    "afterlife": {"class": "downstream_outcome_only", "post_decision_predictors": True},
    "funding_evaluability": {"class": "descriptive_population", "post_decision_predictors": False},
    "patent_examination": {"class": "event_history", "post_decision_predictors": False},
    "licence_safe_content": {"class": "content_availability", "post_decision_predictors": False},
}


def validate_analysis_feature(
    view_name: str,
    *,
    feature_available_at: str | None,
    decision_at: str | None,
    feature_role: str,
) -> None:
    contract = VIEW_TEMPORAL_CONTRACTS.get(view_name)
    if contract is None:
        raise ValueError(f"analysis view has no temporal contract: {view_name}")
    if feature_role in {"final_version", "future_citation", "post_decision_identity"} and contract[
        "class"
    ] != "downstream_outcome_only":
        raise ValueError(f"temporal leakage: {feature_role} is not admissible in {view_name}")
    if contract["class"] != "downstream_outcome_only":
        assert_feature_available(feature_available_at, decision_at)


def audit_stage_outcomes(lake_root: Path, output: Path) -> dict[str, Any]:
    connection = ObservatoryCatalog(lake_root).connect()
    rows = connection.execute(
        """SELECT decision_event_id, source_id, outcome_native,
                  outcome_normalized, stage_native, stage_normalized,
                  decided_at, candidate_version_id, gate_cycle_id
           FROM decision_event ORDER BY decision_event_id"""
    ).fetchall()
    audited = []
    for row in rows:
        (decision_id, source_id, native, normalized, stage_native,
         stage_normalized, decided_at, version_id, cycle_id) = row
        expected = independently_classify_outcome(native)
        supported = expected is not None
        exact = (normalized == expected) if supported else normalized is None
        audited.append(
            {
                "decision_event_id": decision_id,
                "source_id": source_id,
                "native_outcome": native,
                "released_outcome": normalized,
                "independent_expected_outcome": expected,
                "mapping_supported": supported,
                "exact": exact,
                "stage_native_present": bool(stage_native),
                "stage_normalized_present": bool(stage_normalized),
                "decided_at": decided_at.isoformat() if decided_at else None,
                "candidate_version_id": version_id,
                "gate_cycle_id": cycle_id,
            }
        )
    supported_rows = [row for row in audited if row["mapping_supported"]]
    exact = sum(row["exact"] for row in supported_rows)
    precision = exact / len(supported_rows) if supported_rows else 0.0
    report: dict[str, Any] = {
        "schema": "observatory.stage-outcome-audit/1",
        "population_count": len(audited),
        "supported_mapping_count": len(supported_rows),
        "native_only_count": len(audited) - len(supported_rows),
        "exact_count": exact,
        "precision": precision,
        "threshold": 0.98,
        "strata": {
            f"{source}|{native}": {
                "count": len(group),
                "precision": sum(item["exact"] for item in group) / len(group),
            }
            for (source, native), group in _groups(audited).items()
        },
        "unsupported_mapping_policy": "retain native value and release normalized outcome as null",
        "failures": [row for row in audited if not row["exact"]][:100],
    }
    report["passes"] = precision >= report["threshold"] and all(
        row["stage_native_present"] and row["stage_normalized_present"] for row in audited
    )
    report["audit_hash"] = content_hash(json.dumps(report, sort_keys=True))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def _groups(rows: list[dict[str, Any]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault((str(row["source_id"]), str(row["native_outcome"])), []).append(row)
    return groups


def audit_temporal_leakage(output: Path) -> dict[str, Any]:
    missing_contracts = sorted(set(VIEW_SQL) - set(VIEW_TEMPORAL_CONTRACTS))
    unexpected_contracts = sorted(set(VIEW_TEMPORAL_CONTRACTS) - set(VIEW_SQL))
    contaminated = [
        {
            "name": "future_feature",
            "view": "entry_selection",
            "feature_available_at": "2026-03-01T00:00:00+00:00",
            "decision_at": "2026-02-01T00:00:00+00:00",
            "feature_role": "decision_time_feature",
        },
        {
            "name": "final_version_predictor",
            "view": "stage_selection",
            "feature_available_at": "2026-01-01T00:00:00+00:00",
            "decision_at": "2026-02-01T00:00:00+00:00",
            "feature_role": "final_version",
        },
        {
            "name": "future_citation_predictor",
            "view": "evaluation_descriptive",
            "feature_available_at": "2027-01-01T00:00:00+00:00",
            "decision_at": "2026-02-01T00:00:00+00:00",
            "feature_role": "future_citation",
        },
        {
            "name": "postdecision_identity_predictor",
            "view": "funding_evaluability",
            "feature_available_at": "2026-03-01T00:00:00+00:00",
            "decision_at": "2026-02-01T00:00:00+00:00",
            "feature_role": "post_decision_identity",
        },
    ]
    outcomes = []
    for fixture in contaminated:
        try:
            validate_analysis_feature(
                fixture["view"],
                feature_available_at=fixture["feature_available_at"],
                decision_at=fixture["decision_at"],
                feature_role=fixture["feature_role"],
            )
        except ValueError as exc:
            outcomes.append({"name": fixture["name"], "rejected": True, "reason": str(exc)})
        else:
            outcomes.append({"name": fixture["name"], "rejected": False, "reason": None})
    valid_fixture = {
        "view_name": "entry_selection",
        "feature_available_at": "2026-01-01T00:00:00+00:00",
        "decision_at": "2026-02-01T00:00:00+00:00",
        "feature_role": "decision_time_feature",
    }
    validate_analysis_feature(**valid_fixture)
    report: dict[str, Any] = {
        "schema": "observatory.temporal-leakage-audit/1",
        "view_count": len(VIEW_SQL),
        "contracts": VIEW_TEMPORAL_CONTRACTS,
        "missing_contracts": missing_contracts,
        "unexpected_contracts": unexpected_contracts,
        "contaminated_fixtures": outcomes,
        "valid_fixture_accepted": True,
        "feature_timing_labels": [
            "decision_time_feature",
            "final_version",
            "future_citation",
            "post_decision_identity",
            "downstream_outcome",
        ],
    }
    report["passes"] = not missing_contracts and not unexpected_contracts and all(
        row["rejected"] for row in outcomes
    )
    report["audit_hash"] = content_hash(json.dumps(report, sort_keys=True))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report
