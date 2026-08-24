"""Institutional-regime, rubric, and stage-flow products derived from the lake.

The products in this module deliberately preserve unknown policy state.  A
cycle with no dated policy document receives an explicit pointer-only policy
observation whose effective interval is the observed cycle interval and whose
confidence is zero; it is never back-filled from a current undated page.
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import pyarrow as pa
import pyarrow.parquet as pq

from .ids import content_hash, stable_id
from .storage import ObservatoryCatalog
from .storage_guard import storage_preflight

CONSTRUCT_TERMS: dict[str, tuple[str, ...]] = {
    "novelty_originality": ("novel", "original", "new contribution"),
    "significance_interest": ("significance", "impact", "interest", "importance"),
    "soundness_evidence": ("sound", "correct", "evidence", "validity", "rigor"),
    "clarity": ("clarity", "presentation", "writing"),
    "reproducibility": ("reproduc", "replicab", "code", "data availability"),
    "ethics": ("ethic", "responsible", "harm"),
    "confidence": ("confidence", "expertise"),
    "overall_recommendation": ("rating", "recommend", "overall", "score"),
}


def _json(value: Any) -> Any:
    if value in (None, ""):
        return None
    if isinstance(value, (dict, list, int, float, bool)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {"unparsed_native_value": str(value)}


def _timestamp(value: Any) -> str | None:
    return value.isoformat() if hasattr(value, "isoformat") else (str(value) if value is not None else None)


def _write_parquet(path: Path, rows: Iterable[Mapping[str, Any]]) -> Path:
    materialized = [dict(row) for row in rows]
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(materialized), path, compression="zstd")
    return path


def _write_json(path: Path, value: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n")
    return path


def _flatten(value: Any, prefix: str = "") -> list[tuple[str, Any]]:
    if isinstance(value, dict):
        rows: list[tuple[str, Any]] = []
        for key, child in sorted(value.items()):
            path = f"{prefix}.{key}" if prefix else str(key)
            rows.extend(_flatten(child, path))
        return rows
    if isinstance(value, list):
        return [(prefix, value)]
    return [(prefix, value)]


def rubric_constructs(label: str, definition: str | None = None) -> list[str]:
    haystack = f"{label} {definition or ''}".lower()
    return sorted(
        construct
        for construct, terms in CONSTRUCT_TERMS.items()
        if any(term in haystack for term in terms)
    )


def _policy_rows(connection) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    policies = {
        row[0]: {
            "policy_version_id": row[0],
            "gate_id": row[1],
            "native_id": row[2],
            "effective_at": row[3],
            "valid_to": row[4],
            "criteria_json": row[5],
            "rubric_json": row[6],
            "stage_rules_json": row[7],
            "quota_or_cap": row[8],
            "anonymity_model": row[9],
            "revision_rules": row[10],
            "policy_url": row[11],
            "content_hash": row[12],
            "date_confidence": row[13],
            "source_id": row[14],
            "source_object_id": row[15],
            "observed_at": row[16],
        }
        for row in connection.execute(
            """SELECT policy_version_id, gate_id, native_id, effective_at, valid_to,
                      criteria_json, rubric_json, stage_rules_json, quota_or_cap,
                      anonymity_model, revision_rules, policy_url, content_hash,
                      date_confidence, source_id, source_object_id, observed_at
               FROM policy_version"""
        ).fetchall()
    }
    by_gate: dict[str, list[dict[str, Any]]] = {}
    for policy in policies.values():
        by_gate.setdefault(str(policy["gate_id"]), []).append(policy)
    for rows in by_gate.values():
        rows.sort(key=lambda row: (_timestamp(row["effective_at"]) or "", row["policy_version_id"]))

    archive: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    cycles = connection.execute(
        """SELECT gc.gate_cycle_id, gc.gate_id, gc.native_id, gc.cycle_start,
                  gc.cycle_end, gc.policy_version_id, gc.architecture,
                  gc.source_id, gc.source_object_id, gc.observed_at, g.name
           FROM gate_cycle gc JOIN gate g USING(gate_id)"""
    ).fetchall()
    for cycle in cycles:
        (cycle_id, gate_id, native_id, start, end, declared_id, architecture,
         source_id, source_object_id, observed_at, gate_name) = cycle
        chosen = policies.get(declared_id)
        link_method = "source_declared"
        if chosen is None:
            candidates = []
            for policy in by_gate.get(str(gate_id), []):
                effective = policy["effective_at"]
                valid_to = policy["valid_to"]
                overlaps = (end is None or effective is None or effective <= end) and (
                    start is None or valid_to is None or valid_to >= start
                )
                if overlaps:
                    candidates.append(policy)
            if candidates:
                chosen = candidates[-1]
                link_method = "interval_overlap"
        if chosen is None:
            policy_id = stable_id("policy_version", str(source_id), f"unknown:{cycle_id}")
            chosen = {
                "policy_version_id": policy_id,
                "gate_id": gate_id,
                "native_id": f"unknown-policy:{native_id}",
                "effective_at": start,
                "valid_to": end,
                "criteria_json": None,
                "rubric_json": None,
                "stage_rules_json": None,
                "quota_or_cap": None,
                "anonymity_model": None,
                "revision_rules": None,
                "policy_url": None,
                "content_hash": None,
                "date_confidence": 0.0,
                "source_id": source_id,
                "source_object_id": source_object_id,
                "observed_at": observed_at,
            }
            link_method = "explicit_unknown_pointer"
            unresolved.append(
                {
                    "gate_cycle_id": cycle_id,
                    "reason": "no dated overlapping policy record",
                    "release_treatment": "pointer_only_unknown",
                }
            )
        archive.append(
            {
                "gate_cycle_id": cycle_id,
                "gate_id": gate_id,
                "gate_name": gate_name,
                "cycle_native_id": native_id,
                "cycle_start": _timestamp(start),
                "cycle_end": _timestamp(end),
                "architecture": architecture,
                "policy_version_id": chosen["policy_version_id"],
                "policy_native_id": chosen["native_id"],
                "policy_effective_from": _timestamp(chosen["effective_at"]),
                "policy_effective_to": _timestamp(chosen["valid_to"]),
                "date_confidence": float(chosen["date_confidence"] or 0.0),
                "link_method": link_method,
                "structured_fact_status": "normalized" if link_method != "explicit_unknown_pointer" else "unknown",
                "criteria_json": chosen["criteria_json"],
                "rubric_json": chosen["rubric_json"],
                "stage_rules_json": chosen["stage_rules_json"],
                "quota_or_cap": chosen["quota_or_cap"],
                "anonymity_model": chosen["anonymity_model"],
                "revision_rules": chosen["revision_rules"],
                "policy_url": chosen["policy_url"],
                "raw_document_hash": chosen["content_hash"],
                "source_id": source_id,
                "source_object_id": chosen["source_object_id"],
                "observed_at": _timestamp(chosen["observed_at"]),
            }
        )
    return archive, unresolved


def _rubric_rows(connection) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    crosswalk: list[dict[str, Any]] = []
    rules: list[dict[str, Any]] = []
    rows = connection.execute(
        """SELECT policy_version_id, gate_id, native_id, effective_at,
                  criteria_json, rubric_json, stage_rules_json, source_id,
                  source_object_id, content_hash
           FROM policy_version"""
    ).fetchall()
    for row in rows:
        (policy_id, gate_id, native_id, effective_at, criteria_raw, rubric_raw,
         stage_raw, source_id, source_object_id, policy_hash) = row
        criteria = _json(criteria_raw)
        rubric = _json(rubric_raw)
        stage = _json(stage_raw)
        if source_id == "openreview_surface":
            rules.append(
                {
                    "policy_version_id": policy_id,
                    "venue_native_id": native_id,
                    "effective_at": _timestamp(effective_at),
                    "criteria_json": json.dumps(criteria, sort_keys=True),
                    "rubric_json": json.dumps(rubric, sort_keys=True),
                    "stage_rules_json": json.dumps(stage, sort_keys=True),
                    "native_schema_roundtrip": _json(json.dumps(criteria, sort_keys=True)) == criteria,
                    "guide_comparison_status": "not_comparable_without_dated_prose",
                    "configuration_prose_conflict": None,
                    "source_object_id": source_object_id,
                    "policy_hash": policy_hash,
                }
            )
        for path, value in _flatten(rubric or {}):
            definition = None
            scale = value if isinstance(value, list) else None
            label = path.rsplit(".", 1)[-1]
            constructs = rubric_constructs(label, definition)
            crosswalk.append(
                {
                    "rubric_field_id": stable_id("rubric_field", str(source_id), f"{policy_id}|{path}"),
                    "policy_version_id": policy_id,
                    "gate_id": gate_id,
                    "native_label": label,
                    "native_path": path,
                    "native_definition": definition,
                    "native_value_json": json.dumps(value, sort_keys=True, default=str),
                    "native_scale_json": json.dumps(scale, sort_keys=True) if scale is not None else None,
                    "constructs": constructs,
                    "mapping_status": (
                        "unmapped" if not constructs else "multi_mapped" if len(constructs) > 1 else "mapped"
                    ),
                    "source_id": source_id,
                    "source_object_id": source_object_id,
                }
            )
    return crosswalk, rules


def _flow_rows(connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        """WITH events AS (
               SELECT gate_cycle_id,
                      count(*) AS observable,
                      count(DISTINCT candidate_version_id) FILTER (WHERE candidate_version_id IS NOT NULL) AS versions,
                      count(*) FILTER (WHERE lower(coalesce(final_observed_stage,'')) LIKE '%withdraw%') AS withdrawn_stage
               FROM candidate_gate_event GROUP BY 1
           ), evals AS (
               SELECT gate_cycle_id, count(DISTINCT candidate_version_id) AS evaluated
               FROM evaluation GROUP BY 1
           ), decisions AS (
               SELECT gate_cycle_id,
                      count(DISTINCT candidate_version_id) FILTER (WHERE outcome_normalized='accepted') AS selected,
                      count(DISTINCT candidate_version_id) FILTER (WHERE outcome_normalized='rejected') AS rejected,
                      count(DISTINCT candidate_version_id) FILTER (WHERE lower(coalesce(outcome_normalized,'')) LIKE '%withdraw%') AS withdrawn_decision,
                      count(DISTINCT candidate_version_id) AS decided
               FROM decision_event GROUP BY 1
           ), coverage AS (
               SELECT gate_cycle_id, max(observability_grade) AS source_grade,
                      max(expected_count) AS provider_expected,
                      max(found_count) AS provider_found
               FROM coverage_observation GROUP BY 1
           )
           SELECT gc.gate_cycle_id, gc.native_id, gc.name, gc.source_id,
                  gc.architecture, gc.cycle_start, gc.cycle_end,
                  gc.received_count, gc.observable_count, gc.evaluated_count,
                  gc.selected_count, coalesce(e.observable,0), coalesce(ev.evaluated,0),
                  coalesce(d.selected,0), coalesce(d.rejected,0),
                  greatest(coalesce(e.withdrawn_stage,0),coalesce(d.withdrawn_decision,0)),
                  coalesce(d.decided,0), c.source_grade, c.provider_expected, c.provider_found
           FROM gate_cycle gc
           LEFT JOIN events e USING(gate_cycle_id)
           LEFT JOIN evals ev USING(gate_cycle_id)
           LEFT JOIN decisions d USING(gate_cycle_id)
           LEFT JOIN coverage c USING(gate_cycle_id)"""
    ).fetchall()
    output: list[dict[str, Any]] = []
    for row in rows:
        (cycle_id, native_id, name, source_id, architecture, start, end,
         declared_received, declared_observable, declared_evaluated, declared_selected,
         observable, evaluated, selected, rejected, withdrawn, decided,
         grade, provider_expected, provider_found) = row
        received = declared_received if declared_received is not None else provider_expected
        observable_final = declared_observable if declared_observable is not None else observable
        evaluated_final = declared_evaluated if declared_evaluated is not None else evaluated
        selected_final = declared_selected if declared_selected is not None else selected
        violations = []
        if received is not None and observable_final > received:
            violations.append("observable_gt_received")
        if evaluated_final > observable_final:
            violations.append("evaluated_gt_observable")
        if selected_final > observable_final:
            violations.append("selected_gt_observable")
        if decided > observable_final:
            violations.append("decided_gt_observable")
        effective_grade = "U" if violations and grade in {"A", "B"} else grade
        output.append(
            {
                "gate_cycle_id": cycle_id,
                "cycle_native_id": native_id,
                "name": name,
                "source_id": source_id,
                "platform": str(source_id),
                "venue_family": None,
                "selection_context": None,
                "field_of_study": None,
                "architecture": architecture,
                "cycle_start": _timestamp(start),
                "cycle_end": _timestamp(end),
                "received_count": received,
                "observable_count": observable_final,
                "evaluated_count": evaluated_final,
                "withdrawn_count": withdrawn,
                "rejected_count": rejected,
                "selected_count": selected_final,
                "other_or_unresolved_count": max(observable_final - selected_final - rejected - withdrawn, 0),
                "provider_expected_count": provider_expected,
                "provider_found_count": provider_found,
                "source_observability_grade": grade,
                "effective_observability_grade": effective_grade,
                "stage_flow_violations": violations,
                "identity_passes": not violations,
                "denominator_verified": effective_grade in {"A", "B"} and not violations,
                "selection_rate_eligible": effective_grade in {"A", "B"}
                and not violations
                and observable_final > 0,
                "review_rate_eligible": effective_grade in {"A", "B"}
                and not violations
                and observable_final > 0,
                "official_review_count": None,
                "reviewed_candidate_count": None,
                "reviews_per_reviewed_candidate": None,
                "decision_observation_ratio": None,
                "cross_stage_warnings": [],
                "censoring_note": "counts describe public observable stages only; unresolved rows are not inferred outcomes",
            }
        )
    return output


def _verified_openreview_flow_rows(evidence_root: Path) -> list[dict[str, Any]]:
    path = evidence_root / "openreview_verified_cycle_metrics.parquet"
    report_path = evidence_root / "openreview_verified_cycle_metrics_report.json"
    if not path.exists() or not report_path.exists():
        return []
    report = json.loads(report_path.read_text())
    if not report.get("passes"):
        raise RuntimeError("verified OpenReview cycle metrics failed their bound union audit")
    rows = pq.read_table(path).to_pylist()
    output: list[dict[str, Any]] = []
    for row in rows:
        observable = int(row["observable_count"])
        reviewed = int(row["reviewed_candidate_count"])
        accepted = int(row["accepted_count"])
        rejected = int(row["rejected_count"])
        withdrawn = int(row["withdrawn_count"])
        decided = int(row["decided_candidate_count"])
        warnings = []
        if reviewed > observable:
            warnings.append("reviewed_forum_ids_gt_candidate_state_count")
        if decided > observable:
            warnings.append("decision_candidate_versions_gt_candidate_state_count")
        selection_rate_eligible = (
            observable > 0 and decided == observable and accepted + rejected + withdrawn <= observable
        )
        output.append(
            {
                "gate_cycle_id": row["gate_cycle_id"],
                "cycle_native_id": row["venue_id"],
                "name": row["gate_name"],
                "source_id": "openreview_api",
                "platform": row["platform"],
                "venue_family": row["venue_family"],
                "selection_context": row["selection_context"],
                "field_of_study": row["field_of_study"],
                "architecture": row["architecture"],
                "cycle_start": _timestamp(row["cycle_start"]),
                "cycle_end": _timestamp(row["cycle_end"]),
                "received_count": int(row["received_count"]),
                "observable_count": observable,
                "evaluated_count": reviewed,
                "withdrawn_count": withdrawn,
                "rejected_count": rejected,
                "selected_count": accepted,
                "other_or_unresolved_count": max(observable - accepted - rejected - withdrawn, 0),
                "provider_expected_count": int(row["received_count"]),
                "provider_found_count": observable,
                "source_observability_grade": row["observability_grade"],
                "effective_observability_grade": row["observability_grade"],
                "stage_flow_violations": [],
                "identity_passes": int(row["received_count"]) == observable,
                "denominator_verified": int(row["received_count"]) == observable,
                "selection_rate_eligible": selection_rate_eligible,
                "review_rate_eligible": observable > 0 and reviewed <= observable,
                "official_review_count": int(row["official_review_count"]),
                "reviewed_candidate_count": reviewed,
                "reviews_per_reviewed_candidate": row["reviews_per_reviewed_candidate"],
                "decision_observation_ratio": row["decision_observation_ratio"],
                "cross_stage_warnings": warnings,
                "censoring_note": (
                    "complete provider-audited public candidate-state denominator; "
                    "confidential screening and unreadable objects remain outside scope"
                ),
            }
        )
    return output


def build_institutional_products(lake_root: Path, output_root: Path) -> dict[str, Any]:
    """Build the R1 institutional archive and its machine-readable audit."""
    connection = ObservatoryCatalog(lake_root).connect()
    required = {"gate", "gate_cycle", "policy_version", "candidate_gate_event", "evaluation", "decision_event", "coverage_observation"}
    available = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
    missing = sorted(required - available)
    if missing:
        raise RuntimeError(f"institutional build is missing canonical tables: {missing}")
    archive, unresolved = _policy_rows(connection)
    crosswalk, rules = _rubric_rows(connection)
    flows = _flow_rows(connection)
    verified_openreview = _verified_openreview_flow_rows(output_root.parent)
    existing_cycle_ids = {row["gate_cycle_id"] for row in flows}
    overlaps = existing_cycle_ids & {row["gate_cycle_id"] for row in verified_openreview}
    if overlaps:
        raise RuntimeError(f"verified OpenReview cycles collide with lake flow rows: {len(overlaps)}")
    flows.extend(verified_openreview)
    storage_receipt = storage_preflight(
        output_root,
        projected_input_bytes=0,
        projected_output_bytes=max((len(archive) + len(crosswalk) + len(rules) + len(flows)) * 2_048, 1),
    )
    paths = {
        "regime_archive": _write_parquet(output_root / "institutional_regime_archive.parquet", archive),
        "rubric_crosswalk": _write_parquet(output_root / "rubric_construct_crosswalk.parquet", crosswalk),
        "openreview_rules": _write_parquet(output_root / "openreview_extracted_rules.parquet", rules),
        "gate_cycle_flows": _write_parquet(output_root / "gate_cycle_flow_series.parquet", flows),
    }
    flow_violations = [row for row in flows if not row["identity_passes"]]
    report: dict[str, Any] = {
        "schema": "observatory.institutional-products/1",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "counts": {
            "gate_cycles": len(archive),
            "explicit_unknown_policy_links": len(unresolved),
            "rubric_fields": len(crosswalk),
            "openreview_rule_records": len(rules),
            "flow_rows": len(flows),
            "verified_openreview_cycles": len(verified_openreview),
            "verified_openreview_populated_cycles": sum(
                row["observable_count"] > 0 for row in verified_openreview
            ),
            "flow_identity_violations": len(flow_violations),
        },
        "checks": {
            "every_cycle_has_policy_observation": len(archive) > 0 and all(row["policy_version_id"] for row in archive),
            "unknown_policy_is_not_back_projected": all(
                row["link_method"] != "explicit_unknown_pointer" or row["date_confidence"] == 0.0
                for row in archive
            ),
            "rubric_native_values_lossless": all(
                _json(row["native_value_json"]) is not None for row in crosswalk
            ),
            "openreview_native_schema_roundtrip": bool(rules) and all(row["native_schema_roundtrip"] for row in rules),
            "flow_violations_trigger_downgrade": all(
                not row["stage_flow_violations"] or row["effective_observability_grade"] == "U"
                or row["source_observability_grade"] not in {"A", "B"}
                for row in flows
            ),
        },
        "unknown_policy_examples": unresolved[:100],
        "flow_violation_examples": flow_violations[:100],
        "paths": {key: str(path) for key, path in paths.items()},
        "storage_preflight": storage_receipt,
        "source_counts": dict(Counter(row["source_id"] for row in archive)),
    }
    report["passes"] = all(report["checks"].values())
    report["artifact_hashes"] = {key: content_hash(path.read_bytes()) for key, path in paths.items()}
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True, default=str))
    _write_json(output_root / "institutional_products_report.json", report)
    return report
