"""Executable governance, resource forecasting, and budget controls."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import yaml

from .ids import content_hash
from .registry import CONFIG, ROOT, source_cards
from .schema import TABLE_SCHEMAS


def _yaml(path: Path) -> dict[str, Any]:
    value = yaml.safe_load(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"policy must be a mapping: {path}")
    return value


def release_field_catalogue(path: Path | None = None) -> list[dict[str, str]]:
    """Resolve a purpose and release tier for every canonical schema field."""
    policy = _yaml(path or CONFIG / "governance.yaml")
    purposes = policy.get("table_purposes") or {}
    rules = policy.get("field_rules") or {}
    expected_tables = set(TABLE_SCHEMAS)
    if set(purposes) != expected_tables:
        missing = sorted(expected_tables - set(purposes))
        unexpected = sorted(set(purposes) - expected_tables)
        raise ValueError(f"governance table purposes differ from schema: missing={missing}, unexpected={unexpected}")
    assignments: dict[str, str] = {}
    for tier in ("restricted", "pointer_hash", "aggregate_only"):
        for field in rules.get(tier) or []:
            field = str(field)
            if field in assignments:
                raise ValueError(f"release field has multiple tiers: {field}")
            assignments[field] = tier
    allowed_tiers = set(policy.get("release_tiers") or {})
    default_tier = str(rules.get("public_default") or "")
    if default_tier not in allowed_tiers:
        raise ValueError(f"invalid default release tier: {default_tier}")
    known_fields = {field.name for fields in TABLE_SCHEMAS.values() for field in fields}
    unknown_rules = sorted(set(assignments) - known_fields)
    if unknown_rules:
        raise ValueError(f"release policy references unknown fields: {unknown_rules}")
    rows: list[dict[str, str]] = []
    for table, fields in TABLE_SCHEMAS.items():
        table_purpose = str(purposes[table]).strip()
        if not table_purpose:
            raise ValueError(f"empty governance purpose: {table}")
        for field in fields:
            tier = assignments.get(field.name, default_tier)
            if tier not in allowed_tiers:
                raise ValueError(f"invalid release tier for {table}.{field.name}: {tier}")
            rows.append(
                {
                    "table": table,
                    "field": field.name,
                    "release_tier": tier,
                    "purpose": field.description.strip()
                    or f"Support the {table} purpose as its canonical {field.name} field.",
                    "table_purpose": table_purpose,
                }
            )
    return rows


def governance_audit(path: Path | None = None) -> dict[str, Any]:
    policy_path = path or CONFIG / "governance.yaml"
    policy = _yaml(policy_path)
    catalogue = release_field_catalogue(policy_path)
    required = {
        "purposes",
        "public_source_basis",
        "human_subjects_position",
        "risk_groups",
        "access_roles",
        "release_tiers",
        "retention",
        "takedown",
        "prohibited_uses",
        "table_purposes",
        "field_rules",
    }
    missing = sorted(required - set(policy))
    report = {
        "schema": "observatory.governance-audit/1",
        "plan_version": policy.get("plan_version"),
        "effective_at": policy.get("effective_at"),
        "missing_sections": missing,
        "table_count": len(TABLE_SCHEMAS),
        "field_count": len(catalogue),
        "unresolved_fields": [],
        "release_tier_counts": {
            tier: sum(row["release_tier"] == tier for row in catalogue)
            for tier in policy.get("release_tiers") or {}
        },
    }
    report["passes"] = not missing and len(catalogue) == sum(map(len, TABLE_SCHEMAS.values()))
    report["catalogue_hash"] = content_hash(json.dumps(catalogue, sort_keys=True))
    return report


def network_policy_audit(path: Path | None = None) -> dict[str, Any]:
    """Prove each allowlisted outbound host is attached to a free source card."""
    policy = _yaml(path or CONFIG / "network_policy.yaml")
    cards = {card.source_id: card for card in source_cards()}
    links = policy.get("allowed_hosts") or {}
    non_data_hosts = policy.get("non_data_hosts") or {}
    invalid_links: list[dict[str, str]] = []
    for host, source_ids in links.items():
        if not str(host).strip() or "://" in str(host):
            invalid_links.append({"host": str(host), "source_id": "", "reason": "host is not canonical"})
        for source_id in source_ids or []:
            card = cards.get(str(source_id))
            if card is None:
                invalid_links.append({"host": str(host), "source_id": str(source_id), "reason": "missing source card"})
            elif card.cost_class != "free":
                invalid_links.append({"host": str(host), "source_id": str(source_id), "reason": "source is not free"})
    report = {
        "schema": "observatory.network-policy-audit/1",
        "mode": policy.get("mode"),
        "allowed_host_count": len(links),
        "non_data_host_count": len(non_data_hosts),
        "source_card_links": sum(len(value or []) for value in links.values()),
        "invalid_links": invalid_links,
        "unknown_outbound_configuration": policy.get("unknown_outbound_configuration"),
        "metered_or_overage_capable_services": policy.get("metered_or_overage_capable_services"),
    }
    report["passes"] = (
        policy.get("mode") == "deny_by_default"
        and policy.get("unknown_outbound_configuration") == "prohibited"
        and policy.get("metered_or_overage_capable_services") == "prohibited"
        and not invalid_links
        and all(str(host).strip() and str(purpose).strip() for host, purpose in non_data_hosts.items())
    )
    return report


def network_allowed_hosts(path: Path | None = None) -> set[str]:
    policy = _yaml(path or CONFIG / "network_policy.yaml")
    return {str(host).lower() for host in (policy.get("allowed_hosts") or {})}


@dataclass(frozen=True)
class ResourceProjection:
    source_id: str
    feature_family: str
    expected_objects: int
    expected_requests: int
    raw_bytes: int
    compressed_bytes: int
    normalized_bytes: int
    parsing_hours: float
    embedding_documents: int
    embedding_tokens: int
    peak_memory_bytes: int
    modal_upper_cost_usd: float
    assumptions: Mapping[str, float]


def estimate_feature_resources(
    *,
    source_id: str,
    feature_family: str,
    fixture_objects: int,
    fixture_raw_bytes: int,
    provider_objects: int,
    fixture_compressed_bytes: int | None = None,
    fixture_normalized_bytes: int | None = None,
    fixture_parse_seconds: float = 0.0,
    fixture_tokens: int = 0,
    requests_per_object: float = 1 / 200,
    peak_memory_multiplier: float = 4.0,
    modal_cpu_hour_usd: float = 0.20,
) -> ResourceProjection:
    """Project the complete Q2 resource vector from a bounded fixture."""
    if fixture_objects <= 0 or provider_objects < 0 or fixture_raw_bytes < 0:
        raise ValueError("fixture_objects must be positive and counts/bytes non-negative")
    scale = provider_objects / fixture_objects
    raw = round(fixture_raw_bytes * scale)
    compressed_fixture = fixture_compressed_bytes if fixture_compressed_bytes is not None else fixture_raw_bytes
    normalized_fixture = fixture_normalized_bytes if fixture_normalized_bytes is not None else fixture_raw_bytes
    compressed = round(compressed_fixture * scale)
    normalized = round(normalized_fixture * scale)
    parse_hours = fixture_parse_seconds * scale / 3600
    tokens = round(fixture_tokens * scale)
    embedding_documents = provider_objects if fixture_tokens else 0
    peak_memory = round(max(fixture_raw_bytes, normalized_fixture) * peak_memory_multiplier)
    modal_upper = parse_hours * modal_cpu_hour_usd
    return ResourceProjection(
        source_id=source_id,
        feature_family=feature_family,
        expected_objects=provider_objects,
        expected_requests=max(round(provider_objects * requests_per_object), 1) if provider_objects else 0,
        raw_bytes=raw,
        compressed_bytes=compressed,
        normalized_bytes=normalized,
        parsing_hours=parse_hours,
        embedding_documents=embedding_documents,
        embedding_tokens=tokens,
        peak_memory_bytes=peak_memory,
        modal_upper_cost_usd=modal_upper,
        assumptions={
            "fixture_objects": float(fixture_objects),
            "scale_factor": scale,
            "requests_per_object": requests_per_object,
            "peak_memory_multiplier": peak_memory_multiplier,
            "modal_cpu_hour_usd": modal_cpu_hour_usd,
        },
    )


def reconcile_resource_estimate(
    estimate: ResourceProjection | Mapping[str, Any], actual: Mapping[str, int | float]
) -> dict[str, Any]:
    """Compare a shard to its forecast; error above 25% blocks continuation."""
    expected = asdict(estimate) if isinstance(estimate, ResourceProjection) else dict(estimate)
    metrics = (
        "expected_objects",
        "expected_requests",
        "raw_bytes",
        "compressed_bytes",
        "normalized_bytes",
        "parsing_hours",
        "embedding_documents",
        "embedding_tokens",
        "peak_memory_bytes",
        "modal_upper_cost_usd",
    )
    errors: dict[str, float | None] = {}
    for metric in metrics:
        observed = actual.get(metric)
        forecast = expected.get(metric)
        errors[metric] = (
            None
            if observed is None or forecast is None
            else (0.0 if float(forecast) == float(observed) == 0 else abs(float(observed) - float(forecast)) / max(abs(float(forecast)), 1e-12))
        )
    offenders = sorted(metric for metric, error in errors.items() if error is not None and error > 0.25)
    return {
        "schema": "observatory.resource-reconciliation/1",
        "source_id": expected.get("source_id"),
        "feature_family": expected.get("feature_family"),
        "relative_errors": errors,
        "reforecast_required": bool(offenders),
        "offending_metrics": offenders,
        "passes": not offenders,
    }


class BudgetLedger:
    """Append-only preflight/actual ledger for the constitutional Modal cap."""

    def __init__(self, path: Path, policy_path: Path | None = None):
        self.path = path
        self.policy = _yaml(policy_path or CONFIG / "modal_budget.yaml")

    def rows(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        return [json.loads(line) for line in self.path.read_text().splitlines() if line.strip()]

    def actual_total(self) -> float:
        return sum(float(row.get("actual_cost_usd") or 0.0) for row in self.rows() if row.get("event") == "actual")

    def envelope_actual(self, envelope: str) -> float:
        return sum(
            float(row.get("actual_cost_usd") or 0.0)
            for row in self.rows()
            if row.get("event") == "actual" and row.get("envelope") == envelope
        )

    def preflight(
        self,
        *,
        job_id: str,
        envelope: str,
        projected_cost_usd: float,
        retry_number: int = 0,
        job_cap_usd: float | None = None,
        contingency_gate_passed: bool = False,
    ) -> dict[str, Any]:
        envelopes = self.policy["envelopes"]
        if envelope not in envelopes:
            raise ValueError(f"unknown Modal budget envelope: {envelope}")
        cap = float(job_cap_usd if job_cap_usd is not None else self.policy["default_job_cap"])
        reasons = []
        projected = float(projected_cost_usd)
        if projected < 0 or projected > cap:
            reasons.append("job_cap")
        if retry_number > int(self.policy["automatic_retry_cap"]):
            reasons.append("retry_cap")
        if self.actual_total() + projected > float(self.policy["total_cap"]):
            reasons.append("total_cap")
        if self.envelope_actual(envelope) + projected > float(envelopes[envelope]):
            reasons.append("envelope_cap")
        if envelope == "contingency" and not contingency_gate_passed:
            reasons.append("contingency_gate")
        return {
            "schema": "observatory.modal-budget-preflight/1",
            "job_id": job_id,
            "envelope": envelope,
            "retry_number": retry_number,
            "projected_cost_usd": projected,
            "job_cap_usd": cap,
            "actual_cumulative_before_usd": self.actual_total(),
            "projected_cumulative_usd": self.actual_total() + projected,
            "reasons": reasons,
            "passes": not reasons,
        }

    def append_preflight(self, **kwargs: Any) -> dict[str, Any]:
        receipt = self.preflight(**kwargs)
        if not receipt["passes"]:
            raise RuntimeError(f"Modal budget preflight blocked: {','.join(receipt['reasons'])}")
        return self._append({"event": "preflight", **receipt})

    def record_actual(
        self,
        *,
        job_id: str,
        envelope: str,
        actual_cost_usd: float,
        provider_receipt: str,
    ) -> dict[str, Any]:
        if envelope not in self.policy["envelopes"]:
            raise ValueError(f"unknown Modal budget envelope: {envelope}")
        if not provider_receipt.strip() or actual_cost_usd < 0:
            raise ValueError("actual cost requires a non-empty provider receipt and non-negative value")
        projected_total = self.actual_total() + float(actual_cost_usd)
        if projected_total > float(self.policy["total_cap"]):
            raise RuntimeError("recorded Modal actual would exceed the constitutional total cap")
        return self._append(
            {
                "event": "actual",
                "job_id": job_id,
                "envelope": envelope,
                "actual_cost_usd": float(actual_cost_usd),
                "provider_receipt": provider_receipt,
                "actual_cumulative_usd": projected_total,
            }
        )

    def _append(self, row: Mapping[str, Any]) -> dict[str, Any]:
        payload = {
            **row,
            "recorded_at": datetime.now(timezone.utc).isoformat(timespec="microseconds"),
        }
        payload["record_hash"] = content_hash(json.dumps(payload, sort_keys=True))
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
        try:
            os.write(fd, (json.dumps(payload, sort_keys=True) + "\n").encode())
        finally:
            os.close(fd)
        return payload


def operations_audit(root: Path = ROOT) -> dict[str, Any]:
    governance = governance_audit()
    network = network_policy_audit()
    budget_policy = _yaml(CONFIG / "modal_budget.yaml")
    envelope_sum = sum(float(value) for value in budget_policy.get("envelopes", {}).values())
    ledger_path = root / str(budget_policy.get("ledger_path") or "")
    ledger_rows = []
    ledger_errors = []
    if ledger_path.is_file():
        for line_number, line in enumerate(ledger_path.read_text().splitlines(), 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                ledger_errors.append({"line": line_number, "reason": "invalid_json"})
                continue
            ledger_rows.append(row)
    actual_rows = [row for row in ledger_rows if row.get("event") == "actual"]
    actual_total = sum(float(row.get("actual_cost_usd") or 0) for row in actual_rows)
    envelope_actuals = {
        envelope: sum(
            float(row.get("actual_cost_usd") or 0)
            for row in actual_rows
            if row.get("envelope") == envelope
        )
        for envelope in budget_policy.get("envelopes", {})
    }
    if any(float(row.get("actual_cost_usd") or 0) > float(budget_policy["default_job_cap"]) for row in actual_rows):
        ledger_errors.append({"reason": "actual_job_cap_exceeded"})
    if actual_total > float(budget_policy.get("total_cap") or -1):
        ledger_errors.append({"reason": "actual_total_cap_exceeded"})
    for envelope, actual in envelope_actuals.items():
        if actual > float(budget_policy["envelopes"][envelope]):
            ledger_errors.append({"reason": "actual_envelope_cap_exceeded", "envelope": envelope})
    budget = {
        "total_cap": budget_policy.get("total_cap"),
        "default_job_cap": budget_policy.get("default_job_cap"),
        "envelope_sum": envelope_sum,
        "ledger_path": str(ledger_path.relative_to(root)),
        "ledger_actual_rows": len(actual_rows),
        "ledger_actual_total": actual_total,
        "ledger_envelope_actuals": envelope_actuals,
        "ledger_errors": ledger_errors,
        "passes": envelope_sum == float(budget_policy.get("total_cap") or -1)
        and float(budget_policy.get("default_job_cap") or 0) <= 3.0,
    }
    budget["passes"] = budget["passes"] and not ledger_errors
    report = {"governance": governance, "network": network, "budget": budget}
    report["passes"] = all(section["passes"] for section in report.values())
    return report
