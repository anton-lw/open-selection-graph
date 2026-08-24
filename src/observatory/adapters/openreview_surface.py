"""Unauthenticated census of OpenReview venue groups and public configurations."""

from __future__ import annotations

import json
import re
from typing import Any, Iterable, Iterator, Mapping

from ..connectors.base import (
    Connector,
    ConnectorContext,
    CoverageEvidence,
    FetchBatch,
    NormalizedRecord,
    RawItem,
    SourceEstimate,
)
from ..connectors.http import PoliteSession, RatePolicy
from ..ids import content_hash, stable_id
from .common import epoch_ms, json_text


def _value(value: Any) -> Any:
    return value.get("value") if isinstance(value, dict) and "value" in value else value


def _public_configuration(group: Mapping[str, Any]) -> dict[str, Any]:
    content = group.get("content") or {}
    allowed_patterns = (
        "submission", "review", "decision", "comment", "rebuttal", "response", "revision",
        "withdraw", "desk_reject", "public_", "venue", "title", "subtitle", "date", "start_date",
    )
    return {
        key: _value(value)
        for key, value in content.items()
        if any(pattern in key.lower() for pattern in allowed_patterns)
        and not any(sensitive in key.lower() for sensitive in ("email", "contact", "message_sender"))
    }


def _architecture(group_id: str, config: Mapping[str, Any]) -> str:
    low = group_id.lower()
    if low == "tmlr" or "transactions_on_machine_learning_research" in low:
        return "rolling_threshold"
    if any(token in low for token in ("conference", "workshop", "symposium", "meeting")):
        return "competitive_quota"
    if config.get("public_submissions") is True:
        return "rolling_threshold"
    return "unknown"


def _year(group_id: str) -> int | None:
    found = re.search(r"(?:19|20)\d{2}", group_id)
    return int(found.group()) if found else None


class OpenReviewSurfaceConnector(Connector):
    """Map public venue configuration; Notes remain a separate auth-gated pull."""

    source_id = "openreview_surface"
    connector_version = "2"
    endpoint = "https://api2.openreview.net/groups"

    def __init__(self, *, batch_size: int = 50):
        self.batch_size = min(max(batch_size, 1), 100)
        self._venue_ids: tuple[str, ...] = ()
        self._found_cycles: list[str] = []

    def _session(self, context: ConnectorContext) -> PoliteSession:
        return PoliteSession(
            cache_dir=context.cache_dir / self.source_id,
            allowed_hosts={"api2.openreview.net", "api.openreview.net"},
            policy=RatePolicy(min_interval_seconds=0.25, max_retries=5),
        )

    def _ids(self, context: ConnectorContext) -> tuple[str, ...]:
        if self._venue_ids:
            return self._venue_ids
        session = self._session(context)
        members: set[str] = set()
        for base, group_id in (
            ("https://api2.openreview.net/groups", "active_venues"),
            ("https://api.openreview.net/groups", "venues"),
        ):
            response = session.get(base, params={"id": group_id})
            groups = response.json().get("groups") or []
            if groups:
                members.update(str(value) for value in groups[0].get("members") or [] if value)
        self._venue_ids = tuple(sorted(members))
        return self._venue_ids

    def discover(self, context: ConnectorContext) -> Iterable[Mapping[str, Any]]:
        ids = self._ids(context)
        yield {
            "api_v2_active_group": "active_venues", "api_v1_historical_group": "venues",
            "unique_public_venue_ids": len(ids), "first": ids[:10],
        }

    def count(self, context: ConnectorContext) -> SourceEstimate:
        return SourceEstimate(
            self.source_id, len(self._ids(context)),
            expected_requests=2 + (len(self._ids(context)) + self.batch_size - 1) // self.batch_size,
            method="union of public API v2 active_venues and API v1 venues group members",
            confidence="provider enumeration",
        )

    def fetch(
        self,
        context: ConnectorContext,
        *,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> Iterator[FetchBatch]:
        ids = self._ids(context)
        start = int(cursor or 0)
        stop = min(len(ids), start + limit) if limit is not None else len(ids)
        session = self._session(context)
        for offset in range(start, stop, self.batch_size):
            wanted = ids[offset:min(offset + self.batch_size, stop)]
            response = session.get(self.endpoint, params={"ids": ",".join(wanted)})
            returned = {str(row.get("id")): row for row in response.json().get("groups") or []}
            api_versions = {venue_id: "v2" for venue_id in returned}
            missing = [venue_id for venue_id in wanted if venue_id not in returned]
            if missing:
                legacy = session.get(
                    "https://api.openreview.net/groups", params={"ids": ",".join(missing)}
                ).json().get("groups") or []
                for row in legacy:
                    venue_id = str(row.get("id"))
                    returned[venue_id] = row
                    api_versions[venue_id] = "v1"
            items = tuple(
                RawItem(
                    native_id=venue_id,
                    object_type="venue_group_configuration",
                    payload=json.dumps(returned.get(venue_id, {"id": venue_id, "missing": True}), sort_keys=True),
                    source_url=f"https://openreview.net/group?id={venue_id}",
                    created_at=epoch_ms((returned.get(venue_id) or {}).get("tcdate")),
                    modified_at=epoch_ms((returned.get(venue_id) or {}).get("tmdate")),
                    licence="CC-BY-4.0-configuration",
                    release_class="redistribute",
                    metadata={
                        "requested_api": "v2_then_v1_fallback",
                        "resolved_api": api_versions.get(venue_id), "present": venue_id in returned,
                    },
                )
                for venue_id in wanted
            )
            next_offset = offset + len(wanted)
            done = next_offset >= stop
            yield FetchBatch(items, None if done else str(next_offset), done, f"openreview-groups:{offset}", len(ids))

    def normalize(
        self,
        item: RawItem,
        *,
        source_object_id: str,
        provenance_event_id: str,
    ) -> Iterable[NormalizedRecord]:
        group = json.loads(item.payload)
        if group.get("missing"):
            return
        venue_id = str(group["id"])
        config = _public_configuration(group)
        architecture = _architecture(venue_id, config)
        gate_id = stable_id("gate", self.source_id, venue_id)
        cycle_id = stable_id("gate_cycle", self.source_id, venue_id)
        policy_id = stable_id("policy_version", self.source_id, f"{venue_id}|{group.get('tmdate')}")
        self._found_cycles.append(cycle_id)
        year = _year(venue_id)
        common = {
            "source_id": self.source_id, "source_object_id": source_object_id,
            "provenance_event_id": provenance_event_id,
            "observed_at": item.modified_at or item.created_at, "record_version": 1,
        }
        yield NormalizedRecord("gate", {
            "gate_id": gate_id, "native_id": venue_id,
            "name": str(config.get("title") or config.get("subtitle") or venue_id),
            "organization": venue_id.split("/")[0], "domain": None, "country": None,
            "architecture": architecture, "active_from": item.created_at, "active_to": None, **common,
        })
        yield NormalizedRecord("policy_version", {
            "policy_version_id": policy_id, "gate_id": gate_id, "native_id": venue_id,
            "effective_at": item.modified_at or item.created_at, "valid_to": None,
            "criteria_json": json_text({k: v for k, v in config.items() if "review" in k.lower()}),
            "rubric_json": json_text({k: v for k, v in config.items() if any(x in k.lower() for x in ("rating", "score", "confidence"))}),
            "stage_rules_json": json_text({
                k: v for k, v in config.items()
                if any(x in k.lower() for x in ("submission", "decision", "withdraw", "desk_reject", "public_"))
            }),
            "quota_or_cap": None, "anonymity_model": "native configuration retained",
            "revision_rules": json_text({k: v for k, v in config.items() if "revision" in k.lower()}),
            "policy_url": item.source_url, "content_hash": content_hash(json.dumps(config, sort_keys=True)),
            "date_confidence": 1.0 if group.get("tmdate") else 0.5, **common,
        })
        yield NormalizedRecord("gate_cycle", {
            "gate_cycle_id": cycle_id, "gate_id": gate_id, "native_id": venue_id,
            "name": str(config.get("subtitle") or config.get("title") or venue_id), "track": None,
            "cycle_start": f"{year}-01-01T00:00:00+00:00" if year else None,
            "cycle_end": f"{year}-12-31T23:59:59+00:00" if year else None,
            "policy_version_id": policy_id, "architecture": architecture,
            "received_count": None, "observable_count": None, "evaluated_count": None,
            "selected_count": None, "status": "surface-mapped", **common,
        })

    def validate_fixture(self, context: ConnectorContext) -> Mapping[str, Any]:
        fixture = context.fixture_dir / self.source_id / "venue_group.json"
        if not fixture.exists():
            return {"passes": False, "reason": "fixture absent"}
        row = json.loads(fixture.read_text())
        config = _public_configuration(row)
        return {
            "passes": bool(row.get("id") and any("submission" in key for key in config)),
            "id": row.get("id"), "sensitive_fields_released": False,
        }

    def emit_coverage(self, context: ConnectorContext, *, found_count: int) -> Iterable[CoverageEvidence]:
        for cycle_id in sorted(set(self._found_cycles)):
            yield CoverageEvidence(
                gate_cycle_id=cycle_id, object_type="venue_configuration",
                earliest_public_stage="configuration only; candidate invitation audit required",
                observability_grade="U", expected_count=1, found_count=1,
                expected_count_method="venue group enumerated by provider active/historical groups",
                query_or_invitation="api2 /groups + api v1 venues group",
                known_hidden_stages=("candidate Notes require invitation-specific access audit",),
                audit_status="verified_configuration_only",
            )
