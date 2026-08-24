"""Run-scoped OpenReview population manifests and process-graph audits."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

from .ids import content_hash, stable_id


def build_passing_state_manifest(audit_path: Path, output: Path) -> Path:
    """Freeze only cycles whose invitation/readers audit earned grade B."""
    audit = json.loads(audit_path.read_text())
    passing = {row["venue_id"] for row in audit.get("cycles") or [] if row.get("observability_grade") == "B"}
    invitations = []
    for row in audit.get("invitations") or []:
        if row.get("venue_id") not in passing:
            continue
        if row.get("error_class") or row.get("provider_note_count") is None:
            raise ValueError("passing cycle contains an unresolved invitation")
        invitations.append(
            {
                "api_version": row.get("api_version") or "v2",
                "invitation": row["invitation"],
                "kind": row.get("kind") or "unknown",
                "provider_note_count": int(row["provider_note_count"]),
                "venue_id": row["venue_id"],
            }
        )
    if not invitations:
        raise ValueError("no OpenReview cycle passed the public-invitation audit")
    body = {
        "schema": "observatory.openreview-passing-state-manifest/1",
        "audit_report_hash": audit.get("report_hash"),
        "passing_cycle_count": len(passing),
        "invitations": sorted(
            invitations,
            key=lambda row: (row["api_version"], row["invitation"]),
        ),
    }
    body["manifest_hash"] = content_hash(json.dumps(body, sort_keys=True))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n")
    return output


def _checkpoint_start(run_root: Path, query_hash: str) -> str:
    path = run_root / "checkpoints" / f"openreview_api-{query_hash[:12]}.json"
    body = json.loads(path.read_text())
    if not body.get("complete") or not body.get("started_at"):
        raise ValueError(f"OpenReview run is not complete: {path}")
    return str(body["started_at"])


def _run_raw_rows(
    raw_root: Path,
    *,
    run_root: Path,
    query_hash: str | Iterable[str],
) -> Iterable[tuple[dict[str, Any], dict[str, Any]]]:
    from .storage import RawStore

    query_hashes = (query_hash,) if isinstance(query_hash, str) else tuple(query_hash)
    if not query_hashes or len(set(query_hashes)) != len(query_hashes):
        raise ValueError("OpenReview query hashes must be non-empty and unique")
    started_at = {_checkpoint_start(run_root, value) for value in query_hashes}
    raw = RawStore(raw_root)
    # The normalized source_object shards are immutable and run-hash scoped.
    # They are the authoritative page index for concurrently committed Modal
    # shards: a shared append-only JSONL raw manifest cannot provide the same
    # guarantee on an object-backed mounted volume, even though every raw blob
    # and every run-specific Parquet shard remains intact.
    source_partition = (
        raw_root.parent
        / "normalized"
        / "source_object"
        / "source_id=openreview_api"
    )
    run_files = {
        value: sorted(source_partition.glob(f"run-{value[:16]}-*.parquet"))
        for value in query_hashes
    }
    if all(run_files.values()):
        import pyarrow.parquet as pq

        seen: set[str] = set()
        for value in query_hashes:
            for path in run_files[value]:
                table = pq.ParquetFile(path).read(
                    columns=[
                        "source_object_id",
                        "native_id",
                        "object_type",
                        "byte_hash",
                        "retrieved_at",
                    ]
                )
                for row in table.to_pylist():
                    if row.get("object_type") not in {
                        "notes_edits_page_bundle",
                        "forum_graph_page_bundle",
                    }:
                        continue
                    source_object_id = str(row.get("source_object_id") or "")
                    if not source_object_id:
                        raise ValueError(f"run source-object shard omitted its primary key: {path}")
                    if source_object_id in seen:
                        continue
                    seen.add(source_object_id)
                    byte_hash = str(row.get("byte_hash") or "")
                    if not byte_hash:
                        raise ValueError(f"run source-object shard omitted a byte hash: {path}")
                    payload_bytes = raw.get(byte_hash)
                    if content_hash(payload_bytes) != byte_hash:
                        raise ValueError(f"run raw object hash failed: {byte_hash}")
                    payload = json.loads(payload_bytes)
                    receipt = {
                        **row,
                        "query_hash": value,
                        "retrieved_at": (
                            row["retrieved_at"].isoformat()
                            if hasattr(row.get("retrieved_at"), "isoformat")
                            else row.get("retrieved_at")
                        ),
                    }
                    yield receipt, payload
        return

    manifest = raw_root / "manifests" / "openreview_api.jsonl"
    for line in manifest.read_text().splitlines():
        row = json.loads(line)
        if row.get("retrieved_at") not in started_at:
            continue
        if row.get("object_type") not in {
            "notes_edits_page_bundle",
            "forum_graph_page_bundle",
        }:
            continue
        byte_hash = str(row["byte_hash"])
        payload_bytes = raw.get(byte_hash)
        if content_hash(payload_bytes) != byte_hash:
            raise ValueError(f"manifest raw object hash failed: {byte_hash}")
        payload = json.loads(payload_bytes)
        yield row, payload


def _invitation_values(row: Mapping[str, Any], fallback: str | None = None) -> list[str]:
    if fallback:
        return [fallback]
    singular = row.get("invitation")
    if isinstance(singular, str) and singular:
        return [singular]
    return [str(value) for value in (row.get("invitations") or []) if isinstance(value, str) and value]


def _role(invitations: Iterable[str]) -> str:
    joined = " ".join(invitations).lower()
    if "ethic" in joined and "review" in joined:
        return "ethics_review"
    if "meta_review" in joined or "metareview" in joined:
        return "meta_review"
    if "official_review" in joined or "review" in joined:
        return "official_review"
    if "decision" in joined or "recommendation" in joined:
        return "decision"
    if "rebuttal" in joined or "author_response" in joined or "author-response" in joined:
        return "author_response"
    if "revision" in joined:
        return "revision"
    if "comment" in joined or "discussion" in joined:
        return "public_comment"
    if "withdraw" in joined:
        return "withdrawal"
    if "desk_reject" in joined:
        return "desk_rejection"
    if "submission" in joined:
        return "submission"
    return "auxiliary"


def _embedded(bundle: Mapping[str, Any]) -> Iterable[dict[str, Any]]:
    for item in bundle.get("items") or []:
        row = item.get("payload") or {}
        if not isinstance(row, Mapping):
            continue
        yield {
            "native_id": item.get("native_id"),
            "object_type": item.get("object_type"),
            "row": dict(row),
            "metadata": dict(item.get("metadata") or {}),
        }


def build_forum_manifest(
    raw_root: Path,
    output: Path,
    *,
    run_root: Path,
    state_query_hash: str,
    state_manifest_path: Path | None = None,
) -> Path:
    """Enumerate candidate forums from the exact completed state pass."""
    invitation_cycles: dict[tuple[str, str], str] = {}
    if state_manifest_path is not None:
        state_manifest = json.loads(state_manifest_path.read_text())
        invitation_cycles = {
            (str(row.get("api_version") or "v2"), str(row["invitation"])): str(
                row["venue_id"]
            )
            for row in state_manifest.get("invitations") or []
        }
    forums: dict[tuple[str, str], dict[str, str]] = {}
    conflicts: list[dict[str, str]] = []
    for _, bundle in _run_raw_rows(raw_root, run_root=run_root, query_hash=state_query_hash):
        version = str(bundle.get("api_version") or "v2")
        invitation = str(bundle.get("invitation") or "")
        raw_venue_id = str(bundle.get("venue_id") or "")
        manifest_venue_id = invitation_cycles.get((version, invitation), "")
        if raw_venue_id and manifest_venue_id and raw_venue_id != manifest_venue_id:
            raise ValueError("state page cycle conflicts with the passing-state manifest")
        venue_id = manifest_venue_id or raw_venue_id
        if not venue_id:
            raise ValueError("state page cannot be mapped to an audited cycle")
        for item in _embedded(bundle):
            if item["object_type"] != "note":
                continue
            row = item["row"]
            forum = str(row.get("forum") or row.get("id") or "")
            if not forum:
                continue
            key = (version, forum)
            candidate = {"api_version": version, "forum": forum, "venue_id": venue_id}
            previous = forums.get(key)
            if previous and previous["venue_id"] != venue_id:
                conflicts.append(
                    {
                        "api_version": version,
                        "forum": forum,
                        "first_venue_id": previous["venue_id"],
                        "second_venue_id": venue_id,
                    }
                )
            elif not previous:
                forums[key] = candidate
    if conflicts:
        raise ValueError(f"forum-to-cycle conflicts: {conflicts[:5]}")
    if not forums:
        raise ValueError("state pass yielded no candidate forums")
    body = {
        "schema": "observatory.openreview-forum-manifest/1",
        "state_query_hash": state_query_hash,
        "forum_count": len(forums),
        "forums": [forums[key] for key in sorted(forums)],
    }
    body["manifest_hash"] = content_hash(json.dumps(body, sort_keys=True))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n")
    return output


def _timestamp(row: Mapping[str, Any]) -> int | None:
    for key in ("tcdate", "cdate", "tmdate", "mdate"):
        value = row.get(key)
        if isinstance(value, (int, float)):
            return int(value)
    return None


def _merge_note_edit_state(
    states: dict[str, dict[str, Any]],
    *,
    edit: Mapping[str, Any],
    edit_id: str,
    venue_id: str,
) -> None:
    """Apply one partial API2 Edit to a compact inferred Note state.

    Domain Edit pages are ordered by immutable Edit id for resumability, not
    chronology.  Field-level clocks make the merge order-independent while
    retaining only one inferred Note plus clocks per native Note id.
    """
    target = edit.get("note") or {}
    if not isinstance(target, Mapping):
        return
    native_id = str(target.get("id") or "")
    if not native_id:
        return
    stamp = _timestamp(edit) or _timestamp(target) or -1
    order = (stamp, edit_id)
    state = states.setdefault(
        native_id,
        {
            "row": {"id": native_id},
            "edit": {},
            "native_id": native_id,
            "venue_id": venue_id,
            "_field_clock": {},
            "_content_clock": {},
            "_invitations": set(),
            "_last_order": (-1, ""),
        },
    )
    state["_invitations"].update(_invitation_values(edit))
    state["_invitations"].update(_invitation_values(target))
    row = state["row"]
    field_clock = state["_field_clock"]
    for name, value in target.items():
        if name == "content":
            if not isinstance(value, Mapping):
                continue
            content = row.setdefault("content", {})
            content_clock = state["_content_clock"]
            for field_name, field_value in value.items():
                if order < content_clock.get(field_name, (-1, "")):
                    continue
                content_clock[field_name] = order
                if isinstance(field_value, Mapping) and field_value.get("delete") is True:
                    content.pop(field_name, None)
                else:
                    content[field_name] = field_value
            continue
        if order < field_clock.get(name, (-1, "")):
            continue
        field_clock[name] = order
        row[name] = value
    if order >= state["_last_order"]:
        state["_last_order"] = order
        state["edit"] = dict(edit)


def _materialize_note_states(states: Mapping[str, dict[str, Any]]) -> Iterable[dict[str, Any]]:
    for native_id, state in states.items():
        row = state["row"]
        row["invitations"] = sorted(state["_invitations"])
        row.setdefault("forum", native_id)
        yield {
            "row": row,
            "edit": state["edit"],
            "native_id": native_id,
            "venue_id": state["venue_id"],
        }


def _build_openreview_process_audit_legacy(
    raw_root: Path,
    output: Path,
    *,
    run_root: Path,
    state_manifest_path: Path,
    state_query_hash: str,
    forum_query_hash: str,
) -> Path:
    """Audit count reconciliation, timelines, and graph field completeness."""
    state_manifest = json.loads(state_manifest_path.read_text())
    expected = {
        (row["api_version"], row["invitation"]): int(row["provider_note_count"])
        for row in state_manifest["invitations"]
    }
    venue_for_invitation = {
        (row["api_version"], row["invitation"]): row["venue_id"] for row in state_manifest["invitations"]
    }
    state_found: Counter[tuple[str, str]] = Counter()
    current_state: dict[str, str] = {}
    timeline: dict[str, list[tuple[int, str, str]]] = defaultdict(list)
    state_native_ids: set[str] = set()
    for _, bundle in _run_raw_rows(raw_root, run_root=run_root, query_hash=state_query_hash):
        version = str(bundle.get("api_version") or "v2")
        invitation = str(bundle.get("invitation") or "")
        for item in _embedded(bundle):
            row = item["row"]
            if item["object_type"] == "note":
                state_found[(version, invitation)] += 1
                native_id = str(row.get("id") or item["native_id"] or "")
                forum = str(row.get("forum") or native_id)
                state_native_ids.add(native_id)
                current_state[forum] = invitation
                if (stamp := _timestamp(row)) is not None:
                    timeline[forum].append((stamp, invitation, native_id))
            elif item["object_type"] == "note_edit":
                target = row.get("note") or {}
                forum = str(target.get("forum") or target.get("id") or "")
                invitations = _invitation_values(row)
                if forum and invitations and (stamp := _timestamp(row)) is not None:
                    timeline[forum].append((stamp, invitations[-1], str(row.get("id") or item["native_id"])))

    invitation_reconciliation = []
    cycle_counts: dict[str, dict[str, int]] = defaultdict(lambda: {"expected": 0, "found": 0})
    for key, expected_count in sorted(expected.items()):
        found = state_found[key]
        venue_id = venue_for_invitation[key]
        cycle_counts[venue_id]["expected"] += expected_count
        cycle_counts[venue_id]["found"] += found
        invitation_reconciliation.append(
            {
                "api_version": key[0],
                "invitation": key[1],
                "venue_id": venue_id,
                "expected": expected_count,
                "found": found,
                "ratio": found / expected_count if expected_count else 1.0,
            }
        )
    cycle_reconciliation = []
    for venue_id, counts in sorted(cycle_counts.items()):
        ratio = counts["found"] / counts["expected"] if counts["expected"] else 1.0
        cycle_reconciliation.append({"venue_id": venue_id, **counts, "ratio": ratio})

    sampled_forums = sorted(
        timeline,
        key=lambda value: content_hash(value),
    )[: min(100, len(timeline))]
    timeline_audit = []
    for forum in sampled_forums:
        events = sorted(set(timeline[forum]))
        timeline_audit.append(
            {
                "forum": forum,
                "event_count": len(events),
                "timestamps_nondecreasing": all(left[0] <= right[0] for left, right in zip(events, events[1:])),
                "current_state_invitation": current_state.get(forum),
                "transition_invitations": [row[1] for row in events],
            }
        )

    forum_count = 0
    forum_provider_total = 0
    forum_found_total = 0
    forum_exact_count = 0
    forum_seen: set[str] = set()
    duplicate_ids: Counter[str] = Counter()
    orphan_count_by_cycle: Counter[str] = Counter()
    object_count_by_cycle: Counter[str] = Counter()
    role_counts: Counter[str] = Counter()
    violation_counts: Counter[str] = Counter()
    auxiliary_count = 0
    in_scope_count = 0
    for _, raw_bundle in _run_raw_rows(raw_root, run_root=run_root, query_hash=forum_query_hash):
        forum_bundles = raw_bundle.get("forums") or [raw_bundle]
        for bundle in forum_bundles:
            forum_count += 1
            venue_id = str(bundle.get("venue_id") or "unknown")
            provider_total = int(bundle.get("provider_note_count") or 0)
            objects = [item for item in _embedded(bundle) if item["object_type"] == "note"]
            forum_provider_total += provider_total
            forum_found_total += len(objects)
            forum_exact_count += len(objects) == provider_total
            ids = {str(item["row"].get("id") or "") for item in objects}
            for item in objects:
                row = item["row"]
                native_id = str(row.get("id") or item["native_id"] or "")
                duplicate_ids[native_id] += 1
                forum_seen.add(native_id)
                invitations = _invitation_values(
                    row,
                    str(item["metadata"].get("invitation_query") or "") or None,
                )
                role = _role(invitations)
                role_counts[role] += 1
                if role == "auxiliary":
                    auxiliary_count += 1
                    continue
                in_scope_count += 1
                object_count_by_cycle[venue_id] += 1
                checks = {
                    "native_id": bool(native_id),
                    "role": role != "auxiliary",
                    "invitation": bool(invitations),
                    "readers": "readers" in row,
                    "timestamp": _timestamp(row) is not None,
                    "native_fields": isinstance(row.get("content"), Mapping),
                    "forum_version_relation": bool(
                        row.get("forum") or row.get("replyto") or role in {"submission", "withdrawal", "desk_rejection"}
                    ),
                }
                for name, passes in checks.items():
                    if not passes:
                        violation_counts[name] += 1
                reply_to = str(row.get("replyto") or "")
                if reply_to and reply_to not in ids:
                    orphan_count_by_cycle[venue_id] += 1

    duplicate_count = sum(count - 1 for count in duplicate_ids.values() if count > 1)
    state_cycle_passes = bool(cycle_reconciliation) and all(row["ratio"] >= 0.95 for row in cycle_reconciliation)
    timeline_passes = bool(timeline_audit) and all(row["timestamps_nondecreasing"] for row in timeline_audit)
    forum_count_passes = forum_count > 0 and forum_exact_count == forum_count
    completeness_passes = in_scope_count > 0 and not violation_counts
    report: dict[str, Any] = {
        "schema": "observatory.openreview-process-audit/1",
        "state_query_hash": state_query_hash,
        "forum_query_hash": forum_query_hash,
        "passing_cycle_count": len(cycle_reconciliation),
        "state_invitation_count": len(expected),
        "state_unique_native_note_count": len(state_native_ids),
        "state_cycle_reconciliation": cycle_reconciliation,
        "state_invitation_reconciliation": invitation_reconciliation,
        "state_cycles_at_or_above_95_percent": sum(row["ratio"] >= 0.95 for row in cycle_reconciliation),
        "timeline_sample_size": len(timeline_audit),
        "timeline_sample": timeline_audit,
        "forum_count": forum_count,
        "forum_provider_note_count": forum_provider_total,
        "forum_found_note_count": forum_found_total,
        "forum_exact_count_reconciliation_count": forum_exact_count,
        "forum_unique_native_note_count": len(forum_seen),
        "forum_duplicate_note_count": duplicate_count,
        "state_forum_overlap_count": len(state_native_ids & forum_seen),
        "in_scope_object_count": in_scope_count,
        "auxiliary_object_count": auxiliary_count,
        "role_counts": dict(role_counts),
        "field_violation_counts": dict(violation_counts),
        "object_count_by_cycle": dict(object_count_by_cycle),
        "orphan_reply_count_by_cycle": dict(orphan_count_by_cycle),
        "state_cycle_reconciliation_passes": state_cycle_passes,
        "timeline_audit_passes": timeline_passes,
        "forum_count_reconciliation_passes": forum_count_passes,
        "object_field_completeness_passes": completeness_passes,
        "scope_warning": (
            "The population is restricted to cycles with provider-audited public submission "
            "readers. Confidential submission/access screening and unreadable Notes remain hidden."
        ),
    }
    report["passes"] = all(
        (
            state_cycle_passes,
            timeline_passes,
            forum_count_passes,
            completeness_passes,
        )
    )
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return output


def write_openreview_forum_count_sample(
    context: Any,
    forum_manifest_path: Path,
    output: Path,
    *,
    sample_size: int = 100,
) -> Path:
    """Independently count a deterministic forum sample against the API.

    API2 accepts repeated ``forum`` filters and returns their exact union.  A
    complete keyset walk therefore validates 100 individual forums with one
    batched query (plus overflow pages), avoiding one request per forum while
    retaining per-forum comparisons.
    """
    from .adapters.openreview_api import OpenReviewAPINotesConnector

    body = json.loads(forum_manifest_path.read_text())
    forums = sorted(
        body.get("forums") or [],
        key=lambda row: content_hash(f"{row.get('api_version')}|{row.get('forum')}"),
    )[:sample_size]
    connector = OpenReviewAPINotesConnector(include_edits=False)
    rows = [{**row, "provider_note_count": None, "error_class": None} for row in forums]
    try:
        forum_ids = [str(row["forum"]) for row in forums]
        counts: Counter[str] = Counter()
        expected_total: int | None = None
        after: str | None = None
        while True:
            response = connector._get_json(
                context,
                "v2",
                "/notes",
                {
                    "forum": forum_ids,
                    "limit": 1_000,
                    "count": "true" if expected_total is None else "false",
                    "trash": "true",
                    "sort": "id",
                    **({"after": after} if after else {}),
                },
            )
            notes = response.get("notes") or []
            if expected_total is None:
                expected_total = int(response.get("count", len(notes)))
            for note in notes:
                forum = str(note.get("forum") or note.get("id") or "")
                if forum not in forum_ids:
                    raise ValueError("batched OpenReview Notes query returned an unrequested forum")
                counts[forum] += 1
            if not notes or sum(counts.values()) >= expected_total:
                break
            next_after = str(notes[-1].get("id") or "")
            if not next_after or next_after == after:
                raise ValueError("batched OpenReview Notes keyset did not advance")
            after = next_after
        if sum(counts.values()) != expected_total:
            raise ValueError("batched OpenReview Notes count did not reconcile")
        for row in rows:
            row["provider_note_count"] = counts[str(row["forum"])]
    except Exception as exc:
        for row in rows:
            row["error_class"] = type(exc).__name__
    report: dict[str, Any] = {
        "schema": "observatory.openreview-forum-count-sample/1",
        "forum_manifest_hash": body.get("manifest_hash"),
        "sample_method": "lowest deterministic content hashes",
        "provider_query_method": "batched repeated-forum filter with exact keyset exhaustion",
        "requested_sample_size": sample_size,
        "sample_size": len(rows),
        "rows": rows,
        "passes": bool(rows) and not any(row["error_class"] for row in rows),
    }
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return output


def build_openreview_process_audit(
    raw_root: Path,
    output: Path,
    *,
    run_root: Path,
    state_manifest_path: Path,
    state_query_hash: str,
    forum_query_hash: str | Iterable[str],
    note_query_hash: str | Iterable[str] | None = None,
    forum_count_sample_path: Path | None = None,
) -> Path:
    """Audit state counts and the complete readable Note/Edit graph."""
    state_manifest = json.loads(state_manifest_path.read_text())
    expected = {
        (row["api_version"], row["invitation"]): int(row["provider_note_count"])
        for row in state_manifest["invitations"]
    }
    venue_for_invitation = {
        (row["api_version"], row["invitation"]): row["venue_id"] for row in state_manifest["invitations"]
    }
    state_found: Counter[tuple[str, str]] = Counter()
    current_state: dict[str, str] = {}
    forum_venue: dict[str, str] = {}
    timeline: dict[str, list[tuple[int, str, str]]] = defaultdict(list)
    state_native_ids: set[str] = set()
    for _, bundle in _run_raw_rows(raw_root, run_root=run_root, query_hash=state_query_hash):
        version = str(bundle.get("api_version") or "v2")
        invitation = str(bundle.get("invitation") or "")
        raw_venue_id = str(bundle.get("venue_id") or "")
        venue_id = venue_for_invitation.get((version, invitation), "")
        if not venue_id:
            raise ValueError("state page invitation is absent from the passing-state manifest")
        if raw_venue_id and raw_venue_id != venue_id:
            raise ValueError("state page cycle conflicts with the passing-state manifest")
        for item in _embedded(bundle):
            if item["object_type"] != "note":
                continue
            row = item["row"]
            state_found[(version, invitation)] += 1
            native_id = str(row.get("id") or item["native_id"] or "")
            forum = str(row.get("forum") or native_id)
            state_native_ids.add(native_id)
            current_state[forum] = invitation
            prior_venue = forum_venue.setdefault(forum, venue_id)
            if prior_venue != venue_id:
                raise ValueError("candidate forum maps to multiple audited cycles")
            if (stamp := _timestamp(row)) is not None:
                timeline[forum].append((stamp, invitation, native_id))

    invitation_reconciliation = []
    cycle_counts: dict[str, dict[str, int]] = defaultdict(lambda: {"expected": 0, "found": 0})
    for key, expected_count in sorted(expected.items()):
        found = state_found[key]
        venue_id = venue_for_invitation[key]
        cycle_counts[venue_id]["expected"] += expected_count
        cycle_counts[venue_id]["found"] += found
        invitation_reconciliation.append(
            {
                "api_version": key[0],
                "invitation": key[1],
                "venue_id": venue_id,
                "expected": expected_count,
                "found": found,
                "ratio": found / expected_count if expected_count else 1.0,
            }
        )
    cycle_reconciliation = []
    for venue_id, counts in sorted(cycle_counts.items()):
        ratio = counts["found"] / counts["expected"] if counts["expected"] else 1.0
        cycle_reconciliation.append({"venue_id": venue_id, **counts, "ratio": ratio})

    domain_expected: dict[str, int] = {}
    domain_found: Counter[str] = Counter()
    domain_count_conflicts: list[dict[str, Any]] = []
    inferred_notes: dict[str, dict[str, Any]] = {}
    edit_ids: Counter[str] = Counter()
    edit_envelope_violations: Counter[str] = Counter()
    legacy_forums: list[Mapping[str, Any]] = []
    for _, raw_bundle in _run_raw_rows(raw_root, run_root=run_root, query_hash=forum_query_hash):
        if raw_bundle.get("provider_edit_count") is None:
            legacy_forums.extend(raw_bundle.get("forums") or [raw_bundle])
            continue
        domain = str(raw_bundle.get("domain") or raw_bundle.get("venue_id"))
        provider_total = int(raw_bundle["provider_edit_count"])
        prior = domain_expected.setdefault(domain, provider_total)
        if prior != provider_total:
            domain_count_conflicts.append(
                {
                    "domain": domain,
                    "first": prior,
                    "second": provider_total,
                }
            )
        for item in _embedded(raw_bundle):
            if item["object_type"] != "note_edit":
                continue
            domain_found[domain] += 1
            edit = item["row"]
            edit_id = str(edit.get("id") or item["native_id"] or "")
            edit_ids[edit_id] += 1
            target = edit.get("note") or {}
            edit_checks = {
                "native_id": bool(edit_id),
                # ``auxiliary`` is an explicit role for public process edits
                # outside the E3 construct set, not a missing classification.
                "role": bool(_role(_invitation_values(edit))),
                "invitation": bool(_invitation_values(edit)),
                "readers": "readers" in edit,
                "timestamp": _timestamp(edit) is not None,
                "native_fields": isinstance(target, Mapping),
                "forum_version_relation": isinstance(target, Mapping)
                and bool(target.get("id") or target.get("forum")),
            }
            for name, passes in edit_checks.items():
                if not passes:
                    edit_envelope_violations[name] += 1
            if not isinstance(target, Mapping):
                continue
            native_id = str(target.get("id") or "")
            forum = str(target.get("forum") or native_id)
            invitations = _invitation_values(target) or _invitation_values(edit)
            stamp = _timestamp(edit) or _timestamp(target)
            if forum and invitations and stamp is not None:
                timeline[forum].append((stamp, invitations[-1], edit_id))
            if note_query_hash is None:
                _merge_note_edit_state(
                    inferred_notes,
                    edit=edit,
                    edit_id=edit_id,
                    venue_id=domain,
                )

    current_note_batch_expected: dict[str, int] = {}
    current_note_batch_found: Counter[str] = Counter()
    current_note_batch_count_conflicts: list[dict[str, Any]] = []
    current_note_cycle_found: Counter[str] = Counter()
    current_note_ids: Counter[str] = Counter()
    current_note_requested_forums: set[str] = set()
    current_note_forum_batch: dict[str, str] = {}
    current_note_forum_partition_conflicts: list[dict[str, str]] = []
    current_notes_by_forum: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if note_query_hash is not None:
        for _, raw_bundle in _run_raw_rows(
            raw_root,
            run_root=run_root,
            query_hash=note_query_hash,
        ):
            if raw_bundle.get("provider_forum_batch_note_count") is None:
                raise ValueError("forum Note run omitted its exact provider union count")
            batch_id = str(raw_bundle.get("forum_batch_id") or "")
            descriptors = {
                str(row["forum"]): row
                for row in (raw_bundle.get("forum_batch") or [])
            }
            if not batch_id or not descriptors:
                raise ValueError("forum Note run omitted its frozen batch descriptor")
            for forum, descriptor in descriptors.items():
                venue_id = forum_venue.get(forum, "")
                descriptor_venue_id = str(descriptor.get("venue_id") or "")
                if not venue_id or venue_id not in cycle_counts:
                    raise ValueError("forum Note descriptor references an unaudited cycle")
                if descriptor_venue_id and descriptor_venue_id != venue_id:
                    raise ValueError("forum Note descriptor conflicts with candidate-state cycle")
                prior_batch = current_note_forum_batch.setdefault(forum, batch_id)
                if prior_batch != batch_id:
                    current_note_forum_partition_conflicts.append(
                        {
                            "forum": forum,
                            "first_forum_batch_id": prior_batch,
                            "second_forum_batch_id": batch_id,
                        }
                    )
                current_note_requested_forums.add(forum)
            provider_total = int(raw_bundle["provider_forum_batch_note_count"])
            prior = current_note_batch_expected.setdefault(batch_id, provider_total)
            if prior != provider_total:
                current_note_batch_count_conflicts.append(
                    {"forum_batch_id": batch_id, "first": prior, "second": provider_total}
                )
            for item in _embedded(raw_bundle):
                if item["object_type"] != "note":
                    continue
                current_note_batch_found[batch_id] += 1
                row = item["row"]
                native_id = str(row.get("id") or item["native_id"] or "")
                current_note_ids[native_id] += 1
                forum = str(row.get("forum") or native_id)
                descriptor = descriptors.get(forum)
                if descriptor is None:
                    raise ValueError("forum Note page returned an unrequested forum")
                venue_id = forum_venue.get(forum, "")
                current_note_cycle_found[venue_id] += 1
                if forum not in current_state:
                    continue
                # Raw remains lossless. The in-memory audit state retains only
                # fields needed for role, graph, and completeness checks so a
                # million-Note census does not duplicate review text in RAM.
                compact_row = {
                    key: row.get(key)
                    for key in (
                        "id",
                        "forum",
                        "replyto",
                        "invitation",
                        "invitations",
                        "readers",
                        "tcdate",
                        "cdate",
                        "tmdate",
                        "mdate",
                        "ddate",
                    )
                    if key in row
                }
                compact_row["content"] = {} if isinstance(row.get("content"), Mapping) else None
                current_notes_by_forum[forum].append(
                    {"row": compact_row, "edit": {}, "native_id": native_id}
                )

    forum_count = 0
    forum_provider_total = 0
    forum_found_total = 0
    forum_exact_count = 0
    forum_sample_reconciliation: list[dict[str, Any]] = []
    forum_seen: set[str] = set()
    duplicate_ids: Counter[str] = Counter()
    orphan_count_by_cycle: Counter[str] = Counter()
    object_count_by_cycle: Counter[str] = Counter()
    native_id_counts_by_cycle: dict[str, Counter[str]] = defaultdict(Counter)
    role_counts: Counter[str] = Counter()
    violation_counts: Counter[str] = Counter()
    auxiliary_count = 0
    in_scope_count = 0
    current_note_forum_partition_passes = bool(
        note_query_hash is None
        or (
            not current_note_forum_partition_conflicts
            and current_note_requested_forums == set(current_state)
        )
    )

    def audit_objects(
        venue_id: str,
        objects: list[dict[str, Any]],
        *,
        count_duplicates: bool,
    ) -> None:
        nonlocal auxiliary_count, in_scope_count
        ids = {str(item["native_id"]) for item in objects}
        for item in objects:
            row = item["row"]
            edit = item.get("edit") or {}
            native_id = str(item["native_id"])
            if count_duplicates:
                duplicate_ids[native_id] += 1
            forum_seen.add(native_id)
            invitations = _invitation_values(row) or _invitation_values(edit)
            role = _role(invitations)
            role_counts[role] += 1
            if role == "auxiliary":
                auxiliary_count += 1
                continue
            in_scope_count += 1
            object_count_by_cycle[venue_id] += 1
            native_id_counts_by_cycle[venue_id][native_id] += 1
            checks = {
                "native_id": bool(native_id),
                "role": role != "auxiliary",
                "invitation": bool(invitations),
                "readers": "readers" in row or "readers" in edit,
                "timestamp": (_timestamp(edit) or _timestamp(row)) is not None,
                "native_fields": isinstance(row.get("content"), Mapping),
                "forum_version_relation": bool(
                    row.get("forum") or row.get("replyto") or role in {"submission", "withdrawal", "desk_rejection"}
                ),
            }
            for name, passes in checks.items():
                if not passes:
                    violation_counts[name] += 1
            reply_to = str(row.get("replyto") or "")
            if reply_to and reply_to not in ids:
                orphan_count_by_cycle[venue_id] += 1

    domain_graph = bool(domain_expected)
    if domain_graph:
        by_forum: dict[str, list[dict[str, Any]]] = current_notes_by_forum
        if note_query_hash is None:
            by_forum = defaultdict(list)
            for item in _materialize_note_states(inferred_notes):
                row = item["row"]
                by_forum[str(row.get("forum") or item["native_id"])].append(item)
        forum_count = len(current_state)
        for forum in sorted(current_state):
            objects = by_forum.get(forum, [])
            forum_found_total += len(objects)
            audit_objects(forum_venue.get(forum, "unknown"), objects, count_duplicates=False)
        current_note_nonempty_forum_count = sum(
            bool(by_forum.get(forum)) for forum in current_state
        )
        current_note_zero_count_forum_count = (
            forum_count - current_note_nonempty_forum_count
        )
        state_root_current_note_overlap_count = len(state_native_ids & forum_seen)
        state_root_current_note_overlap_ratio = (
            state_root_current_note_overlap_count / len(state_native_ids)
            if state_native_ids
            else 0.0
        )
        sample = (
            json.loads(forum_count_sample_path.read_text())
            if forum_count_sample_path and forum_count_sample_path.exists()
            else {"passes": False, "rows": []}
        )
        sample_rows = sample.get("rows") or []
        for row in sample_rows:
            provider = int(row.get("provider_note_count") or 0)
            objects = by_forum.get(str(row["forum"]), [])
            found = len(objects)
            found_non_deleted = sum(
                not bool(item["row"].get("ddate")) for item in objects
            )
            found_public_reader = sum(
                "everyone"
                in {
                    str(reader)
                    for reader in (
                        item["row"].get("readers")
                        or item["edit"].get("readers")
                        or []
                    )
                }
                for item in objects
            )
            forum_provider_total += provider
            forum_exact_count += found == provider
            forum_sample_reconciliation.append(
                {
                    "forum": str(row["forum"]),
                    "venue_id": str(row.get("venue_id") or ""),
                    "provider_note_count": provider,
                    "current_note_count": found,
                    "current_exact": found == provider,
                    "inferred_historical_note_count": found,
                    "inferred_non_deleted_note_count": found_non_deleted,
                    "inferred_public_reader_note_count": found_public_reader,
                    "historical_exact": found == provider,
                    "non_deleted_exact": found_non_deleted == provider,
                    "public_reader_exact": found_public_reader == provider,
                }
            )
        sample_size = len(sample_rows)
        forum_count_passes = bool(
            forum_count
            and current_note_forum_partition_passes
            and sample.get("passes")
            and sample_size
            and forum_exact_count == sample_size
            and (
                all(by_forum.get(forum) for forum in current_state)
                if note_query_hash is None
                else state_root_current_note_overlap_ratio >= 0.95
            )
        )
    else:
        current_note_nonempty_forum_count = 0
        current_note_zero_count_forum_count = 0
        state_root_current_note_overlap_count = 0
        state_root_current_note_overlap_ratio = 0.0
        sample_size = 0
        for bundle in legacy_forums:
            forum_count += 1
            venue_id = str(bundle.get("venue_id") or "unknown")
            provider_total = int(bundle.get("provider_note_count") or 0)
            objects = [
                {
                    "row": item["row"],
                    "edit": {},
                    "native_id": str(item["row"].get("id") or item["native_id"] or ""),
                }
                for item in _embedded(bundle)
                if item["object_type"] == "note"
            ]
            forum_provider_total += provider_total
            forum_found_total += len(objects)
            forum_exact_count += len(objects) == provider_total
            audit_objects(venue_id, objects, count_duplicates=True)
        forum_count_passes = forum_count > 0 and forum_exact_count == forum_count

    sampled_forums = sorted(
        (forum for forum in current_state if forum in timeline),
        key=content_hash,
    )[:100]
    timeline_audit = []
    for forum in sampled_forums:
        events = sorted(set(timeline[forum]))
        timeline_audit.append(
            {
                "forum": forum,
                "event_count": len(events),
                "timestamps_nondecreasing": all(left[0] <= right[0] for left, right in zip(events, events[1:])),
                "current_state_invitation": current_state.get(forum),
                "transition_invitations": [row[1] for row in events],
            }
        )

    duplicate_count = sum(count - 1 for count in duplicate_ids.values() if count > 1)
    duplicate_edit_count = sum(count - 1 for count in edit_ids.values() if count > 1)
    cycle_graph_quality = []
    for venue_id in sorted(cycle_counts):
        object_count = object_count_by_cycle[venue_id]
        orphan_count = orphan_count_by_cycle[venue_id]
        duplicate_object_count = sum(
            count - 1 for count in native_id_counts_by_cycle[venue_id].values() if count > 1
        )
        cycle_graph_quality.append(
            {
                "venue_id": venue_id,
                "object_count": object_count,
                "orphan_reply_count": orphan_count,
                "orphan_reply_rate": orphan_count / object_count if object_count else 0.0,
                "duplicate_object_count": duplicate_object_count,
                "duplicate_object_rate": duplicate_object_count / object_count if object_count else 0.0,
            }
        )
    state_cycle_passes = bool(cycle_reconciliation) and all(row["ratio"] >= 0.95 for row in cycle_reconciliation)
    timeline_passes = bool(timeline_audit) and all(row["timestamps_nondecreasing"] for row in timeline_audit)
    domain_count_passes = bool(
        not domain_graph
        or (
            domain_expected
            and not domain_count_conflicts
            and not duplicate_edit_count
            and all(domain_found[key] == value for key, value in domain_expected.items())
        )
    )
    duplicate_current_note_count = sum(
        count - 1 for count in current_note_ids.values() if count > 1
    )
    current_note_count_passes = bool(
        note_query_hash is None
        or (
            current_note_batch_expected
            and not current_note_batch_count_conflicts
            and not duplicate_current_note_count
            and all(
                current_note_batch_found[key] == value
                for key, value in current_note_batch_expected.items()
            )
        )
    )
    edit_envelope_completeness_passes = bool(edit_ids) and not edit_envelope_violations
    completeness_passes = in_scope_count > 0 and not violation_counts and edit_envelope_completeness_passes
    domain_reconciliation = [
        {
            "venue_id": venue_id,
            "expected": expected_count,
            "found": domain_found[venue_id],
            "ratio": (
                domain_found[venue_id] / expected_count
                if expected_count
                else 1.0
            ),
        }
        for venue_id, expected_count in sorted(domain_expected.items())
    ]
    current_note_batch_reconciliation = [
        {
            "forum_batch_id": batch_id,
            "expected": expected_count,
            "found": current_note_batch_found[batch_id],
            "ratio": (
                current_note_batch_found[batch_id] / expected_count
                if expected_count
                else 1.0
            ),
        }
        for batch_id, expected_count in sorted(current_note_batch_expected.items())
    ]
    current_note_cycle_reconciliation = [
        {
            "venue_id": venue_id,
            "expected": current_note_cycle_found[venue_id],
            "found": current_note_cycle_found[venue_id],
            "ratio": 1.0,
        }
        for venue_id in sorted(cycle_counts)
    ]
    report: dict[str, Any] = {
        "schema": "observatory.openreview-process-audit/3",
        "raw_page_index_method": (
            "query-hash-scoped normalized source_object shards with inline raw byte-hash verification"
        ),
        "state_query_hash": state_query_hash,
        "forum_query_hash": (forum_query_hash if isinstance(forum_query_hash, str) else list(forum_query_hash)),
        "note_query_hash": (
            note_query_hash
            if isinstance(note_query_hash, str) or note_query_hash is None
            else list(note_query_hash)
        ),
        "passing_cycle_count": len(cycle_reconciliation),
        "state_invitation_count": len(expected),
        "state_unique_native_note_count": len(state_native_ids),
        "state_cycle_reconciliation": cycle_reconciliation,
        "state_invitation_reconciliation": invitation_reconciliation,
        "state_cycles_at_or_above_95_percent": sum(row["ratio"] >= 0.95 for row in cycle_reconciliation),
        "timeline_sample_size": len(timeline_audit),
        "timeline_sample": timeline_audit,
        "forum_count": forum_count,
        "forum_provider_note_count_in_sample": forum_provider_total,
        "forum_found_note_count": forum_found_total,
        "forum_exact_count_reconciliation_count": forum_exact_count,
        "forum_count_sample_size": sample_size,
        "forum_sample_reconciliation": forum_sample_reconciliation,
        "forum_unique_native_note_count": len(forum_seen),
        "forum_duplicate_note_count": duplicate_count,
        "state_forum_overlap_count": len(state_native_ids & forum_seen),
        "domain_edit_graph": domain_graph,
        "domain_count": len(domain_expected),
        "domain_expected_edit_count": sum(domain_expected.values()),
        "domain_found_edit_count": sum(domain_found.values()),
        "domain_reconciliation": domain_reconciliation,
        "domain_count_conflicts": domain_count_conflicts,
        "duplicate_edit_count": duplicate_edit_count,
        "current_forum_note_graph": bool(current_note_batch_expected),
        "current_note_batch_count": len(current_note_batch_expected),
        "current_expected_note_count": sum(current_note_batch_expected.values()),
        "current_found_note_count": sum(current_note_batch_found.values()),
        "current_note_batch_reconciliation": current_note_batch_reconciliation,
        "current_note_cycle_reconciliation": current_note_cycle_reconciliation,
        "current_note_batch_count_conflicts": current_note_batch_count_conflicts,
        "current_note_requested_forum_count": len(current_note_requested_forums),
        "current_note_unrequested_candidate_forum_count": len(
            set(current_state) - current_note_requested_forums
        ),
        "current_note_unexpected_requested_forum_count": len(
            current_note_requested_forums - set(current_state)
        ),
        "current_note_forum_partition_conflicts": current_note_forum_partition_conflicts,
        "current_note_forum_partition_passes": current_note_forum_partition_passes,
        "current_note_nonempty_forum_count": current_note_nonempty_forum_count,
        "current_note_zero_count_forum_count": current_note_zero_count_forum_count,
        "state_root_current_note_overlap_count": state_root_current_note_overlap_count,
        "state_root_current_note_overlap_ratio": state_root_current_note_overlap_ratio,
        "duplicate_current_note_count": duplicate_current_note_count,
        "edit_envelope_violation_counts": dict(edit_envelope_violations),
        "edit_envelope_completeness_passes": edit_envelope_completeness_passes,
        "in_scope_object_count": in_scope_count,
        "auxiliary_object_count": auxiliary_count,
        "role_counts": dict(role_counts),
        "field_violation_counts": dict(violation_counts),
        "object_count_by_cycle": dict(object_count_by_cycle),
        "orphan_reply_count_by_cycle": dict(orphan_count_by_cycle),
        "cycle_graph_quality": cycle_graph_quality,
        "state_cycle_reconciliation_passes": state_cycle_passes,
        "timeline_audit_passes": timeline_passes,
        "forum_count_reconciliation_passes": forum_count_passes,
        "domain_edit_count_reconciliation_passes": domain_count_passes,
        "current_note_count_reconciliation_passes": current_note_count_passes,
        "object_field_completeness_passes": completeness_passes,
        "scope_warning": (
            "The population is restricted to cycles with provider-audited public submission "
            "readers. Confidential submission/access screening and unreadable Notes/Edits "
            "remain hidden."
        ),
    }
    report["passes"] = all(
        (
            state_cycle_passes,
            timeline_passes,
            forum_count_passes,
            domain_count_passes,
            current_note_count_passes,
            completeness_passes,
        )
    )
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return output


def write_openreview_population_coverage(report_path: Path, output: Path) -> Path:
    """Export every audited OpenReview cycle/object denominator for the atlas."""
    report = json.loads(report_path.read_text())
    if not report.get("passes"):
        raise ValueError("OpenReview process report must pass before coverage export")
    state_rows = report.get("state_cycle_reconciliation") or []
    edit_rows = report.get("domain_reconciliation") or []
    note_rows = report.get("current_note_cycle_reconciliation") or []
    state_cycles = {str(row["venue_id"]) for row in state_rows}
    edit_cycles = {str(row["venue_id"]) for row in edit_rows}
    note_cycles = {str(row["venue_id"]) for row in note_rows}
    if not state_cycles or state_cycles != edit_cycles or (
        note_rows and note_cycles != state_cycles
    ):
        raise ValueError("OpenReview state/Edit coverage cycles are incomplete or unequal")

    hidden = [
        "confidential submission/access screening",
        "Notes or Edits unreadable by the authenticated free account",
    ]
    rows: list[dict[str, Any]] = []
    for object_type, reconciliation, stage, method, audit_status in (
        (
            "candidate_state",
            state_rows,
            "provider-audited public submission invitation",
            "sum of invitation-specific OpenReview count=true responses after native Note-id deduplication",
            "invitation_state_reconciled",
        ),
        (
            "note_edit_history",
            edit_rows,
            "provider-readable public Note/Edit graph",
            "authenticated API2 domain count=true plus provider-supported keyset exhaustion",
            "domain_edit_count_exact",
        ),
        *(
            (
                (
                    "current_note_graph",
                    note_rows,
                    "provider-readable public Note graph",
                    "authenticated API2 repeated-forum union count=true plus provider-supported keyset exhaustion",
                    "forum_union_note_count_exact",
                ),
            )
            if note_rows
            else ()
        ),
    ):
        for reconciliation_row in sorted(reconciliation, key=lambda row: str(row["venue_id"])):
            venue_id = str(reconciliation_row["venue_id"])
            expected = int(reconciliation_row["expected"])
            found = int(reconciliation_row["found"])
            ratio = float(reconciliation_row["ratio"])
            if ratio < 0.95 or (
                object_type in {"note_edit_history", "current_note_graph"}
                and found != expected
            ):
                raise ValueError(f"OpenReview coverage gate failed for {venue_id} {object_type}")
            rows.append(
                {
                    "gate_cycle_id": stable_id("gate_cycle", "openreview_api", venue_id),
                    "venue_id": venue_id,
                    "architecture": "unknown; venue-cycle policy archive required",
                    "object_type": object_type,
                    "earliest_public_stage": stage,
                    "observability_grade": "B",
                    "expected_count": expected,
                    "found_count": found,
                    "coverage_ratio": ratio,
                    "expected_count_method": method,
                    "query_or_invitation": (
                        str(report["state_query_hash"])
                        if object_type == "candidate_state"
                        else (
                            "v2:/notes?forum=<frozen candidate-forum batch>"
                            if object_type == "current_note_graph"
                            else f"v2:/notes/edits?domain={venue_id}"
                        )
                    ),
                    "known_hidden_stages": hidden,
                    "known_exclusions": [],
                    "missing_reason": None,
                    "audit_status": audit_status,
                }
            )
    body: dict[str, Any] = {
        "schema": "observatory.population-coverage/1",
        "source_id": "openreview_api",
        "process_report_hash": report["report_hash"],
        "cycle_count": len(state_cycles),
        "row_count": len(rows),
        "rows": rows,
    }
    body["export_hash"] = content_hash(json.dumps(body, sort_keys=True))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n")
    return output
