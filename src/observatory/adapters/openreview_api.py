"""Lossless OpenReview Notes/Edits acquisition using environment-only auth.

The surface census intentionally stops at venue configuration.  This adapter
is the second step: it consumes an explicit invitation manifest, preserves the
complete provider JSON for every Note/Edit, and emits conservative process
graph records.  Credentials and bearer tokens are read only from the process
environment and are never written to checkpoints, manifests, caches, or raw
object metadata.
"""

from __future__ import annotations

import json
import math
import os
import re
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

from ..connectors.base import (
    Connector,
    ConnectorContext,
    CoverageEvidence,
    FetchBatch,
    NormalizedRecord,
    RawItem,
    SourceEstimate,
    coverage_observation_id,
)
from ..connectors.http import NetworkPolicyError, PoliteSession, RatePolicy
from ..ids import content_hash, stable_id
from .common import epoch_ms, json_text


def _value(value: Any) -> Any:
    return value.get("value") if isinstance(value, dict) and "value" in value else value


def _content(row: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): _value(value) for key, value in (row.get("content") or {}).items()}


def _number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    found = re.search(r"[-+]?\d+(?:\.\d+)?", str(value))
    return float(found.group()) if found else None


def _invitation_kind(invitation: str) -> str:
    low = invitation.lower()
    if "desk_reject" in low or "decision" in low or "recommendation" in low:
        return "decision"
    if any(token in low for token in ("official_review", "review", "meta_review")):
        return "evaluation"
    if any(token in low for token in ("rebuttal", "author_response", "revision", "comment")):
        return "discussion"
    if "withdraw" in low:
        return "withdrawal"
    if "submission" in low:
        return "submission"
    return "unknown"


def _primary_invitation(row: Mapping[str, Any], fallback: str | None = None) -> str:
    """Choose a typed invitation while the complete lineage remains in raw JSON."""
    singular = row.get("invitation")
    if isinstance(singular, str) and singular:
        return singular
    invitations = [str(value) for value in (row.get("invitations") or []) if isinstance(value, str) and value]
    if not invitations:
        return fallback or "unknown"
    venue_id = str(_content(row).get("venueid") or "").rstrip("/")
    if venue_id:
        venue_state = venue_id.rsplit("/", 1)[-1].lower()
        declared_state = next(
            (value for value in reversed(invitations) if value.rstrip("/").rsplit("/", 1)[-1].lower() == venue_state),
            None,
        )
        if declared_state:
            return declared_state
    priority = {
        "decision": 5,
        "withdrawal": 4,
        "evaluation": 3,
        "discussion": 2,
        "submission": 1,
        "unknown": 0,
    }
    return max(
        enumerate(invitations),
        key=lambda pair: (priority[_invitation_kind(pair[1])], pair[0]),
    )[1]


def _manifest_rows(context: ConnectorContext) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for value in context.parameters.get("identifiers") or ():
        rows.append({"invitation": str(value), "api_version": "v2"})
    for value in context.parameters.get("files") or ():
        path = Path(value)
        if not path.is_absolute():
            path = context.workspace / path
        text = path.read_text()
        try:
            loaded = json.loads(text)
            candidates = loaded.get("invitations", loaded) if isinstance(loaded, dict) else loaded
            if not isinstance(candidates, list):
                candidates = [candidates]
        except json.JSONDecodeError:
            candidates = [line.strip() for line in text.splitlines() if line.strip()]
        for candidate in candidates:
            if isinstance(candidate, str):
                rows.append({"invitation": candidate, "api_version": "v2"})
            elif isinstance(candidate, Mapping) and candidate.get("invitation"):
                parsed = {
                    "invitation": str(candidate["invitation"]),
                    "api_version": str(candidate.get("api_version") or "v2"),
                    "venue_id": str(candidate.get("venue_id") or ""),
                }
                if candidate.get("provider_note_count") is not None:
                    parsed["provider_note_count"] = str(candidate["provider_note_count"])
                if candidate.get("kind"):
                    parsed["kind"] = str(candidate["kind"])
                rows.append(parsed)
    unique: dict[tuple[str, str], dict[str, str]] = {}
    for row in rows:
        version = "v1" if row.get("api_version") == "v1" else "v2"
        row["api_version"] = version
        unique[(version, row["invitation"])] = row
    return [unique[key] for key in sorted(unique)]


class OpenReviewAPINotesConnector(Connector):
    """Read-only, invitation-explicit OpenReview Notes/Edits connector."""

    source_id = "openreview_api"
    connector_version = "3"

    def __init__(
        self,
        *,
        page_size: int = 500,
        include_edits: bool = True,
        bundle_pages: bool = False,
    ):
        self.page_size = min(max(page_size, 1), 1000)
        self.include_edits = include_edits
        self.bundle_pages = bundle_pages
        self.force_streaming = bundle_pages
        self._sessions: dict[str, tuple[PoliteSession, bool]] = {}
        self._counts: Counter[tuple[str, str]] = Counter()
        self._provider_counts: dict[tuple[str, str], int] = {}
        self._emitted: dict[str, set[str]] = {
            "gate": set(),
            "gate_cycle": set(),
            "candidate": set(),
            "candidate_version": set(),
        }
        self._last_edit_version: dict[str, str] = {}

    @staticmethod
    def _base(version: str) -> str:
        return "https://api.openreview.net" if version == "v1" else "https://api2.openreview.net"

    def _session(self, context: ConnectorContext, version: str) -> tuple[PoliteSession, bool]:
        if version in self._sessions:
            return self._sessions[version]
        session = PoliteSession(
            cache_dir=context.cache_dir / self.source_id / version,
            allowed_hosts={"api.openreview.net", "api2.openreview.net"},
            policy=RatePolicy(
                min_interval_seconds=3.0,
                max_retries=10,
                timeout_seconds=120,
                max_backoff_seconds=300,
                max_concurrency_per_host=1,
                daily_request_ceiling=50_000,
            ),
        )
        token = os.environ.get("OPENREVIEW_TOKEN")
        username = os.environ.get("OPENREVIEW_USERNAME")
        password = os.environ.get("OPENREVIEW_PASSWORD")
        if bool(username) != bool(password):
            raise NetworkPolicyError("both OPENREVIEW_USERNAME and OPENREVIEW_PASSWORD are required")
        authenticated = False
        if token:
            session.session.headers["Authorization"] = f"Bearer {token.removeprefix('Bearer ')}"
            authenticated = True
        elif username and password:
            response = session.post_json(
                f"{self._base(version)}/login",
                payload={"id": username, "password": password, "expiresIn": 86_400},
            )
            body = response.json()
            if body.get("mfaPending"):
                raise NetworkPolicyError("OpenReview MFA is enabled; provide a short-lived OPENREVIEW_TOKEN instead")
            if not body.get("token"):
                raise NetworkPolicyError("OpenReview login succeeded without a bearer token")
            session.session.headers["Authorization"] = f"Bearer {body['token']}"
            authenticated = True
        self._sessions[version] = (session, authenticated)
        return session, authenticated

    def _get_json(
        self, context: ConnectorContext, version: str, path: str, params: Mapping[str, Any]
    ) -> dict[str, Any]:
        session, authenticated = self._session(context, version)
        # Authenticated payloads are never duplicated in the HTTP cache.  The
        # immutable raw store is the sole controlled persistence layer.
        return session.get(f"{self._base(version)}{path}", params=params, use_cache=not authenticated).json()

    def discover(self, context: ConnectorContext) -> Iterable[Mapping[str, Any]]:
        for row in _manifest_rows(context):
            yield {**row, "object_kind": _invitation_kind(row["invitation"])}

    def count(self, context: ConnectorContext) -> SourceEstimate:
        note_total = 0
        rows = _manifest_rows(context)
        for row in rows:
            if row.get("provider_note_count") is not None:
                count = int(row["provider_note_count"])
            else:
                body = self._get_json(
                    context,
                    row["api_version"],
                    "/notes",
                    {
                        "invitation": row["invitation"],
                        "limit": 1,
                        "count": "true",
                        "trash": "true",
                    },
                )
                count = int(body.get("count", len(body.get("notes") or [])))
            self._provider_counts[(row["api_version"], row["invitation"])] = count
            note_total += count
        # Edit counts are not exposed cheaply at the invitation level in a way
        # that is portable across API generations.  Four raw objects per Note
        # is an explicit conservative planning multiplier; the run manifest
        # still reports exact Note and Edit counts separately.
        object_total = (
            sum(math.ceil(count / self.page_size) for count in self._provider_counts.values())
            if self.bundle_pages
            else note_total * (4 if self.include_edits else 1)
        )
        page_requests = (note_total + self.page_size - 1) // self.page_size
        edit_requests = note_total if self.include_edits else 0
        return SourceEstimate(
            self.source_id,
            object_total,
            expected_requests=len(rows) + page_requests + edit_requests,
            expected_bytes=note_total * 50_000 if self.bundle_pages else None,
            method=(
                (
                    "provider invitation Note counts stored in lossless immutable API-page bundles; "
                    "each bundle embeds the complete per-Note Edit histories"
                    if self.include_edits
                    else "provider invitation Note counts stored in lossless immutable API-page bundles; "
                    "Edit histories acquired separately by the domain connector"
                )
                if self.bundle_pages
                else (
                    "provider invitation Note counts plus explicit 4x raw-object planning multiplier "
                    "for per-Note Edit histories"
                    if self.include_edits
                    else "exact provider invitation Note counts; Edit histories excluded from this run"
                )
            ),
            confidence=(
                (
                    "exact Note count and page count; Edit bytes conservatively estimated"
                    if self.include_edits
                    else "exact Note count and page count"
                )
                if self.bundle_pages
                else (
                    "exact Note count; conservative Edit planning estimate"
                    if self.include_edits
                    else "exact provider Note count"
                )
            ),
            objects_per_limit_unit=4.0 if self.include_edits else 1.0,
            requests_per_limit_unit=((1.0 if self.include_edits else 0.0) + 1.0 / self.page_size),
        )

    def fetch(
        self, context: ConnectorContext, *, cursor: str | None = None, limit: int | None = None
    ) -> Iterator[FetchBatch]:
        rows = _manifest_rows(context)
        invitation_index, offset = map(int, cursor.split(":")) if cursor else (0, 0)
        emitted = 0
        for index in range(invitation_index, len(rows)):
            row = rows[index]
            current = offset if index == invitation_index else 0
            while True:
                remaining = None if limit is None else limit - emitted
                if remaining is not None and remaining <= 0:
                    return
                page_limit = self.page_size if remaining is None else min(self.page_size, remaining)
                params = {
                    "invitation": row["invitation"],
                    "limit": page_limit,
                    "offset": current,
                    "count": "true",
                    "trash": "true",
                }
                body = self._get_json(context, row["api_version"], "/notes", params)
                notes = body.get("notes") or []
                provider_total = int(body.get("count", current + len(notes)))
                self._provider_counts[(row["api_version"], row["invitation"])] = provider_total
                items: list[RawItem] = []
                for note in notes:
                    note_id = str(note.get("id") or note.get("forum") or f"offset-{current}")
                    metadata = {
                        "api_version": row["api_version"],
                        "invitation_query": row["invitation"],
                        "venue_id": row.get("venue_id"),
                        "authenticated": self._sessions[row["api_version"]][1],
                    }
                    items.append(
                        RawItem(
                            native_id=f"{row['api_version']}:note:{note_id}",
                            object_type="note",
                            payload=json.dumps(note, sort_keys=True),
                            source_url=f"https://openreview.net/forum?id={note.get('forum') or note_id}",
                            created_at=epoch_ms(note.get("tcdate") or note.get("cdate")),
                            modified_at=epoch_ms(note.get("tmdate") or note.get("mdate")),
                            licence="per-object",
                            release_class="pointer_hash",
                            metadata=metadata,
                        )
                    )
                    if self.include_edits and row["api_version"] == "v2":
                        edits_body = self._get_json(
                            context,
                            "v2",
                            "/notes/edits",
                            {"note.id": note_id, "limit": 1000, "trash": "true", "sort": "tcdate:asc"},
                        )
                        edits = edits_body.get("edits") or []
                        if len(edits) >= 1000:
                            raise NetworkPolicyError(
                                f"edit history reached provider page ceiling for note {note_id}; quarantine"
                            )
                        for edit in edits:
                            edit_id = str(edit.get("id") or content_hash(json.dumps(edit, sort_keys=True)))
                            items.append(
                                RawItem(
                                    native_id=f"v2:edit:{edit_id}",
                                    object_type="note_edit",
                                    payload=json.dumps(edit, sort_keys=True),
                                    source_url=f"https://openreview.net/forum?id={note.get('forum') or note_id}",
                                    created_at=epoch_ms(edit.get("tcdate") or edit.get("cdate")),
                                    modified_at=epoch_ms(edit.get("tmdate") or edit.get("mdate")),
                                    licence="per-object",
                                    release_class="pointer_hash",
                                    metadata={**metadata, "note_id": note_id},
                                )
                            )
                self._counts[(row["invitation"], "note")] += len(notes)
                self._counts[(row["invitation"], "note_edit")] += len(items) - len(notes)
                emitted += len(notes)
                current += len(notes)
                done_invitation = not notes or current >= provider_total
                done_all = done_invitation and index + 1 >= len(rows)
                next_cursor = None if done_all else (f"{index + 1}:0" if done_invitation else f"{index}:{current}")
                batch_items: tuple[RawItem, ...]
                if self.bundle_pages and items:
                    embedded = [
                        {
                            "native_id": child.native_id,
                            "object_type": child.object_type,
                            "payload": json.loads(child.payload),
                            "source_url": child.source_url,
                            "created_at": child.created_at,
                            "modified_at": child.modified_at,
                            "licence": child.licence,
                            "release_class": child.release_class,
                            "metadata": dict(child.metadata),
                        }
                        for child in items
                    ]
                    page_native = content_hash(f"{row['api_version']}|{row['invitation']}|{current - len(notes)}")[:24]
                    batch_items = (
                        RawItem(
                            native_id=f"page:{page_native}",
                            object_type="notes_edits_page_bundle",
                            payload=json.dumps(
                                {
                                    "api_version": row["api_version"],
                                    "invitation": row["invitation"],
                                    "offset": current - len(notes),
                                    "provider_note_count": provider_total,
                                    "items": embedded,
                                },
                                sort_keys=True,
                            ),
                            source_url=f"{self._base(row['api_version'])}/notes",
                            created_at=min(
                                filter(None, (child.created_at for child in items)),
                                default=None,
                            ),
                            modified_at=max(
                                filter(None, (child.modified_at for child in items)),
                                default=None,
                            ),
                            licence="per-object",
                            release_class="pointer_hash",
                            metadata={
                                "api_version": row["api_version"],
                                "invitation_query": row["invitation"],
                                "venue_id": row.get("venue_id"),
                                "note_count": len(notes),
                                "embedded_object_count": len(embedded),
                            },
                        ),
                    )
                else:
                    batch_items = tuple(items)
                yield FetchBatch(
                    batch_items,
                    next_cursor,
                    done_all,
                    f"openreview:{row['api_version']}:{row['invitation']}:{current - len(notes)}",
                    provider_total,
                )
                if done_invitation:
                    break
                if limit is not None and emitted >= limit:
                    return

    @staticmethod
    def _common(item: RawItem, source_object_id: str, provenance_event_id: str) -> dict[str, Any]:
        return {
            "source_id": "openreview_api",
            "source_object_id": source_object_id,
            "provenance_event_id": provenance_event_id,
            "observed_at": item.modified_at or item.created_at,
            "record_version": 1,
        }

    def normalize(
        self, item: RawItem, *, source_object_id: str, provenance_event_id: str
    ) -> Iterable[NormalizedRecord]:
        if item.object_type == "notes_edits_page_bundle":
            bundle = json.loads(item.payload)
            for index, embedded in enumerate(bundle.get("items") or []):
                child = RawItem(
                    native_id=str(embedded["native_id"]),
                    object_type=str(embedded["object_type"]),
                    payload=json.dumps(embedded["payload"], sort_keys=True),
                    source_url=embedded.get("source_url"),
                    created_at=embedded.get("created_at"),
                    modified_at=embedded.get("modified_at"),
                    licence=embedded.get("licence"),
                    release_class=str(embedded.get("release_class") or "pointer_hash"),
                    metadata={**(embedded.get("metadata") or {}), "bundle_item_index": index},
                )
                yield from self.normalize(
                    child,
                    source_object_id=source_object_id,
                    provenance_event_id=provenance_event_id,
                )
            return
        row = json.loads(item.payload)
        invitation = _primary_invitation(
            row,
            str(item.metadata.get("invitation_query") or "") or None,
        )
        kind = _invitation_kind(invitation)
        forum = str(row.get("forum") or (row.get("note") or {}).get("forum") or row.get("id") or "")
        note = row.get("note") if item.object_type == "note_edit" else row
        if isinstance(note, Mapping):
            forum = str(note.get("forum") or note.get("id") or forum)
        if not forum:
            return
        venue_id = str(item.metadata.get("venue_id") or invitation.split("/-/")[0])
        gate_native = venue_id.split("/")[0]
        gate_id = stable_id("gate", self.source_id, gate_native)
        cycle_id = stable_id("gate_cycle", self.source_id, venue_id)
        candidate_id = stable_id("candidate", self.source_id, forum)
        version_native = str((note or {}).get("id") or forum)
        version_id = stable_id("candidate_version", self.source_id, version_native)
        common = self._common(item, source_object_id, provenance_event_id)
        if gate_id not in self._emitted["gate"]:
            self._emitted["gate"].add(gate_id)
            yield NormalizedRecord(
                "gate",
                {
                    "gate_id": gate_id,
                    "native_id": gate_native,
                    "name": gate_native,
                    "organization": "OpenReview venue",
                    "domain": "machine learning",
                    "country": None,
                    "architecture": "unknown",
                    "active_from": None,
                    "active_to": None,
                    **common,
                },
            )
        if cycle_id not in self._emitted["gate_cycle"]:
            self._emitted["gate_cycle"].add(cycle_id)
            yield NormalizedRecord(
                "gate_cycle",
                {
                    "gate_cycle_id": cycle_id,
                    "gate_id": gate_id,
                    "native_id": venue_id,
                    "name": venue_id,
                    "track": None,
                    "cycle_start": None,
                    "cycle_end": None,
                    "policy_version_id": None,
                    "architecture": "unknown",
                    "received_count": None,
                    "observable_count": None,
                    "evaluated_count": None,
                    "selected_count": None,
                    "status": "invitation-observed",
                    **common,
                },
            )
        if item.object_type == "note_edit":
            target = row.get("note") or {}
            target_id = str(target.get("id") or item.metadata.get("note_id") or forum)
            edit_id = str(row.get("id") or item.native_id)
            target_content = _content(target) if isinstance(target, Mapping) else {}
            if kind in {"submission", "discussion"} and any(
                key in target_content for key in ("title", "abstract", "authors", "authorids")
            ):
                if candidate_id not in self._emitted["candidate"]:
                    self._emitted["candidate"].add(candidate_id)
                    yield NormalizedRecord(
                        "candidate",
                        {
                            "candidate_id": candidate_id,
                            "first_observed_at": item.created_at,
                            "domain": "machine learning",
                            "candidate_type": "manuscript",
                            "canonical_title": target_content.get("title"),
                            "status": "observed",
                            **common,
                        },
                    )
                edit_version_id = stable_id("candidate_version", self.source_id, f"{forum}|edit|{edit_id}")
                if edit_version_id not in self._emitted["candidate_version"]:
                    self._emitted["candidate_version"].add(edit_version_id)
                    authors_visible = bool(target_content.get("authors") or target_content.get("authorids"))
                    yield NormalizedRecord(
                        "candidate_version",
                        {
                            "candidate_version_id": edit_version_id,
                            "candidate_id": candidate_id,
                            "native_id": edit_id,
                            "version_label": invitation,
                            "version_number": None,
                            "created_at": item.created_at,
                            "modified_at": item.modified_at,
                            "title": target_content.get("title"),
                            "abstract": target_content.get("abstract"),
                            "content_artifact_id": None,
                            "content_hash": content_hash(json.dumps(target_content, sort_keys=True)),
                            "licence": item.licence,
                            "language": None,
                            "authorship_visible": authors_visible,
                            "withdrawn": bool(row.get("ddate") or target.get("ddate")),
                            **common,
                        },
                    )
                    yield NormalizedRecord(
                        "identity_visibility",
                        {
                            "identity_visibility_id": stable_id(
                                "identity_visibility", self.source_id, f"{edit_version_id}|authors"
                            ),
                            "candidate_version_id": edit_version_id,
                            "identity_kind": "authors",
                            "visible_from": item.created_at if authors_visible else None,
                            "visible_to": None,
                            "audience": "edit_readers",
                            "source_evidence": json_text(
                                {"authors_present": authors_visible, "readers": row.get("readers")}
                            ),
                            **common,
                        },
                    )
                previous = self._last_edit_version.get(target_id)
                self._last_edit_version[target_id] = edit_version_id
                yield NormalizedRecord(
                    "lineage_edge",
                    {
                        "lineage_edge_id": stable_id(
                            "lineage_edge", self.source_id, f"edit|{edit_id}|{edit_version_id}"
                        ),
                        "source_candidate_id": candidate_id,
                        "source_version_id": previous,
                        "target_candidate_id": candidate_id,
                        "target_version_id": edit_version_id,
                        "relation_type": "source_declared_note_edit",
                        "declared": True,
                        "confidence": 1.0,
                        "linkage_tier": "source_declared",
                        "method_version": "openreview-api-edit/2",
                        "evidence_json": json_text(
                            {
                                "edit_id": edit_id,
                                "invitation": row.get("invitation"),
                                "readers": row.get("readers"),
                                "signatures": row.get("signatures"),
                            }
                        ),
                        **common,
                    },
                )
                return
            yield NormalizedRecord(
                "lineage_edge",
                {
                    "lineage_edge_id": stable_id("lineage_edge", self.source_id, f"edit|{edit_id}|{target_id}"),
                    "source_candidate_id": candidate_id,
                    "source_version_id": None,
                    "target_candidate_id": candidate_id,
                    "target_version_id": stable_id("candidate_version", self.source_id, target_id),
                    "relation_type": "source_declared_note_edit",
                    "declared": True,
                    "confidence": 1.0,
                    "linkage_tier": "source_declared",
                    "method_version": "openreview-api-edit/1",
                    "evidence_json": json_text(
                        {
                            "edit_id": row.get("id"),
                            "invitation": row.get("invitation"),
                            "readers": row.get("readers"),
                            "signatures": row.get("signatures"),
                        }
                    ),
                    **common,
                },
            )
            # Every API2 Edit contains the complete target Note state at that
            # revision. Preserve the immutable Edit as lineage evidence and
            # also normalize an edit-addressed snapshot for evaluations,
            # decisions, replies, and other process objects. Submission edits
            # already take the richer candidate-version branch above.
            if isinstance(target, Mapping):
                snapshot = dict(target)
                snapshot["id"] = f"{target_id}@edit:{edit_id}"
                snapshot.setdefault("forum", forum)
                if not snapshot.get("invitations") and row.get("invitations"):
                    snapshot["invitations"] = row.get("invitations")
                if not snapshot.get("invitation") and row.get("invitation"):
                    snapshot["invitation"] = row.get("invitation")
                for key in (
                    "readers",
                    "writers",
                    "signatures",
                    "replyto",
                    "domain",
                    "tcdate",
                    "tmdate",
                    "cdate",
                    "mdate",
                ):
                    if snapshot.get(key) is None and row.get(key) is not None:
                        snapshot[key] = row.get(key)
                yield from self.normalize(
                    RawItem(
                        native_id=f"v2:note-snapshot:{edit_id}",
                        object_type="note",
                        payload=json.dumps(snapshot, sort_keys=True),
                        source_url=item.source_url,
                        created_at=item.created_at,
                        modified_at=item.modified_at,
                        licence=item.licence,
                        release_class=item.release_class,
                        metadata={
                            **dict(item.metadata),
                            "logical_note_id": target_id,
                            "edit_id": edit_id,
                            "invitation_query": _primary_invitation(row),
                        },
                    ),
                    source_object_id=source_object_id,
                    provenance_event_id=provenance_event_id,
                )
            return
        content = _content(row)
        if kind == "submission":
            if candidate_id not in self._emitted["candidate"]:
                self._emitted["candidate"].add(candidate_id)
                yield NormalizedRecord(
                    "candidate",
                    {
                        "candidate_id": candidate_id,
                        "first_observed_at": item.created_at,
                        "domain": "machine learning",
                        "candidate_type": "manuscript",
                        "canonical_title": content.get("title"),
                        "status": "observed",
                        **common,
                    },
                )
            if version_id not in self._emitted["candidate_version"]:
                self._emitted["candidate_version"].add(version_id)
                authors_visible = bool(content.get("authors") or content.get("authorids"))
                yield NormalizedRecord(
                    "candidate_version",
                    {
                        "candidate_version_id": version_id,
                        "candidate_id": candidate_id,
                        "native_id": version_native,
                        "version_label": "provider_note",
                        "version_number": None,
                        "created_at": item.created_at,
                        "modified_at": item.modified_at,
                        "title": content.get("title"),
                        "abstract": content.get("abstract"),
                        "content_artifact_id": None,
                        "content_hash": None,
                        "licence": item.licence,
                        "language": None,
                        "authorship_visible": authors_visible,
                        "withdrawn": False,
                        **common,
                    },
                )
                yield NormalizedRecord(
                    "identity_visibility",
                    {
                        "identity_visibility_id": stable_id(
                            "identity_visibility", self.source_id, f"{version_id}|authors|{item.created_at}"
                        ),
                        "candidate_version_id": version_id,
                        "identity_kind": "authors",
                        "visible_from": item.created_at if authors_visible else None,
                        "visible_to": None,
                        "audience": "raw_note_readers",
                        "source_evidence": json_text(
                            {"authors_present": authors_visible, "readers": row.get("readers")}
                        ),
                        **common,
                    },
                )
            yield NormalizedRecord(
                "candidate_gate_event",
                {
                    "candidate_gate_event_id": stable_id("candidate_gate_event", self.source_id, f"{cycle_id}|{forum}"),
                    "candidate_id": candidate_id,
                    "candidate_version_id": version_id,
                    "gate_cycle_id": cycle_id,
                    "native_id": forum,
                    "submitted_at": item.created_at,
                    "earliest_observed_stage": "invitation_note",
                    "final_observed_stage": "outcome_unresolved",
                    "coverage_observation_id": coverage_observation_id(self.source_id, cycle_id, "submission"),
                    **common,
                },
            )
            return
        if kind in {"decision", "withdrawal"}:
            outcome = (
                content.get("decision")
                or content.get("recommendation")
                or ("withdrawn" if kind == "withdrawal" else "unknown")
            )
            yield NormalizedRecord(
                "decision_event",
                {
                    "decision_event_id": stable_id(
                        "decision_event", self.source_id, str(row.get("id") or item.native_id)
                    ),
                    "candidate_version_id": stable_id("candidate_version", self.source_id, forum),
                    "gate_cycle_id": cycle_id,
                    "native_id": str(row.get("id") or item.native_id),
                    "stage_native": invitation,
                    "stage_normalized": "final_observed_decision",
                    "outcome_native": str(outcome),
                    "outcome_normalized": str(outcome).lower(),
                    "tier_or_band": content.get("venue"),
                    "reason": content.get("comment"),
                    "deciding_body": venue_id,
                    "decided_at": item.created_at,
                    "policy_version_id": None,
                    **common,
                },
            )
            return
        if kind not in {"evaluation", "discussion"}:
            return
        text_fields = [
            key
            for key, value in content.items()
            if isinstance(value, str)
            and value.strip()
            and any(
                token in key.lower()
                for token in (
                    "review",
                    "comment",
                    "summary",
                    "strength",
                    "weakness",
                    "question",
                    "response",
                    "rebuttal",
                )
            )
        ]
        text = "\n\n".join(f"{key}: {content[key]}" for key in text_fields)
        artifact_id = None
        if text:
            artifact_id = stable_id("content_artifact", self.source_id, str(row.get("id") or item.native_id))
            if item.metadata.get("bundle_forum_index") is not None:
                local_pointer = (
                    f"bundle.forums[{item.metadata['bundle_forum_index']}].items[{item.metadata['bundle_item_index']}]"
                )
            elif item.metadata.get("bundle_item_index") is not None:
                local_pointer = f"bundle.items[{item.metadata['bundle_item_index']}]"
            else:
                local_pointer = f"raw:{source_object_id}"
            yield NormalizedRecord(
                "content_artifact",
                {
                    "content_artifact_id": artifact_id,
                    "object_type": kind,
                    "media_type": "text/plain",
                    "byte_hash": content_hash(text),
                    "normalized_text_hash": content_hash(" ".join(text.split())),
                    "source_url": item.source_url,
                    "local_pointer": local_pointer,
                    "licence": item.licence,
                    "release_class": item.release_class,
                    "size_bytes": len(text.encode()),
                    "language": None,
                    "parser_version": self.connector_version,
                    **common,
                },
            )
        rating_key = next((key for key in content if any(t in key.lower() for t in ("rating", "recommendation"))), None)
        confidence_key = next((key for key in content if "confidence" in key.lower()), None)
        yield NormalizedRecord(
            "evaluation",
            {
                "evaluation_id": stable_id("evaluation", self.source_id, str(row.get("id") or item.native_id)),
                "candidate_version_id": stable_id("candidate_version", self.source_id, forum),
                "gate_cycle_id": cycle_id,
                "native_id": str(row.get("id") or item.native_id),
                "evaluation_type": "official_review" if kind == "evaluation" else "discussion",
                "evaluator_role": "reviewer" if kind == "evaluation" else "participant",
                "evaluator_public_id": None,
                "evaluator_protected_id": None,
                "anonymous": any("anonymous" in str(value).lower() for value in row.get("signatures") or []),
                "official": kind == "evaluation",
                "criterion_native": rating_key,
                "criterion_normalized": "overall_recommendation" if rating_key else None,
                "criterion_value": str(content.get(rating_key)) if rating_key else None,
                "criterion_value_numeric": _number(content.get(rating_key)) if rating_key else None,
                "scale_json": json_text({key: content[key] for key in content if "rating" in key.lower()}),
                "confidence_value": _number(content.get(confidence_key)) if confidence_key else None,
                "text_artifact_id": artifact_id,
                "created_at": item.created_at,
                "forum_native_id": forum,
                "invitation_native": invitation,
                "readers_json": json_text(row.get("readers")),
                "signatures_json": json_text(row.get("signatures")),
                "reply_to_native_id": row.get("replyto"),
                **common,
            },
        )

    def validate_fixture(self, context: ConnectorContext) -> Mapping[str, Any]:
        fixture = context.fixture_dir / self.source_id / "note.json"
        if not fixture.exists():
            return {"passes": False, "reason": "fixture absent"}
        row = json.loads(fixture.read_text())
        return {
            "passes": bool(row.get("id") and (row.get("invitation") or row.get("invitations"))),
            "lossless_fields": all(key in row for key in ("id", "content")),
        }

    def emit_coverage(self, context: ConnectorContext, *, found_count: int) -> Iterable[CoverageEvidence]:
        for row in _manifest_rows(context):
            invitation = row["invitation"]
            count = self._counts[(invitation, "note")]
            expected = self._provider_counts.get((row["api_version"], invitation))
            venue_id = row.get("venue_id") or invitation.split("/-/")[0]
            cycle_id = stable_id("gate_cycle", self.source_id, venue_id)
            yield CoverageEvidence(
                gate_cycle_id=cycle_id,
                object_type=_invitation_kind(invitation),
                earliest_public_stage="invitation-specific accessible Note",
                observability_grade="A" if expected is not None and count == expected else "U",
                expected_count=expected,
                found_count=count,
                expected_count_method="OpenReview invitation count=true response",
                query_or_invitation=f"{row['api_version']}:{invitation}",
                known_hidden_stages=("objects not readable by the authenticated account remain unobserved",),
                audit_status="checkpoint_exact" if expected is not None and count == expected else "unresolved",
            )


def _forum_manifest_rows(context: ConnectorContext) -> list[dict[str, str]]:
    rows = []
    for value in context.parameters.get("forum_files") or ():
        path = Path(value)
        if not path.is_absolute():
            path = context.workspace / path
        body = json.loads(path.read_text())
        candidates = body.get("forums", body) if isinstance(body, dict) else body
        for candidate in candidates:
            if isinstance(candidate, str):
                rows.append({"forum": candidate, "api_version": "v2", "venue_id": ""})
            elif isinstance(candidate, Mapping) and candidate.get("forum"):
                rows.append(
                    {
                        "forum": str(candidate["forum"]),
                        "api_version": "v1" if candidate.get("api_version") == "v1" else "v2",
                        "venue_id": str(candidate.get("venue_id") or ""),
                    }
                )
    unique = {(row["api_version"], row["forum"]): row for row in rows}
    return [unique[key] for key in sorted(unique)]


class OpenReviewForumGraphConnector(OpenReviewAPINotesConnector):
    """Fetch every readable Note in each already enumerated submission forum.

    Multiple complete forums share one immutable raw bundle. This preserves the
    exact provider payload for every forum while keeping inode use bounded for
    six-figure populations on the Modal Volume.
    """

    force_streaming = True

    def __init__(self, *, page_size: int = 1000, forums_per_bundle: int = 50):
        super().__init__(page_size=page_size, include_edits=False, bundle_pages=True)
        self.forums_per_bundle = max(int(forums_per_bundle), 1)
        self._forum_cycle_counts: Counter[str] = Counter()

    def discover(self, context: ConnectorContext) -> Iterable[Mapping[str, Any]]:
        for row in _forum_manifest_rows(context):
            yield {**row, "object_kind": "complete_readable_forum_graph"}

    def count(self, context: ConnectorContext) -> SourceEstimate:
        forums = _forum_manifest_rows(context)
        return SourceEstimate(
            self.source_id,
            math.ceil(len(forums) / self.forums_per_bundle),
            expected_bytes=len(forums) * 250_000,
            expected_requests=len(forums),
            method=(
                f"one lossless immutable bundle per {self.forums_per_bundle} previously "
                "enumerated public forums; all readable Notes paginated within each forum"
            ),
            confidence="exact forum list; provider Note count checked during fetch",
            objects_per_limit_unit=1.0 / self.forums_per_bundle,
            requests_per_limit_unit=1.0,
        )

    def fetch(
        self,
        context: ConnectorContext,
        *,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> Iterator[FetchBatch]:
        rows = _forum_manifest_rows(context)
        start = int(cursor or 0)
        stop = len(rows) if limit is None else min(len(rows), start + limit)
        for bundle_start in range(start, stop, self.forums_per_bundle):
            bundle_end = min(bundle_start + self.forums_per_bundle, stop)
            forum_bundles: list[dict[str, Any]] = []
            for descriptor in rows[bundle_start:bundle_end]:
                offset = 0
                notes = []
                provider_total = None
                while provider_total is None or offset < provider_total:
                    body = self._get_json(
                        context,
                        descriptor["api_version"],
                        "/notes",
                        {
                            "forum": descriptor["forum"],
                            "limit": self.page_size,
                            "offset": offset,
                            "count": "true",
                            "trash": "true",
                        },
                    )
                    page = body.get("notes") or []
                    provider_total = int(body.get("count", offset + len(page)))
                    notes.extend(page)
                    offset += len(page)
                    if not page:
                        break
                embedded = []
                for note in notes:
                    note_id = str(note.get("id") or content_hash(json.dumps(note, sort_keys=True)))
                    embedded.append(
                        {
                            "native_id": f"{descriptor['api_version']}:note:{note_id}",
                            "object_type": "note",
                            "payload": note,
                            "source_url": (f"https://openreview.net/forum?id={descriptor['forum']}"),
                            "created_at": epoch_ms(note.get("tcdate") or note.get("cdate")),
                            "modified_at": epoch_ms(note.get("tmdate") or note.get("mdate")),
                            "licence": "per-object",
                            "release_class": "pointer_hash",
                            "metadata": {
                                "api_version": descriptor["api_version"],
                                "venue_id": descriptor.get("venue_id"),
                                "invitation_query": _primary_invitation(note),
                                "authenticated": self._sessions[descriptor["api_version"]][1],
                            },
                        }
                    )
                venue_id = descriptor.get("venue_id") or "unknown"
                self._forum_cycle_counts[venue_id] += 1
                forum_bundles.append(
                    {
                        "api_version": descriptor["api_version"],
                        "forum": descriptor["forum"],
                        "venue_id": descriptor.get("venue_id"),
                        "provider_note_count": provider_total,
                        "items": embedded,
                    }
                )
            payload = json.dumps(
                {
                    "bundle_start": bundle_start,
                    "bundle_end": bundle_end,
                    "forum_count": len(forum_bundles),
                    "forums": forum_bundles,
                },
                sort_keys=True,
            )
            embedded_rows = [row for forum_bundle in forum_bundles for row in forum_bundle["items"]]
            native_fingerprint = content_hash(
                "|".join(f"{row['api_version']}:{row['forum']}" for row in forum_bundles)
            )[:24]
            item = RawItem(
                native_id=f"forum-page:{native_fingerprint}",
                object_type="forum_graph_page_bundle",
                payload=payload,
                source_url=f"{self._base('v2')}/notes",
                created_at=min(
                    filter(None, (row["created_at"] for row in embedded_rows)),
                    default=None,
                ),
                modified_at=max(
                    filter(None, (row["modified_at"] for row in embedded_rows)),
                    default=None,
                ),
                licence="per-object",
                release_class="pointer_hash",
                metadata={
                    "forum_count": len(forum_bundles),
                    "bundle_start": bundle_start,
                    "bundle_end": bundle_end,
                },
            )
            done = bundle_end >= stop
            yield FetchBatch(
                (item,),
                None if done else str(bundle_end),
                done,
                f"openreview-forum-page:{bundle_start}:{bundle_end}:{native_fingerprint}",
                len(rows),
            )

    def normalize(
        self, item: RawItem, *, source_object_id: str, provenance_event_id: str
    ) -> Iterable[NormalizedRecord]:
        if item.object_type != "forum_graph_page_bundle":
            yield from super().normalize(
                item,
                source_object_id=source_object_id,
                provenance_event_id=provenance_event_id,
            )
            return
        bundle = json.loads(item.payload)
        for forum_index, forum_bundle in enumerate(bundle.get("forums") or []):
            for item_index, embedded in enumerate(forum_bundle.get("items") or []):
                child = RawItem(
                    native_id=str(embedded["native_id"]),
                    object_type=str(embedded["object_type"]),
                    payload=json.dumps(embedded["payload"], sort_keys=True),
                    source_url=embedded.get("source_url"),
                    created_at=embedded.get("created_at"),
                    modified_at=embedded.get("modified_at"),
                    licence=embedded.get("licence"),
                    release_class=str(embedded.get("release_class") or "pointer_hash"),
                    metadata={
                        **(embedded.get("metadata") or {}),
                        "bundle_forum_index": forum_index,
                        "bundle_item_index": item_index,
                    },
                )
                yield from super().normalize(
                    child,
                    source_object_id=source_object_id,
                    provenance_event_id=provenance_event_id,
                )

    def emit_coverage(self, context: ConnectorContext, *, found_count: int) -> Iterable[CoverageEvidence]:
        for venue_id, count in sorted(self._forum_cycle_counts.items()):
            cycle_id = stable_id("gate_cycle", self.source_id, venue_id)
            yield CoverageEvidence(
                gate_cycle_id=cycle_id,
                object_type="readable_forum_graph",
                earliest_public_stage="provider-readable public forum",
                observability_grade="B",
                expected_count=count,
                found_count=count,
                expected_count_method="pre-enumerated candidate forum manifest",
                query_or_invitation="OpenReview /notes?forum=<id>",
                known_hidden_stages=("Notes not readable by the authenticated account",),
                audit_status="forum_cursor_exact",
            )


def _domain_manifest_rows(context: ConnectorContext) -> list[dict[str, str]]:
    rows: dict[tuple[str, str], dict[str, str]] = {}
    for value in context.parameters.get("domain_files") or ():
        path = Path(value)
        if not path.is_absolute():
            path = context.workspace / path
        body = json.loads(path.read_text())
        invitations = body.get("invitations", body) if isinstance(body, dict) else body
        for candidate in invitations:
            if not isinstance(candidate, Mapping) or not candidate.get("venue_id"):
                continue
            version = "v1" if candidate.get("api_version") == "v1" else "v2"
            venue_id = str(candidate["venue_id"])
            key = (version, venue_id)
            row = rows.setdefault(
                key,
                {
                    "api_version": version,
                    "venue_id": venue_id,
                    "provider_state_note_count": "0",
                    "state_count_observed": "false",
                },
            )
            if candidate.get("provider_note_count") is not None:
                row["state_count_observed"] = "true"
                row["provider_state_note_count"] = str(
                    int(row["provider_state_note_count"]) + int(candidate["provider_note_count"])
                )
    unsupported = [row for row in rows.values() if row["api_version"] != "v2"]
    if unsupported:
        raise NetworkPolicyError("domain-wide edit enumeration is unavailable for passing API1 cycles")
    return [rows[key] for key in sorted(rows)]


class OpenReviewDomainEditsConnector(OpenReviewAPINotesConnector):
    """Losslessly enumerate API2 Edits for an explicit audited venue cohort."""

    force_streaming = True
    compile_buckets = 64

    def __init__(self, *, page_size: int = 1000, min_interval_seconds: float = 3.0):
        super().__init__(page_size=page_size, include_edits=False, bundle_pages=True)
        self.min_interval_seconds = max(float(min_interval_seconds), 3.0)
        self._domain_provider_counts: dict[str, int] = {}
        self._domain_found_counts: Counter[str] = Counter()

    @staticmethod
    def _selected_rows(context: ConnectorContext, rows: list[dict[str, str]]) -> list[dict[str, str]]:
        selected = context.parameters.get("domain_indices")
        if selected is None:
            return rows
        indices = sorted({int(value) for value in selected})
        if any(value < 0 or value >= len(rows) for value in indices):
            raise ValueError("OpenReview domain_indices contains an out-of-range value")
        return [rows[index] for index in indices]

    def _session(self, context: ConnectorContext, version: str) -> tuple[PoliteSession, bool]:
        session, authenticated = super()._session(context, version)
        if session.policy.min_interval_seconds < self.min_interval_seconds:
            session.policy = RatePolicy(
                min_interval_seconds=self.min_interval_seconds,
                max_retries=max(session.policy.max_retries, 30),
                timeout_seconds=session.policy.timeout_seconds,
                max_backoff_seconds=max(session.policy.max_backoff_seconds, 3_700),
                max_concurrency_per_host=session.policy.max_concurrency_per_host,
                daily_request_ceiling=session.policy.daily_request_ceiling,
            )
        return session, authenticated

    @property
    def domain_provider_counts(self) -> Mapping[str, int]:
        return dict(self._domain_provider_counts)

    def discover(self, context: ConnectorContext) -> Iterable[Mapping[str, Any]]:
        for row in self._selected_rows(context, _domain_manifest_rows(context)):
            yield {**row, "object_kind": "complete_readable_domain_edit_history"}

    def count(self, context: ConnectorContext) -> SourceEstimate:
        rows = self._selected_rows(context, _domain_manifest_rows(context))
        # Do not issue one preflight request per domain: that duplicates every
        # first acquisition request and can exhaust a provider burst quota
        # before the checkpointed run starts.  Fifty Edits per audited state
        # Note is a deliberately conservative planning multiplier calibrated
        # above the proof domains. Exact domain counts are taken from the first
        # acquired page and reconciled in the population audit.
        state_notes = sum(int(row.get("provider_state_note_count") or 0) for row in rows)
        planned_edits = max(len(rows) * self.page_size, state_notes * 50)
        total_pages = math.ceil(planned_edits / self.page_size)
        return SourceEstimate(
            self.source_id,
            total_pages,
            expected_requests=total_pages,
            expected_bytes=planned_edits * 7_500,
            method=(
                "conservative 50-Edits-per-audited-state-Note planning bound; "
                "exact authenticated API2 domain counts captured on first pages"
            ),
            confidence=(
                "budget planning bound only; acquisition and acceptance use exact "
                "provider domain counts plus offset exhaustion"
            ),
            requests_per_limit_unit=1.0,
        )

    def fetch(
        self,
        context: ConnectorContext,
        *,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> Iterator[FetchBatch]:
        rows = self._selected_rows(context, _domain_manifest_rows(context))
        if cursor and cursor.startswith("["):
            decoded = json.loads(cursor)
            if not isinstance(decoded, list) or len(decoded) != 4:
                raise ValueError("invalid OpenReview domain keyset cursor")
            domain_index = int(decoded[0])
            offset = int(decoded[1])
            after_id = str(decoded[2]) if decoded[2] else None
            resumed_total = int(decoded[3]) if decoded[3] is not None else None
        elif cursor:
            # Offset checkpoints predate keyset pagination and cannot safely be
            # continued with another ordering. A dedicated domain restart is
            # required; raw receipts remain immutable evidence.
            raise ValueError("legacy OpenReview offset cursor requires an audited domain restart")
        else:
            domain_index, offset, after_id, resumed_total = 0, 0, None, None
        emitted_pages = 0
        for index in range(domain_index, len(rows)):
            descriptor = rows[index]
            current = offset if index == domain_index else 0
            current_after = after_id if index == domain_index else None
            if index == domain_index and resumed_total is not None:
                self._domain_provider_counts[descriptor["venue_id"]] = resumed_total
            if descriptor.get("state_count_observed") == "true" and int(descriptor["provider_state_note_count"]) == 0:
                self._domain_provider_counts[descriptor["venue_id"]] = 0
                self._domain_found_counts[descriptor["venue_id"]] = 0
                done_all = index + 1 >= len(rows)
                next_cursor = None if done_all else json.dumps([index + 1, 0, None, None], separators=(",", ":"))
                item = RawItem(
                    native_id=(f"domain-edit-page:zero-state:{content_hash(descriptor['venue_id'])[:24]}"),
                    object_type="notes_edits_page_bundle",
                    payload=json.dumps(
                        {
                            "api_version": "v2",
                            "domain": descriptor["venue_id"],
                            "venue_id": descriptor["venue_id"],
                            "offset": 0,
                            "provider_edit_count": 0,
                            "items": [],
                            "candidate_relevant_scope": "zero_public_state_notes",
                        },
                        sort_keys=True,
                    ),
                    source_url=None,
                    licence="CC0",
                    release_class="releasable",
                    metadata={
                        "api_version": "v2",
                        "venue_id": descriptor["venue_id"],
                        "domain": descriptor["venue_id"],
                        "offset": 0,
                        "edit_count": 0,
                        "provider_edit_count": 0,
                        "candidate_relevant_scope": "zero_public_state_notes",
                    },
                )
                yield FetchBatch(
                    (item,),
                    next_cursor,
                    done_all,
                    f"openreview-domain-edits:{descriptor['venue_id']}:zero-state",
                    0,
                )
                continue
            while True:
                if limit is not None and emitted_pages >= limit:
                    return
                venue_id = descriptor["venue_id"]
                needs_count = venue_id not in self._domain_provider_counts
                body = self._get_json(
                    context,
                    "v2",
                    "/notes/edits",
                    {
                        "domain": descriptor["venue_id"],
                        "limit": self.page_size,
                        "count": "true" if needs_count else "false",
                        "trash": "true",
                        "sort": "id",
                        **({"after": current_after} if current_after else {}),
                    },
                )
                edits = body.get("edits") or []
                if needs_count and body.get("count") is None:
                    raise NetworkPolicyError(f"OpenReview omitted the requested exact domain count: {venue_id}")
                provider_total = int(
                    body["count"] if body.get("count") is not None else self._domain_provider_counts[venue_id]
                )
                self._domain_provider_counts[venue_id] = provider_total
                edit_ids = [str(edit.get("id") or "") for edit in edits]
                if any(not edit_id for edit_id in edit_ids):
                    raise ValueError("OpenReview domain Edit page omitted an Edit id")
                if len(set(edit_ids)) != len(edit_ids):
                    raise ValueError("OpenReview domain Edit page contains duplicate ids")
                if current_after and current_after in edit_ids:
                    raise ValueError("OpenReview domain Edit keyset repeated its boundary id")
                embedded = []
                for edit in edits:
                    edit_id = str(edit.get("id") or content_hash(json.dumps(edit, sort_keys=True)))
                    target = edit.get("note") or {}
                    note_id = str(target.get("id") or "")
                    embedded.append(
                        {
                            "native_id": f"v2:edit:{edit_id}",
                            "object_type": "note_edit",
                            "payload": edit,
                            "source_url": (f"https://openreview.net/forum?id={target.get('forum') or note_id}"),
                            "created_at": epoch_ms(edit.get("tcdate") or edit.get("cdate")),
                            "modified_at": epoch_ms(edit.get("tmdate") or edit.get("mdate")),
                            "licence": "per-object",
                            "release_class": "pointer_hash",
                            "metadata": {
                                "api_version": "v2",
                                "venue_id": descriptor["venue_id"],
                                "domain": descriptor["venue_id"],
                                "note_id": note_id,
                                "invitation_query": _primary_invitation(edit),
                                "authenticated": self._sessions["v2"][1],
                            },
                        }
                    )
                page_start = current
                current += len(edits)
                current_after = edit_ids[-1] if edit_ids else current_after
                self._domain_found_counts[venue_id] += len(edits)
                emitted_pages += 1
                done_domain = not edits or current >= provider_total
                done_all = done_domain and index + 1 >= len(rows)
                next_cursor = (
                    None
                    if done_all
                    else (
                        json.dumps([index + 1, 0, None, None], separators=(",", ":"))
                        if done_domain
                        else json.dumps(
                            [index, current, current_after, provider_total],
                            separators=(",", ":"),
                        )
                    )
                )
                native = content_hash(f"{descriptor['venue_id']}|{page_start}|{provider_total}")[:24]
                item = RawItem(
                    native_id=f"domain-edit-page:{native}",
                    object_type="notes_edits_page_bundle",
                    payload=json.dumps(
                        {
                            "api_version": "v2",
                            "domain": descriptor["venue_id"],
                            "venue_id": descriptor["venue_id"],
                            "offset": page_start,
                            "after": current_after,
                            "provider_edit_count": provider_total,
                            "items": embedded,
                        },
                        sort_keys=True,
                    ),
                    source_url=f"{self._base('v2')}/notes/edits",
                    created_at=min(
                        filter(None, (row["created_at"] for row in embedded)),
                        default=None,
                    ),
                    modified_at=max(
                        filter(None, (row["modified_at"] for row in embedded)),
                        default=None,
                    ),
                    licence="per-object",
                    release_class="pointer_hash",
                    metadata={
                        "api_version": "v2",
                        "venue_id": descriptor["venue_id"],
                        "domain": descriptor["venue_id"],
                        "offset": page_start,
                        "after": current_after,
                        "edit_count": len(edits),
                        "provider_edit_count": provider_total,
                    },
                )
                yield FetchBatch(
                    (item,),
                    next_cursor,
                    done_all,
                    (f"openreview-domain-edits:{descriptor['venue_id']}:after={current_after or 'start'}"),
                    provider_total,
                )
                if done_domain:
                    break

    def emit_coverage(self, context: ConnectorContext, *, found_count: int) -> Iterable[CoverageEvidence]:
        for row in self._selected_rows(context, _domain_manifest_rows(context)):
            venue_id = row["venue_id"]
            expected = self._domain_provider_counts.get(venue_id)
            found = self._domain_found_counts[venue_id]
            yield CoverageEvidence(
                gate_cycle_id=stable_id("gate_cycle", self.source_id, venue_id),
                object_type="note_edit_history",
                earliest_public_stage="provider-readable public Note/Edit graph",
                observability_grade=("B" if expected is not None and found == expected else "U"),
                expected_count=expected,
                found_count=found,
                expected_count_method=(
                    "zero audited public state Notes; candidate-relevant Edit set is empty"
                    if row.get("state_count_observed") == "true" and int(row["provider_state_note_count"]) == 0
                    else "authenticated API2 domain count=true"
                ),
                query_or_invitation=f"v2:/notes/edits?domain={venue_id}",
                known_hidden_stages=("Notes/Edits not readable by the authenticated account",),
                audit_status=("domain_offset_exact" if expected is not None and found == expected else "unresolved"),
            )


class OpenReviewBatchedForumNotesConnector(OpenReviewAPINotesConnector):
    """Enumerate current Notes for a frozen forum cohort in 100-forum unions."""

    force_streaming = True
    compile_buckets = 64

    def __init__(
        self,
        *,
        page_size: int = 1000,
        forums_per_query: int = 100,
        min_interval_seconds: float = 3.0,
    ):
        super().__init__(page_size=page_size, include_edits=False, bundle_pages=True)
        self.forums_per_query = min(max(int(forums_per_query), 1), 100)
        self.min_interval_seconds = max(float(min_interval_seconds), 3.0)
        self._provider_total = 0
        self._found_total = 0

    def _session(self, context: ConnectorContext, version: str) -> tuple[PoliteSession, bool]:
        session, authenticated = super()._session(context, version)
        if session.policy.min_interval_seconds < self.min_interval_seconds:
            session.policy = RatePolicy(
                min_interval_seconds=self.min_interval_seconds,
                max_retries=max(session.policy.max_retries, 30),
                timeout_seconds=session.policy.timeout_seconds,
                max_backoff_seconds=max(session.policy.max_backoff_seconds, 3_700),
                max_concurrency_per_host=session.policy.max_concurrency_per_host,
                daily_request_ceiling=session.policy.daily_request_ceiling,
            )
        return session, authenticated

    @staticmethod
    def _selected_rows(
        context: ConnectorContext,
        rows: list[dict[str, str]],
    ) -> list[dict[str, str]]:
        start = int(context.parameters.get("forum_start") or 0)
        stop = int(context.parameters.get("forum_stop") or len(rows))
        if start < 0 or stop <= start or stop > len(rows):
            raise ValueError("OpenReview forum range is empty or out of bounds")
        return rows[start:stop]

    def discover(self, context: ConnectorContext) -> Iterable[Mapping[str, Any]]:
        rows = self._selected_rows(context, _forum_manifest_rows(context))
        for offset in range(0, len(rows), self.forums_per_query):
            batch = rows[offset : offset + self.forums_per_query]
            yield {
                "forum_count": len(batch),
                "first_forum": batch[0]["forum"],
                "last_forum": batch[-1]["forum"],
                "object_kind": "complete_readable_current_forum_note_union",
            }

    def count(self, context: ConnectorContext) -> SourceEstimate:
        rows = self._selected_rows(context, _forum_manifest_rows(context))
        query_count = math.ceil(len(rows) / self.forums_per_query)
        # The audited 100-forum sample required two 1,000-Note pages. Two
        # pages/query is therefore the empirical planning estimate; exact
        # counts and keyset exhaustion remain the acceptance criterion.
        planned_pages = query_count * 2
        return SourceEstimate(
            self.source_id,
            planned_pages,
            expected_requests=planned_pages,
            expected_bytes=len(rows) * 15 * 12_000,
            method=(
                f"{self.forums_per_query}-forum repeated-filter unions; two pages/query "
                "from the frozen 100-forum probe"
            ),
            confidence="empirical planning estimate; exact union counts captured per batch",
            requests_per_limit_unit=1.0,
        )

    def fetch(
        self,
        context: ConnectorContext,
        *,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> Iterator[FetchBatch]:
        rows = self._selected_rows(context, _forum_manifest_rows(context))
        batches = [
            rows[offset : offset + self.forums_per_query]
            for offset in range(0, len(rows), self.forums_per_query)
        ]
        if cursor:
            decoded = json.loads(cursor)
            if not isinstance(decoded, list) or len(decoded) != 4:
                raise ValueError("invalid OpenReview forum-batch keyset cursor")
            batch_index = int(decoded[0])
            offset = int(decoded[1])
            after_id = str(decoded[2]) if decoded[2] else None
            resumed_total = int(decoded[3]) if decoded[3] is not None else None
        else:
            batch_index, offset, after_id, resumed_total = 0, 0, None, None
        emitted_pages = 0
        for index in range(batch_index, len(batches)):
            descriptors = batches[index]
            requested = {str(row["forum"]): row for row in descriptors}
            current = offset if index == batch_index else 0
            current_after = after_id if index == batch_index else None
            provider_total = resumed_total if index == batch_index else None
            batch_id = content_hash(
                json.dumps(
                    [(row["api_version"], row["forum"]) for row in descriptors],
                    separators=(",", ":"),
                )
            )
            while True:
                if limit is not None and emitted_pages >= limit:
                    return
                needs_count = provider_total is None
                body = self._get_json(
                    context,
                    "v2",
                    "/notes",
                    {
                        "forum": list(requested),
                        "limit": self.page_size,
                        "count": "true" if needs_count else "false",
                        "trash": "true",
                        "sort": "id",
                        **({"after": current_after} if current_after else {}),
                    },
                )
                notes = body.get("notes") or []
                if needs_count and body.get("count") is None:
                    raise NetworkPolicyError(
                        "OpenReview omitted the exact batched-forum Note count"
                    )
                if body.get("count") is not None:
                    provider_total = int(body["count"])
                    self._provider_total += provider_total
                assert provider_total is not None
                note_ids = [str(note.get("id") or "") for note in notes]
                if any(not note_id for note_id in note_ids):
                    raise ValueError("OpenReview forum Note page omitted a Note id")
                if len(set(note_ids)) != len(note_ids):
                    raise ValueError("OpenReview forum Note page contains duplicate ids")
                if current_after and current_after in note_ids:
                    raise ValueError("OpenReview forum Note keyset repeated its boundary id")
                embedded = []
                for note in notes:
                    forum = str(note.get("forum") or note.get("id") or "")
                    if forum not in requested:
                        raise ValueError(
                            "batched OpenReview Notes query returned an unrequested forum"
                        )
                    descriptor = requested[forum]
                    embedded.append(
                        {
                            "native_id": f"v2:note:{note['id']}",
                            "object_type": "note",
                            "payload": note,
                            "source_url": f"https://openreview.net/forum?id={forum}",
                            "created_at": epoch_ms(note.get("tcdate") or note.get("cdate")),
                            "modified_at": epoch_ms(note.get("tmdate") or note.get("mdate")),
                            "licence": "per-object",
                            "release_class": "pointer_hash",
                            "metadata": {
                                "api_version": "v2",
                                "venue_id": descriptor["venue_id"],
                                "forum": forum,
                                "invitation_query": _primary_invitation(note),
                                "authenticated": self._sessions["v2"][1],
                            },
                        }
                    )
                page_start = current
                current += len(notes)
                current_after = note_ids[-1] if note_ids else current_after
                self._found_total += len(notes)
                emitted_pages += 1
                done_batch = not notes or current >= provider_total
                done_all = done_batch and index + 1 >= len(batches)
                next_cursor = (
                    None
                    if done_all
                    else (
                        json.dumps([index + 1, 0, None, None], separators=(",", ":"))
                        if done_batch
                        else json.dumps(
                            [index, current, current_after, provider_total],
                            separators=(",", ":"),
                        )
                    )
                )
                native = content_hash(
                    f"forums|{batch_id}|{page_start}|{provider_total}"
                )[:24]
                item = RawItem(
                    native_id=f"forum-note-page:{native}",
                    object_type="notes_edits_page_bundle",
                    payload=json.dumps(
                        {
                            "api_version": "v2",
                            "forum_batch_id": batch_id,
                            "forum_batch": descriptors,
                            "offset": page_start,
                            "after": current_after,
                            "provider_forum_batch_note_count": provider_total,
                            "items": embedded,
                        },
                        sort_keys=True,
                    ),
                    source_url=f"{self._base('v2')}/notes",
                    created_at=min(
                        filter(None, (row["created_at"] for row in embedded)),
                        default=None,
                    ),
                    modified_at=max(
                        filter(None, (row["modified_at"] for row in embedded)),
                        default=None,
                    ),
                    licence="per-object",
                    release_class="pointer_hash",
                    metadata={
                        "api_version": "v2",
                        "forum_batch_id": batch_id,
                        "forum_count": len(descriptors),
                        "offset": page_start,
                        "after": current_after,
                        "note_count": len(notes),
                        "provider_forum_batch_note_count": provider_total,
                    },
                )
                yield FetchBatch(
                    (item,),
                    next_cursor,
                    done_all,
                    f"openreview-forum-notes:{batch_id}:after={current_after or 'start'}",
                    provider_total,
                )
                if done_batch:
                    break

    def emit_coverage(
        self,
        context: ConnectorContext,
        *,
        found_count: int,
    ) -> Iterable[CoverageEvidence]:
        start = int(context.parameters.get("forum_start") or 0)
        stop = int(context.parameters.get("forum_stop") or 0)
        exact = self._provider_total > 0 and self._provider_total == self._found_total
        yield CoverageEvidence(
            gate_cycle_id=stable_id(
                "gate_cycle",
                self.source_id,
                f"forum-shard-{start}-{stop}",
            ),
            object_type="batched_current_forum_note_state",
            earliest_public_stage="provider-readable public candidate forum",
            observability_grade="B" if exact else "U",
            expected_count=self._provider_total or None,
            found_count=self._found_total,
            expected_count_method="exact repeated-forum union count=true",
            query_or_invitation=f"frozen forum range [{start},{stop})",
            known_hidden_stages=("Notes not readable by the authenticated account",),
            audit_status="forum_union_keyset_exact" if exact else "run_scoped_report_required",
        )
