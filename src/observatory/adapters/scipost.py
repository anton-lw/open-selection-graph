"""Provider-native SciPost public submission/review process connector."""

from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Iterable, Iterator, Mapping
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit

from bs4 import BeautifulSoup, Tag

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
from ..connectors.http import PoliteSession, RatePolicy
from ..ids import canonical_doi, content_hash, stable_id
from .common import iso_datetime, json_text

BASE_URL = "https://scipost.org"
LIST_URL = f"{BASE_URL}/submissions/"
POLICY_URL = f"{BASE_URL}/submissions/editorial_procedure"
TERMS_URL = f"{BASE_URL}/terms_and_conditions"
SUBMISSION_RE = re.compile(r"^/submissions/(?P<identifier>[^/?#]+)v(?P<version>\d+)/$")
REPORT_HEADER_RE = re.compile(
    r"Report\s+#(?P<number>\d+)\s+by\s+(?P<name>.*?)\s+"
    r"\(Referee\s+\d+\)\s+on\s+(?P<date>\d{4}-\d{1,2}-\d{1,2})\s+"
    r"\((?P<kind>Invited|Contributed)\s+Report\)",
    flags=re.IGNORECASE,
)
REPORT_DOI_RE = re.compile(r"10\.21468/SciPost\.Report\.\d+", flags=re.IGNORECASE)


def _clean(node: Tag | None) -> str | None:
    if node is None:
        return None
    value = " ".join(node.get_text(" ", strip=True).split())
    return value or None


def _meta(soup: BeautifulSoup, name: str) -> list[str]:
    return [str(node.get("content")) for node in soup.select(f"meta[name='{name}']") if node.get("content")]


def _date(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip().replace("/", "-")
    direct = iso_datetime(normalized)
    if direct:
        return direct
    match = re.search(r"([A-Za-z]+)\.?\s+(\d{1,2}),\s+(\d{4})", normalized)
    if match:
        for fmt in ("%B %d %Y", "%b %d %Y"):
            try:
                parsed = datetime.strptime(" ".join(match.groups()), fmt).replace(tzinfo=timezone.utc)
                return parsed.isoformat()
            except ValueError:
                pass
    return None


def _summary(soup: BeautifulSoup) -> dict[str, str]:
    result = {}
    for row in soup.select("table.submission.summary tr"):
        cells = row.select("th,td")
        if len(cells) >= 2:
            key = _clean(cells[0])
            if key:
                result[key.rstrip(":")] = _clean(cells[1]) or ""
    return result


def _abstract(soup: BeautifulSoup) -> str | None:
    for heading in soup.select("h2,h3,h4"):
        if _clean(heading) == "Abstract":
            sibling = heading.find_next_sibling()
            return _clean(sibling if isinstance(sibling, Tag) else None)
    return None


def _status(soup: BeautifulSoup) -> str | None:
    return _clean(soup.select_one(".submission.status"))


def _version_pages(soup: BeautifulSoup, current_url: str) -> list[str]:
    def canonical_page(url: str) -> str:
        parts = urlsplit(urljoin(BASE_URL, url))
        return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))

    current_match = SUBMISSION_RE.match(urlparse(current_url).path)
    if not current_match:
        return [canonical_page(current_url)]
    series_id = current_match.group("identifier")
    urls = {canonical_page(current_url)}
    for anchor in soup.select(".submission-contents a[href]"):
        href = str(anchor.get("href") or "")
        match = SUBMISSION_RE.match(urlparse(href).path)
        # Submission pages may link to related manuscripts as well as their
        # own earlier versions.  Only same-series links are version history;
        # ingesting a neighbour here fabricates revisions and colliding event
        # identifiers at population scale.
        if match and match.group("identifier") == series_id:
            urls.add(canonical_page(href))
    return sorted(
        urls,
        key=lambda url: int(SUBMISSION_RE.match(urlparse(url).path).group("version")),
    )


class SciPostProcessConnector(Connector):
    source_id = "scipost_process"
    connector_version = "3"
    force_streaming = True
    compile_buckets = 64

    def __init__(self, *, page_size: int = 20):
        self.page_size = min(max(page_size, 1), 50)
        self._series_urls: list[str] = []
        self._list_page_count = 0
        self._cycle_counts: Counter[str] = Counter()
        self._cycle_journal: dict[str, str] = {}
        self._version_count = 0
        self._report_count = 0
        self._reply_count = 0
        self._raw_replay_count = 0
        self._network_series_count = 0
        self._raw_bundle_index: dict[str, dict[str, Any]] | None = None
        self._emitted: dict[str, set[str]] = {
            table: set()
            for table in (
                "gate",
                "gate_cycle",
                "policy_version",
                "candidate",
                "candidate_version",
                "candidate_gate_event",
                "decision_event",
                "evaluation",
                "content_artifact",
                "lineage_edge",
                "identifier_alias",
            )
        }

    def _session(self, context: ConnectorContext) -> PoliteSession:
        return PoliteSession(
            cache_dir=context.cache_dir / self.source_id,
            allowed_hosts={"scipost.org"},
            policy=RatePolicy(
                min_interval_seconds=0.25,
                max_retries=5,
                timeout_seconds=120,
                daily_request_ceiling=20_000,
            ),
        )

    def _enumerate_series(self, context: ConnectorContext) -> list[str]:
        snapshot_path = context.workspace / "results" / "observatory" / "scipost_current_public_enumeration.json"
        if snapshot_path.exists():
            snapshot = json.loads(snapshot_path.read_text())
            declared_hash = snapshot.pop("snapshot_hash", None)
            if declared_hash != content_hash(json.dumps(snapshot, sort_keys=True)):
                raise ValueError("SciPost enumeration snapshot hash mismatch")
            urls = [str(value) for value in snapshot.get("series_urls") or []]
            if (
                snapshot.get("schema") != "observatory.scipost-enumeration/1"
                or urls != sorted(set(urls))
                or not all(SUBMISSION_RE.match(urlparse(url).path) for url in urls)
            ):
                raise ValueError("invalid frozen SciPost enumeration snapshot")
            self._list_page_count = int(snapshot["list_page_count"])
            self._series_urls = urls
            return self._series_urls
        session = self._session(context)
        first = session.get(LIST_URL)
        soup = BeautifulSoup(first.content, "lxml")
        pages = [
            int(anchor.get_text(strip=True))
            for anchor in soup.select("a.page")
            if anchor.get_text(strip=True).isdigit()
        ]
        self._list_page_count = max(pages or [1])
        latest: dict[str, tuple[int, str]] = {}
        for page in range(1, self._list_page_count + 1):
            page_soup = (
                soup if page == 1 else BeautifulSoup(session.get(LIST_URL, params={"page": page}).content, "lxml")
            )
            for anchor in page_soup.select("div.submission h3.title a[href]"):
                href = str(anchor.get("href") or "")
                match = SUBMISSION_RE.match(urlparse(href).path)
                if not match:
                    continue
                identifier = match.group("identifier")
                version = int(match.group("version"))
                current = latest.get(identifier)
                if current is None or version > current[0]:
                    latest[identifier] = (version, urljoin(BASE_URL, href))
        self._series_urls = [latest[key][1] for key in sorted(latest)]
        if not self._series_urls:
            raise ValueError("SciPost public submission census returned no series")
        snapshot = {
            "schema": "observatory.scipost-enumeration/1",
            "list_page_count": self._list_page_count,
            "series_urls": self._series_urls,
        }
        snapshot["snapshot_hash"] = content_hash(json.dumps(snapshot, sort_keys=True))
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = snapshot_path.with_suffix(".tmp.json")
        temporary.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
        temporary.replace(snapshot_path)
        return self._series_urls

    def _existing_raw_bundle_index(self, context: ConnectorContext) -> dict[str, dict[str, Any]]:
        """Index exact prior bundles eligible for immutable replay.

        Reuse is opt-in and requires the prior bundle's latest-version URL to
        equal the frozen census URL. This prevents a newly published version
        from being hidden behind an older same-series raw object.
        """
        if self._raw_bundle_index is not None:
            return self._raw_bundle_index
        self._raw_bundle_index = {}
        if not context.parameters.get("reuse_existing_raw_bundles"):
            return self._raw_bundle_index
        wanted = {
            match.group("identifier"): url
            for url in self._series_urls
            if (match := SUBMISSION_RE.match(urlparse(url).path))
        }
        manifest = context.workspace / "data" / "observatory" / "raw" / "manifests" / f"{self.source_id}.jsonl"
        if not manifest.exists():
            return self._raw_bundle_index
        for line in manifest.read_text().splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            native_id = str(row.get("native_id") or "")
            metadata = row.get("metadata") or {}
            if row.get("object_type") == "submission_process_bundle" and wanted.get(native_id) == metadata.get(
                "latest_url"
            ):
                self._raw_bundle_index[native_id] = row
        return self._raw_bundle_index

    def discover(self, context: ConnectorContext) -> Iterable[Mapping[str, Any]]:
        if not self._series_urls:
            self._enumerate_series(context)
        yield {
            "platform": "SciPost",
            "entry_stage": "public_page_after_editor_assignment",
            "enumeration": LIST_URL,
            "public_series_count": len(self._series_urls),
            "list_page_count": self._list_page_count,
            "selection_attrition": ("rejected/withdrawn pages normally removed unless authors request retention"),
        }

    def count(self, context: ConnectorContext) -> SourceEstimate:
        series = self._enumerate_series(context)
        return SourceEstimate(
            self.source_id,
            len(series),
            expected_requests=self._list_page_count + 3 * len(series),
            method=(
                "complete dated pagination of currently public SciPost submission series; "
                "each raw object bundles every public version discovered from its history"
            ),
            confidence="exact current-public listing with structural attrition caveat",
            requests_per_limit_unit=3.0,
        )

    def fetch(
        self,
        context: ConnectorContext,
        *,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> Iterator[FetchBatch]:
        if not self._series_urls:
            self._enumerate_series(context)
        start = int(cursor or 0)
        stop = len(self._series_urls) if limit is None else min(len(self._series_urls), start + limit)
        replay_index = self._existing_raw_bundle_index(context)
        raw_store = None
        session = None
        while start < stop:
            end = min(start + self.page_size, stop)
            items = []
            for latest_url in self._series_urls[start:end]:
                match = SUBMISSION_RE.match(urlparse(latest_url).path)
                if not match:
                    raise ValueError(f"unparseable SciPost submission URL: {latest_url}")
                series_id = match.group("identifier")
                replay = replay_index.get(series_id)
                acquisition_metadata: dict[str, Any]
                if replay is not None:
                    if raw_store is None:
                        from ..storage import RawStore

                        raw_store = RawStore(context.workspace / "data" / "observatory" / "raw")
                    payload = raw_store.get(str(replay["byte_hash"]))
                    bundle = json.loads(payload)
                    versions = list(bundle.get("versions") or [])
                    if (
                        bundle.get("series_id") != series_id
                        or not versions
                        or latest_url not in {row.get("url") for row in versions}
                    ):
                        raise ValueError(f"immutable SciPost replay bundle failed identity audit: {series_id}")
                    self._raw_replay_count += 1
                    acquisition_metadata = {
                        "acquisition_method": "immutable_raw_replay",
                        "replayed_byte_hash": replay["byte_hash"],
                        "replayed_original_retrieved_at": replay.get("retrieved_at"),
                    }
                else:
                    if session is None:
                        session = self._session(context)
                    latest_response = session.get(latest_url, use_cache=False)
                    latest_soup = BeautifulSoup(latest_response.content, "lxml")
                    versions = []
                    for url in _version_pages(latest_soup, latest_url):
                        response = latest_response if url == latest_url else session.get(url, use_cache=False)
                        versions.append({"url": url, "html": response.text})
                    payload = json.dumps(
                        {"series_id": series_id, "versions": versions},
                        sort_keys=True,
                    ).encode()
                    self._network_series_count += 1
                    acquisition_metadata = {
                        "acquisition_method": "provider_http",
                    }
                earliest = min(
                    filter(
                        None,
                        (
                            _date((_meta(BeautifulSoup(row["html"], "lxml"), "citation_online_date") or [None])[0])
                            for row in versions
                        ),
                    ),
                    default=None,
                )
                items.append(
                    RawItem(
                        native_id=series_id,
                        object_type="submission_process_bundle",
                        payload=payload,
                        source_url=latest_url,
                        created_at=earliest,
                        licence=None,
                        release_class="pointer_hash",
                        metadata={
                            "latest_url": latest_url,
                            "public_version_count": len(versions),
                            "enumeration_url": LIST_URL,
                            **acquisition_metadata,
                        },
                    )
                )
            done = end >= stop
            yield FetchBatch(
                tuple(items),
                None if done else str(end),
                done,
                f"scipost-current-public-series:{start}:{end}",
                len(self._series_urls),
            )
            start = end

    def normalize(
        self, item: RawItem, *, source_object_id: str, provenance_event_id: str
    ) -> Iterable[NormalizedRecord]:
        bundle = json.loads(item.payload)
        series_id = str(bundle["series_id"])
        pages = []
        for row in bundle["versions"]:
            match = SUBMISSION_RE.match(urlparse(row["url"]).path)
            if not match:
                continue
            soup = BeautifulSoup(row["html"], "lxml")
            pages.append((int(match.group("version")), row["url"], soup))
        if not pages:
            return
        pages.sort(key=lambda value: value[0])
        first_summary = _summary(pages[0][2])
        journal = first_summary.get("Submitted to") or "SciPost journal unresolved"
        submitted = _date(
            (_meta(pages[0][2], "citation_online_date") or [None])[0] or first_summary.get("Date submitted")
        )
        year = int((submitted or "1970")[:4])
        gate_id = stable_id("gate", self.source_id, journal)
        cycle_id = stable_id("gate_cycle", self.source_id, f"{journal}|{year}")
        policy_id = stable_id("policy_version", self.source_id, f"{journal}|peer-witnessed-current")
        candidate_id = stable_id("candidate", self.source_id, series_id)
        common = {
            "source_id": self.source_id,
            "source_object_id": source_object_id,
            "provenance_event_id": provenance_event_id,
            "observed_at": item.created_at,
            "record_version": 1,
        }
        if gate_id not in self._emitted["gate"]:
            self._emitted["gate"].add(gate_id)
            yield NormalizedRecord(
                "gate",
                {
                    "gate_id": gate_id,
                    "native_id": journal,
                    "name": journal,
                    "organization": "SciPost Foundation",
                    "domain": first_summary.get("Academic field"),
                    "country": "NL",
                    "architecture": "access_public_discussion",
                    "active_from": None,
                    "active_to": None,
                    **common,
                },
            )
        if policy_id not in self._emitted["policy_version"]:
            self._emitted["policy_version"].add(policy_id)
            rules = {
                "entry": "public page after successful editor assignment",
                "report_types": ["invited", "contributed"],
                "all_public_contributions_vetted": True,
                "reporter_may_choose_public_anonymity": True,
                "binding_publish_or_reject_vote": "relevant specialty Editorial College",
                "submission_exclusivity": "not under consideration elsewhere",
                "rejected_or_withdrawn_page_default": "removed unless author requests retention",
            }
            yield NormalizedRecord(
                "policy_version",
                {
                    "policy_version_id": policy_id,
                    "gate_id": gate_id,
                    "native_id": f"{journal}|peer-witnessed-current",
                    "effective_at": None,
                    "valid_to": None,
                    "criteria_json": json_text(
                        {"report": ["validity", "significance", "originality", "clarity", "formatting", "grammar"]}
                    ),
                    "rubric_json": json_text(
                        {"recommendations": ["publish", "minor revision", "major revision", "reject"]}
                    ),
                    "stage_rules_json": json_text(rules),
                    "quota_or_cap": None,
                    "anonymity_model": "report-level optional public anonymity",
                    "revision_rules": json_text({"resubmission_creates_public_version": True}),
                    "policy_url": POLICY_URL,
                    "content_hash": content_hash(json.dumps(rules, sort_keys=True)),
                    "date_confidence": 0.5,
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
                    "native_id": f"{journal}|{year}",
                    "name": f"{journal} {year}",
                    "track": None,
                    "cycle_start": f"{year}-01-01T00:00:00+00:00",
                    "cycle_end": f"{year}-12-31T23:59:59+00:00",
                    "policy_version_id": policy_id,
                    "architecture": "access_public_discussion",
                    "received_count": None,
                    "observable_count": None,
                    "evaluated_count": None,
                    "selected_count": None,
                    "status": "current-public attrited census",
                    **common,
                },
            )
        self._cycle_counts[cycle_id] += 1
        self._cycle_journal[cycle_id] = journal
        if candidate_id not in self._emitted["candidate"]:
            self._emitted["candidate"].add(candidate_id)
            yield NormalizedRecord(
                "candidate",
                {
                    "candidate_id": candidate_id,
                    "first_observed_at": submitted,
                    "domain": first_summary.get("Academic field"),
                    "candidate_type": "manuscript",
                    "canonical_title": (_meta(pages[-1][2], "citation_title") or [None])[0],
                    "status": _status(pages[-1][2]) or "public submission",
                    **common,
                },
            )

        previous_version_id = None
        for version_number, url, soup in pages:
            self._version_count += 1
            version_native = f"{series_id}v{version_number}"
            version_id = stable_id("candidate_version", self.source_id, version_native)
            page_summary = _summary(soup)
            version_date = _date(
                (_meta(soup, "citation_online_date") or [None])[0] or page_summary.get("Date submitted")
            )
            if version_id not in self._emitted["candidate_version"]:
                self._emitted["candidate_version"].add(version_id)
                yield NormalizedRecord(
                    "candidate_version",
                    {
                        "candidate_version_id": version_id,
                        "candidate_id": candidate_id,
                        "native_id": version_native,
                        "version_label": f"submission version {version_number}",
                        "version_number": version_number,
                        "created_at": version_date,
                        "modified_at": None,
                        "title": (_meta(soup, "citation_title") or [None])[0],
                        "abstract": _abstract(soup),
                        "content_artifact_id": stable_id("content_artifact", self.source_id, version_native),
                        "content_hash": content_hash(soup.encode()),
                        "licence": None,
                        "language": "en",
                        "authorship_visible": True,
                        "withdrawn": False,
                        **common,
                    },
                )
            yield from self._artifact(
                native=version_native,
                object_type="submission_page",
                text=_clean(soup) or "",
                source_url=url,
                local_pointer="html",
                common=common,
            )
            if previous_version_id:
                edge_id = stable_id("lineage_edge", self.source_id, f"{previous_version_id}|{version_id}")
                if edge_id not in self._emitted["lineage_edge"]:
                    self._emitted["lineage_edge"].add(edge_id)
                    yield NormalizedRecord(
                        "lineage_edge",
                        {
                            "lineage_edge_id": edge_id,
                            "source_candidate_id": candidate_id,
                            "source_version_id": previous_version_id,
                            "target_candidate_id": candidate_id,
                            "target_version_id": version_id,
                            "relation_type": "source_declared_resubmission",
                            "declared": True,
                            "confidence": 1.0,
                            "linkage_tier": "source_declared",
                            "method_version": "scipost-history/1",
                            "evidence_json": json_text({"target_url": url}),
                            **common,
                        },
                    )
            previous_version_id = version_id
            status = _status(soup) or "public_stage_censored"
            event_id = stable_id("candidate_gate_event", self.source_id, f"{cycle_id}|{version_native}")
            yield NormalizedRecord(
                "candidate_gate_event",
                {
                    "candidate_gate_event_id": event_id,
                    "candidate_id": candidate_id,
                    "candidate_version_id": version_id,
                    "gate_cycle_id": cycle_id,
                    "native_id": version_native,
                    "submitted_at": version_date,
                    "earliest_observed_stage": "public_page_after_editor_assignment",
                    "final_observed_stage": status,
                    "coverage_observation_id": coverage_observation_id(
                        self.source_id, cycle_id, "current_public_submission_series"
                    ),
                    **common,
                },
            )
            yield from self._decision(
                soup,
                version_id=version_id,
                cycle_id=cycle_id,
                policy_id=policy_id,
                version_native=version_native,
                common=common,
            )
            yield from self._reports(
                soup,
                version_id=version_id,
                cycle_id=cycle_id,
                version_native=version_native,
                source_url=url,
                common=common,
            )

        aliases = [("scipost_submission", series_id)]
        if re.fullmatch(r"\d{4}\.\d{4,5}", series_id):
            aliases.append(("arxiv", series_id))
        for scheme, value in aliases:
            alias_id = stable_id("identifier_alias", self.source_id, f"{scheme}|{value}")
            if alias_id in self._emitted["identifier_alias"]:
                continue
            self._emitted["identifier_alias"].add(alias_id)
            yield NormalizedRecord(
                "identifier_alias",
                {
                    "identifier_alias_id": alias_id,
                    "entity_kind": "candidate",
                    "entity_id": candidate_id,
                    "scheme": scheme,
                    "value": value,
                    "canonical_value": value.lower(),
                    "relation": "source_declared",
                    "confidence": 1.0,
                    "conflict_status": "none",
                    **common,
                },
            )

    def _decision(
        self,
        soup: BeautifulSoup,
        *,
        version_id: str,
        cycle_id: str,
        policy_id: str,
        version_native: str,
        common: Mapping[str, Any],
    ) -> Iterable[NormalizedRecord]:
        status = _status(soup) or ""
        match = re.search(
            r"Editorial decision:\s*For Journal\s+.*?:\s*(Publish|Reject)",
            status,
            flags=re.IGNORECASE,
        )
        if not match:
            return
        native = match.group(1)
        normalized = "selected" if native.lower() == "publish" else "rejected"
        decision_id = stable_id("decision_event", self.source_id, f"{version_native}|{native}")
        if decision_id in self._emitted["decision_event"]:
            return
        self._emitted["decision_event"].add(decision_id)
        yield NormalizedRecord(
            "decision_event",
            {
                "decision_event_id": decision_id,
                "candidate_version_id": version_id,
                "gate_cycle_id": cycle_id,
                "native_id": f"{version_native}|{native}",
                "stage_native": "Editorial decision",
                "stage_normalized": "final_observed_decision",
                "outcome_native": native,
                "outcome_normalized": normalized,
                "tier_or_band": None,
                "reason": None,
                "deciding_body": "Editorial College",
                "decided_at": None,
                "policy_version_id": policy_id,
                **common,
            },
        )

    def _reports(
        self,
        soup: BeautifulSoup,
        *,
        version_id: str,
        cycle_id: str,
        version_native: str,
        source_url: str,
        common: Mapping[str, Any],
    ) -> Iterable[NormalizedRecord]:
        for report in soup.select("#reports .report"):
            header = _clean(report.select_one(".reportid")) or ""
            match = REPORT_HEADER_RE.search(header)
            if not match:
                continue
            report_native = str(report.get("id") or f"report_{match.group('number')}")
            report_native = f"{version_native}|{report_native}"
            doi_match = REPORT_DOI_RE.search(header)
            report_doi = canonical_doi(doi_match.group(0)) if doi_match else None
            report_copy = BeautifulSoup(str(report), "lxml")
            for node in report_copy.select(".reportid,.comment"):
                node.decompose()
            report_text = _clean(report_copy) or ""
            artifact_native = report_doi or report_native
            yield from self._artifact(
                native=artifact_native,
                object_type="referee_report",
                text=report_text,
                source_url=source_url,
                local_pointer=f"#{report.get('id')}",
                common=common,
            )
            recommendation_match = re.search(
                r"Recommendation\s+(Publish|Reject|Minor Revision|Major Revision)",
                report_text,
                flags=re.IGNORECASE,
            )
            evaluation_id = stable_id("evaluation", self.source_id, report_native)
            if evaluation_id not in self._emitted["evaluation"]:
                self._emitted["evaluation"].add(evaluation_id)
                self._report_count += 1
                anonymous = match.group("name").strip().lower() == "anonymous"
                yield NormalizedRecord(
                    "evaluation",
                    {
                        "evaluation_id": evaluation_id,
                        "candidate_version_id": version_id,
                        "gate_cycle_id": cycle_id,
                        "native_id": report_doi or report_native,
                        "evaluation_type": f"{match.group('kind').lower()}_report",
                        "evaluator_role": "referee",
                        "evaluator_public_id": None,
                        "evaluator_protected_id": None,
                        "anonymous": anonymous,
                        "official": True,
                        "criterion_native": "Recommendation" if recommendation_match else None,
                        "criterion_normalized": "editorial_recommendation" if recommendation_match else None,
                        "criterion_value": recommendation_match.group(1).lower() if recommendation_match else None,
                        "criterion_value_numeric": None,
                        "scale_json": json_text(
                            {"native_values": ["publish", "minor revision", "major revision", "reject"]}
                        )
                        if recommendation_match
                        else None,
                        "confidence_value": None,
                        "text_artifact_id": stable_id("content_artifact", self.source_id, artifact_native),
                        "created_at": _date(match.group("date")),
                        "forum_native_id": version_native,
                        "invitation_native": f"{match.group('kind').lower()}_report",
                        "readers_json": json_text(["public"]),
                        "signatures_json": json_text([] if anonymous else [{"name": match.group("name").strip()}]),
                        "reply_to_native_id": version_native,
                        **common,
                    },
                )
            for comment in report.select(".comment"):
                yield from self._reply(
                    comment,
                    version_id=version_id,
                    cycle_id=cycle_id,
                    report_native=report_doi or report_native,
                    version_native=version_native,
                    source_url=source_url,
                    common=common,
                )

    def _reply(
        self,
        comment: Tag,
        *,
        version_id: str,
        cycle_id: str,
        report_native: str,
        version_native: str,
        source_url: str,
        common: Mapping[str, Any],
    ) -> Iterable[NormalizedRecord]:
        header = _clean(comment.select_one(".commentid")) or ""
        identifier = str(
            (comment.select_one(".commentid") or {}).get("id")
            if comment.select_one(".commentid")
            else comment.get("id") or content_hash(header)[:12]
        )
        native = f"{version_native}|{identifier}"
        copy = BeautifulSoup(str(comment), "lxml")
        for node in copy.select(".commentid"):
            node.decompose()
        text = _clean(copy) or ""
        yield from self._artifact(
            native=native,
            object_type="author_reply",
            text=text,
            source_url=source_url,
            local_pointer=f"#{identifier}",
            common=common,
        )
        evaluation_id = stable_id("evaluation", self.source_id, native)
        if evaluation_id in self._emitted["evaluation"]:
            return
        self._emitted["evaluation"].add(evaluation_id)
        self._reply_count += 1
        date_match = re.search(r"on\s+(\d{4}-\d{1,2}-\d{1,2})", header)
        yield NormalizedRecord(
            "evaluation",
            {
                "evaluation_id": evaluation_id,
                "candidate_version_id": version_id,
                "gate_cycle_id": cycle_id,
                "native_id": native,
                "evaluation_type": "author_reply",
                "evaluator_role": "author",
                "evaluator_public_id": None,
                "evaluator_protected_id": None,
                "anonymous": False,
                "official": False,
                "criterion_native": None,
                "criterion_normalized": None,
                "criterion_value": None,
                "criterion_value_numeric": None,
                "scale_json": None,
                "confidence_value": None,
                "text_artifact_id": stable_id("content_artifact", self.source_id, native),
                "created_at": _date(date_match.group(1)) if date_match else None,
                "forum_native_id": version_native,
                "invitation_native": "author_reply",
                "readers_json": json_text(["public"]),
                "signatures_json": json_text([]),
                "reply_to_native_id": report_native,
                **common,
            },
        )

    def _artifact(
        self,
        *,
        native: str,
        object_type: str,
        text: str,
        source_url: str,
        local_pointer: str,
        common: Mapping[str, Any],
    ) -> Iterable[NormalizedRecord]:
        artifact_id = stable_id("content_artifact", self.source_id, native)
        if artifact_id in self._emitted["content_artifact"]:
            return
        self._emitted["content_artifact"].add(artifact_id)
        yield NormalizedRecord(
            "content_artifact",
            {
                "content_artifact_id": artifact_id,
                "object_type": object_type,
                "media_type": "text/html",
                "byte_hash": None,
                "normalized_text_hash": content_hash(" ".join(text.split())),
                "source_url": source_url,
                "local_pointer": local_pointer,
                "licence": None,
                "release_class": "pointer_hash",
                "size_bytes": len(text.encode()),
                "language": "en",
                "parser_version": self.connector_version,
                **common,
            },
        )

    def validate_fixture(self, context: ConnectorContext) -> Mapping[str, Any]:
        fixture = context.fixture_dir / self.source_id / "submission_bundle.json"
        if not fixture.exists():
            return {"passes": False, "reason": "fixture absent"}
        bundle = json.loads(fixture.read_text())
        pages = bundle.get("versions") or []
        reports = sum(len(BeautifulSoup(row["html"], "lxml").select("#reports .report")) for row in pages)
        return {
            "passes": bool(pages and reports),
            "version_count": len(pages),
            "report_count": reports,
        }

    def emit_coverage(self, context: ConnectorContext, *, found_count: int) -> Iterable[CoverageEvidence]:
        for cycle_id, count in sorted(self._cycle_counts.items()):
            yield CoverageEvidence(
                gate_cycle_id=cycle_id,
                object_type="current_public_submission_series",
                earliest_public_stage="public page after editor assignment",
                observability_grade="U",
                expected_count=None,
                found_count=count,
                expected_count_method=("complete current-public listing; historical rejected/withdrawn pages attrit"),
                query_or_invitation=LIST_URL,
                known_hidden_stages=("plagiarism/conflict checks and editor assignment before page activation",),
                known_exclusions=("rejected or withdrawn pages removed unless authors request retention",),
                missing_reason="structural outcome-dependent public-page attrition",
                audit_status=(
                    f"current_public_census; journal={self._cycle_journal[cycle_id]}; "
                    f"series_total={found_count}; versions={self._version_count}; "
                    f"reports={self._report_count}; replies={self._reply_count}"
                    f"; immutable_raw_replays={self._raw_replay_count}; "
                    f"provider_http_series={self._network_series_count}"
                ),
            )
