"""Provider-native F1000Research publish-review-revise connector."""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from lxml import etree

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
from ..ids import canonical_doi, content_hash, stable_id
from ..licensing import canonical_licence, decide_release
from .common import iso_datetime, json_text

APPROVAL_HIGH_TO_LOW = ("approve", "approve-with-reservations", "reject")
RECOMMENDATION_MAP = {
    "approve": "approved",
    "approved": "approved",
    "approve-with-reservations": "approved_with_reservations",
    "approved-with-reservations": "approved_with_reservations",
    "approve with reservations": "approved_with_reservations",
    "reject": "not_approved",
    "not-approved": "not_approved",
    "not approved": "not_approved",
}
URL_RE = re.compile(r"/articles/(?P<article>\d+-\d+)/v(?P<version>\d+)/xml/?$")

# These sites share F1000's public XML publishing stack, but they are separate
# institutional gates. HRB is probed on every census and is included only when
# its corpus endpoint passes the same unauthenticated access gate.
PLATFORMS: dict[str, dict[str, str]] = {
    "f1000research": {
        "name": "F1000Research",
        "organization": "F1000 Research Limited",
        "host": "f1000research.com",
        "policy_url": "https://f1000research.com/for-referees/guidelines",
        "terms_url": "https://f1000research.com/about/legal/termsandconditions",
    },
    "wellcome_open_research": {
        "name": "Wellcome Open Research",
        "organization": "Wellcome Trust",
        "host": "wellcomeopenresearch.org",
        "policy_url": "https://wellcomeopenresearch.org/for-referees/guidelines",
        "terms_url": "https://wellcomeopenresearch.org/about/legal/termsandconditions",
    },
    "gates_open_research": {
        "name": "Gates Open Research",
        "organization": "Gates Foundation",
        "host": "gatesopenresearch.org",
        "policy_url": "https://gatesopenresearch.org/for-referees/guidelines",
        "terms_url": "https://gatesopenresearch.org/about/legal/termsandconditions",
    },
    "nihr_open_research": {
        "name": "NIHR Open Research",
        "organization": "National Institute for Health and Care Research",
        "host": "openresearch.nihr.ac.uk",
        "policy_url": "https://openresearch.nihr.ac.uk/about/policies",
        "terms_url": "https://openresearch.nihr.ac.uk/about/legal/termsandconditions",
    },
    "hrb_open_research": {
        "name": "HRB Open Research",
        "organization": "Health Research Board",
        "host": "hrbopenresearch.org",
        "policy_url": "https://hrbopenresearch.org/for-referees/guidelines",
        "terms_url": "https://hrbopenresearch.org/about/legal/termsandconditions",
    },
}
DOI_NAMESPACE_PLATFORM = {
    "f1000research": "f1000research",
    "wellcomeopenres": "wellcome_open_research",
    "gatesopenres": "gates_open_research",
    "nihropenres": "nihr_open_research",
    "hrbopenres": "hrb_open_research",
}


def _root(payload: bytes | str):
    data = payload if isinstance(payload, bytes) else payload.encode()
    return etree.fromstring(
        data,
        parser=etree.XMLParser(
            resolve_entities=False, no_network=True, recover=False, huge_tree=False
        ),
    )


def _html_fallback_root(payload: bytes | str):
    soup = BeautifulSoup(payload, "lxml")

    def meta(name: str) -> str | None:
        node = soup.select_one(f"meta[name='{name}']")
        return str(node.get("content")) if node and node.get("content") else None

    article = etree.Element("article")
    front = etree.SubElement(article, "front")
    article_meta = etree.SubElement(front, "article-meta")
    doi = meta("citation_doi")
    if doi:
        etree.SubElement(article_meta, "article-id", {"pub-id-type": "doi"}).text = doi
    title_group = etree.SubElement(article_meta, "title-group")
    etree.SubElement(title_group, "article-title").text = meta("citation_title")
    status_match = re.search(
        r"\[version\s+\d+;\s*peer review:[^\]]+\]",
        soup.get_text(" ", strip=True),
        flags=re.IGNORECASE,
    )
    if status_match:
        fn_group = etree.SubElement(title_group, "fn-group", {"content-type": "pub-status"})
        etree.SubElement(etree.SubElement(fn_group, "fn"), "p").text = status_match.group(0)
    date = meta("citation_publication_date")
    if date:
        parts = [int(value) for value in re.findall(r"\d+", date)[:3]]
        if parts:
            pub = etree.SubElement(article_meta, "pub-date")
            etree.SubElement(pub, "year").text = str(parts[0])
            if len(parts) > 1:
                etree.SubElement(pub, "month").text = str(parts[1])
            if len(parts) > 2:
                etree.SubElement(pub, "day").text = str(parts[2])
    abstract = etree.SubElement(article_meta, "abstract")
    etree.SubElement(abstract, "p").text = meta("citation_abstract")
    return article


def _metadata_recovery_payload(record: Mapping[str, Any]) -> bytes:
    """Build a minimal JATS parent from an immutable bibliographic evidence row."""
    article = etree.Element("article")
    front = etree.SubElement(article, "front")
    article_meta = etree.SubElement(front, "article-meta")
    etree.SubElement(article_meta, "article-id", {"pub-id-type": "doi"}).text = str(
        record["doi"]
    )
    title_group = etree.SubElement(article_meta, "title-group")
    etree.SubElement(title_group, "article-title").text = str(record["title"])
    published = str(record["published"])
    year, month, day = published.split("-")
    pub_date = etree.SubElement(article_meta, "pub-date")
    etree.SubElement(pub_date, "year").text = year
    etree.SubElement(pub_date, "month").text = month
    etree.SubElement(pub_date, "day").text = day
    permissions = etree.SubElement(article_meta, "permissions")
    etree.SubElement(
        permissions,
        "license",
        {"{http://www.w3.org/1999/xlink}href": str(record["licence"])},
    )
    return etree.tostring(article, encoding="utf-8", xml_declaration=True)


def _text(node, xpath: str) -> str | None:
    value = " ".join(node.xpath(f"string({xpath})").split())
    return value or None


def _pub_date(node, xpath: str) -> str | None:
    found = node.xpath(xpath)
    if not found:
        return None
    date = found[0]
    year = _text(date, "./year")
    if not year:
        return None
    month = int(_text(date, "./month") or 1)
    day = int(_text(date, "./day") or 1)
    return iso_datetime(f"{int(year):04d}-{month:02d}-{day:02d}")


def _licence(node) -> str | None:
    values = node.xpath(
        ".//permissions/license/@*[local-name()='href']"
    )
    return canonical_licence(str(values[0])) if values else None


def _version_doi(doi: str) -> tuple[str, int, str]:
    match = re.match(
        r"^(?P<base>10\.\d{4,9}/\S+)\.(?P<style>v?)(?P<version>\d+)$", doi
    )
    if not match:
        raise ValueError(f"unexpected F1000Research version DOI: {doi}")
    return match.group("base"), int(match.group("version")), match.group("style")


class F1000ProcessConnector(Connector):
    source_id = "f1000_process"
    connector_version = "6"
    force_streaming = True
    # Host-sharded runs fit comfortably in 32 local compile buckets. This
    # retains deterministic primary-key partitioning while respecting the
    # mounted volume's finite inode budget.
    compile_buckets = 32

    def __init__(self, *, page_size: int = 50):
        self.page_size = min(max(page_size, 1), 100)
        self._urls: list[str] = []
        self._platform_by_url: dict[str, str] = {}
        self._platform_expected_counts: Counter[str] = Counter()
        self._platform_article_counts: Counter[str] = Counter()
        self._platform_access: dict[str, dict[str, Any]] = {}
        self._platform_found_counts: Counter[str] = Counter()
        self._cycle_version_counts: Counter[str] = Counter()
        self._cycle_platform: dict[str, str] = {}
        self._cycle_candidate_counts: Counter[str] = Counter()
        self._html_fallback_count = 0
        self._later_version_recovery_count = 0
        self._metadata_recovery_count = 0
        self._emitted: dict[str, set[str]] = {
            "gate": set(), "gate_cycle": set(), "policy_version": set(),
            "candidate": set(), "candidate_version": set(), "lineage_edge": set(),
            "identifier_alias": set(), "evaluation": set(), "content_artifact": set(),
        }

    def _metadata_recoveries(
        self, context: ConnectorContext
    ) -> dict[str, dict[str, Any]]:
        configured = context.parameters.get("recovery_manifest")
        candidates = []
        if configured:
            candidates.append(Path(str(configured)))
        candidates.extend([
            context.workspace / "configs" / "observatory" / "f1000_broken_versions.json",
            Path("/opt/observatory/configs/observatory/f1000_broken_versions.json"),
        ])
        path = next((candidate for candidate in candidates if candidate.exists()), None)
        if path is None:
            return {}
        snapshot = json.loads(path.read_text())
        declared_hash = snapshot.pop("snapshot_hash", None)
        if declared_hash != content_hash(json.dumps(snapshot, sort_keys=True)):
            raise ValueError("F1000 metadata recovery manifest hash mismatch")
        if snapshot.get("schema") != "observatory.f1000-metadata-recovery/1":
            raise ValueError("unsupported F1000 metadata recovery manifest schema")
        recoveries = snapshot.get("recoveries") or {}
        required = {"doi", "title", "published", "licence", "evidence_url"}
        if not all(
            URL_RE.search(str(url)) and required <= set(record)
            for url, record in recoveries.items()
        ):
            raise ValueError("invalid F1000 metadata recovery manifest row")
        return {str(url): dict(record) for url, record in recoveries.items()}

    @property
    def platform_census(self) -> dict[str, dict[str, Any]]:
        return {
            platform_id: {
                **PLATFORMS[platform_id],
                **self._platform_access.get(platform_id, {}),
                "expected_version_count": self._platform_expected_counts[platform_id],
                "article_series_count": self._platform_article_counts[platform_id],
                "found_version_count": self._platform_found_counts[platform_id],
            }
            for platform_id in PLATFORMS
        }

    def _session(self, context: ConnectorContext) -> PoliteSession:
        return PoliteSession(
            cache_dir=context.cache_dir / self.source_id,
            allowed_hosts={row["host"] for row in PLATFORMS.values()},
            policy=RatePolicy(
                min_interval_seconds=0.5,
                max_retries=3,
                timeout_seconds=30,
                max_backoff_seconds=10,
                max_concurrency_per_host=1,
                # The five independently gated public corpora currently total
                # more than 26k version URLs. This remains a free, throttled
                # public crawl; the ceiling includes bounded XML retries.
                daily_request_ceiling=50_000,
            ),
        )

    def _enumerate_urls(self, context: ConnectorContext) -> list[str]:
        selected_value = context.parameters.get("platform_ids")
        selected = (
            {str(value) for value in selected_value}
            if selected_value is not None
            else set(PLATFORMS)
        )
        unknown = selected - set(PLATFORMS)
        if unknown:
            raise ValueError(f"unknown F1000-family platform_ids: {sorted(unknown)}")
        if not selected:
            raise ValueError("platform_ids must select at least one F1000-family platform")
        snapshot_value = context.parameters.get("enumeration_snapshot")
        snapshot_path = Path(str(snapshot_value)) if snapshot_value else None
        if snapshot_path and not snapshot_path.is_absolute():
            snapshot_path = context.workspace / snapshot_path
        if snapshot_path and snapshot_path.exists():
            snapshot = json.loads(snapshot_path.read_text())
            declared_hash = snapshot.pop("snapshot_hash", None)
            if declared_hash != content_hash(json.dumps(snapshot, sort_keys=True)):
                raise ValueError("F1000 enumeration snapshot hash mismatch")
            urls = [str(value) for value in snapshot.get("urls") or []]
            if urls != sorted(set(urls)) or not all(URL_RE.search(url) for url in urls):
                raise ValueError("invalid or non-canonical frozen F1000 enumeration snapshot")
            access = snapshot.get("platform_access") or {}
            if set(access) != set(PLATFORMS):
                raise ValueError("frozen F1000 snapshot does not cover every configured platform")
            self._urls = urls
            self._platform_access = {
                str(key): dict(value) for key, value in access.items()
            }
            self._platform_by_url = {
                url: next(
                    key for key, row in PLATFORMS.items()
                    if urlparse(url).hostname == row["host"]
                )
                for url in urls
            }
            self._platform_expected_counts = Counter(
                self._platform_by_url[url] for url in urls
            )
            article_sets: dict[str, set[str]] = defaultdict(set)
            for url, platform_id in self._platform_by_url.items():
                match = URL_RE.search(url)
                if match:
                    article_sets[platform_id].add(match.group("article"))
            self._platform_article_counts = Counter({
                key: len(value) for key, value in article_sets.items()
            })
            self._urls = [
                url for url in self._urls
                if self._platform_by_url[url] in selected
            ]
            return self._urls
        session = self._session(context)
        listed: list[str] = []
        articles: dict[str, set[int]] = {}
        self._platform_by_url.clear()
        self._platform_expected_counts.clear()
        self._platform_article_counts.clear()
        self._platform_access.clear()
        for platform_id, platform in PLATFORMS.items():
            list_url = f"https://{platform['host']}/published-xml-urls"
            response = session.get(list_url, accepted_statuses={403, 404})
            if response.status_code != 200:
                self._platform_access[platform_id] = {
                    "included": False,
                    "http_status": response.status_code,
                    "exclusion_reason": "public_corpus_endpoint_access_gate_failed",
                    "list_url": list_url,
                }
                continue
            soup = BeautifulSoup(response.content, "lxml")
            platform_urls = [
                str(anchor.get("href") or "")
                for anchor in soup.select("a[href]")
                if URL_RE.search(str(anchor.get("href") or ""))
            ]
            platform_urls = sorted(set(platform_urls))
            if not platform_urls:
                self._platform_access[platform_id] = {
                    "included": False,
                    "http_status": response.status_code,
                    "exclusion_reason": "public_corpus_endpoint_returned_no_version_urls",
                    "list_url": list_url,
                }
                continue
            platform_articles: dict[str, set[int]] = {}
            for url in platform_urls:
                match = URL_RE.search(url)
                if not match:
                    continue
                article_key = f"{platform_id}|{match.group('article')}"
                platform_articles.setdefault(article_key, set()).add(
                    int(match.group("version"))
                )
                self._platform_by_url[url] = platform_id
            listed.extend(platform_urls)
            articles.update(platform_articles)
            self._platform_expected_counts[platform_id] = len(platform_urls)
            self._platform_article_counts[platform_id] = len(platform_articles)
            self._platform_access[platform_id] = {
                "included": True,
                "http_status": response.status_code,
                "exclusion_reason": None,
                "list_url": list_url,
            }
        gaps = {
            article: sorted(set(range(1, max(versions) + 1)) - versions)
            for article, versions in articles.items()
            if versions and set(range(1, max(versions) + 1)) != versions
        }
        if gaps:
            raise ValueError(f"provider XML corpus list has non-contiguous versions: {gaps}")
        self._urls = sorted(
            url for url in set(listed) if self._platform_by_url[url] in selected
        )
        if snapshot_path:
            body = {
                "schema": "observatory.f1000-enumeration-snapshot/1",
                "connector_version": self.connector_version,
                "urls": self._urls,
                "platform_access": self._platform_access,
            }
            body["snapshot_hash"] = content_hash(json.dumps(body, sort_keys=True))
            snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            temporary = snapshot_path.with_suffix(".tmp.json")
            temporary.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n")
            temporary.replace(snapshot_path)
        return self._urls

    def discover(self, context: ConnectorContext) -> Iterable[Mapping[str, Any]]:
        if not self._platform_access:
            self._enumerate_urls(context)
        for platform_id, platform in PLATFORMS.items():
            yield {
                "platform_id": platform_id,
                "platform": platform["name"],
                "entry_stage": "publication_after_editorial_screen",
                "enumeration": f"https://{platform['host']}/published-xml-urls",
                "review_model": "invited_named_post_publication_review",
                **self._platform_access.get(platform_id, {}),
            }

    def count(self, context: ConnectorContext) -> SourceEstimate:
        urls = self._enumerate_urls(context)
        return SourceEstimate(
            self.source_id,
            len(urls),
            expected_requests=len(PLATFORMS) + len(urls),
            method=(
                "platform-specific published-xml-urls censuses for every publicly "
                "passing named F1000-family endpoint; per-article version sequences "
                "verified contiguous from v1; failed access gates explicitly excluded"
            ),
            confidence="provider corpus list",
            requests_per_limit_unit=1.0,
        )

    def fetch(
        self, context: ConnectorContext, *, cursor: str | None = None, limit: int | None = None
    ) -> Iterator[FetchBatch]:
        if not self._urls:
            self._enumerate_urls(context)
        start = int(cursor or 0)
        stop = len(self._urls) if limit is None else min(len(self._urls), start + limit)
        session = self._session(context)
        metadata_recoveries = self._metadata_recoveries(context)
        while start < stop:
            end = min(start + self.page_size, stop)
            items = []
            for url in self._urls[start:end]:
                response = None
                root = None
                for _attempt in range(3):
                    try:
                        response = session.get(url, use_cache=False)
                    except NetworkPolicyError:
                        response = None
                        break
                    if response.content.strip():
                        try:
                            root = _root(response.content)
                            break
                        except etree.XMLSyntaxError:
                            pass
                if response is None or root is None:
                    html_url = re.sub(r"/xml/?$", "", url)
                    try:
                        response = session.get(html_url, use_cache=False)
                    except NetworkPolicyError:
                        response = session.get(
                            html_url,
                            use_cache=False,
                            accepted_statuses=(404, 410, 500, 502, 503, 504),
                        )
                    if response.ok and response.content.strip():
                        root = _html_fallback_root(response.content)
                        self._html_fallback_count += 1
                        object_type = "article_process_html_fallback"
                    else:
                        recovery = metadata_recoveries.get(url)
                        if recovery:
                            payload = _metadata_recovery_payload(recovery)
                            root = _root(payload)
                            self._metadata_recovery_count += 1
                            object_type = "article_process_crossref_metadata_recovery"
                            items.append(RawItem(
                                native_id=url,
                                object_type=object_type,
                                payload=payload,
                                source_url=str(recovery["evidence_url"]),
                                created_at=_pub_date(root, ".//article-meta/pub-date[1]"),
                                licence=canonical_licence(str(recovery["licence"])),
                                release_class=decide_release(
                                    object_type="bibliographic_metadata",
                                    licence=str(recovery["licence"]),
                                    source_allows_redistribution=True,
                                ).release_class.value,
                                metadata={
                                    "platform_id": self._platform_by_url[url],
                                    "enumeration_url": self._platform_access[
                                        self._platform_by_url[url]
                                    ]["list_url"],
                                    "xml_unavailable": True,
                                    "intended_xml_url": url,
                                    "recovery_evidence_type": "crossref_bibliographic_metadata",
                                    "review_material_recovered": False,
                                },
                            ))
                            continue
                        match = URL_RE.search(url)
                        if not match:
                            raise ValueError(f"unrecoverable enumerated F1000 URL: {url}")
                        next_version = int(match.group("version")) + 1
                        later_url = re.sub(
                            r"/v\d+/xml/?$", f"/v{next_version}/xml", url
                        )
                        response = session.get(later_url, use_cache=False)
                        root = _root(response.content)
                        self._later_version_recovery_count += 1
                        object_type = "article_process_later_version_recovery"
                else:
                    object_type = "article_process_xml"
                declared = _licence(root)
                decision = decide_release(
                    object_type="article_and_review_jats",
                    licence=declared,
                    source_allows_redistribution=None,
                )
                items.append(RawItem(
                    native_id=url, object_type=object_type, payload=response.content,
                    source_url=response.url,
                    created_at=_pub_date(root, ".//article-meta/pub-date[1]"),
                    licence=decision.licence,
                    release_class=decision.release_class.value,
                    metadata={
                        "platform_id": self._platform_by_url[url],
                        "enumeration_url": self._platform_access[
                            self._platform_by_url[url]
                        ]["list_url"],
                        "xml_unavailable": object_type != "article_process_xml",
                        "intended_xml_url": url,
                        "recovered_from_later_version_url": (
                            response.url
                            if object_type == "article_process_later_version_recovery"
                            else None
                        ),
                    },
                ))
            done = end >= stop
            yield FetchBatch(
                tuple(items), None if done else str(end), done,
                f"f1000-corpus-offset:{start}:{end}", len(self._urls),
            )
            start = end

    def normalize(
        self, item: RawItem, *, source_object_id: str, provenance_event_id: str
    ) -> Iterable[NormalizedRecord]:
        root = (
            _html_fallback_root(item.payload)
            if item.object_type == "article_process_html_fallback"
            else _root(item.payload)
        )
        doi = canonical_doi(_text(root, ".//article-meta/article-id[@pub-id-type='doi']"))
        if not doi:
            return
        if item.object_type in {
            "article_process_later_version_recovery",
            "article_process_crossref_metadata_recovery",
        }:
            match = URL_RE.search(str(item.metadata.get("intended_xml_url") or ""))
            if not match:
                raise ValueError("later-version recovery lacks its intended version URL")
            base_doi, _, version_style = _version_doi(doi)
            doi = f"{base_doi}.{version_style}{int(match.group('version'))}"
        base_doi, version_number, version_style = _version_doi(doi)
        platform_id = str(item.metadata.get("platform_id") or "")
        if not platform_id:
            host = (urlparse(item.source_url or "").hostname or "").lower()
            platform_id = next(
                (key for key, row in PLATFORMS.items() if row["host"] == host), ""
            )
        if not platform_id:
            namespace = base_doi.split("/", 1)[-1].split(".", 1)[0]
            platform_id = DOI_NAMESPACE_PLATFORM.get(namespace, "")
        if platform_id not in PLATFORMS:
            raise ValueError(f"unresolved F1000-family platform for {item.source_url}")
        platform = PLATFORMS[platform_id]
        published = _pub_date(root, ".//article-meta/pub-date[1]") or item.created_at
        year = int((published or "1970")[:4])
        gate_id = stable_id("gate", self.source_id, platform_id)
        cycle_id = stable_id("gate_cycle", self.source_id, f"{platform_id}|{year}")
        policy_id = stable_id(
            "policy_version", self.source_id, f"{platform_id}|publish-review-revise"
        )
        candidate_id = stable_id("candidate", "doi", base_doi)
        version_id = stable_id("candidate_version", "doi", doi)
        status = _text(root, ".//article-meta/title-group/fn-group[@content-type='pub-status']")
        declared = _licence(root) or item.licence
        common = {
            "source_id": self.source_id, "source_object_id": source_object_id,
            "provenance_event_id": provenance_event_id,
            "observed_at": published, "record_version": 1,
        }
        if gate_id not in self._emitted["gate"]:
            self._emitted["gate"].add(gate_id)
            yield NormalizedRecord("gate", {
                "gate_id": gate_id, "native_id": platform_id,
                "name": platform["name"], "organization": platform["organization"],
                "domain": "multidisciplinary",
                "country": "GB", "architecture": "post_publication_review",
                "active_from": None, "active_to": None, **common,
            })
        if policy_id not in self._emitted["policy_version"]:
            self._emitted["policy_version"].add(policy_id)
            policy = {
                "approval_status_high_to_low": APPROVAL_HIGH_TO_LOW,
                "pass_rule": "two approved OR one approved plus two approved with reservations",
            }
            yield NormalizedRecord("policy_version", {
                "policy_version_id": policy_id, "gate_id": gate_id,
                "native_id": f"{platform_id}|publish-review-revise",
                "effective_at": None, "valid_to": None,
                "criteria_json": json_text({"criterion": "scientific validity, not novelty"}),
                "rubric_json": json_text(policy),
                "stage_rules_json": json_text({
                    "hidden_entry_screen": "editorial checks before publication",
                    "entry": "publication_after_editorial_screen",
                    "review": "invited named post-publication reports",
                    "not_approved_is_rejection": False,
                }),
                "quota_or_cap": None, "anonymity_model": "all reviewer identities visible",
                "revision_rules": json_text({"all_versions_retained": True}),
                "policy_url": platform["policy_url"],
                "content_hash": content_hash(json.dumps(policy, sort_keys=True)),
                "date_confidence": 0.5, **common,
            })
        if cycle_id not in self._emitted["gate_cycle"]:
            self._emitted["gate_cycle"].add(cycle_id)
            yield NormalizedRecord("gate_cycle", {
                "gate_cycle_id": cycle_id, "gate_id": gate_id,
                "native_id": f"{platform_id}|{year}",
                "name": f"{platform['name']} {year}", "track": None,
                "cycle_start": f"{year}-01-01T00:00:00+00:00",
                "cycle_end": f"{year}-12-31T23:59:59+00:00",
                "policy_version_id": policy_id, "architecture": "post_publication_review",
                "received_count": None, "observable_count": None, "evaluated_count": None,
                "selected_count": None, "status": "provider XML census", **common,
            })
        self._cycle_version_counts[cycle_id] += 1
        self._cycle_platform[cycle_id] = platform_id
        self._platform_found_counts[platform_id] += 1
        if version_number == 1:
            self._cycle_candidate_counts[cycle_id] += 1
        if candidate_id not in self._emitted["candidate"]:
            self._emitted["candidate"].add(candidate_id)
            yield NormalizedRecord("candidate", {
                "candidate_id": candidate_id, "first_observed_at": published,
                "domain": None, "candidate_type": "published_article",
                "canonical_title": _text(root, ".//article-meta/title-group/article-title"),
                "status": "published_before_peer_review", **common,
            })
        if version_id not in self._emitted["candidate_version"]:
            self._emitted["candidate_version"].add(version_id)
            yield NormalizedRecord("candidate_version", {
                "candidate_version_id": version_id, "candidate_id": candidate_id,
                "native_id": doi, "version_label": f"version {version_number}",
                "version_number": version_number, "created_at": published, "modified_at": None,
                "title": _text(root, ".//article-meta/title-group/article-title"),
                "abstract": _text(root, ".//article-meta/abstract"),
                "content_artifact_id": stable_id("content_artifact", self.source_id, doi),
                "content_hash": content_hash(item.payload), "licence": declared, "language": "en",
                "authorship_visible": True, "withdrawn": False, **common,
            })
        yield from self._emit_artifact(
            native=doi, object_type=item.object_type, text=" ".join(root.itertext()),
            media_type=(
                "text/html" if item.object_type == "article_process_html_fallback"
                else "application/jats+xml"
            ), licence=declared,
            local_pointer="article", source_url=item.source_url, common=common,
        )
        if version_number > 1:
            previous_doi = f"{base_doi}.{version_style}{version_number - 1}"
            edge_id = stable_id("lineage_edge", self.source_id, f"{previous_doi}|{doi}")
            if edge_id not in self._emitted["lineage_edge"]:
                self._emitted["lineage_edge"].add(edge_id)
                yield NormalizedRecord("lineage_edge", {
                    "lineage_edge_id": edge_id, "source_candidate_id": candidate_id,
                    "source_version_id": stable_id("candidate_version", "doi", previous_doi),
                    "target_candidate_id": candidate_id, "target_version_id": version_id,
                    "relation_type": "provider_version_sequence", "declared": True,
                    "confidence": 1.0, "linkage_tier": "source_declared",
                    "method_version": "f1000-version-doi/1",
                    "evidence_json": json_text({"source": previous_doi, "target": doi}), **common,
                })
        yield NormalizedRecord("candidate_gate_event", {
            "candidate_gate_event_id": stable_id(
                "candidate_gate_event", self.source_id, f"{cycle_id}|{doi}"
            ),
            "candidate_id": candidate_id, "candidate_version_id": version_id,
            "gate_cycle_id": cycle_id, "native_id": doi, "submitted_at": None,
            "earliest_observed_stage": "publication_after_editorial_screen",
            "final_observed_stage": status or "post_publication_review_pending_or_complete",
            "coverage_observation_id": coverage_observation_id(
                self.source_id, cycle_id, "published_article_version"
            ), **common,
        })
        yield from self._emit_alias(
            entity_id=candidate_id, scheme="doi", value=base_doi, relation="canonical_work",
            common=common,
        )
        yield from self._emit_alias(
            entity_id=candidate_id, scheme="doi", value=doi, relation="version_doi",
            common=common,
        )
        if item.object_type in {
            "article_process_later_version_recovery",
            "article_process_crossref_metadata_recovery",
        }:
            # Review/response sub-articles belong to the later source version
            # and will be emitted when that enumerated version is processed.
            return
        for index, subarticle in enumerate(root.xpath(".//sub-article")):
            yield from self._normalize_subarticle(
                subarticle, index=index, default_version_id=version_id,
                candidate_id=candidate_id, cycle_id=cycle_id, parent_doi=doi,
                parent_licence=declared, common=common, source_url=item.source_url,
            )

    def _normalize_subarticle(
        self, node, *, index: int, default_version_id: str, candidate_id: str,
        cycle_id: str, parent_doi: str, parent_licence: str | None,
        common: Mapping[str, Any], source_url: str | None,
    ) -> Iterable[NormalizedRecord]:
        kind = str(node.get("article-type") or "review_material")
        native = canonical_doi(_text(node, ".//article-id[@pub-id-type='doi']")) or (
            f"{parent_doi}|{kind}|{index}"
        )
        target_doi = canonical_doi(_text(node, ".//related-article/@*[local-name()='href']"))
        target_version_id = (
            stable_id("candidate_version", "doi", target_doi)
            if target_doi else default_version_id
        )
        declared = _licence(node) or parent_licence
        text = "\n".join(
            " ".join("".join(part.itertext()).split())
            for part in node.xpath("./body/*")
        )
        yield from self._emit_artifact(
            native=str(native), object_type=kind, text=text, media_type="text/xml",
            licence=declared, local_pointer=f"sub-article[{index}]",
            source_url=source_url, common=common,
        )
        recommendation = _text(
            node,
            ".//custom-meta[meta-name='recommendation']/meta-value",
        )
        normalized = RECOMMENDATION_MAP.get(
            (recommendation or "").strip().lower().replace("_", "-")
        )
        participants = []
        for contributor in node.xpath(".//contrib-group/contrib"):
            participants.append({
                "given": _text(contributor, ".//given-names"),
                "family": _text(contributor, ".//surname"),
                "role": _text(contributor, ".//role"),
                "orcid": _text(contributor, ".//uri[@content-type='orcid']"),
            })
        evaluation_id = stable_id("evaluation", self.source_id, str(native))
        if evaluation_id not in self._emitted["evaluation"]:
            self._emitted["evaluation"].add(evaluation_id)
            yield NormalizedRecord("evaluation", {
                "evaluation_id": evaluation_id, "candidate_version_id": target_version_id,
                "gate_cycle_id": cycle_id, "native_id": str(native),
                "evaluation_type": kind,
                "evaluator_role": "author" if kind in {"response", "author-comment"} else "reviewer",
                "evaluator_public_id": None, "evaluator_protected_id": None,
                "anonymous": False, "official": kind not in {"response", "author-comment"},
                "criterion_native": "recommendation" if recommendation else None,
                "criterion_normalized": "f1000_approval_status" if recommendation else None,
                "criterion_value": normalized or recommendation,
                "criterion_value_numeric": (
                    float({"not_approved": 1, "approved_with_reservations": 2, "approved": 3}[normalized])
                    if normalized else None
                ),
                "scale_json": json_text({
                    "native_high_to_low": APPROVAL_HIGH_TO_LOW,
                    "normalized_low_to_high": [
                        "not_approved", "approved_with_reservations", "approved"
                    ],
                    "not_approved_is_rejection": False,
                }) if recommendation else None,
                "confidence_value": None,
                "text_artifact_id": stable_id("content_artifact", self.source_id, str(native)),
                "created_at": _pub_date(node, ".//pub-date[1]"),
                "forum_native_id": parent_doi, "invitation_native": kind,
                "readers_json": json_text(["public"]),
                "signatures_json": json_text(participants),
                "reply_to_native_id": target_doi or parent_doi, **common,
            })

    def _emit_artifact(
        self, *, native: str, object_type: str, text: str, media_type: str,
        licence: str | None, local_pointer: str, source_url: str | None,
        common: Mapping[str, Any],
    ) -> Iterable[NormalizedRecord]:
        artifact_id = stable_id("content_artifact", self.source_id, native)
        if artifact_id in self._emitted["content_artifact"]:
            return
        self._emitted["content_artifact"].add(artifact_id)
        decision = decide_release(
            object_type=object_type, licence=licence, source_allows_redistribution=None
        )
        yield NormalizedRecord("content_artifact", {
            "content_artifact_id": artifact_id, "object_type": object_type,
            "media_type": media_type, "byte_hash": None,
            "normalized_text_hash": content_hash(" ".join(text.split())),
            "source_url": source_url, "local_pointer": local_pointer,
            "licence": decision.licence, "release_class": decision.release_class.value,
            "size_bytes": len(text.encode()), "language": "en",
            "parser_version": self.connector_version, **common,
        })

    def _emit_alias(
        self, *, entity_id: str, scheme: str, value: str, relation: str,
        common: Mapping[str, Any],
    ) -> Iterable[NormalizedRecord]:
        alias_id = stable_id(
            "identifier_alias", self.source_id, f"candidate|{scheme}|{value}"
        )
        if alias_id in self._emitted["identifier_alias"]:
            return
        self._emitted["identifier_alias"].add(alias_id)
        yield NormalizedRecord("identifier_alias", {
            "identifier_alias_id": alias_id, "entity_kind": "candidate",
            "entity_id": entity_id, "scheme": scheme, "value": value,
            "canonical_value": value.lower(), "relation": relation,
            "confidence": 1.0, "conflict_status": "none", **common,
        })

    def validate_fixture(self, context: ConnectorContext) -> Mapping[str, Any]:
        fixture = context.fixture_dir / self.source_id / "article.xml"
        if not fixture.exists():
            return {"passes": False, "reason": "fixture absent"}
        root = _root(fixture.read_bytes())
        doi = canonical_doi(_text(root, ".//article-meta/article-id[@pub-id-type='doi']"))
        return {
            "passes": bool(doi and root.xpath(".//sub-article")),
            "doi": doi, "review_material_count": len(root.xpath(".//sub-article")),
        }

    def emit_coverage(
        self, context: ConnectorContext, *, found_count: int
    ) -> Iterable[CoverageEvidence]:
        for cycle_id, count in sorted(self._cycle_version_counts.items()):
            platform_id = self._cycle_platform[cycle_id]
            yield CoverageEvidence(
                gate_cycle_id=cycle_id, object_type="published_article_version",
                earliest_public_stage="publication after editorial screen",
                observability_grade="B", expected_count=None, found_count=count,
                expected_count_method=(
                    "completed provider XML corpus; year is parent-version publication year"
                ),
                query_or_invitation=self._platform_access[platform_id]["list_url"],
                known_hidden_stages=("pre-publication editorial screen",),
                known_exclusions=(
                    "non-article document types may not undergo peer review",
                ),
                audit_status=(
                    f"provider_version_census; platform={platform_id}; "
                    f"platform_expected_versions={self._platform_expected_counts[platform_id]}; "
                    "html_fallbacks_for_empty_xml="
                    f"{self._html_fallback_count}; later_version_recoveries="
                    f"{self._later_version_recovery_count}; "
                    "crossref_metadata_recoveries="
                    f"{self._metadata_recovery_count}"
                ),
            )
