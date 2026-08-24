"""Provider-status outcomes and relation audit for Copernicus review chains."""

from __future__ import annotations

import json
import os
import re
import tempfile
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from .connectors.http import NetworkPolicyError, PoliteSession, RatePolicy
from .ids import content_hash
from .registry import ROOT

_DOI_SESSION = PoliteSession(
    cache_dir=ROOT / "data" / "observatory" / "cache" / "doi-outcomes",
    allowed_hosts={"doi.org", "dx.doi.org"},
    policy=RatePolicy(min_interval_seconds=0.1, max_retries=1, timeout_seconds=90),
)

REJECTION_PATTERNS = (
    r"revision was not accepted",
    r"not accepted for further review after discussion",
    r"(?:preprint|manuscript|paper|revision) (?:was|has been|is) rejected",
    r"not accepted for publication",
    r"final status[^.]{0,80}rejection",
)


def classify_provider_status(notification: str | None) -> str:
    text = " ".join(str(notification or "").lower().split())
    if any(re.search(pattern, text) for pattern in REJECTION_PATTERNS):
        return "affirmative_rejected_after_public_discussion"
    if "withdrawn by the authors" in text:
        return "affirmative_author_withdrawal_after_public_discussion"
    if "has been retracted" in text:
        return "affirmative_retraction_after_public_discussion"
    if "revision for further review has not been submitted" in text or "a final paper is not foreseen" in text:
        return "affirmative_discontinued_after_public_discussion"
    if "under review" in text:
        return "provider_visible_review_ongoing_censored"
    if text:
        return "provider_status_other_censored"
    return "public_discussion_outcome_unresolved_censored"


class _RateGate:
    def __init__(self, interval: float):
        self.interval = interval
        self.lock = threading.Lock()
        self.last = 0.0

    def wait(self) -> None:
        with self.lock:
            delay = self.interval - (time.monotonic() - self.last)
            if delay > 0:
                time.sleep(delay)
            self.last = time.monotonic()


def _fetch_page(
    discussion_doi: str,
    *,
    audit_comment_dois: list[str],
    gate: _RateGate,
) -> dict[str, Any]:
    url = f"https://doi.org/{discussion_doi}"
    response = None
    error_class = None
    for attempt in range(4):
        try:
            gate.wait()
            response = _DOI_SESSION.get(
                url,
                headers={"User-Agent": "OpenSelectionGraph/0.1 (public metadata audit)"},
                use_cache=True,
                accepted_statuses=range(400, 600),
            )
            if response.status_code == 429 or response.status_code >= 500:
                time.sleep(2**attempt)
                continue
            error_class = None
            break
        except NetworkPolicyError as exc:
            error_class = type(exc).__name__
            time.sleep(2**attempt)
    if response is None:
        return {
            "discussion_doi": discussion_doi,
            "http_status": None,
            "resolved_url": None,
            "page_byte_hash": None,
            "notification": None,
            "provider_status": "source_page_unavailable_censored",
            "error_class": error_class or "request_failed",
            "audit_comment_presence": {doi: None for doi in audit_comment_dois},
        }
    body = response.content
    soup = BeautifulSoup(body, "lxml")
    notifications = [" ".join(node.get_text(" ", strip=True).split()) for node in soup.select(".co-notification")]
    notification = " | ".join(notifications) or None
    page_text = body.decode(errors="ignore").lower()
    status = (
        classify_provider_status(notification) if response.status_code == 200 else "source_page_unavailable_censored"
    )
    return {
        "discussion_doi": discussion_doi,
        "http_status": response.status_code,
        "resolved_url": response.url,
        "page_byte_hash": content_hash(body),
        "notification": notification,
        "provider_status": status,
        "error_class": error_class,
        "audit_comment_presence": {doi: doi.lower() in page_text for doi in audit_comment_dois},
    }


def _observe_or_fetch_chain(
    chain: dict[str, Any], *, audit_sample: dict[str, list[str]], gate: _RateGate
) -> dict[str, Any]:
    discussion_doi = str(chain["discussion_doi"])
    audit_comment_dois = audit_sample.get(discussion_doi, [])
    if chain.get("final_article_dois") and not audit_comment_dois:
        return {
            "discussion_doi": discussion_doi,
            "http_status": None,
            "resolved_url": None,
            "page_byte_hash": None,
            "notification": None,
            "provider_status": "published_final_observed_from_relation",
            "error_class": None,
            "audit_comment_presence": {},
            "acquisition_status": "http_not_required_final_relation_observed",
        }
    result = _fetch_page(discussion_doi, audit_comment_dois=audit_comment_dois, gate=gate)
    result["acquisition_status"] = "provider_landing_page_fetched"
    return result


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _audit_sample(records: list[dict[str, Any]], size: int = 400) -> dict[str, list[str]]:
    candidates = []
    for chain in records:
        for comment in chain.get("comments") or []:
            candidates.append(
                (
                    str(comment["comment_doi"]),
                    str(chain["discussion_doi"]),
                    str(comment.get("role") or "unknown"),
                    str(chain.get("journal") or "unknown"),
                    str(chain.get("year") or "unknown"),
                )
            )
    # Hash ordering is deterministic; round-robin strata protect rare roles
    # and journal-years from a large journal dominating the audit.
    strata: dict[tuple[str, str, str], list[tuple[str, str]]] = defaultdict(list)
    for comment_doi, discussion_doi, role, journal, year in candidates:
        strata[(role, journal, year)].append((comment_doi, discussion_doi))
    for values in strata.values():
        values.sort(key=lambda row: content_hash("|".join(row)))
    selected: list[tuple[str, str]] = []
    while strata and len(selected) < size:
        for key in sorted(list(strata)):
            values = strata[key]
            if values:
                selected.append(values.pop(0))
            if not values:
                del strata[key]
            if len(selected) >= size:
                break
    by_discussion: dict[str, list[str]] = defaultdict(list)
    for comment_doi, discussion_doi in selected:
        by_discussion[discussion_doi].append(comment_doi)
    return dict(by_discussion)


def crawl_copernicus_outcomes(
    chain_report: dict[str, Any],
    *,
    staging_dir: Path,
    output: Path,
    restart: bool = False,
    workers: int = 4,
    batch_size: int = 100,
    repair_transient_errors: bool = True,
) -> Path:
    records = list(chain_report.get("records") or [])
    audit_sample = _audit_sample(records)
    if restart and staging_dir.exists():
        for path in staging_dir.glob("batch-*.json"):
            path.unlink()
    staging_dir.mkdir(parents=True, exist_ok=True)
    gate = _RateGate(0.25)
    for start in range(0, len(records), batch_size):
        path = staging_dir / f"batch-{start // batch_size:06d}.json"
        if path.exists():
            if not repair_transient_errors:
                continue
            existing = json.loads(path.read_text())
            retry_indices = [
                index
                for index, row in enumerate(existing)
                if row.get("provider_status") == "source_page_unavailable_censored" and row.get("error_class")
            ]
            if not retry_indices:
                continue
            batch = records[start : start + batch_size]
            with ThreadPoolExecutor(max_workers=workers) as executor:
                replacements = list(
                    executor.map(
                        lambda index: _observe_or_fetch_chain(batch[index], audit_sample=audit_sample, gate=gate),
                        retry_indices,
                    )
                )
            for index, replacement in zip(retry_indices, replacements):
                existing[index] = replacement
            _atomic_json(path, existing)
            continue
        batch = records[start : start + batch_size]
        with ThreadPoolExecutor(max_workers=workers) as executor:
            rows = list(
                executor.map(
                    lambda chain: _observe_or_fetch_chain(chain, audit_sample=audit_sample, gate=gate),
                    batch,
                )
            )
        _atomic_json(path, rows)

    outcomes = []
    for path in sorted(staging_dir.glob("batch-*.json")):
        outcomes.extend(json.loads(path.read_text()))
    for outcome in outcomes:
        # Batches created by connector version 1 predate the explicit field,
        # but every row in those batches came from `_fetch_page`.
        outcome.setdefault("acquisition_status", "provider_landing_page_fetched")
    chain_by_doi = {str(row["discussion_doi"]): row for row in records}
    status_counts = Counter()
    for outcome in outcomes:
        chain = chain_by_doi.get(outcome["discussion_doi"], {})
        provider_status = (
            classify_provider_status(outcome.get("notification"))
            if outcome.get("http_status") == 200
            else outcome["provider_status"]
        )
        outcome["provider_status"] = provider_status
        status = "published_final_observed" if chain.get("final_article_dois") else provider_status
        outcome["outcome_state"] = status
        outcome["final_article_dois"] = chain.get("final_article_dois") or []
        status_counts[status] += 1

    relation_rows = []
    for outcome in outcomes:
        for comment_doi, present in outcome["audit_comment_presence"].items():
            relation_rows.append(
                {
                    "discussion_doi": outcome["discussion_doi"],
                    "comment_doi": comment_doi,
                    "provider_page_contains_comment_doi": present,
                    "page_http_status": outcome["http_status"],
                }
            )
    checked = [
        row
        for row in relation_rows
        if row["provider_page_contains_comment_doi"] is not None and row["page_http_status"] == 200
    ]
    correct = sum(bool(row["provider_page_contains_comment_doi"]) for row in checked)
    precision = correct / len(checked) if checked else None
    report: dict[str, Any] = {
        "schema": "observatory.copernicus-provider-outcomes/1",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "chain_count": len(records),
        "outcome_count": len(outcomes),
        "outcome_states": dict(sorted(status_counts.items())),
        "affirmative_rejected_non_ml_count": status_counts["affirmative_rejected_after_public_discussion"],
        "relation_audit": {
            "method": (
                "Crossref comment-to-discussion relations checked against independent "
                "provider landing-page presence, stratified by role/journal/year"
            ),
            "sample_target": 400,
            "checked": len(checked),
            "correct": correct,
            "precision": precision,
            "rows": relation_rows,
        },
        "hidden_stage": "access review before public discussion",
        "absence_of_final_relation_means_rejection": False,
        "scope_rule": (
            "only an affirmative provider status is rejected; missing final-article "
            "relations and unavailable/ongoing pages remain explicitly censored"
        ),
        "provider_page_fetch_scope": {
            "rule": (
                "fetch every chain lacking a final-article relation plus every chain "
                "selected for the independent stratified relation audit; an affirmative "
                "final relation establishes published outcome without a redundant page fetch"
            ),
            "fetched": sum(row.get("acquisition_status") == "provider_landing_page_fetched" for row in outcomes),
            "not_required_final_relation_observed": sum(
                row.get("acquisition_status") == "http_not_required_final_relation_observed" for row in outcomes
            ),
            "transient_failures_retained_as_censored": sum(bool(row.get("error_class")) for row in outcomes),
            "repair_transient_errors": repair_transient_errors,
        },
        "outcomes": outcomes,
    }
    report["passes"] = bool(
        len(outcomes) == len(records)
        and report["affirmative_rejected_non_ml_count"] >= 2_000
        and len(checked) >= 200
        and (precision or 0) >= 0.98
        and report["hidden_stage"]
        and not report["absence_of_final_relation_means_rejection"]
    )
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True, default=str))
    _atomic_json(output, report)
    return output
