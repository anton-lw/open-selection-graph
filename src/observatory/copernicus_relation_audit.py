"""Independent DOI-redirect audit of Copernicus comment-to-discussion relations."""

from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from .connectors.http import NetworkPolicyError, PoliteSession, RatePolicy
from .ids import content_hash
from .registry import ROOT

_DOI_SESSION = PoliteSession(
    cache_dir=ROOT / "data" / "observatory" / "cache" / "doi-relations",
    allowed_hosts={"doi.org"},
    policy=RatePolicy(min_interval_seconds=0.1, max_retries=1, timeout_seconds=60),
)


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


def redirect_matches_parent(discussion_doi: str, location: str | None) -> bool:
    """Return whether a comment DOI resolves into its declared parent namespace."""
    parent_token = discussion_doi.lower().split("/", 1)[-1]
    normalized = unquote(str(location or "")).lower()
    return bool(parent_token and parent_token in normalized)


def _audit_row(row: dict[str, Any], gate: _RateGate) -> dict[str, Any]:
    doi = str(row["comment_doi"])
    response = None
    error_class = None
    for attempt in range(4):
        try:
            gate.wait()
            response = _DOI_SESSION.get(
                f"https://doi.org/{doi}",
                allow_redirects=False,
                headers={"User-Agent": ("OpenSelectionGraph/0.1 (public metadata relation audit)")},
                use_cache=False,
                accepted_statuses=range(300, 600),
            )
            if response.status_code == 429 or response.status_code >= 500:
                time.sleep(2**attempt)
                continue
            error_class = None
            break
        except NetworkPolicyError as exc:
            error_class = type(exc).__name__
            time.sleep(2**attempt)
    location = response.headers.get("Location") if response is not None else None
    status = response.status_code if response is not None else None
    return {
        "discussion_doi": row["discussion_doi"],
        "comment_doi": doi,
        "doi_http_status": status,
        "redirect_location": location,
        "redirect_location_hash": content_hash(location) if location else None,
        "redirect_matches_parent": (
            redirect_matches_parent(str(row["discussion_doi"]), location)
            if status is not None and 300 <= status < 400
            else False
        ),
        "error_class": error_class,
    }


def build_redirect_relation_audit(outcome_report: dict[str, Any], *, workers: int = 4) -> dict[str, Any]:
    source_rows = list(outcome_report.get("relation_audit", {}).get("rows") or [])
    if not source_rows:
        raise ValueError("Copernicus outcome report contains no relation audit sample")
    gate = _RateGate(0.1)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        rows = list(executor.map(lambda row: _audit_row(row, gate), source_rows))
    checked = [row for row in rows if row["doi_http_status"] is not None and 300 <= int(row["doi_http_status"]) < 400]
    correct = sum(bool(row["redirect_matches_parent"]) for row in checked)
    precision = correct / len(checked) if checked else None
    report: dict[str, Any] = {
        "schema": "observatory.copernicus-doi-relation-audit/1",
        "method": (
            "Deterministic stratified Crossref comment sample independently checked "
            "through DOI registry redirects; a relation is correct only when the "
            "comment DOI redirects into the declared discussion DOI namespace."
        ),
        "sample_target": len(source_rows),
        "checked": len(checked),
        "correct": correct,
        "precision": precision,
        "landing_page_literal_doi_audit": {
            "checked": outcome_report.get("relation_audit", {}).get("checked"),
            "precision": outcome_report.get("relation_audit", {}).get("precision"),
            "status": "retained_as_non_authoritative_rendering_diagnostic",
        },
        "rows": rows,
    }
    report["passes"] = bool(len(checked) >= 200 and (precision or 0) >= 0.98)
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    return report


def write_redirect_relation_audit(outcome_report_path: Path, output: Path, *, workers: int = 4) -> Path:
    report = build_redirect_relation_audit(json.loads(outcome_report_path.read_text()), workers=workers)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return output
