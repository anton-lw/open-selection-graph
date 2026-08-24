"""Polite, cached HTTP with bounded retries and deny-by-default hosts."""

from __future__ import annotations

import hashlib
import json
import os
import random
import threading
import time
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests

USER_AGENT = "OpenSelectionGraph/0.1 (public-data research; contact via project repository)"


@dataclass(frozen=True)
class RatePolicy:
    min_interval_seconds: float = 0.1
    max_retries: int = 4
    timeout_seconds: float = 45.0
    max_backoff_seconds: float = 60.0
    max_concurrency_per_host: int = 2
    daily_request_ceiling: int = 20_000


class NetworkPolicyError(RuntimeError):
    pass


class PoliteSession:
    _semaphore_guard = threading.Lock()
    _host_semaphores: dict[tuple[str, int], threading.BoundedSemaphore] = {}

    def __init__(
        self,
        *,
        cache_dir: Path,
        allowed_hosts: Iterable[str],
        denied_hosts: Iterable[str] = (),
        policy: RatePolicy | None = None,
        user_agent: str = USER_AGENT,
    ):
        from ..operations import network_allowed_hosts

        self.cache_dir = cache_dir
        self.allowed_hosts = {host.lower() for host in allowed_hosts}
        self.denied_hosts = {host.lower() for host in denied_hosts}
        unregistered = sorted(self.allowed_hosts - network_allowed_hosts())
        if unregistered:
            raise NetworkPolicyError(
                f"hosts lack a free-source network-policy link: {unregistered}"
            )
        self.policy = policy or RatePolicy()
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"})
        self._last_request: dict[str, float] = {}
        self._robots: dict[str, RobotFileParser] = {}
        self._daily_counts: dict[tuple[str, str], int] = {}

    def _host(self, url: str) -> str:
        host = (urlparse(url).hostname or "").lower()
        if not host or host in self.denied_hosts or host not in self.allowed_hosts:
            raise NetworkPolicyError(f"host is not approved by the source card: {host or url}")
        return host

    def _wait(self, host: str) -> None:
        since = time.monotonic() - self._last_request.get(host, 0.0)
        if since < self.policy.min_interval_seconds:
            time.sleep(self.policy.min_interval_seconds - since)

    def _semaphore(self, host: str) -> threading.BoundedSemaphore:
        key = (host, self.policy.max_concurrency_per_host)
        with self._semaphore_guard:
            return self._host_semaphores.setdefault(
                key, threading.BoundedSemaphore(self.policy.max_concurrency_per_host)
            )

    def _record_request(self, host: str) -> None:
        """Enforce a persistent per-host UTC-day request ceiling.

        Each network attempt is an append-only ledger row.  The in-memory
        counter avoids repeatedly scanning it during a run; the initial scan
        makes separate/restarted processes share the same ceiling.
        """
        day = time.strftime("%Y-%m-%d", time.gmtime())
        key = (host, day)
        ledger = self.cache_dir / "request-ledger" / f"{day}-{host}.jsonl"
        if key not in self._daily_counts:
            self._daily_counts[key] = (
                sum(1 for line in ledger.read_text().splitlines() if line.strip()) if ledger.exists() else 0
            )
        if self._daily_counts[key] >= self.policy.daily_request_ceiling:
            raise NetworkPolicyError(f"daily request ceiling reached for {host}: {self.policy.daily_request_ceiling}")
        ledger.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(ledger, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
        try:
            os.write(fd, (json.dumps({"host": host, "utc_day": day}) + "\n").encode())
        finally:
            os.close(fd)
        self._daily_counts[key] += 1

    @staticmethod
    def _cache_key(url: str, params: Mapping[str, Any] | None) -> str:
        payload = json.dumps([url, sorted((params or {}).items())], default=str, separators=(",", ":"))
        return hashlib.sha256(payload.encode()).hexdigest()

    def robots_allowed(self, url: str) -> bool:
        host = self._host(url)
        if host not in self._robots:
            parser = RobotFileParser()
            parser.set_url(f"https://{host}/robots.txt")
            try:
                parser.read()
            except Exception:
                # An unreadable robots file is not affirmative permission; source
                # cards/terms remain authoritative and the caller can quarantine.
                return False
            self._robots[host] = parser
        return self._robots[host].can_fetch(self.session.headers["User-Agent"], url)

    def get(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        use_cache: bool = True,
        require_robots: bool = False,
        accepted_statuses: Iterable[int] = (),
        allow_redirects: bool = True,
    ) -> requests.Response:
        host = self._host(url)
        if require_robots and not self.robots_allowed(url):
            raise NetworkPolicyError(f"robots policy does not affirm access to {url}")
        key = self._cache_key(url, params)
        body_path = self.cache_dir / key[:2] / f"{key}.body"
        meta_path = body_path.with_suffix(".json")
        conditional: dict[str, str] = dict(headers or {})
        if use_cache and body_path.exists() and meta_path.exists():
            meta = json.loads(meta_path.read_text())
            if meta.get("etag"):
                conditional["If-None-Match"] = meta["etag"]
            if meta.get("last_modified"):
                conditional["If-Modified-Since"] = meta["last_modified"]
        last: Exception | None = None
        for attempt in range(self.policy.max_retries):
            self._wait(host)
            try:
                self._record_request(host)
                with self._semaphore(host):
                    response = self.session.get(
                        url,
                        params=params,
                        headers=conditional,
                        timeout=self.policy.timeout_seconds,
                        allow_redirects=allow_redirects,
                    )
                self._last_request[host] = time.monotonic()
                if response.status_code == 304 and body_path.exists():
                    cached = requests.Response()
                    cached.status_code = 200
                    cached.url = response.url
                    cached.headers.update(json.loads(meta_path.read_text()).get("headers") or {})
                    cached._content = body_path.read_bytes()
                    cached.encoding = response.encoding or "utf-8"
                    return cached
                if response.status_code in set(accepted_statuses):
                    return response
                if response.status_code in {429, 500, 502, 503, 504}:
                    wait = self._retry_wait(response, attempt)
                    last = requests.HTTPError(f"HTTP {response.status_code}", response=response)
                    time.sleep(wait)
                    continue
                response.raise_for_status()
                if use_cache:
                    body_path.parent.mkdir(parents=True, exist_ok=True)
                    body_path.write_bytes(response.content)
                    meta_path.write_text(
                        json.dumps(
                            {
                                "url": response.url,
                                "etag": response.headers.get("ETag"),
                                "last_modified": response.headers.get("Last-Modified"),
                                "headers": dict(response.headers),
                            },
                            indent=2,
                            sort_keys=True,
                        )
                    )
                return response
            except requests.RequestException as exc:
                last = exc
                if attempt + 1 < self.policy.max_retries:
                    time.sleep(min(2**attempt + random.random(), self.policy.max_backoff_seconds))
        raise NetworkPolicyError(f"request failed after bounded retries: {type(last).__name__}: {last}")

    def post_json(
        self,
        url: str,
        *,
        payload: Mapping[str, Any],
        headers: Mapping[str, str] | None = None,
    ) -> requests.Response:
        """POST a JSON object under the same host, retry, and ceiling policy.

        POST responses are deliberately never cached.  This method exists for
        read-only authentication handshakes; acquisition adapters must not use
        it to mutate provider state.
        """
        host = self._host(url)
        last: Exception | None = None
        for attempt in range(self.policy.max_retries):
            self._wait(host)
            try:
                self._record_request(host)
                with self._semaphore(host):
                    response = self.session.post(
                        url,
                        json=dict(payload),
                        headers=dict(headers or {}),
                        timeout=self.policy.timeout_seconds,
                    )
                self._last_request[host] = time.monotonic()
                if response.status_code in {429, 500, 502, 503, 504}:
                    wait = self._retry_wait(response, attempt)
                    last = requests.HTTPError(f"HTTP {response.status_code}", response=response)
                    time.sleep(wait)
                    continue
                response.raise_for_status()
                return response
            except requests.RequestException as exc:
                last = exc
                if attempt + 1 < self.policy.max_retries:
                    time.sleep(min(2**attempt + random.random(), self.policy.max_backoff_seconds))
        raise NetworkPolicyError(f"request failed after bounded retries: {type(last).__name__}: {last}")

    def _retry_wait(self, response: requests.Response, attempt: int) -> float:
        value = response.headers.get("Retry-After")
        if value:
            try:
                return min(float(value), self.policy.max_backoff_seconds)
            except ValueError:
                try:
                    wait = parsedate_to_datetime(value).timestamp() - time.time()
                    return min(max(wait, 0.0), self.policy.max_backoff_seconds)
                except Exception:
                    pass
        reset = response.headers.get("X-RateLimit-Reset")
        if reset:
            try:
                wait = float(reset) - time.time()
                return min(max(wait, 0.0), self.policy.max_backoff_seconds)
            except ValueError:
                pass
        return min(2**attempt + random.random(), self.policy.max_backoff_seconds)
