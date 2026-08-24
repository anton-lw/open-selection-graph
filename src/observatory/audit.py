"""Configuration, cost, secret, and source-policy audits."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path
from urllib.parse import urlparse

from .ids import content_hash
from .limitations import audit_pointer_rebuild_registry
from .operations import network_allowed_hosts, operations_audit
from .registry import CONFIG, load_yaml, source_cards, validate_all

SECRET_PATTERNS = (
    re.compile(r"\bak-(?=[A-Za-z0-9_-]{20,}\b)(?=[A-Za-z0-9_-]*[A-Z0-9])[A-Za-z0-9_-]+"),
    re.compile(r"\bas-(?=[A-Za-z0-9_-]{20,}\b)(?=[A-Za-z0-9_-]*[A-Z0-9])[A-Za-z0-9_-]+"),
    re.compile(r"(?i)(?:password|token_secret|authorization)\s*[:=]\s*['\"][^'\"]{8,}"),
    re.compile(r"(?i)authorization\s*:\s*bearer\s+[A-Za-z0-9._~-]{8,}"),
    re.compile(
        r"(?i)(?:username|login(?:_id)?|email)\s*[:=]\s*['\"]?"
        r"(?![^\s'\"]+@example\.(?:com|org))[^\s'\"]+@[^\s'\"]+"
    ),
)

_TEXT_SUFFIXES = {
    "", ".env", ".ini", ".ipynb", ".json", ".jsonl", ".md", ".py", ".sh",
    ".toml", ".txt", ".yaml", ".yml",
}
_SKIP_PARTS = {".git", ".venv", "__pycache__", "node_modules", "raw", "normalized", "staging"}
_URL_PATTERN = re.compile(r"https?://[^\s\"'<>)}]+")
_DIRECT_HTTP_PATTERN = re.compile(r"\b(?:requests|httpx)\.(?:get|post|put|patch|delete)\s*\(")


def prohibited_names() -> set[str]:
    data = load_yaml(CONFIG / "excluded_sources.yaml")
    return {str(row["name"]).lower() for row in data.get("sources", [])}


def approved_hosts() -> set[str]:
    hosts = set()
    for card in source_cards():
        hosts.add((urlparse(card.official_url).hostname or "").lower())
    # Documented API/bulk hosts may differ from the provider landing page.
    hosts.update({
        "api.crossref.org", "api.openalex.org", "export.arxiv.org", "oai-pmh.copernicus.org",
        "www.ebi.ac.uk", "api.openreview.net", "api2.openreview.net", "openreview.net",
        "europepmc.org", "data.uspto.gov", "openalex.s3.amazonaws.com",
    })
    return hosts


def released_public_contacts(root: Path) -> set[str]:
    """Read author contacts that publication metadata marks for release."""
    path = root / "configs" / "observatory" / "publication.yaml"
    if not path.is_file():
        return set()
    creator = (load_yaml(path).get("creator") or {})
    if creator.get("contact_released") is True and creator.get("email"):
        return {str(creator["email"])}
    return set()


def scan_secrets(
    paths: list[Path], *, released_contacts: set[str] | None = None
) -> list[dict[str, object]]:
    """Return possible credential findings from text files.

    ``released_contacts`` is limited to contact addresses that a publication
    configuration explicitly marks for public release. Replacing those exact
    values with the scanner's reserved example address keeps all other secret
    patterns active on the same line.
    """
    public_contacts = {value.strip().lower() for value in (released_contacts or set())}
    findings = []
    for path in paths:
        if not path.is_file() or path.suffix in {".pdf", ".png", ".jpg", ".parquet", ".gz"}:
            continue
        try:
            text = path.read_text(errors="ignore")
        except OSError:
            continue
        for lineno, line in enumerate(text.splitlines(), 1):
            scan_line = line
            for contact in public_contacts:
                scan_line = re.sub(
                    re.escape(contact),
                    "public@example.org",
                    scan_line,
                    flags=re.IGNORECASE,
                )
            if any(pattern.search(scan_line) for pattern in SECRET_PATTERNS):
                findings.append({"path": str(path), "line": lineno, "kind": "possible_secret"})
    return findings


def _audit_paths(root: Path) -> list[Path]:
    paths = []
    for base in (
        root / "src",
        root / "configs",
        root / "docs",
        root / "scripts",
        root / "results" / "observatory",
    ):
        if not base.exists():
            continue
        for path in base.rglob("*"):
            if (
                path.is_file()
                and not (_SKIP_PARTS & set(path.parts))
                and path.suffix.lower() in _TEXT_SUFFIXES
                and path.stat().st_size <= 10_000_000
            ):
                paths.append(path)
    paths.extend(path for path in root.glob("modal*.py") if path.is_file())
    paths.extend(path for path in root.glob("*.env") if path.is_file())
    return sorted(set(paths))


def scan_git_secret_surfaces(root: Path) -> dict[str, object]:
    """Scan staged content and history without returning matched secret text."""
    staged_findings = []
    try:
        staged = subprocess.run(
            ["git", "diff", "--cached", "--unified=0", "--no-ext-diff"],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        ).stdout
        for lineno, line in enumerate(staged.splitlines(), 1):
            if line.startswith("+") and not line.startswith("+++") and any(
                pattern.search(line) for pattern in SECRET_PATTERNS
            ):
                staged_findings.append({"surface": "staged_diff", "line": lineno})
    except OSError:
        staged_findings.append({"surface": "staged_diff", "error": "git unavailable"})
    history_findings = []
    history_pattern = (
        r"(ak-[A-Za-z0-9_-]{20,}|as-[A-Za-z0-9_-]{20,}|"
        r"OPENREVIEW_PASSWORD=.{8,}|"
        r"Authorization:[[:space:]]*Bearer[[:space:]]+[A-Za-z0-9._~-]{8,})"
    )
    try:
        process = subprocess.run(
            ["git", "log", "--all", "--format=%H", "-G", history_pattern, "--"],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
            timeout=45,
        )
        commits = sorted(
            set(line for line in process.stdout.splitlines() if re.fullmatch(r"[0-9a-f]{40}", line))
        )
        history_findings.extend(
            {"surface": "git_history", "commit": commit, "pattern_class": "high_confidence_secret"}
            for commit in commits
        )
    except subprocess.TimeoutExpired:
        history_findings.append({"surface": "git_history", "error": "scan timed out"})
    except OSError:
        history_findings.append({"surface": "git_history", "error": "git unavailable"})
    return {
        "staged_findings": staged_findings,
        "history_findings": history_findings,
        "passes": not staged_findings and not history_findings,
    }


def audit_historical_secret_quarantine(
    root: Path, git_secrets: dict[str, object]
) -> dict[str, object]:
    """Accept a redacted, exact history quarantine without erasing shared history.

    The repository contains projects outside the OSG scope, so rewriting
    shared Git history would be destructive and unauthorized. A quarantine is
    valid only when current/staged OSG surfaces are clean, every live
    historical finding is represented by a one-way commit fingerprint, public
    rebuilds need no credentials, and frozen authenticated refreshes are off.
    """
    import json

    path = root / "results" / "observatory" / "r5" / "credential_history_quarantine.json"
    if not path.is_file():
        return {"configured": False, "passes": False, "reason": "missing quarantine report"}
    try:
        body = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {"configured": True, "passes": False, "reason": "invalid quarantine report"}
    expected = sorted(
        content_hash(str(row.get("commit", "")))[:16]
        for row in git_secrets.get("history_findings", [])
        if row.get("commit")
    )
    declared = sorted(
        str(row.get("commit_fingerprint"))
        for row in body.get("historical_findings_redacted", [])
        if row.get("commit_fingerprint")
    )
    staged = git_secrets.get("staged_findings", [])
    passes = bool(
        body.get("passes_public_release_surface")
        and body.get("history_is_not_release_input")
        and body.get("public_rebuild_requires_credentials") is False
        and body.get("authenticated_refresh_jobs_enabled_in_frozen_release") is False
        and not body.get("current_observatory_findings")
        and not staged
        and declared == expected
    )
    return {
        "configured": True,
        "path": str(path.relative_to(root)),
        "historical_finding_count": len(expected),
        "fingerprints_match_live_scan": declared == expected,
        "passes": passes,
    }


def audit_outbound_configuration(root: Path) -> dict[str, object]:
    policy = load_yaml(CONFIG / "network_policy.yaml")
    allowed = network_allowed_hosts()
    static = {str(host).lower() for host in policy.get("static_namespaces") or []}
    non_data = {str(host).lower() for host in (policy.get("non_data_hosts") or {})}
    direct_http_modules = {
        str(path) for path in (policy.get("direct_http_modules") or {})
    }
    files = list((root / "src" / "observatory").rglob("*.py")) + list(root.glob("modal*.py"))
    unknown_hosts = []
    direct_http_bypasses = []
    prohibited_environment = []
    markers = tuple(str(value).upper() for value in policy.get("prohibited_environment_markers") or [])
    for path in files:
        text = path.read_text(errors="ignore")
        for lineno, line in enumerate(text.splitlines(), 1):
            for value in _URL_PATTERN.findall(line):
                try:
                    host = (urlparse(value.rstrip(".,;")).hostname or "").lower()
                except ValueError:
                    continue
                if (
                    host
                    and "{" not in host
                    and host not in allowed
                    and host not in static
                    and host not in non_data
                ):
                    unknown_hosts.append({"path": str(path.relative_to(root)), "line": lineno, "host": host})
            if (
                _DIRECT_HTTP_PATTERN.search(line)
                and path.name != "http.py"
                and str(path.relative_to(root)) not in direct_http_modules
                and path.name not in direct_http_modules
            ):
                direct_http_bypasses.append({"path": str(path.relative_to(root)), "line": lineno})
            for marker in markers:
                # These are environment/service identifiers, not ordinary
                # English words (for example a model may legitimately have
                # several lower-case ``dimensions``). Require the canonical
                # upper-case identifier as a token to avoid semantic matches.
                if re.search(rf"\b{re.escape(marker)}\b", line) and not line.lstrip().startswith("#"):
                    prohibited_environment.append(
                        {"path": str(path.relative_to(root)), "line": lineno, "marker": marker}
                    )
    return {
        "unknown_hosts": unknown_hosts,
        "direct_http_bypasses": direct_http_bypasses,
        "prohibited_environment_configuration": prohibited_environment,
        "passes": not unknown_hosts and not direct_http_bypasses and not prohibited_environment,
    }


def audit_configuration(root: Path) -> dict[str, object]:
    registry = validate_all()
    observatory_files = _audit_paths(root)
    secrets = scan_secrets(
        observatory_files,
        released_contacts=released_public_contacts(root),
    )
    git_secrets = scan_git_secret_surfaces(root)
    quarantine = audit_historical_secret_quarantine(root, git_secrets)
    effective_git_secret_pass = bool(git_secrets["passes"] or quarantine["passes"])
    operations = operations_audit(root)
    outbound = audit_outbound_configuration(root)
    pointer_registry = audit_pointer_rebuild_registry(root)
    paid = [card.source_id for card in source_cards() if card.cost_class != "free"]
    prohibited = prohibited_names()
    dependency_files = [
        root / "pyproject.toml", root / "requirements.txt", root / "environment.yml",
    ]
    prohibited_dependencies = []
    for path in dependency_files:
        if not path.exists():
            continue
        for lineno, line in enumerate(path.read_text(errors="ignore").splitlines(), 1):
            low = line.lower()
            for name in prohibited:
                if name in low and not line.lstrip().startswith("#"):
                    prohibited_dependencies.append({"path": str(path), "line": lineno, "service": name})
    return {
        "passes": (
            not secrets
            and effective_git_secret_pass
            and not paid
            and not prohibited_dependencies
            and operations["passes"]
            and outbound["passes"]
            and pointer_registry["passes"]
        ),
        "registries": registry,
        "possible_secrets": secrets,
        "git_secret_surfaces": git_secrets,
        "historical_secret_quarantine": quarantine,
        "nonfree_sources": paid,
        "prohibited_dependencies": prohibited_dependencies,
        "operations": operations,
        "outbound_configuration": outbound,
        "pointer_rebuild_registry": pointer_registry,
        "approved_hosts": sorted(approved_hosts()),
    }


def audit_no_paid_api_policy(root: Path) -> dict[str, object]:
    """CI-sized Q1 gate, independent of the slower Git-history secret audit."""
    operations = operations_audit(root)
    outbound = audit_outbound_configuration(root)
    prohibited = prohibited_names()
    dependency_files = [root / "pyproject.toml", root / "requirements.txt", root / "environment.yml"]
    prohibited_dependencies = []
    for path in dependency_files:
        if not path.exists():
            continue
        for lineno, line in enumerate(path.read_text(errors="ignore").splitlines(), 1):
            for name in prohibited:
                if name in line.lower() and not line.lstrip().startswith("#"):
                    prohibited_dependencies.append(
                        {"path": str(path.relative_to(root)), "line": lineno, "service": name}
                    )
    result = {
        "schema": "observatory.no-paid-api-audit/1",
        "operations": operations,
        "outbound_configuration": outbound,
        "prohibited_dependencies": prohibited_dependencies,
    }
    result["passes"] = operations["passes"] and outbound["passes"] and not prohibited_dependencies
    return result
