"""Stable, namespaced identifiers and canonical identifier normalization."""

from __future__ import annotations

import hashlib
import re
import unicodedata
import uuid
from urllib.parse import unquote, urlparse

OBSERVATORY_NAMESPACE = uuid.UUID("7087e635-619a-5b41-99bc-c8ab98842348")


def normalize_text(value: object) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    return " ".join(text.strip().split())


def canonical_doi(value: object) -> str | None:
    """Normalize DOI URLs/prefixes without guessing malformed identifiers."""

    text = unquote(normalize_text(value)).lower()
    if not text:
        return None
    if "://" in text:
        parsed = urlparse(text)
        if parsed.netloc in {"doi.org", "dx.doi.org", "www.doi.org"}:
            text = parsed.path.lstrip("/")
    text = re.sub(r"^(?:doi\s*:\s*|https?://(?:dx\.)?doi\.org/)", "", text)
    text = text.rstrip(" .;,)")
    return text if re.match(r"^10\.\d{4,9}/\S+$", text) else None


def stable_id(kind: str, source: str, native_id: object) -> str:
    """Generate a deterministic UUIDv5 while keeping a human-readable kind."""

    parts = [normalize_text(x).lower() for x in (kind, source, native_id)]
    if not all(parts):
        raise ValueError("kind, source, and native_id must all be non-empty")
    return f"obs:{parts[0]}:{uuid.uuid5(OBSERVATORY_NAMESPACE, '|'.join(parts))}"


def content_hash(data: bytes | str) -> str:
    payload = data.encode("utf-8") if isinstance(data, str) else data
    return hashlib.sha256(payload).hexdigest()
