"""Adapter normalization helpers."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

from ..ids import canonical_doi, stable_id


def epoch_ms(value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OSError):
        return None


def iso_datetime(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if re.fullmatch(r"\d{4}", text):
        return f"{text}-01-01T00:00:00+00:00"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return f"{text}T00:00:00+00:00"
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()
    except ValueError:
        try:
            return parsedate_to_datetime(text).astimezone(timezone.utc).isoformat()
        except (TypeError, ValueError):
            return None


def json_text(value: Any) -> str | None:
    return json.dumps(value, sort_keys=True) if value is not None else None


def candidate_from_doi(source_id: str, doi: str) -> tuple[str, str, str]:
    normalized = canonical_doi(doi)
    if not normalized:
        raise ValueError(f"invalid DOI: {doi}")
    candidate_id = stable_id("candidate", "doi", normalized)
    version_id = stable_id("candidate_version", "doi", normalized)
    alias_id = stable_id("identifier_alias", source_id, f"candidate|doi|{normalized}")
    return candidate_id, version_id, alias_id


def year_quarter(stamp: str | None) -> tuple[int, int]:
    parsed = datetime.fromisoformat(stamp.replace("Z", "+00:00")) if stamp else datetime(1970, 1, 1, tzinfo=timezone.utc)
    return parsed.year, (parsed.month - 1) // 3 + 1
