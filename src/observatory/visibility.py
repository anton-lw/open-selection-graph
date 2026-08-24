"""Decision-time identity/feature visibility."""

from __future__ import annotations

from datetime import datetime
from typing import Mapping


def _dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


def identity_visible_at(row: Mapping[str, object], decision_time: str, *, audience: str) -> bool:
    if row.get("audience") not in {audience, "public", "all"}:
        return False
    at = _dt(decision_time)
    start = _dt(row.get("visible_from") if isinstance(row.get("visible_from"), str) else None)
    end = _dt(row.get("visible_to") if isinstance(row.get("visible_to"), str) else None)
    return bool(at and (start is None or start <= at) and (end is None or at <= end))


def assert_feature_available(feature_time: str | None, decision_time: str | None) -> None:
    if feature_time and decision_time and _dt(feature_time) and _dt(decision_time) and _dt(feature_time) > _dt(decision_time):
        raise ValueError("temporal leakage: feature became available after the decision")
