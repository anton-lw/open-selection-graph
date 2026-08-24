"""Native-preserving scientific-field mappings."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping


@dataclass(frozen=True)
class FieldMapping:
    native_taxonomy: str
    native_label: str
    normalized_label: str | None
    score: float
    mapping_version: str


DEFAULT_CROSSWALK: dict[str, tuple[str, ...]] = {
    "computer science": ("cs", "machine learning", "artificial intelligence", "informatics"),
    "physics": ("physics", "hep", "quantum", "astrophysics", "condensed matter"),
    "mathematics": ("math", "mathematics", "statistics"),
    "engineering": ("engineering", "electrical", "mechanical", "chemical engineering"),
    "biology and medicine": ("biology", "biomedical", "medicine", "health", "life science"),
    "earth and environmental sciences": ("earth", "climate", "geoscience", "environment", "atmospheric"),
    "social sciences": ("social science", "economics", "psychology", "sociology", "political"),
}


def map_native_field(
    native_taxonomy: str,
    native_label: str,
    *,
    crosswalk: Mapping[str, Iterable[str]] = DEFAULT_CROSSWALK,
    version: str = "observatory-crosswalk/1",
) -> FieldMapping:
    low = native_label.lower()
    matches = [normalized for normalized, needles in crosswalk.items() if any(needle in low for needle in needles)]
    return FieldMapping(
        native_taxonomy=native_taxonomy,
        native_label=native_label,
        normalized_label=matches[0] if len(matches) == 1 else None,
        score=1.0 if len(matches) == 1 else 0.0,
        mapping_version=version,
    )


def map_native_field_candidates(
    native_taxonomy: str,
    native_label: str,
    *,
    crosswalk: Mapping[str, Iterable[str]] = DEFAULT_CROSSWALK,
    version: str = "observatory-crosswalk/1",
) -> tuple[FieldMapping, ...]:
    """Return all matching broad fields with normalized uncertainty weights."""
    low = native_label.lower()
    labels = [
        normalized for normalized, needles in crosswalk.items()
        if any(str(needle).lower() in low for needle in needles)
    ]
    if not labels:
        return (FieldMapping(native_taxonomy, native_label, None, 0.0, version),)
    weight = 1.0 / len(labels)
    return tuple(
        FieldMapping(native_taxonomy, native_label, label, weight, version)
        for label in labels
    )


def comparison_eligible(
    mappings: Iterable[FieldMapping], *, minimum_score: float = 0.8
) -> tuple[FieldMapping, ...]:
    """Filter broad-field comparisons while leaving native mappings intact."""
    return tuple(
        row for row in mappings
        if row.normalized_label is not None and row.score >= minimum_score
    )
