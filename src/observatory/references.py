"""Reference extraction reconciliation: identifiers first, bibliographic match second."""

from __future__ import annotations

import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Iterable, Mapping

from .ids import canonical_doi, content_hash

_DOI = re.compile(r"10\.\d{4,9}/[-._;()/:A-Z0-9]+", re.I)


@dataclass(frozen=True)
class ReferenceMatch:
    cited_identifier: str | None
    candidate_id: str | None
    method: str
    confidence: float
    raw_hash: str


def extract_doi(text: str) -> str | None:
    for match in _DOI.findall(text or ""):
        doi = canonical_doi(match)
        if doi:
            return doi
    return None


def normalize_bibliography_text(text: str) -> str:
    text = re.sub(r"https?://\S+", " ", text or "", flags=re.I)
    text = re.sub(r"[^\w\s]", " ", text.lower())
    return " ".join(text.split())


def match_reference(
    reference: Mapping[str, object],
    *,
    by_doi: Mapping[str, str],
    candidates: Iterable[Mapping[str, object]] = (),
    threshold: float = 0.94,
    ambiguity_margin: float = 0.03,
) -> ReferenceMatch:
    raw = str(reference.get("text") or reference.get("raw") or "")
    doi = canonical_doi(reference.get("doi")) or extract_doi(raw)
    if doi and doi in by_doi:
        return ReferenceMatch(doi, by_doi[doi], "structured_or_text_doi", 1.0, content_hash(raw))
    title = normalize_bibliography_text(str(reference.get("title") or raw))
    year = str(reference.get("year") or "")
    scored: list[tuple[float, str]] = []
    for row in candidates:
        candidate_title = normalize_bibliography_text(str(row.get("title") or ""))
        if not candidate_title:
            continue
        if year and row.get("year") and year != str(row.get("year")):
            continue
        score = SequenceMatcher(None, title, candidate_title).ratio()
        scored.append((score, str(row["candidate_id"])))
    scored.sort(reverse=True)
    best = scored[0] if scored else None
    ambiguous = bool(
        best and len(scored) > 1 and scored[1][1] != best[1]
        and best[0] - scored[1][0] < ambiguity_margin
    )
    if best and best[0] >= threshold and not ambiguous:
        return ReferenceMatch(None, best[1], "bibliographic_title_year", best[0], content_hash(raw))
    if best and best[0] >= threshold and ambiguous:
        return ReferenceMatch(doi, None, "ambiguous_bibliographic_hash", 0.0, content_hash(raw))
    return ReferenceMatch(doi, None, "unresolved_text_hash", 0.0, content_hash(raw))


def benchmark_reference_matching(
    rows: Iterable[Mapping[str, object]],
    *,
    by_doi: Mapping[str, str],
    candidates: Iterable[Mapping[str, object]] = (),
) -> dict[str, object]:
    """Score matches against structured-source gold without forcing misses."""
    candidate_rows = tuple(candidates)
    total = predicted = correct = 0
    methods: dict[str, int] = {}
    for row in rows:
        reference = row.get("reference")
        if not isinstance(reference, Mapping):
            raise ValueError("benchmark row requires a reference mapping")
        expected = row.get("expected_candidate_id")
        match = match_reference(reference, by_doi=by_doi, candidates=candidate_rows)
        total += 1
        methods[match.method] = methods.get(match.method, 0) + 1
        if match.candidate_id is not None:
            predicted += 1
            correct += match.candidate_id == expected
    return {
        "schema": "observatory.reference-benchmark/1", "total": total,
        "predicted": predicted, "correct": correct,
        "precision": correct / predicted if predicted else None,
        "recall": correct / total if total else None,
        "unresolved": total - predicted, "methods": methods,
    }
