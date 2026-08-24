"""Typed identifier aliases, conflicts, and auditable entity resolution."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable

from .ids import canonical_doi


def canonical_identifier(scheme: str, value: object) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    scheme = scheme.lower()
    if scheme == "doi":
        return canonical_doi(text)
    if scheme == "arxiv":
        text = re.sub(r"^(?:arxiv:|https?://arxiv\.org/(?:abs|pdf)/)", "", text, flags=re.I)
        return re.sub(r"v\d+(?:\.pdf)?$", "", text).rstrip(".pdf")
    if scheme == "orcid":
        text = re.sub(r"^https?://orcid\.org/", "", text, flags=re.I).upper()
        return text if re.fullmatch(r"\d{4}-\d{4}-\d{4}-\d{3}[\dX]", text) else None
    if scheme == "ror":
        text = re.sub(r"^https?://ror\.org/", "", text, flags=re.I).lower()
        return text if re.fullmatch(r"0[0-9a-hj-km-np-tv-z]{8}", text) else None
    if scheme in {"pmid", "pmcid"}:
        text = text.upper().removeprefix(scheme.upper())
        if scheme == "pmcid" and not text.startswith("PMC"):
            text = "PMC" + text
        return text if (text.removeprefix("PMC").isdigit()) else None
    if scheme == "openalex":
        text = text.rsplit("/", 1)[-1].upper()
        return text if re.fullmatch(r"[WASITKPF]\d+", text) else None
    return " ".join(text.split())


@dataclass(frozen=True)
class Alias:
    entity_id: str
    scheme: str
    value: str
    confidence: float = 1.0
    evidence: str | None = None


@dataclass(frozen=True)
class Resolution:
    scheme: str
    canonical_value: str | None
    entity_ids: tuple[str, ...]
    status: str
    evidence_paths: tuple[tuple[str, ...], ...]


class AliasGraph:
    def __init__(self):
        self._by_alias: dict[tuple[str, str], set[str]] = {}
        self._aliases: list[Alias] = []

    def add(self, alias: Alias) -> str:
        canonical = canonical_identifier(alias.scheme, alias.value)
        if canonical is None:
            return "invalid"
        key = (alias.scheme.lower(), canonical)
        self._by_alias.setdefault(key, set()).add(alias.entity_id)
        self._aliases.append(Alias(alias.entity_id, alias.scheme.lower(), canonical, alias.confidence, alias.evidence))
        return "conflict" if len(self._by_alias[key]) > 1 else "ok"

    def resolve(self, scheme: str, value: object) -> set[str]:
        canonical = canonical_identifier(scheme, value)
        return set() if canonical is None else set(self._by_alias.get((scheme.lower(), canonical), set()))

    def conflicts(self) -> dict[tuple[str, str], set[str]]:
        return {key: set(values) for key, values in self._by_alias.items() if len(values) > 1}

    def aliases(self, entity_id: str | None = None) -> list[Alias]:
        return [a for a in self._aliases if entity_id is None or a.entity_id == entity_id]

    def explain(self, scheme: str, value: object) -> Resolution:
        """Return a non-destructive, evidence-bearing alias resolution.

        A one-to-many alias is quarantined instead of being resolved with an
        arbitrary winner.  Each path is alias -> evidence object -> entity;
        a missing evidence object is explicit rather than silently invented.
        """
        canonical = canonical_identifier(scheme, value)
        if canonical is None:
            return Resolution(scheme.lower(), None, (), "invalid", ())
        matched = [
            alias for alias in self._aliases
            if alias.scheme == scheme.lower() and alias.value == canonical
        ]
        entity_ids = tuple(sorted({alias.entity_id for alias in matched}))
        status = "unresolved" if not entity_ids else ("resolved" if len(entity_ids) == 1 else "quarantined_conflict")
        paths = tuple(
            (f"{alias.scheme}:{alias.value}", alias.evidence or "evidence:unrecorded", alias.entity_id)
            for alias in sorted(matched, key=lambda row: (row.entity_id, row.evidence or ""))
        )
        return Resolution(scheme.lower(), canonical, entity_ids, status, paths)

    def require_unique(self, scheme: str, value: object) -> str | None:
        resolution = self.explain(scheme, value)
        if resolution.status == "quarantined_conflict":
            raise ValueError(
                f"conflicting {resolution.scheme} alias {resolution.canonical_value}: "
                f"{', '.join(resolution.entity_ids)}"
            )
        return resolution.entity_ids[0] if resolution.entity_ids else None

    @classmethod
    def from_rows(cls, rows: Iterable[dict]) -> "AliasGraph":
        graph = cls()
        for row in rows:
            graph.add(Alias(
                entity_id=row["entity_id"], scheme=row["scheme"], value=row["value"],
                confidence=float(row.get("confidence") or 0.0), evidence=row.get("source_object_id"),
            ))
        return graph
