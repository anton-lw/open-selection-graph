"""Source, estimand, partition, and claim registries."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

import yaml

from .constitution import Architecture, ObservabilityGrade, SourceStatus, evaluate_estimand
from .governance import validate_claim_rows, validate_partition_manifest, validate_source_transitions

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "configs" / "observatory"


def load_yaml(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"registry must be a mapping: {path}")
    return data


@dataclass(frozen=True)
class SourceCard:
    source_id: str
    provider: str
    family: str
    official_url: str
    access_mode: str
    authentication: str
    cost_class: str
    object_types: tuple[str, ...]
    architecture: str
    provisional_grade: str
    earliest_public_stage: str
    status: str
    terms_url: str | None = None
    robots_url: str | None = None
    licences: Mapping[str, str] = field(default_factory=dict)
    denominator_methods: tuple[str, ...] = ()
    identifiers: tuple[str, ...] = ()
    update_cadence: str | None = None
    rate_limit: str | None = None
    notes: str | None = None
    release_gate_tickets: tuple[str, ...] = ()

    @classmethod
    def from_dict(cls, row: Mapping[str, Any]) -> "SourceCard":
        values = dict(row)
        for name in (
            "object_types",
            "denominator_methods",
            "identifiers",
            "release_gate_tickets",
        ):
            values[name] = tuple(values.get(name) or ())
        values["licences"] = dict(values.get("licences") or {})
        card = cls(**values)
        card.validate()
        return card

    def validate(self) -> None:
        if self.cost_class != "free":
            raise ValueError(f"{self.source_id}: OSG sources must be free")
        if self.authentication not in {"none", "free_account", "optional_free_account"}:
            raise ValueError(f"{self.source_id}: unsupported authentication class {self.authentication}")
        Architecture(self.architecture)
        ObservabilityGrade(self.provisional_grade)
        SourceStatus(self.status)
        if not self.official_url.startswith("https://"):
            raise ValueError(f"{self.source_id}: official_url must be HTTPS")
        if not self.object_types:
            raise ValueError(f"{self.source_id}: object_types cannot be empty")
        if not self.denominator_methods:
            raise ValueError(f"{self.source_id}: denominator method must be declared, even if unresolved")
        if len(self.release_gate_tickets) != len(set(self.release_gate_tickets)):
            raise ValueError(f"{self.source_id}: duplicate release gate ticket")
        for name, value in self.__dict__.items():
            if isinstance(value, str) and not value.strip():
                raise ValueError(f"{self.source_id}: {name} must use an explicit unknown, not an empty string")


def source_card_json_schema() -> dict[str, Any]:
    string_or_null = {"anyOf": [{"type": "string", "minLength": 1}, {"type": "null"}]}
    required = [
        "source_id", "provider", "family", "official_url", "access_mode", "authentication",
        "cost_class", "object_types", "architecture", "provisional_grade",
        "earliest_public_stage", "status", "denominator_methods",
    ]
    properties = {
        name: {"type": "string", "minLength": 1}
        for name in required
        if name not in {"object_types", "denominator_methods"}
    }
    properties.update({
        "object_types": {"type": "array", "minItems": 1, "items": {"type": "string", "minLength": 1}},
        "denominator_methods": {"type": "array", "minItems": 1, "items": {"type": "string", "minLength": 1}},
        "identifiers": {"type": "array", "items": {"type": "string", "minLength": 1}},
        "release_gate_tickets": {
            "type": "array",
            "items": {"type": "string", "pattern": "^[A-Z][0-9]+$"},
        },
        "licences": {"type": "object", "additionalProperties": {"type": "string", "minLength": 1}},
        "terms_url": string_or_null,
        "robots_url": string_or_null,
        "update_cadence": string_or_null,
        "rate_limit": string_or_null,
        "notes": string_or_null,
    })
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "urn:observatory:source-card:1",
        "type": "object",
        "additionalProperties": False,
        "properties": properties,
        "required": required,
    }


def source_card_rows(path: Path | None = None) -> list[dict[str, Any]]:
    """Return a stable tabular view; collections use JSON strings."""
    import json

    rows = []
    for card in source_cards(path):
        row = dict(card.__dict__)
        for name in (
            "object_types",
            "denominator_methods",
            "identifiers",
            "release_gate_tickets",
            "licences",
        ):
            row[name] = json.dumps(row[name], sort_keys=True)
        rows.append(row)
    return rows


def source_cards(path: Path | None = None) -> list[SourceCard]:
    data = load_yaml(path or CONFIG / "sources.yaml")
    cards = [SourceCard.from_dict(row) for row in data.get("sources", [])]
    ids = [card.source_id for card in cards]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate source_id in source registry")
    return cards


def source_by_id(source_id: str, path: Path | None = None) -> SourceCard:
    found = [card for card in source_cards(path) if card.source_id == source_id]
    if len(found) != 1:
        raise KeyError(source_id)
    return found[0]


def estimands(path: Path | None = None) -> list[dict[str, Any]]:
    rows = load_yaml(path or CONFIG / "estimands.yaml").get("estimands", [])
    ids = [row.get("id") for row in rows]
    if len(ids) != len(set(ids)) or None in ids:
        raise ValueError("estimand ids must be unique and non-null")
    for row in rows:
        if not row.get("required_fields") or not row.get("admissible_grades"):
            raise ValueError(f"{row['id']}: missing field/grade requirements")
    return rows


def evaluate_registered_estimand(
    estimand_id: str,
    *,
    observed_fields: Mapping[str, object] | Iterable[str],
    grade: str,
):
    matches = [row for row in estimands() if row["id"] == estimand_id]
    if len(matches) != 1:
        raise KeyError(estimand_id)
    row = matches[0]
    return evaluate_estimand(
        estimand_id=estimand_id,
        analysis_class=row["analysis_class"],
        admissible_grades=row["admissible_grades"],
        required_fields=row["required_fields"],
        observed_fields=observed_fields,
        grade=grade,
        partial_if_missing=bool(row.get("partial_if_missing")),
    )


def validate_all() -> dict[str, int]:
    cards = source_cards()
    est = estimands()
    filenames = (
        "architectures.yaml", "partitions.yaml", "source_states.yaml", "stop_rules.yaml",
        "claims.yaml", "mappings.yaml", "excluded_sources.yaml", "publication_census.yaml",
        "funding_census.yaml", "patent_source_matrix.yaml",
    )
    registries = {filename: load_yaml(CONFIG / filename) for filename in filenames}
    all_registries = [load_yaml(CONFIG / "sources.yaml"), load_yaml(CONFIG / "estimands.yaml"), *registries.values()]
    for data in all_registries:
        if data.get("constitution_version") != "0.1.0":
            raise ValueError(f"registry lacks constitution version 0.1.0: {data.get('schema')}")
    validate_partition_manifest(registries["partitions.yaml"])
    validate_claim_rows(registries["claims.yaml"].get("claims") or ())
    validate_source_transitions(
        registries["source_states.yaml"].get("transitions") or (),
        {card.source_id for card in cards},
    )
    ticket_rows = load_yaml(CONFIG / "ticket_evidence.yaml").get("tickets") or []
    ticket_ids = {str(row.get("id")) for row in ticket_rows}
    missing_ticket_gates = {
        card.source_id: sorted(set(card.release_gate_tickets) - ticket_ids)
        for card in cards
        if set(card.release_gate_tickets) - ticket_ids
    }
    if missing_ticket_gates:
        raise ValueError(f"source cards reference missing ticket gates: {missing_ticket_gates}")
    from .census import validate_censuses

    census = validate_censuses(ROOT)
    if not census["passes"]:
        raise ValueError(f"census validation failed: {census['errors']}")
    return {"source_cards": len(cards), "estimands": len(est), "registries": 12}
