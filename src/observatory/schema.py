"""Canonical relational/Arrow/JSON/SQL schema for the OSG."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping


@dataclass(frozen=True)
class FieldSpec:
    name: str
    type: str = "string"
    nullable: bool = True
    description: str = ""
    provenance_class: str = "source_normalized"


def F(
    name: str,
    type: str = "string",
    nullable: bool = True,
    description: str = "",
    provenance_class: str = "source_normalized",
) -> FieldSpec:
    return FieldSpec(name, type, nullable, description, provenance_class)


COMMON = (
    F("source_id", nullable=False),
    F("source_object_id", nullable=False),
    F("provenance_event_id", nullable=False),
    F("observed_at", "timestamp", nullable=False),
    F("record_version", "int64", nullable=False),
)


TABLE_SCHEMAS: dict[str, tuple[FieldSpec, ...]] = {
    "gate": (
        F("gate_id", nullable=False), F("native_id", nullable=False), F("name", nullable=False),
        F("organization"), F("domain"), F("country"), F("architecture", nullable=False),
        F("active_from", "timestamp"), F("active_to", "timestamp"), *COMMON,
    ),
    "gate_cycle": (
        F("gate_cycle_id", nullable=False), F("gate_id", nullable=False), F("native_id", nullable=False),
        F("name"), F("track"), F("cycle_start", "timestamp"), F("cycle_end", "timestamp"),
        F("policy_version_id"), F("architecture", nullable=False), F("received_count", "int64"),
        F("observable_count", "int64"), F("evaluated_count", "int64"),
        F("selected_count", "int64"), F("status"), *COMMON,
    ),
    "policy_version": (
        F("policy_version_id", nullable=False), F("gate_id", nullable=False), F("native_id", nullable=False),
        F("effective_at", "timestamp"), F("valid_to", "timestamp"), F("criteria_json", "json"),
        F("rubric_json", "json"), F("stage_rules_json", "json"), F("quota_or_cap"),
        F("anonymity_model"), F("revision_rules"), F("policy_url"), F("content_hash"),
        F("date_confidence", "float64"), *COMMON,
    ),
    "candidate": (
        F("candidate_id", nullable=False), F("first_observed_at", "timestamp"), F("domain"),
        F("candidate_type", nullable=False), F("canonical_title"), F("status"), *COMMON,
    ),
    "candidate_version": (
        F("candidate_version_id", nullable=False), F("candidate_id", nullable=False),
        F("native_id", nullable=False), F("version_label"), F("version_number", "int64"),
        F("created_at", "timestamp"), F("modified_at", "timestamp"), F("title"), F("abstract"),
        F("content_artifact_id"), F("content_hash"), F("licence"), F("language"),
        F("authorship_visible", "bool"), F("withdrawn", "bool"), *COMMON,
    ),
    "candidate_gate_event": (
        F("candidate_gate_event_id", nullable=False), F("candidate_id", nullable=False),
        F("candidate_version_id"), F("gate_cycle_id", nullable=False), F("native_id", nullable=False),
        F("submitted_at", "timestamp"), F("earliest_observed_stage", nullable=False),
        F("final_observed_stage"), F("coverage_observation_id", nullable=False), *COMMON,
    ),
    "evaluation": (
        F("evaluation_id", nullable=False), F("candidate_version_id", nullable=False),
        F("gate_cycle_id", nullable=False), F("native_id", nullable=False), F("evaluation_type", nullable=False),
        F("evaluator_role"), F("evaluator_public_id"), F("evaluator_protected_id"),
        F("anonymous", "bool"), F("official", "bool"), F("criterion_native"),
        F("criterion_normalized"), F("criterion_value"), F("criterion_value_numeric", "float64"),
        F("scale_json", "json"), F("confidence_value", "float64"), F("text_artifact_id"),
        F("created_at", "timestamp"), F("forum_native_id"), F("invitation_native"),
        F("readers_json", "json"), F("signatures_json", "json"), F("reply_to_native_id"),
        *COMMON,
    ),
    "decision_event": (
        F("decision_event_id", nullable=False), F("candidate_version_id", nullable=False),
        F("gate_cycle_id", nullable=False), F("native_id", nullable=False), F("stage_native", nullable=False),
        F("stage_normalized", nullable=False), F("outcome_native"), F("outcome_normalized"),
        F("tier_or_band"), F("reason"), F("deciding_body"), F("decided_at", "timestamp"),
        F("policy_version_id"), *COMMON,
    ),
    "content_artifact": (
        F("content_artifact_id", nullable=False), F("object_type", nullable=False), F("media_type"),
        F("byte_hash"), F("normalized_text_hash"), F("source_url"), F("local_pointer"),
        F("licence"), F("release_class", nullable=False), F("size_bytes", "int64"),
        F("language"), F("parser_version"), *COMMON,
    ),
    "lineage_edge": (
        F("lineage_edge_id", nullable=False), F("source_candidate_id"), F("source_version_id"),
        F("target_candidate_id"), F("target_version_id"), F("relation_type", nullable=False),
        F("declared", "bool", nullable=False), F("confidence", "float64"), F("linkage_tier"),
        F("method_version"), F("evidence_json", "json"), *COMMON,
    ),
    "capacity_observation": (
        F("capacity_observation_id", nullable=False), F("gate_cycle_id", nullable=False),
        F("period_start", "timestamp"), F("period_end", "timestamp"), F("submitted_count", "int64"),
        F("review_count", "int64"), F("evaluator_count", "int64"), F("panel_size", "int64"),
        F("assignments_per_evaluator", "float64"), F("turnaround_days", "float64"),
        F("missing_review_share", "float64"), F("proxy_definition"), F("measurement_caveat"), *COMMON,
    ),
    "coverage_observation": (
        F("coverage_observation_id", nullable=False), F("gate_cycle_id", nullable=False),
        F("object_type", nullable=False), F("earliest_public_stage", nullable=False),
        F("observability_grade", nullable=False), F("expected_count", "int64"),
        F("found_count", "int64"), F("coverage_ratio", "float64"), F("expected_count_method"),
        F("query_or_invitation"), F("known_hidden_stages", "list_string"),
        F("known_exclusions", "list_string"), F("missing_reason"), F("audit_status"),
        F("valid_from", "timestamp"), F("valid_to", "timestamp"), *COMMON,
    ),
    "downstream_outcome": (
        F("downstream_outcome_id", nullable=False), F("candidate_id", nullable=False),
        F("candidate_version_id"), F("outcome_type", nullable=False), F("native_id"), F("doi"),
        F("venue"), F("occurred_at", "timestamp"), F("window_years", "int64"),
        F("value_numeric", "float64"), F("value_json", "json"), F("censoring_date", "timestamp"), *COMMON,
    ),
    "source_object": (
        F("source_object_id", nullable=False), F("source_id", nullable=False), F("native_id", nullable=False),
        F("object_type", nullable=False), F("source_url"), F("created_at", "timestamp"),
        F("modified_at", "timestamp"), F("retrieved_at", "timestamp", nullable=False),
        F("deleted_at", "timestamp"), F("byte_hash"), F("raw_pointer"), F("http_status", "int64"),
        F("etag"), F("last_modified"), F("licence"), F("release_class", nullable=False),
        F("status", nullable=False),
    ),
    "provenance_event": (
        F("provenance_event_id", nullable=False), F("source_id", nullable=False),
        F("source_object_id", nullable=False), F("event_type", nullable=False),
        F("occurred_at", "timestamp", nullable=False), F("parser_name"), F("parser_version"),
        F("code_hash"), F("input_hash"), F("output_hash"), F("parameters_json", "json"),
        F("parent_event_ids", "list_string"), F("success", "bool", nullable=False), F("error"),
    ),
    "field_provenance": (
        F("field_provenance_id", nullable=False), F("table_name", nullable=False),
        F("record_id", nullable=False), F("field_name", nullable=False),
        F("source_object_id", nullable=False), F("provenance_event_id", nullable=False),
        F("source_selector"), F("confidence", "float64"), F("override_reason"),
        F("observed_at", "timestamp", nullable=False),
    ),
    "identifier_alias": (
        F("identifier_alias_id", nullable=False), F("entity_kind", nullable=False),
        F("entity_id", nullable=False), F("scheme", nullable=False), F("value", nullable=False),
        F("canonical_value"), F("relation"), F("confidence", "float64"),
        F("conflict_status"), *COMMON,
    ),
    "reference_edge": (
        F("reference_edge_id", nullable=False), F("citing_version_id", nullable=False),
        F("reference_position", "int64"), F("cited_candidate_id"), F("cited_version_id"),
        F("cited_identifier"), F("raw_citation_hash"), F("match_method"),
        F("confidence", "float64"), F("time_valid", "bool"), *COMMON,
    ),
    "field_assignment": (
        F("field_assignment_id", nullable=False), F("entity_kind", nullable=False),
        F("entity_id", nullable=False), F("taxonomy", nullable=False), F("native_label"),
        F("normalized_label"), F("score", "float64"), F("mapping_version"), *COMMON,
    ),
    "identity_visibility": (
        F("identity_visibility_id", nullable=False), F("candidate_version_id", nullable=False),
        F("identity_kind", nullable=False), F("visible_from", "timestamp"), F("visible_to", "timestamp"),
        F("audience", nullable=False), F("source_evidence"), *COMMON,
    ),
    "authorship_observation": (
        F("authorship_observation_id", nullable=False),
        F("candidate_version_id", nullable=False), F("position", "int64"),
        F("public_name"), F("public_identifier"), F("protected_person_id"),
        F("anonymous_label"), F("visible_from", "timestamp"), F("visible_to", "timestamp"),
        F("audience", nullable=False), F("source_evidence"), *COMMON,
    ),
    "language_derivative": (
        F("language_derivative_id", nullable=False), F("content_artifact_id", nullable=False),
        F("source_language"), F("target_language"), F("method"), F("model_version"),
        F("derived_artifact_id"), F("validated", "bool"), *COMMON,
    ),
}


PRIMARY_KEYS: dict[str, str] = {
    table: fields[0].name for table, fields in TABLE_SCHEMAS.items()
}


_PYARROW_TYPES = {
    "string": "string", "json": "large_string", "int64": "int64", "float64": "float64",
    "bool": "bool", "timestamp": "timestamp[us, tz=UTC]", "list_string": "list<string>",
}
_SQL_TYPES = {
    "string": "VARCHAR", "json": "JSON", "int64": "BIGINT", "float64": "DOUBLE",
    "bool": "BOOLEAN", "timestamp": "TIMESTAMPTZ", "list_string": "VARCHAR[]",
}
_JSON_TYPES = {
    "string": "string", "json": "string", "int64": "integer", "float64": "number",
    "bool": "boolean", "timestamp": "string", "list_string": "array",
}


def validate_record(table: str, record: Mapping[str, Any], *, reject_unknown: bool = True) -> None:
    if table not in TABLE_SCHEMAS:
        raise KeyError(f"unknown OSG table: {table}")
    fields = {field.name: field for field in TABLE_SCHEMAS[table]}
    missing = [name for name, field in fields.items() if not field.nullable and record.get(name) is None]
    if missing:
        raise ValueError(f"{table} record missing non-null fields: {missing}")
    unknown = sorted(set(record) - set(fields))
    if reject_unknown and unknown:
        raise ValueError(f"{table} record has unknown fields: {unknown}")


def validate_records(table: str, records: Iterable[Mapping[str, Any]]) -> None:
    seen: set[object] = set()
    key = PRIMARY_KEYS[table]
    for row in records:
        validate_record(table, row)
        value = row.get(key)
        if value in seen:
            raise ValueError(f"duplicate {table}.{key}: {value}")
        seen.add(value)


def pyarrow_schema(table: str):
    try:
        import pyarrow as pa
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise RuntimeError("pyarrow is required for OSG Parquet storage") from exc

    def dtype(name: str):
        return {
            "string": pa.string(), "json": pa.large_string(), "int64": pa.int64(),
            "float64": pa.float64(), "bool": pa.bool_(),
            "timestamp": pa.timestamp("us", tz="UTC"), "list_string": pa.list_(pa.string()),
        }[name]

    return pa.schema([pa.field(f.name, dtype(f.type), nullable=f.nullable) for f in TABLE_SCHEMAS[table]])


def json_schema(table: str) -> dict[str, Any]:
    properties: dict[str, Any] = {}
    required: list[str] = []
    for field in TABLE_SCHEMAS[table]:
        item: dict[str, Any] = {"type": _JSON_TYPES[field.type]}
        if field.type == "timestamp":
            item["format"] = "date-time"
        if field.type == "list_string":
            item["items"] = {"type": "string"}
        if field.description:
            item["description"] = field.description
        if field.nullable:
            item = {"anyOf": [item, {"type": "null"}]}
        else:
            required.append(field.name)
        properties[field.name] = item
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": f"https://example.invalid/observatory/{table}.schema.json",
        "title": f"OSG {table}",
        "type": "object",
        "additionalProperties": False,
        "properties": properties,
        "required": required,
    }


def sql_ddl(table: str) -> str:
    columns = []
    for field in TABLE_SCHEMAS[table]:
        null = "" if field.nullable else " NOT NULL"
        columns.append(f'  "{field.name}" {_SQL_TYPES[field.type]}{null}')
    columns.append(f'  PRIMARY KEY ("{PRIMARY_KEYS[table]}")')
    return f'CREATE TABLE "{table}" (\n' + ",\n".join(columns) + "\n);"


def write_schema_artifacts(root: Path) -> list[Path]:
    root.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for table in sorted(TABLE_SCHEMAS):
        path = root / f"{table}.schema.json"
        path.write_text(json.dumps(json_schema(table), indent=2, sort_keys=True) + "\n")
        written.append(path)
    ddl = root / "observatory.sql"
    ddl.write_text("\n\n".join(sql_ddl(t) for t in sorted(TABLE_SCHEMAS)) + "\n")
    written.append(ddl)
    catalog = root / "catalog.json"
    catalog.write_text(json.dumps({
        "schema": "observatory.schema-catalog/1",
        "tables": {t: [asdict(f) for f in fields] for t, fields in sorted(TABLE_SCHEMAS.items())},
        "primary_keys": PRIMARY_KEYS,
        "arrow_types": _PYARROW_TYPES,
    }, indent=2, sort_keys=True) + "\n")
    written.append(catalog)
    from .registry import source_card_json_schema, source_card_rows

    source_schema = root / "source_card.schema.json"
    source_schema.write_text(json.dumps(source_card_json_schema(), indent=2, sort_keys=True) + "\n")
    written.append(source_schema)
    source_table = root / "source_cards.jsonl"
    source_table.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in source_card_rows()))
    written.append(source_table)
    return written
