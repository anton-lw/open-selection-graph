"""Semantic versioning and fail-closed, licence-separated release packages."""

from __future__ import annotations

import csv
import json
import os
import re
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from .ids import content_hash
from .release_validation import assert_release_packagable
from .storage_guard import storage_preflight

SEMVER = re.compile(r"^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$")
FORBIDDEN_RELEASE_COLUMNS = {
    "evaluator_public_id",
    "evaluator_protected_id",
    "protected_person_id",
    "public_name",
    "public_identifier",
    "email",
    "contact_email",
    "authorization",
}

# Large census tables stay as first-class Parquet assets instead of being
# duplicated inside DuckDB. PACKAGE.json and the release verifier cover both
# storage modes, so release coverage is unchanged.
EXTERNAL_PARQUET_TABLE_THRESHOLD_BYTES = 250_000_000


def _yaml(path: Path) -> dict[str, Any]:
    value = yaml.safe_load(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"release registry must be a mapping: {path}")
    return value


def validate_version_registry(registry: Mapping[str, Any]) -> dict[str, Any]:
    release = dict(registry.get("release") or {})
    required = {
        "release_version",
        "immutable_cutoff",
        "schema_version",
        "source_snapshot_version",
        "normalized_data_version",
        "linkage_model_version",
        "feature_versions",
        "release_package_version",
    }
    missing = sorted(required - set(release))
    invalid_semver = sorted(
        key
        for key in ("release_version", "schema_version", "normalized_data_version", "release_package_version")
        if key in release and not SEMVER.fullmatch(str(release[key]))
    )
    feature_versions = release.get("feature_versions") or {}
    invalid_features = sorted(
        key for key, value in feature_versions.items() if not SEMVER.fullmatch(str(value))
    )
    columns = list(registry.get("row_version_columns") or [])
    expected_columns = {
        "schema_version",
        "source_snapshot_version",
        "normalized_data_version",
        "linkage_model_version",
        "feature_version",
        "release_package_version",
    }
    report = {
        "schema": "observatory.version-registry-audit/1",
        "missing": missing,
        "invalid_semver": invalid_semver,
        "invalid_feature_versions": invalid_features,
        "missing_row_version_columns": sorted(expected_columns - set(columns)),
        "snapshot_id": content_hash(json.dumps(release, sort_keys=True))[:24] if not missing else None,
    }
    report["passes"] = not any(
        (report["missing"], report["invalid_semver"], report["invalid_feature_versions"], report["missing_row_version_columns"])
    )
    return report


def release_version_metadata(workspace: Path) -> dict[str, Any]:
    registry = _yaml(workspace / "configs" / "observatory" / "versions.yaml")
    audit = validate_version_registry(registry)
    if not audit["passes"]:
        raise RuntimeError(f"invalid release version registry: {audit}")
    return {
        **dict(registry["release"]),
        "snapshot_id": audit["snapshot_id"],
        "version_registry_hash": content_hash(
            (workspace / "configs" / "observatory" / "versions.yaml").read_bytes()
        ),
    }


def _manifest_rows(root: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(item for item in root.rglob("*") if item.is_file() and item.name != "MANIFEST.json"):
        rows.append(
            {
                "path": str(path.relative_to(root)),
                "size_bytes": path.stat().st_size,
                "sha256": content_hash(path.read_bytes()),
            }
        )
    return rows


def _privacy_scan(root: Path) -> dict[str, Any]:
    findings = []
    parquet_files = sorted(root.rglob("*.parquet"))
    for path in parquet_files:
        columns = set(pq.read_schema(path).names)
        forbidden = sorted(columns & FORBIDDEN_RELEASE_COLUMNS)
        if forbidden:
            findings.append({"path": str(path.relative_to(root)), "columns": forbidden})
    raw_paths = [
        str(path.relative_to(root))
        for path in root.rglob("*")
        if path.is_file() and any(part in {"raw", "objects", "cache"} for part in path.parts)
    ]
    return {
        "schema": "observatory.release-privacy-scan/1",
        "parquet_count": len(parquet_files),
        "forbidden_column_findings": findings,
        "raw_or_cache_paths": raw_paths,
        "passes": not findings and not raw_paths,
    }


def _release_feature_version(path: Path, versions: Mapping[str, Any]) -> str:
    features = dict(versions.get("feature_versions") or {})
    exact = {
        "semantic_novelty": "semantic_novelty",
        "evaluation_objects": "evaluation_objects",
        "recombinatorial_novelty": "recombinatorial_novelty",
        "afterlife_panel": "afterlife_outcomes",
        "fixed_window_outcomes": "afterlife_outcomes",
        "lineage_edges_release": "lineage",
        "lineage_sensitivity": "lineage",
        "evaluator_supply_strain": "evaluator_strain",
        "timing_strain_series": "evaluator_strain",
        "funding_instrument_evaluability": "funding_evaluability",
        "patent_application_panel": "patent_gate",
        "community_benchmark_tasks": "benchmarks",
    }
    if path.stem in exact:
        return str(features[exact[path.stem]])
    if "construct" in path.stem or path.stem == "novelty_evaluation_atlas":
        return str(features["construct_atlas"])
    return str(features["institutional_regimes"])


def _copy_release_file(source: Path, destination: Path, versions: Mapping[str, Any]) -> None:
    if source.suffix != ".parquet":
        shutil.copy2(source, destination)
        return
    values = {
        "schema_version": versions["schema_version"],
        "source_snapshot_version": versions["source_snapshot_version"],
        "normalized_data_version": versions["normalized_data_version"],
        "linkage_model_version": versions["linkage_model_version"],
        "feature_version": _release_feature_version(source, versions),
        "release_package_version": versions["release_package_version"],
    }
    parquet = pq.ParquetFile(source)
    additions = [
        (name, str(value))
        for name, value in values.items()
        if name not in parquet.schema_arrow.names
    ]
    schema = parquet.schema_arrow
    for name, _ in additions:
        schema = schema.append(pa.field(name, pa.string(), nullable=False))
    writer = pq.ParquetWriter(destination, schema, compression="zstd")
    try:
        for batch in parquet.iter_batches(batch_size=100_000):
            table = pa.Table.from_batches([batch])
            for name, value in additions:
                table = table.append_column(
                    name,
                    pa.array([value] * table.num_rows, type=pa.string()),
                )
            # Appended Arrow arrays are nullable by default. Cast only after
            # populating every value so the emitted schema preserves the
            # release contract that version columns are non-null.
            table = table.cast(schema)
            writer.write_table(table)
    finally:
        writer.close()


def verify_release_package(root: Path) -> dict[str, Any]:
    manifest_path = root / "MANIFEST.json"
    if not manifest_path.exists():
        raise FileNotFoundError(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    failures = []
    for row in manifest.get("files") or []:
        path = root / str(row["path"])
        if not path.is_file():
            failures.append({"path": row["path"], "reason": "missing"})
        elif path.stat().st_size != int(row["size_bytes"]):
            failures.append({"path": row["path"], "reason": "size_mismatch"})
        elif content_hash(path.read_bytes()) != row["sha256"]:
            failures.append({"path": row["path"], "reason": "hash_mismatch"})
    database = root / "observatory.duckdb"
    table_counts = {}
    if database.exists():
        connection = duckdb.connect(str(database), read_only=True)
        for (table,) in connection.execute("SHOW TABLES").fetchall():
            table_counts[table] = connection.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]
        connection.close()
    package_path = root / "PACKAGE.json"
    package = json.loads(package_path.read_text()) if package_path.exists() else {}
    external_table_counts: dict[str, int] = {}
    external_failures: list[dict[str, Any]] = []
    for table, row in (package.get("external_parquet_tables") or {}).items():
        relative = str(row.get("path") or "")
        path = root / relative
        if not relative or not path.is_file():
            external_failures.append({"table": table, "reason": "missing", "path": relative})
            continue
        observed = int(pq.ParquetFile(path).metadata.num_rows)
        expected = int(row.get("row_count") or -1)
        if observed != expected:
            external_failures.append(
                {
                    "table": table,
                    "reason": "row_count_mismatch",
                    "expected": expected,
                    "observed": observed,
                }
            )
            continue
        external_table_counts[str(table)] = observed
    parquet_table_counts = {**table_counts, **external_table_counts}
    privacy = _privacy_scan(root)
    report: dict[str, Any] = {
        "schema": "observatory.release-load-test/1",
        "manifest_file_count": len(manifest.get("files") or []),
        "hash_failures": failures,
        "duckdb_table_counts": table_counts,
        "external_parquet_table_counts": external_table_counts,
        "external_parquet_failures": external_failures,
        "parquet_table_counts": parquet_table_counts,
        "privacy": privacy,
    }
    report["passes"] = (
        not failures
        and not external_failures
        and bool(parquet_table_counts)
        and privacy["passes"]
    )
    return report


def build_release_package(
    workspace: Path,
    destination: Path,
    *,
    validation_report: Mapping[str, Any] | Path,
) -> dict[str, Any]:
    """Build one immutable release; refuse overwrite and any failed P0 gate."""
    if destination.exists():
        raise FileExistsError(f"immutable release destination already exists: {destination}")
    report = (
        json.loads(validation_report.read_text())
        if isinstance(validation_report, Path)
        else dict(validation_report)
    )
    assert_release_packagable(report)
    versions = release_version_metadata(workspace)
    component_registry = _yaml(workspace / "configs" / "observatory" / "release_components.yaml")
    source_files = [
        workspace / str(relative)
        for component in (component_registry.get("components") or {}).values()
        for relative in component.get("files") or []
    ]
    source_files.extend(sorted((workspace / "schemas" / "observatory").glob("*")))
    source_files.extend(
        [
            workspace / "configs" / "observatory" / "sources.yaml",
            workspace / "configs" / "observatory" / "governance.yaml",
            workspace / "configs" / "observatory" / "versions.yaml",
            workspace / "configs" / "observatory" / "release_components.yaml",
        ]
    )
    missing = [str(path) for path in source_files if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"release source files missing: {missing}")
    projected = sum(path.stat().st_size for path in source_files)
    materialized_parquet_bytes = sum(
        path.stat().st_size
        for path in source_files
        if path.suffix == ".parquet"
        and path.stat().st_size < EXTERNAL_PARQUET_TABLE_THRESHOLD_BYTES
    )
    projected_output_bytes = projected + (2 * materialized_parquet_bytes) + 100_000_000
    storage_receipt = storage_preflight(
        destination.parent,
        projected_input_bytes=0,
        projected_output_bytes=max(projected_output_bytes, 1),
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent))
    try:
        for component_name, component in (component_registry.get("components") or {}).items():
            component_root = temporary / "components" / str(component_name)
            component_root.mkdir(parents=True, exist_ok=True)
            (component_root / "LICENCE.json").write_text(
                json.dumps(
                    {
                        "licence": component["licence"],
                        "notice": component["notice"],
                        "source_files": component.get("files") or [],
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n"
            )
            for relative in component.get("files") or []:
                source = workspace / str(relative)
                _copy_release_file(source, component_root / source.name, versions)
        schema_root = temporary / "schemas"
        schema_root.mkdir()
        for source in sorted((workspace / "schemas" / "observatory").glob("*")):
            if source.is_file():
                shutil.copy2(source, schema_root / source.name)
        config_root = temporary / "metadata"
        config_root.mkdir()
        for name in ("sources.yaml", "governance.yaml", "versions.yaml", "release_components.yaml"):
            shutil.copy2(workspace / "configs" / "observatory" / name, config_root / name)
        (temporary / "VERSION.json").write_text(json.dumps(versions, indent=2, sort_keys=True) + "\n")
        (temporary / "VALIDATION.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        database = temporary / "observatory.duckdb"
        connection = duckdb.connect(str(database))
        table_counts: dict[str, int] = {}
        external_parquet_tables: dict[str, dict[str, Any]] = {}
        for parquet in sorted((temporary / "components").rglob("*.parquet")):
            table = parquet.stem
            if table in table_counts:
                raise RuntimeError(f"duplicate release table name: {table}")
            row_count = int(pq.ParquetFile(parquet).metadata.num_rows)
            table_counts[table] = row_count
            if parquet.stat().st_size >= EXTERNAL_PARQUET_TABLE_THRESHOLD_BYTES:
                external_parquet_tables[table] = {
                    "path": str(parquet.relative_to(temporary)),
                    "row_count": row_count,
                    "storage": "parquet",
                }
                continue
            quoted = str(parquet).replace("'", "''")
            connection.execute(f'CREATE TABLE "{table}" AS SELECT * FROM read_parquet(\'{quoted}\')')
            materialized_count = connection.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]
            if materialized_count != row_count:
                raise RuntimeError(
                    f"DuckDB materialisation count mismatch for {table}: "
                    f"{materialized_count} != {row_count}"
                )
        connection.close()
        with (temporary / "TABLE_COUNTS.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["table", "row_count"])
            writer.writeheader()
            writer.writerows(
                {"table": table, "row_count": count} for table, count in sorted(table_counts.items())
            )
        privacy = _privacy_scan(temporary)
        if not privacy["passes"]:
            raise RuntimeError(f"release privacy/licence surface failed: {privacy}")
        package = {
            "schema": "observatory.release-package/1",
            "release_id": component_registry["release_id"],
            "snapshot_id": versions["snapshot_id"],
            "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "components": {
                name: {"licence": row["licence"], "notice": row["notice"]}
                for name, row in (component_registry.get("components") or {}).items()
            },
            "excluded_content": component_registry.get("excluded_content") or [],
            "table_counts": table_counts,
            "external_parquet_tables": external_parquet_tables,
            "storage_preflight": storage_receipt,
            "privacy_scan": privacy,
            "validation_hash": report.get("validation_hash"),
        }
        (temporary / "PACKAGE.json").write_text(json.dumps(package, indent=2, sort_keys=True) + "\n")
        manifest_rows = _manifest_rows(temporary)
        manifest = {
            "schema": "observatory.release-manifest/1",
            "snapshot_id": versions["snapshot_id"],
            "files": manifest_rows,
            "aggregate_hash": content_hash(json.dumps(manifest_rows, sort_keys=True)),
        }
        (temporary / "MANIFEST.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        load_test = verify_release_package(temporary)
        if not load_test["passes"]:
            raise RuntimeError(f"fresh release load test failed: {load_test}")
        os.replace(temporary, destination)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    final = verify_release_package(destination)
    final.update(
        {
            "release_id": component_registry["release_id"],
            "snapshot_id": versions["snapshot_id"],
            "destination": str(destination),
        }
    )
    return final
