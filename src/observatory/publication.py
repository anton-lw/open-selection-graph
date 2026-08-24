"""Deterministic, licence-separated deposit bundles for external publication."""

from __future__ import annotations

import json
import tarfile
from pathlib import Path
from typing import Any, Iterable

import yaml

from .ids import content_hash
from .release_engineering import verify_release_package


def _files(paths: Iterable[Path]) -> list[Path]:
    excluded_directory_names = {
        "__pycache__",
        ".ipynb_checkpoints",
        ".pytest_cache",
        ".ruff_cache",
        "build",
    }
    excluded_suffixes = {".pyc", ".pyo"}
    excluded_names = {".DS_Store"}

    def publishable(path: Path) -> bool:
        return (
            not excluded_directory_names.intersection(path.parts)
            and path.suffix not in excluded_suffixes
            and path.name not in excluded_names
        )

    found: set[Path] = set()
    for path in paths:
        if path.is_file() and publishable(path):
            found.add(path)
        elif path.is_dir():
            found.update(
                item for item in path.rglob("*") if item.is_file() and publishable(item)
            )
    return sorted(found)


def _deterministic_tar(workspace: Path, paths: Iterable[Path], output: Path) -> dict[str, Any]:
    members = _files(paths)
    output.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(output, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for path in members:
            relative = str(path.relative_to(workspace))
            info = archive.gettarinfo(str(path), arcname=relative)
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mtime = 0
            with path.open("rb") as handle:
                archive.addfile(info, handle)
    return {
        "path": str(output.relative_to(workspace)),
        "member_count": len(members),
        "size_bytes": output.stat().st_size,
        "sha256": content_hash(output.read_bytes()),
    }


def build_publication_bundles(workspace: Path, output: Path) -> dict[str, Any]:
    plan = yaml.safe_load((workspace / "configs" / "observatory" / "publication.yaml").read_text())
    release_version = str(plan["release_id"]).removeprefix("open-selection-graph-")
    release_root = workspace / str(plan["release_root"])
    verification = verify_release_package(release_root)
    if not verification["passes"]:
        raise RuntimeError("publication bundle blocked by failed release verification")
    output.mkdir(parents=True, exist_ok=True)

    code_paths = [
        workspace / "src" / "observatory",
        workspace / "configs" / "observatory",
        workspace / "schemas" / "observatory",
        workspace / "docs" / "observatory",
        workspace / "tests" / "fixtures" / "observatory",
        workspace / "tests" / "test_observatory_core.py",
        workspace / "tests" / "test_observatory_hardening.py",
        workspace / "tests" / "test_observatory_limitations.py",
        workspace / "modal_observatory.py",
        workspace / "modal_validity.py",
        workspace / "TICKETBOOK_OPEN_SELECTION_GRAPH.md",
        workspace / "scripts" / "observatory_pack_raw_source.py",
        workspace / "LICENSE",
        workspace / "results" / "observatory" / "release_validation.json",
        workspace / "results" / "observatory" / "ticket_evidence_audit.json",
        workspace
        / "results"
        / "observatory"
        / "openreview_derived_corpora_reconciliation.json",
        workspace
        / "results"
        / "observatory"
        / "r5"
        / "quantitative_claim_ledger.json",
    ]
    bundles = [
        {
            "kind": "code_metadata",
            "licence": "software and file-specific licences",
            **_deterministic_tar(
                workspace,
                code_paths,
                output / f"open-selection-graph-code-metadata-{release_version}.tar",
            ),
        }
    ]
    for component in sorted((release_root / "components").iterdir()):
        licence = json.loads((component / "LICENCE.json").read_text())["licence"]
        bundles.append(
            {
                "kind": f"dataset_component:{component.name}",
                "licence": licence,
                **_deterministic_tar(
                    workspace,
                    [component, release_root / "MANIFEST.json", release_root / "schemas"],
                    output / f"open-selection-graph-{component.name}-{release_version}.tar",
                ),
            }
        )
    paper_paths = [
        workspace / "docs" / "observatory" / "DATA_METHODS_PAPER.tex",
        workspace / "docs" / "observatory" / "figures",
        workspace / "docs" / "observatory" / "generated",
        workspace / "output" / "pdf" / "OPEN_SELECTION_GRAPH_DATA_PAPER.pdf",
        workspace / "docs" / "observatory" / "CITATION.cff",
        workspace / "docs" / "observatory" / "DATA_CARD.md",
        workspace / "results" / "observatory" / "r5" / "quantitative_claim_ledger.json",
        workspace / "results" / "observatory" / "release_validation.json",
        workspace / "results" / "observatory" / "source_coverage_atlas.json",
    ]
    bundles.append(
        {
            "kind": "data_methods_paper",
            "licence": "CC-BY-4.0",
            **_deterministic_tar(
                workspace,
                paper_paths,
                output / f"open-selection-graph-data-methods-paper-{release_version}.tar",
            ),
        }
    )
    body = {
        "schema": "observatory.publication-bundles/1",
        "release_id": plan["release_id"],
        "release_verification": {
            "passes": verification["passes"],
            "manifest_file_count": verification["manifest_file_count"],
            "parquet_tables": len(verification["parquet_table_counts"]),
            "duckdb_materialized_tables": len(verification["duckdb_table_counts"]),
            "external_parquet_tables": len(verification["external_parquet_table_counts"]),
        },
        "creator": plan["creator"],
        "targets": plan["targets"],
        "bundles": bundles,
        "persistent_tokens_stored": False,
        "external_uploads_performed": 0,
        "external_publication_status": "awaiting authenticated action-time confirmation",
        "passes": bool(bundles) and verification["passes"] and all(row["sha256"] for row in bundles),
    }
    manifest = output / "PUBLICATION_MANIFEST.json"
    manifest.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n")
    return body
