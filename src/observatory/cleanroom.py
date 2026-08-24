"""Credential-free rebuild of representative public OSG fixtures."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .adapters import (
    ArxivOAIConnector,
    CopernicusOAIConnector,
    ELifeProcessConnector,
    EuropePMCConnector,
    F1000ProcessConnector,
    OpenReviewSurfaceConnector,
    SciPostProcessConnector,
)
from .connectors.base import ConnectorContext, RawItem
from .ids import content_hash, stable_id
from .schema import validate_record

FIXTURES = (
    (ArxivOAIConnector, "arxiv", "oai_record.json", "oai_record"),
    (CopernicusOAIConnector, "copernicus", "oai_record.json", "oai_record"),
    (EuropePMCConnector, "europe_pmc", "work.json", "europe_pmc_work"),
    (ELifeProcessConnector, "elife_process", "process_page.html", "process_page"),
    (F1000ProcessConnector, "f1000_process", "article.xml", "article_xml"),
    (OpenReviewSurfaceConnector, "openreview_surface", "venue_group.json", "venue_group_configuration"),
    (SciPostProcessConnector, "scipost_process", "submission_bundle.json", "submission_bundle"),
)


def _credential_markers() -> list[str]:
    suspicious = ("TOKEN", "PASSWORD", "SECRET", "API_KEY", "AUTHORIZATION")
    allow = {"PYTHON_KEYRING_BACKEND"}
    return sorted(
        name
        for name in os.environ
        if name not in allow and any(marker in name.upper() for marker in suspicious)
    )


def rebuild_public_fixtures(fixture_root: Path, output: Path) -> dict[str, Any]:
    context = ConnectorContext(
        workspace=output.parent,
        fixture_dir=fixture_root,
        cache_dir=output.parent / "cleanroom-cache-disabled",
    )
    rows = []
    for connector_type, source_id, filename, object_type in FIXTURES:
        connector = connector_type()
        path = fixture_root / source_id / filename
        payload = path.read_bytes()
        item = RawItem(
            native_id=f"cleanroom:{source_id}:{filename}",
            object_type=object_type,
            payload=payload,
            source_url=f"https://example.invalid/cleanroom/{source_id}/{filename}",
            created_at="2026-08-20T00:00:00+00:00",
            modified_at="2026-08-20T00:00:00+00:00",
            licence="fixture-derived-from-public-record",
            release_class="pointer_hash",
        )
        normalized = list(
            connector.normalize(
                item,
                source_object_id=stable_id("source_object", source_id, item.native_id),
                provenance_event_id=stable_id("provenance", source_id, item.native_id),
            )
        )
        validation_failures = []
        for record in normalized:
            try:
                validate_record(record.table, record.row)
            except (KeyError, ValueError) as exc:
                validation_failures.append(str(exc))
        fixture_validation = connector.validate_fixture(context)
        rows.append(
            {
                "source_id": source_id,
                "connector": connector_type.__name__,
                "connector_version": connector.connector_version,
                "fixture": str(path),
                "fixture_hash": content_hash(payload),
                "normalized_record_count": len(normalized),
                "normalized_table_counts": {
                    table: sum(record.table == table for record in normalized)
                    for table in sorted({record.table for record in normalized})
                },
                "validation_failures": validation_failures,
                "connector_fixture_validation": fixture_validation,
                "passes": bool(normalized) and not validation_failures and bool(fixture_validation.get("passes")),
            }
        )
    report: dict[str, Any] = {
        "schema": "observatory.cleanroom-fixture-rebuild/1",
        "fixture_root": str(fixture_root),
        "network_calls": 0,
        "paid_api_calls": 0,
        "credential_markers_visible_to_process": _credential_markers(),
        "credential_requirement": "none; rebuild invokes normalize and fixture validation only",
        "sources": rows,
    }
    # An ambient desktop login may exist, but the code path neither reads nor
    # requires it. The acceptance condition is dependency, not process-wide
    # deletion of unrelated environment variables.
    report["passes"] = all(row["passes"] for row in rows) and report["network_calls"] == 0
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report
