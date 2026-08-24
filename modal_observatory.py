"""Bounded Modal harness for population-scale OSG normalization.

The input and outputs live in the ``open-selection-graph`` Volume.  No
credentials are embedded in this file. Source-specific entry points perform
bounded public acquisition or normalize already-collected corpora, with proofs,
provider counts, checkpointing, integrity audits, and the constitutional cost
ceiling applied before population work.

Upload once::

    modal volume create open-selection-graph
    modal volume put -f open-selection-graph data/p2/openreview /workspace/data/p2/

Run and inspect::

    modal run modal_observatory.py --estimate-only
    modal run modal_observatory.py
    modal volume get open-selection-graph /workspace/results/observatory/runs ./results/observatory/modal-runs
"""

from __future__ import annotations

import modal

APP_NAME = "open-selection-graph"
VOLUME_NAME = "open-selection-graph"
MODAL_BUDGET_USD = 30.0

app = modal.App(APP_NAME)
volume = modal.Volume.from_name(VOLUME_NAME, create_if_missing=True)
image = (
    modal.Image.debian_slim(python_version="3.12")
    .apt_install("poppler-utils", "tesseract-ocr")
    .pip_install(
        "duckdb>=1.1",
        "jsonschema>=4.21",
        "pyarrow>=16",
        "pytz>=2024.1",
        "pyyaml>=6",
        "requests>=2.31",
        "beautifulsoup4>=4.12",
        "lxml>=5",
        "pypdf>=4",
    )
    .add_local_dir("src", "/opt/observatory/src")
    .add_local_file(
        "scripts/observatory_pack_raw_source.py",
        "/opt/observatory/observatory_pack_raw_source.py",
    )
    .add_local_dir("configs/observatory", "/opt/observatory/configs/observatory")
    .add_local_dir("tests/fixtures/observatory", "/opt/observatory/fixtures")
    .add_local_file(
        "results/observatory/openreview_invitation_manifest.json",
        "/opt/observatory/openreview_invitation_manifest.json",
    )
    .add_local_file(
        "configs/observatory/openreview_core_probe_manifest.json",
        "/opt/observatory/openreview_core_probe_manifest.json",
    )
)

openreview_secret = modal.Secret.from_name("openreview-credentials")


@app.function(
    image=image,
    cpu=2.0,
    memory=4_096,
    timeout=21_600,
    secrets=[openreview_secret],
    volumes={"/volume": volume},
)
def audit_openreview_public_invitations(
    *, snapshot_label: str = "current", restart: bool = False
) -> dict:
    """Audit every surface-declared public invitation and its Note count."""
    import json
    import re
    import sys
    from pathlib import Path

    sys.path.insert(0, "/opt/observatory/src")
    from observatory.connectors.base import ConnectorContext
    from observatory.openreview_invitation_audit import (
        write_openreview_invitation_audit,
    )

    root = Path("/volume/workspace")
    if not re.fullmatch(r"[0-9A-Za-z._-]+", snapshot_label):
        raise ValueError("snapshot_label contains unsupported characters")
    snapshot_root = root / "results" / "observatory" / "observability_snapshots"
    snapshot_root.mkdir(parents=True, exist_ok=True)
    output_path = snapshot_root / f"openreview_public_invitation_audit_{snapshot_label}.json"
    checkpoint_path = (
        root
        / "results"
        / "observatory"
        / "staging"
        / f"openreview_invitation_audit_{snapshot_label}_checkpoint.json"
    )
    if restart:
        checkpoint_path.unlink(missing_ok=True)
    context = ConnectorContext(
        workspace=root,
        fixture_dir=Path("/opt/observatory/fixtures"),
        cache_dir=root / "data" / "observatory" / "cache",
    )
    output = write_openreview_invitation_audit(
        context,
        output_path,
        manifest_path=Path("/opt/observatory/openreview_invitation_manifest.json"),
        public_only=True,
        checkpoint_path=checkpoint_path,
    )
    report = json.loads(output.read_text())
    volume.commit()
    return {key: value for key, value in report.items() if key not in {"cycles", "invitations"}}


@app.function(
    image=image,
    cpu=2.0,
    memory=4_096,
    timeout=21_600,
    secrets=[openreview_secret],
    volumes={"/volume": volume},
)
def probe_openreview_api(*, restart: bool = False) -> dict:
    """Create the authenticated 100-Note/Edit proof without leaking auth."""
    import json
    import sys
    from pathlib import Path

    sys.path.insert(0, "/opt/observatory/src")
    from observatory.adapters.openreview_api import (
        OpenReviewAPINotesConnector,
        OpenReviewDomainEditsConnector,
    )
    from observatory.connectors.base import ConnectorContext
    from observatory.connectors.runner import RunOptions, run_connector
    from observatory.integrity import verify_raw_manifests
    from observatory.probes import build_probe_manifest
    from observatory.storage import NormalizedLake, RawStore

    root = Path("/volume/workspace")
    # One invitation page proves the public Note surface.  The companion
    # domain connector below proves lossless Edit/revision acquisition in a
    # single 1000-object request; issuing one Edit request per proof Note is
    # redundant and triggers the provider's burst limiter.
    connector = OpenReviewAPINotesConnector(page_size=100, include_edits=False)
    context = ConnectorContext(
        workspace=root,
        fixture_dir=Path("/opt/observatory/fixtures"),
        cache_dir=root / "data" / "observatory" / "cache",
        parameters={"files": ["/opt/observatory/openreview_core_probe_manifest.json"]},
    )
    raw = RawStore(root / "data" / "observatory" / "raw")
    lake = NormalizedLake(root / "data" / "observatory" / "normalized")
    run_root = root / "results" / "observatory"
    result = run_connector(
        connector,
        context,
        raw_store=raw,
        lake=lake,
        run_root=run_root,
        options=RunOptions(
            limit=100,
            restart=restart,
            estimate_storage=True,
            estimate_modal_cost=True,
        ),
    )
    domain_connector = OpenReviewDomainEditsConnector(page_size=100)
    domain_context = ConnectorContext(
        workspace=root,
        fixture_dir=Path("/opt/observatory/fixtures"),
        cache_dir=root / "data" / "observatory" / "cache",
        parameters={"domain_files": ["/opt/observatory/openreview_core_probe_manifest.json"]},
    )
    domain_connector.count(domain_context)
    domain_result = run_connector(
        domain_connector,
        domain_context,
        raw_store=raw,
        lake=lake,
        run_root=run_root,
        options=RunOptions(
            limit=1,
            restart=restart,
            estimate_storage=True,
            estimate_modal_cost=True,
        ),
    )
    proof = build_probe_manifest(
        source_id=connector.source_id,
        raw_root=raw.root,
        run_root=run_root,
        fixture_root=run_root / "openreview_api_proof",
        cache_root=context.cache_dir,
    )
    domain_probe = {
        "schema": "observatory.openreview-domain-edit-proof/1",
        "connector_version": domain_connector.connector_version,
        "query_hash": domain_result["query_hash"],
        "provider_expected_edits": sum(domain_connector.domain_provider_counts.values()),
        "found_page_bundles": domain_result["found_count"],
        "tables": domain_result["tables"],
        "passes": bool(
            sum(domain_connector.domain_provider_counts.values()) > 0
            and domain_result["found_count"] == 1
            and domain_result["tables"].get("lineage_edge", 0) > 0
        ),
    }
    domain_probe_path = run_root / "openreview_api_proof" / "openreview_api" / "domain_edit_probe.json"
    domain_probe_path.parent.mkdir(parents=True, exist_ok=True)
    domain_probe_path.write_text(json.dumps(domain_probe, indent=2, sort_keys=True) + "\n")
    if not domain_probe["passes"]:
        raise RuntimeError("OpenReview domain-edit proof failed")
    audit = verify_raw_manifests(raw.root, source_ids={connector.source_id})
    if not audit["passes"]:
        raise RuntimeError(json.dumps(audit, sort_keys=True))
    volume.commit()
    return {
        "run": {
            key: result[key]
            for key in (
                "status",
                "source_id",
                "connector_version",
                "query_hash",
                "found_count",
                "tables",
                "resource_estimate",
            )
        },
        "proof_manifest": str(proof),
        "domain_edit_proof": str(domain_probe_path),
        "raw_audit": {"passes": True, "checked": audit["checked"]},
    }


@app.function(
    image=image,
    cpu=4.0,
    memory=16_384,
    timeout=86_400,
    secrets=[openreview_secret],
    volumes={"/volume": volume},
)
def harvest_openreview_process(*, restart: bool = False, estimate_only: bool = False) -> dict:
    """Harvest every audited public state and its complete readable forum graph."""
    import json
    import shutil
    import sys
    from pathlib import Path

    sys.path.insert(0, "/opt/observatory/src")
    from observatory.adapters.openreview_api import (
        OpenReviewAPINotesConnector,
        OpenReviewDomainEditsConnector,
    )
    from observatory.connectors.base import ConnectorContext
    from observatory.connectors.runner import (
        RunOptions,
        estimate_resources,
        run_connector,
    )
    from observatory.integrity import verify_raw_manifests
    from observatory.openreview_process import (
        build_forum_manifest,
        build_openreview_process_audit,
        build_passing_state_manifest,
        write_openreview_forum_count_sample,
    )
    from observatory.storage import NormalizedLake, RawStore

    root = Path("/volume/workspace")
    run_root = root / "results" / "observatory"
    audit_path = run_root / "openreview_public_invitation_audit.json"
    if not audit_path.exists():
        raise FileNotFoundError("run the authenticated OpenReview invitation audit first")
    audit = json.loads(audit_path.read_text())
    if not audit.get("passes"):
        raise RuntimeError("OpenReview public-invitation audit did not pass")
    domain_proof_path = Path("/opt/observatory/fixtures") / "openreview_api" / "domain_edit_probe.json"
    if not domain_proof_path.exists():
        raise FileNotFoundError("run and install the OpenReview domain-edit proof first")
    domain_proof = json.loads(domain_proof_path.read_text())
    if (
        not domain_proof.get("passes")
        or str(domain_proof.get("connector_version")) != OpenReviewDomainEditsConnector.connector_version
    ):
        raise RuntimeError("OpenReview domain-edit proof is stale or failing")
    state_manifest = build_passing_state_manifest(audit_path, run_root / "openreview_passing_state_manifest.json")
    state_connector = OpenReviewAPINotesConnector(page_size=500, include_edits=False, bundle_pages=True)
    state_context = ConnectorContext(
        workspace=root,
        fixture_dir=Path("/opt/observatory/fixtures"),
        cache_dir=root / "data" / "observatory" / "cache",
        parameters={"files": [str(state_manifest)]},
    )
    state_estimate = state_connector.count(state_context)
    state_resource = estimate_resources(state_connector, state_context, state_estimate, limit=None)
    domain_connector = OpenReviewDomainEditsConnector(page_size=100)
    domain_context = ConnectorContext(
        workspace=root,
        fixture_dir=Path("/opt/observatory/fixtures"),
        cache_dir=root / "data" / "observatory" / "cache",
        parameters={"domain_files": [str(state_manifest)]},
    )
    domain_estimate = domain_connector.count(domain_context)
    domain_resource = estimate_resources(domain_connector, domain_context, domain_estimate, limit=None)
    costs = [
        state_resource["estimated_modal_cost_usd"],
        domain_resource["estimated_modal_cost_usd"],
    ]
    if not state_resource["proof_passes"] or not domain_resource["proof_passes"]:
        raise RuntimeError("OpenReview API proof-of-access is absent, stale, or failing")
    if any(value is None for value in costs) or sum(float(value) for value in costs) > MODAL_BUDGET_USD:
        raise RuntimeError(f"estimated state plus domain-edit cost violates ${MODAL_BUDGET_USD:.0f} ceiling")
    if estimate_only:
        return {
            "state_estimate": state_estimate.__dict__,
            "state_resource_estimate": state_resource,
            "domain_edit_estimate": domain_estimate.__dict__,
            "domain_edit_resource_estimate": domain_resource,
            "forum_count_audit_requests": 100,
        }

    raw = RawStore(root / "data" / "observatory" / "raw")
    lake = NormalizedLake(root / "data" / "observatory" / "normalized")
    options = RunOptions(restart=restart, estimate_storage=True, estimate_modal_cost=True)
    state_result = run_connector(
        state_connector,
        state_context,
        raw_store=raw,
        lake=lake,
        run_root=run_root,
        options=options,
    )
    forum_manifest = build_forum_manifest(
        raw.root,
        run_root / "openreview_forum_manifest.json",
        run_root=run_root,
        state_query_hash=state_result["query_hash"],
        state_manifest_path=state_manifest,
    )
    domain_result = run_connector(
        domain_connector,
        domain_context,
        raw_store=raw,
        lake=lake,
        run_root=run_root,
        options=options,
    )
    forum_sample_path = write_openreview_forum_count_sample(
        state_context,
        forum_manifest,
        run_root / "openreview_forum_count_sample.json",
        sample_size=100,
    )
    report_path = build_openreview_process_audit(
        raw.root,
        run_root / "openreview_process_audit.json",
        run_root=run_root,
        state_manifest_path=state_manifest,
        state_query_hash=state_result["query_hash"],
        forum_query_hash=domain_result["query_hash"],
        forum_count_sample_path=forum_sample_path,
    )
    report = json.loads(report_path.read_text())
    if not report["passes"]:
        volume.commit()
        raise RuntimeError("OpenReview process acceptance audit failed; report preserved")
    raw_audit = verify_raw_manifests(raw.root, source_ids={state_connector.source_id})
    lake_audit = lake.verify(source_id=state_connector.source_id)
    if not raw_audit["passes"] or not lake_audit["passes"]:
        raise RuntimeError(json.dumps({"raw": raw_audit, "lake": lake_audit}, sort_keys=True))
    for result in (state_result, domain_result):
        stage = run_root / "staging" / result["source_id"] / result["query_hash"][:16]
        if stage.exists():
            shutil.rmtree(stage)
    receipt = {
        "status": "complete",
        "source_id": state_connector.source_id,
        "state_query_hash": state_result["query_hash"],
        "domain_edit_query_hash": domain_result["query_hash"],
        "state_found_page_bundles": state_result["found_count"],
        "domain_edit_found_page_bundles": domain_result["found_count"],
        "state_tables": state_result["tables"],
        "domain_edit_tables": domain_result["tables"],
        "state_resource_estimate": state_resource,
        "domain_edit_resource_estimate": domain_resource,
        "forum_count_sample": str(forum_sample_path),
        "process_report": str(report_path),
        "raw_audit": {"passes": True, "checked": raw_audit["checked"]},
        "lake_audit": {"passes": True, "checked": lake_audit["checked"]},
        "volume": VOLUME_NAME,
    }
    receipt_path = run_root / "modal_openreview_process_receipt.json"
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    volume.commit()
    return receipt


@app.function(
    image=image,
    # Acquisition is account-rate-limited, while the resumable final compile
    # is CPU/disk bound. One CPU keeps both phases inexpensive without
    # throttling the already-fetched population compile.
    cpu=1.0,
    memory=8_192,
    timeout=86_400,
    secrets=[openreview_secret],
    volumes={"/volume": volume},
)
def harvest_openreview_domain_edits(
    *,
    restart: bool = False,
    estimate_only: bool = False,
    min_interval_seconds: float = 10.0,
    page_size: int = 1_000,
    domain_indices: str = "",
) -> dict:
    """Run only the API2 domain-Edit pass, preserving the completed state pass."""
    import json
    import sys
    from pathlib import Path

    sys.path.insert(0, "/opt/observatory/src")
    from observatory.adapters.openreview_api import OpenReviewDomainEditsConnector
    from observatory.connectors.base import ConnectorContext
    from observatory.connectors.runner import RunOptions, estimate_resources, run_connector
    from observatory.openreview_process import build_passing_state_manifest
    from observatory.storage import NormalizedLake, RawStore

    root = Path("/volume/workspace")
    run_root = root / "results" / "observatory"
    audit_path = run_root / "openreview_public_invitation_audit.json"
    audit = json.loads(audit_path.read_text())
    if not audit.get("passes"):
        raise RuntimeError("OpenReview public-invitation audit did not pass")
    state_manifest = build_passing_state_manifest(audit_path, run_root / "openreview_passing_state_manifest.json")
    connector = OpenReviewDomainEditsConnector(
        page_size=page_size,
        min_interval_seconds=min_interval_seconds,
    )
    proof_path = Path("/opt/observatory/fixtures") / "openreview_api" / "domain_edit_probe.json"
    proof = json.loads(proof_path.read_text())
    if not proof.get("passes") or str(proof.get("connector_version")) != connector.connector_version:
        raise RuntimeError("OpenReview domain-edit proof is stale or failing")
    selected_indices = [int(value) for value in domain_indices.split(",") if value.strip()]
    context = ConnectorContext(
        workspace=root,
        fixture_dir=Path("/opt/observatory/fixtures"),
        cache_dir=root / "data" / "observatory" / "cache",
        parameters={
            "domain_files": [str(state_manifest)],
            **({"domain_indices": selected_indices} if selected_indices else {}),
        },
    )
    estimate = connector.count(context)
    resource = estimate_resources(connector, context, estimate, limit=None)
    if (
        not resource["proof_passes"]
        or resource["estimated_modal_cost_usd"] is None
        or resource["estimated_modal_cost_usd"] > MODAL_BUDGET_USD
    ):
        raise RuntimeError("OpenReview domain Edit resource/proof gate failed")
    if estimate_only:
        return {
            "estimate": estimate.__dict__,
            "resource_estimate": resource,
            "pagination": "provider-supported id keyset",
        }
    result = run_connector(
        connector,
        context,
        raw_store=RawStore(root / "data" / "observatory" / "raw"),
        lake=NormalizedLake(root / "data" / "observatory" / "normalized"),
        run_root=run_root,
        options=RunOptions(restart=restart, estimate_storage=True, estimate_modal_cost=True),
    )
    receipt = {
        "schema": "observatory.modal-openreview-domain-edits/1",
        "status": result["status"],
        "source_id": result["source_id"],
        "query_hash": result["query_hash"],
        "found_page_bundles": result["found_count"],
        "coverage_count": result["coverage_count"],
        "resource_estimate": result["resource_estimate"],
        "pagination": "provider-supported id keyset",
        "page_size": connector.page_size,
        "min_interval_seconds": connector.min_interval_seconds,
        "domain_indices": selected_indices,
        "volume": VOLUME_NAME,
    }
    receipt_path = run_root / "modal_openreview_domain_edits_receipt.json"
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    volume.commit()
    return receipt



@app.function(
    image=image,
    cpu=1.0,
    memory=8_192,
    timeout=86_400,
    secrets=[openreview_secret],
    volumes={"/volume": volume},
)
def harvest_openreview_forum_notes(
    *,
    restart: bool = False,
    estimate_only: bool = False,
    min_interval_seconds: float = 3.0,
    page_size: int = 1_000,
    forums_per_query: int = 100,
    forum_start: int,
    forum_stop: int,
) -> dict:
    """Harvest exact current Notes for a frozen contiguous forum shard."""
    import json
    import sys
    from pathlib import Path

    sys.path.insert(0, "/opt/observatory/src")
    from observatory.adapters.openreview_api import (
        OpenReviewBatchedForumNotesConnector,
    )
    from observatory.connectors.base import ConnectorContext
    from observatory.connectors.runner import RunOptions, estimate_resources, run_connector
    from observatory.storage import NormalizedLake, RawStore

    root = Path("/volume/workspace")
    run_root = root / "results" / "observatory"
    forum_manifest = run_root / "openreview_forum_manifest.json"
    manifest = json.loads(forum_manifest.read_text())
    if (
        manifest.get("schema") != "observatory.openreview-forum-manifest/1"
        or int(manifest.get("forum_count") or 0) != len(manifest.get("forums") or [])
    ):
        raise RuntimeError("OpenReview frozen forum manifest is absent or invalid")
    connector = OpenReviewBatchedForumNotesConnector(
        page_size=page_size,
        forums_per_query=forums_per_query,
        min_interval_seconds=min_interval_seconds,
    )
    proof_path = Path("/opt/observatory/fixtures") / "openreview_api" / "domain_edit_probe.json"
    proof = json.loads(proof_path.read_text())
    if not proof.get("passes") or str(proof.get("connector_version")) != connector.connector_version:
        raise RuntimeError("OpenReview forum Note proof dependency is stale or failing")
    context = ConnectorContext(
        workspace=root,
        fixture_dir=Path("/opt/observatory/fixtures"),
        cache_dir=root / "data" / "observatory" / "cache",
        parameters={
            "forum_files": [str(forum_manifest)],
            "forum_start": int(forum_start),
            "forum_stop": int(forum_stop),
            "forums_per_query": int(forums_per_query),
            "population_surface": "batched_forum_current_notes",
        },
    )
    estimate = connector.count(context)
    resource = estimate_resources(connector, context, estimate, limit=None)
    if (
        not resource["proof_passes"]
        or resource["estimated_modal_cost_usd"] is None
        or resource["estimated_modal_cost_usd"] > MODAL_BUDGET_USD
    ):
        raise RuntimeError("OpenReview forum Note resource/proof gate failed")
    if estimate_only:
        return {
            "estimate": estimate.__dict__,
            "resource_estimate": resource,
            "pagination": "provider-supported id keyset over repeated-forum unions",
        }
    result = run_connector(
        connector,
        context,
        raw_store=RawStore(root / "data" / "observatory" / "raw"),
        lake=NormalizedLake(root / "data" / "observatory" / "normalized"),
        run_root=run_root,
        options=RunOptions(
            restart=restart,
            estimate_storage=True,
            estimate_modal_cost=True,
        ),
    )
    receipt = {
        "schema": "observatory.modal-openreview-forum-notes/1",
        "status": result["status"],
        "source_id": result["source_id"],
        "query_hash": result["query_hash"],
        "found_page_bundles": result["found_count"],
        "coverage_count": result["coverage_count"],
        "resource_estimate": result["resource_estimate"],
        "pagination": "provider-supported id keyset over repeated-forum unions",
        "page_size": connector.page_size,
        "forums_per_query": connector.forums_per_query,
        "forum_start": int(forum_start),
        "forum_stop": int(forum_stop),
        "volume": VOLUME_NAME,
    }
    receipt_path = (
        run_root
        / f"modal_openreview_forum_notes_{result['query_hash'][:12]}_receipt.json"
    )
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    volume.commit()
    return receipt


@app.function(
    image=image,
    cpu=1.0,
    memory=4_096,
    timeout=86_400,
    volumes={"/volume": volume},
)
def pack_inactive_observatory_raw(*, source_ids: str) -> dict:
    """Inode-safe, hash-verifying compaction for explicit inactive sources."""
    import json
    import sys
    from pathlib import Path

    sys.path.insert(0, "/opt/observatory/src")
    from observatory.storage import RawStore

    requested = tuple(value.strip() for value in source_ids.split(",") if value.strip())
    if not requested or len(set(requested)) != len(requested):
        raise ValueError("source_ids must be a non-empty unique comma-separated list")
    if "openreview_api" in requested:
        raise ValueError("the active OpenReview API source cannot be compacted")
    root = Path("/volume/workspace")
    raw = RawStore(root / "data" / "observatory" / "raw")
    rows = []
    for source_id in requested:
        result = raw.pack_source(source_id)
        if not result.get("passes"):
            raise RuntimeError(json.dumps(result, sort_keys=True))
        rows.append(result)
    receipt = {
        "schema": "observatory.modal-raw-pack-receipt/1",
        "status": "complete",
        "sources": rows,
        "volume": VOLUME_NAME,
    }
    path = root / "results" / "observatory" / "modal_raw_pack_receipt.json"
    path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    volume.commit()
    return receipt


@app.function(
    image=image,
    cpu=4.0,
    memory=16_384,
    timeout=86_400,
    secrets=[openreview_secret],
    volumes={"/volume": volume},
)
def finalize_openreview_domain_shards(
    *,
    query_hashes: str,
    note_query_hashes: str = "",
) -> dict:
    """Fail-closed union audit for disjoint completed OpenReview domain shards."""
    import json
    import shutil
    import sys
    from pathlib import Path

    sys.path.insert(0, "/opt/observatory/src")
    from observatory.adapters.openreview_api import (
        OpenReviewBatchedForumNotesConnector,
        OpenReviewDomainEditsConnector,
    )
    from observatory.atlas import write_lake_population_coverage
    from observatory.connectors.base import ConnectorContext
    from observatory.connectors.checkpoint import query_hash
    from observatory.ids import content_hash
    from observatory.integrity import verify_raw_manifests
    from observatory.openreview_process import (
        build_forum_manifest,
        build_openreview_process_audit,
        build_passing_state_manifest,
        write_openreview_forum_count_sample,
        write_openreview_population_coverage,
    )
    from observatory.storage import NormalizedLake, RawStore

    hashes = tuple(value.strip() for value in query_hashes.split(",") if value.strip())
    if not hashes or len(set(hashes)) != len(hashes):
        raise ValueError("OpenReview shard query hashes must be non-empty and unique")
    note_hashes = tuple(
        value.strip() for value in note_query_hashes.split(",") if value.strip()
    )
    if not note_hashes or len(set(note_hashes)) != len(note_hashes):
        raise ValueError("OpenReview Note shard query hashes must be non-empty and unique")
    root = Path("/volume/workspace")
    run_root = root / "results" / "observatory"
    audit_path = run_root / "openreview_public_invitation_audit.json"
    state_manifest = build_passing_state_manifest(audit_path, run_root / "openreview_passing_state_manifest.json")
    state_hash = "42defaa261bd355b0370c2dee810c2c4c25bb120d68ec8bf9b1436ef849385cd"
    forum_manifest = build_forum_manifest(
        root / "data" / "observatory" / "raw",
        run_root / "openreview_forum_manifest.json",
        run_root=run_root,
        state_query_hash=state_hash,
        state_manifest_path=state_manifest,
    )
    forum_body = json.loads(forum_manifest.read_text())
    forum_count = int(forum_body.get("forum_count") or 0)
    if forum_count <= 0 or forum_count != len(forum_body.get("forums") or []):
        raise RuntimeError("OpenReview frozen forum manifest is empty or inconsistent")
    # The Edit shards partition the sorted 176-domain cohort. Current-Note
    # shards separately partition the frozen forum manifest into five
    # contiguous ranges. Recompute both query-hash sets from the canonical
    # parameters used by ``run_connector``: this proves membership without
    # relying on run manifests, which intentionally omit request parameters.
    shard_domains = (
        (21,),
        (23,),
        (26,),
        tuple((*range(21), 22, 24, 25)),
        tuple(range(27, 176)),
    )
    connector = OpenReviewDomainEditsConnector(page_size=1_000, min_interval_seconds=10.0)
    expected_by_hash = {}
    for indices in shard_domains:
        parameters = {
            "source_id": connector.source_id,
            "version": connector.connector_version,
            "parameters": {
                "domain_files": [str(state_manifest)],
                "domain_indices": list(indices),
            },
            "since": None,
            "until": None,
            "limit": None,
            "fixture": False,
            "no_text": False,
        }
        expected_by_hash[query_hash(parameters)] = indices
    if set(hashes) != set(expected_by_hash):
        raise RuntimeError("OpenReview shard hashes do not match the frozen exhaustive partition")
    note_connector = OpenReviewBatchedForumNotesConnector(
        page_size=1_000,
        forums_per_query=100,
        min_interval_seconds=3.0,
    )
    shard_size, remainder = divmod(forum_count, 5)
    forum_ranges = []
    range_start = 0
    for shard_index in range(5):
        range_stop = range_start + shard_size + (1 if shard_index < remainder else 0)
        forum_ranges.append((range_start, range_stop))
        range_start = range_stop
    expected_note_by_hash = {}
    for forum_start, forum_stop in forum_ranges:
        parameters = {
            "source_id": note_connector.source_id,
            "version": note_connector.connector_version,
            "parameters": {
                "forum_files": [str(forum_manifest)],
                "forum_start": forum_start,
                "forum_stop": forum_stop,
                "forums_per_query": 100,
                "population_surface": "batched_forum_current_notes",
            },
            "since": None,
            "until": None,
            "limit": None,
            "fixture": False,
            "no_text": False,
        }
        expected_note_by_hash[query_hash(parameters)] = (forum_start, forum_stop)
    if set(note_hashes) != set(expected_note_by_hash):
        raise RuntimeError(
            "OpenReview forum-Note shard hashes do not match the frozen exhaustive partition"
        )

    complete = []
    selected_domains = []
    for shard_hash in hashes:
        checkpoint_path = run_root / "checkpoints" / f"openreview_api-{shard_hash[:12]}.json"
        checkpoint = json.loads(checkpoint_path.read_text())
        if checkpoint.get("query_hash") != shard_hash or not checkpoint.get("complete"):
            raise RuntimeError(f"OpenReview shard is incomplete: {shard_hash}")
        manifest_path = run_root / "runs" / f"openreview_api-{shard_hash[:12]}.json"
        if not manifest_path.exists():
            raise RuntimeError(f"OpenReview shard run manifest is missing: {shard_hash}")
        indices = expected_by_hash[shard_hash]
        selected_domains.extend(indices)
        complete.append({"query_hash": shard_hash, "domain_indices": list(indices)})
    if len(selected_domains) != len(set(selected_domains)) or set(selected_domains) != set(range(176)):
        raise RuntimeError("OpenReview shards must cover each of the 176 domains exactly once")
    complete_note_shards = []
    selected_note_ranges = []
    for shard_hash in note_hashes:
        checkpoint_path = (
            run_root / "checkpoints" / f"openreview_api-{shard_hash[:12]}.json"
        )
        checkpoint = json.loads(checkpoint_path.read_text())
        if checkpoint.get("query_hash") != shard_hash or not checkpoint.get("complete"):
            raise RuntimeError(f"OpenReview Note shard is incomplete: {shard_hash}")
        manifest_path = run_root / "runs" / f"openreview_api-{shard_hash[:12]}.json"
        if not manifest_path.exists():
            raise RuntimeError(f"OpenReview Note shard run manifest is missing: {shard_hash}")
        forum_start, forum_stop = expected_note_by_hash[shard_hash]
        selected_note_ranges.append((forum_start, forum_stop))
        complete_note_shards.append(
            {
                "query_hash": shard_hash,
                "forum_start": forum_start,
                "forum_stop": forum_stop,
            }
        )
    if sorted(selected_note_ranges) != forum_ranges:
        raise RuntimeError(
            "OpenReview Note shards must cover the frozen forum cohort exactly once"
        )
    context = ConnectorContext(
        workspace=root,
        fixture_dir=Path("/opt/observatory/fixtures"),
        cache_dir=root / "data" / "observatory" / "cache",
        parameters={"files": [str(state_manifest)]},
    )
    sample_path = write_openreview_forum_count_sample(
        context,
        forum_manifest,
        run_root / "openreview_forum_count_sample.json",
        sample_size=100,
    )
    report_path = build_openreview_process_audit(
        root / "data" / "observatory" / "raw",
        run_root / "openreview_process_audit.json",
        run_root=run_root,
        state_manifest_path=state_manifest,
        state_query_hash=state_hash,
        forum_query_hash=hashes,
        note_query_hash=note_hashes,
        forum_count_sample_path=sample_path,
    )
    report = json.loads(report_path.read_text())
    if not report.get("passes"):
        volume.commit()
        raise RuntimeError("OpenReview sharded process audit failed; report preserved")
    raw = RawStore(root / "data" / "observatory" / "raw")
    lake = NormalizedLake(root / "data" / "observatory" / "normalized")
    coverage_exports = [
        str(
            write_openreview_population_coverage(
                report_path,
                run_root / "openreview_api_population_coverage.json",
            )
        )
    ]
    truncated_runs: dict[str, tuple[str, str]] = {}
    for manifest_path in sorted((run_root / "runs").glob("*.json")):
        manifest = json.loads(manifest_path.read_text())
        source_id = str(manifest.get("source_id") or "")
        if (
            source_id
            and source_id != "openreview_api"
            and manifest.get("status") == "complete"
            and manifest.get("coverage_truncated_in_manifest")
        ):
            candidate = (
                str(manifest.get("completed_at") or ""),
                str(manifest["query_hash"]),
            )
            if candidate > truncated_runs.get(source_id, ("", "")):
                truncated_runs[source_id] = candidate
    for source_id, (_, source_hash) in sorted(truncated_runs.items()):
        coverage_exports.append(
            str(
                write_lake_population_coverage(
                    lake.root,
                    run_root / f"{source_id}_population_coverage.json",
                    source_id=source_id,
                    query_hashes=[source_hash],
                )
            )
        )
    raw_audit = verify_raw_manifests(raw.root, source_ids={"openreview_api"})
    lake_audit = lake.verify(source_id="openreview_api")
    if not raw_audit["passes"] or not lake_audit["passes"]:
        raise RuntimeError(json.dumps({"raw": raw_audit, "lake": lake_audit}, sort_keys=True))
    removed_stage_dirs = []
    for shard_hash in (*hashes, *note_hashes):
        stage = run_root / "staging" / "openreview_api" / shard_hash[:16]
        if stage.exists():
            shutil.rmtree(stage)
            removed_stage_dirs.append(str(stage))
    union_hash = content_hash(
        json.dumps(
            {
                "domain_edit_query_hashes": sorted(hashes),
                "forum_note_query_hashes": sorted(note_hashes),
            },
            sort_keys=True,
        )
    )
    receipt = {
        "schema": "observatory.modal-openreview-process-receipt/4",
        "status": "complete",
        "source_id": "openreview_api",
        "state_query_hash": state_hash,
        "domain_edit_query_hashes": list(hashes),
        "forum_note_query_hashes": list(note_hashes),
        "union_hash": union_hash,
        "shards": complete,
        "forum_note_shards": complete_note_shards,
        "process_report": str(report_path),
        "population_coverage_exports": coverage_exports,
        "raw_audit": {"passes": True, "checked": raw_audit["checked"]},
        "lake_audit": {"passes": True, "checked": lake_audit["checked"]},
        "removed_stage_dirs": removed_stage_dirs,
        "volume": VOLUME_NAME,
    }
    receipt_path = run_root / "modal_openreview_process_receipt.json"
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    volume.commit()
    return receipt


@app.function(
    image=image,
    cpu=0.125,
    memory=512,
    timeout=86_400,
    volumes={"/volume": volume},
)
def await_openreview_domain_shards(
    *,
    query_hashes: str,
    note_query_hashes: str = "",
    poll_seconds: int = 60,
) -> dict:
    """Await immutable shard checkpoints and dispatch one fail-closed finalizer."""
    import json
    import time
    from pathlib import Path

    hashes = tuple(value.strip() for value in query_hashes.split(",") if value.strip())
    if not hashes or len(set(hashes)) != len(hashes):
        raise ValueError("OpenReview shard query hashes must be non-empty and unique")
    note_hashes = tuple(
        value.strip() for value in note_query_hashes.split(",") if value.strip()
    )
    if not note_hashes or len(set(note_hashes)) != len(note_hashes):
        raise ValueError("OpenReview Note shard query hashes must be non-empty and unique")
    if poll_seconds < 30 or poll_seconds > 600:
        raise ValueError("poll_seconds must be between 30 and 600")
    run_root = Path("/volume/workspace/results/observatory")
    deadline = time.monotonic() + 86_000
    while time.monotonic() < deadline:
        volume.reload()
        states = []
        all_hashes = (*hashes, *note_hashes)
        for shard_hash in all_hashes:
            path = run_root / "checkpoints" / f"openreview_api-{shard_hash[:12]}.json"
            states.append(json.loads(path.read_text()) if path.exists() else {})
        if all(
            state.get("complete") and state.get("query_hash") == shard_hash
            for state, shard_hash in zip(states, all_hashes)
        ):
            call = finalize_openreview_domain_shards.spawn(
                query_hashes=",".join(hashes),
                note_query_hashes=",".join(note_hashes),
            )
            return {
                "schema": "observatory.modal-openreview-watchdog/1",
                "status": "finalizer_dispatched",
                "query_hashes": list(hashes),
                "note_query_hashes": list(note_hashes),
                "finalizer_call_id": call.object_id,
            }
        time.sleep(poll_seconds)
    raise TimeoutError("OpenReview shard watchdog reached its bounded 24-hour deadline")


@app.function(
    image=image,
    cpu=4.0,
    memory=16_384,
    timeout=21_600,
    volumes={"/volume": volume},
)
def normalize_openreview(*, restart: bool = False, estimate_only: bool = False) -> dict:
    import json
    import shutil
    import sys
    from pathlib import Path

    sys.path.insert(0, "/opt/observatory/src")
    from observatory.adapters.openreview import OpenReviewLocalConnector
    from observatory.connectors.base import ConnectorContext
    from observatory.connectors.runner import RunOptions, estimate_resources, run_connector
    from observatory.integrity import verify_raw_manifests
    from observatory.storage import NormalizedLake, RawStore

    root = Path("/volume/workspace")
    input_root = root / "data" / "p2" / "openreview"
    if not input_root.exists():
        raise FileNotFoundError("upload data/p2/openreview to /workspace/data/p2/ in the Volume first")
    connector = OpenReviewLocalConnector(batch_size=500)
    context = ConnectorContext(
        workspace=root,
        fixture_dir=Path("/opt/observatory/fixtures"),
        cache_dir=root / "data" / "observatory" / "cache",
    )
    estimate = connector.count(context)
    resource = estimate_resources(connector, context, estimate, limit=None)
    # The estimator is intentionally conservative; the hard constitutional
    # budget gate is checked again inside run_connector.
    if resource["estimated_modal_cost_usd"] is None or resource["estimated_modal_cost_usd"] > MODAL_BUDGET_USD:
        raise RuntimeError(f"estimated cost violates ${MODAL_BUDGET_USD:.0f} Modal ceiling")
    if estimate_only:
        return {"estimate": estimate.__dict__, "resource_estimate": resource}

    raw = RawStore(root / "data" / "observatory" / "raw")
    lake = NormalizedLake(root / "data" / "observatory" / "normalized")
    run_root = root / "results" / "observatory"
    result = run_connector(
        connector,
        context,
        raw_store=raw,
        lake=lake,
        run_root=run_root,
        options=RunOptions(restart=restart),
    )
    raw_audit = verify_raw_manifests(raw.root, source_ids={connector.source_id})
    lake_audit = lake.verify(source_id=connector.source_id)
    if not raw_audit["passes"] or not lake_audit["passes"]:
        raise RuntimeError(json.dumps({"raw": raw_audit, "lake": lake_audit}, sort_keys=True))

    # Completed staging is derivable from immutable raw objects. Removing only
    # this exact query directory prevents it from doubling persistent storage.
    stage = run_root / "staging" / connector.source_id / result["query_hash"][:16]
    if stage.exists():
        shutil.rmtree(stage)
    receipt = {
        "status": result["status"],
        "source_id": result["source_id"],
        "query_hash": result["query_hash"],
        "found_count": result["found_count"],
        "tables": result["tables"],
        "coverage_count": result["coverage_count"],
        "resource_estimate": result["resource_estimate"],
        "raw_audit": {"passes": True, "checked": raw_audit["checked"]},
        "lake_audit": {"passes": True, "checked": lake_audit["checked"]},
        "volume": VOLUME_NAME,
    }
    receipt_path = run_root / "modal_openreview_receipt.json"
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    volume.commit()
    return receipt


@app.function(
    image=image,
    cpu=4.0,
    memory=16_384,
    timeout=21_600,
    volumes={"/volume": volume},
)
def harvest_crossref(*, restart: bool = False, estimate_only: bool = False) -> dict:
    """Harvest the complete dated Crossref peer-review metadata snapshot."""
    import json
    import shutil
    import sys
    from pathlib import Path

    sys.path.insert(0, "/opt/observatory/src")
    from observatory.adapters.crossref import CrossrefPeerReviewConnector
    from observatory.connectors.base import ConnectorContext
    from observatory.connectors.runner import RunOptions, estimate_resources, run_connector
    from observatory.crossref_profile import write_crossref_profile
    from observatory.integrity import verify_raw_manifests
    from observatory.storage import NormalizedLake, RawStore

    root = Path("/volume/workspace")
    connector = CrossrefPeerReviewConnector(rows=1000, bundle_pages=True)
    context = ConnectorContext(
        workspace=root,
        fixture_dir=Path("/opt/observatory/fixtures"),
        cache_dir=root / "data" / "observatory" / "cache",
    )
    estimate = connector.count(context)
    resource = estimate_resources(connector, context, estimate, limit=None)
    if resource["estimated_modal_cost_usd"] is None or resource["estimated_modal_cost_usd"] > MODAL_BUDGET_USD:
        raise RuntimeError(f"estimated cost violates ${MODAL_BUDGET_USD:.0f} Modal ceiling")
    if not resource["proof_passes"]:
        raise RuntimeError("Crossref proof-of-access manifest is absent, stale, or failing")
    if estimate_only:
        return {"estimate": estimate.__dict__, "resource_estimate": resource}

    raw = RawStore(root / "data" / "observatory" / "raw")
    lake = NormalizedLake(root / "data" / "observatory" / "normalized")
    run_root = root / "results" / "observatory"
    result = run_connector(
        connector,
        context,
        raw_store=raw,
        lake=lake,
        run_root=run_root,
        options=RunOptions(restart=restart, estimate_storage=True, estimate_modal_cost=True),
    )
    raw_audit = verify_raw_manifests(raw.root, source_ids={connector.source_id})
    lake_audit = lake.verify(source_id=connector.source_id)
    if not raw_audit["passes"] or not lake_audit["passes"]:
        raise RuntimeError(json.dumps({"raw": raw_audit, "lake": lake_audit}, sort_keys=True))
    profile_path = write_crossref_profile(raw.root, run_root / "crossref_peer_review_profile.json")
    stage = run_root / "staging" / connector.source_id / result["query_hash"][:16]
    if stage.exists():
        shutil.rmtree(stage)
    receipt = {
        "status": result["status"],
        "source_id": result["source_id"],
        "query_hash": result["query_hash"],
        "found_count": result["found_count"],
        "tables": result["tables"],
        "coverage_count": result["coverage_count"],
        "resource_estimate": result["resource_estimate"],
        "raw_audit": {"passes": True, "checked": raw_audit["checked"]},
        "lake_audit": {"passes": True, "checked": lake_audit["checked"]},
        "profile": str(profile_path),
        "volume": VOLUME_NAME,
        "provider_peer_review_object_count": json.loads(profile_path.read_text())["snapshot_objects"],
        "raw_api_page_bundle_count": result["found_count"],
    }
    receipt_path = run_root / "modal_crossref_receipt.json"
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    volume.commit()
    return receipt


@app.function(
    image=image,
    cpu=4.0,
    memory=16_384,
    timeout=21_600,
    volumes={"/volume": volume},
)
def benchmark_fulltext(*, document_count: int = 1000, estimate_only: bool = False) -> dict:
    """Run the required mixed-format benchmark without retaining source files."""
    import json
    import sys
    import tempfile
    import time
    from collections import Counter
    from dataclasses import asdict
    from pathlib import Path

    sys.path.insert(0, "/opt/observatory/src")
    from observatory.connectors.formats import parse_jats
    from observatory.connectors.http import PoliteSession, RatePolicy
    from observatory.fulltext import FullTextOrchestrator, TextJob
    from observatory.ids import content_hash
    from observatory.licensing import ReleaseClass, canonical_licence, decide_release

    if document_count != 1000:
        raise ValueError("acceptance benchmark is fixed at exactly 1000 distinct articles")
    assumed_seconds_per_document = 8.0
    assumed_core_cost_per_second = 0.00002
    conservative_cost = document_count * assumed_seconds_per_document * 4 * assumed_core_cost_per_second
    if conservative_cost > MODAL_BUDGET_USD:
        raise RuntimeError(f"estimated cost violates ${MODAL_BUDGET_USD:.0f} Modal ceiling")
    estimate = {
        "documents": document_count,
        "jats": 400,
        "html": 300,
        "pdf": 300,
        "distinct_articles": document_count,
        "estimated_cost_usd": conservative_cost,
        "source_documents_retained": False,
    }
    if estimate_only:
        return estimate

    root = Path("/volume/workspace")
    output = root / "results" / "observatory" / "fulltext_benchmark_1000.json"
    session = PoliteSession(
        cache_dir=Path("/tmp/observatory-http-cache"),
        allowed_hosts={"www.ebi.ac.uk", "europepmc.org"},
        policy=RatePolicy(
            min_interval_seconds=0.2,
            max_retries=5,
            timeout_seconds=120,
            daily_request_ceiling=5_000,
        ),
    )
    search = session.get(
        "https://www.ebi.ac.uk/europepmc/webservices/rest/search",
        params={
            "query": "OPEN_ACCESS:Y AND HAS_PDF:Y",
            "format": "json",
            "pageSize": document_count,
            "resultType": "core",
            "cursorMark": "*",
        },
    ).json()
    works = [row for row in search.get("resultList", {}).get("result", []) if row.get("pmcid")]
    if len(works) < document_count:
        raise RuntimeError(f"Europe PMC returned only {len(works)} benchmarkable records")
    works = works[:document_count]
    if len({row["pmcid"] for row in works}) != document_count:
        raise RuntimeError("benchmark search returned duplicate PMCIDs")

    orchestrator = FullTextOrchestrator(Path("/tmp/fulltext-derived"))
    results = []
    format_counts = Counter()
    licence_counts = Counter()
    start = time.monotonic()
    with tempfile.TemporaryDirectory() as directory:
        temp_root = Path(directory)
        for index, work in enumerate(works):
            pmcid = str(work["pmcid"])
            xml = session.get(
                f"https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML",
                use_cache=False,
            ).content
            parsed = parse_jats(xml)
            expected_references = len(parsed.get("references") or [])
            declared = canonical_licence(str(work.get("license") or ""))
            decision = decide_release(
                object_type="fulltext",
                licence=declared,
                source_allows_redistribution=True if work.get("isOpenAccess") == "Y" else None,
            )
            licence_counts[decision.release_class.value] += 1
            if index < 400:
                route, media_type, payload = "jats", "application/jats+xml", xml
            elif index < 700:
                route, media_type = "html", "text/html"
                payload = session.get(f"https://europepmc.org/articles/{pmcid}", use_cache=False).content
            else:
                route, media_type = "pdf", "application/pdf"
                payload = session.get(f"https://europepmc.org/articles/{pmcid}?pdf=render", use_cache=False).content
                if not payload.startswith(b"%PDF"):
                    result = {
                        "native_id": pmcid,
                        "route": "pdf",
                        "success": False,
                        "text_hash": None,
                        "character_count": 0,
                        "extracted_reference_count": 0,
                        "reference_recall_proxy": None,
                        "elapsed_seconds": 0.0,
                        "failure": "InvalidPDF: provider response is not PDF",
                    }
                    results.append(result)
                    format_counts[route] += 1
                    continue
            path = temp_root / f"{pmcid}.{route}"
            path.write_bytes(payload)
            result = orchestrator.process(
                TextJob(
                    native_id=pmcid,
                    media_type=media_type,
                    payload_path=str(path),
                    expected_reference_count=expected_references or None,
                    ocr_permitted=decision.release_class is ReleaseClass.REDISTRIBUTE,
                    max_ocr_pages=20,
                )
            )
            results.append(asdict(result))
            format_counts[route] += 1
            path.unlink(missing_ok=True)

    elapsed = time.monotonic() - start
    failures = Counter(str(row.get("failure") or "").split(":", 1)[0] for row in results if not row.get("success"))
    recalls = [float(row["reference_recall_proxy"]) for row in results if row.get("reference_recall_proxy") is not None]
    report = {
        "schema": "observatory.fulltext-benchmark/1",
        "document_count": len(results),
        "distinct_article_count": len({row["native_id"] for row in results}),
        "source": "Europe PMC OPEN_ACCESS:Y AND HAS_PDF:Y dated live cursor sample",
        "query_hit_count": search.get("hitCount"),
        "routes": dict(format_counts),
        "licence_release_classes": dict(licence_counts),
        "success_count": sum(bool(row.get("success")) for row in results),
        "success_rate": sum(bool(row.get("success")) for row in results) / len(results),
        "reference_recall_proxy_mean": sum(recalls) / len(recalls) if recalls else None,
        "elapsed_seconds": elapsed,
        "estimated_compute_cost_usd": conservative_cost,
        "failure_taxonomy": dict(failures),
        "source_documents_retained": False,
        "results": results,
    }
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    volume.commit()
    return {key: value for key, value in report.items() if key != "results"}


@app.function(
    image=image,
    cpu=4.0,
    memory=16_384,
    timeout=21_600,
    volumes={"/volume": volume},
)
def harvest_copernicus(*, restart: bool = False, estimate_only: bool = False) -> dict:
    """Harvest the complete Copernicus OAI population at its public stage."""
    import json
    import shutil
    import sys
    from pathlib import Path

    sys.path.insert(0, "/opt/observatory/src")
    from observatory.adapters.copernicus import CopernicusOAIConnector
    from observatory.connectors.base import ConnectorContext
    from observatory.connectors.runner import RunOptions, estimate_resources, run_connector
    from observatory.copernicus_census import write_copernicus_census_report
    from observatory.integrity import verify_raw_manifests
    from observatory.storage import NormalizedLake, RawStore

    root = Path("/volume/workspace")
    connector = CopernicusOAIConnector(metadata_prefix="oai_dc")
    context = ConnectorContext(
        workspace=root,
        fixture_dir=Path("/opt/observatory/fixtures"),
        cache_dir=root / "data" / "observatory" / "cache",
    )
    estimate = connector.count(context)
    resource = estimate_resources(connector, context, estimate, limit=None)
    if resource["estimated_modal_cost_usd"] is None or resource["estimated_modal_cost_usd"] > MODAL_BUDGET_USD:
        raise RuntimeError(f"estimated cost violates ${MODAL_BUDGET_USD:.0f} Modal ceiling")
    if not resource["proof_passes"]:
        raise RuntimeError("Copernicus proof-of-access manifest is absent, stale, or failing")
    if estimate_only:
        return {"estimate": estimate.__dict__, "resource_estimate": resource}

    raw = RawStore(root / "data" / "observatory" / "raw")
    lake = NormalizedLake(root / "data" / "observatory" / "normalized")
    run_root = root / "results" / "observatory"
    crossref_posted_report = run_root / "copernicus_crossref_posted_census.json"
    if not crossref_posted_report.exists():
        raise RuntimeError("complete Copernicus Crossref posted-content census is required")
    result = run_connector(
        connector,
        context,
        raw_store=raw,
        lake=lake,
        run_root=run_root,
        options=RunOptions(restart=restart, estimate_storage=True, estimate_modal_cost=True),
    )
    census_path = run_root / "copernicus_population_census.json"
    census_report = json.loads(census_path.read_text()) if census_path.exists() else {}
    if census_report.get("query_hash") != result["query_hash"] or not census_report.get("passes"):
        census_path = write_copernicus_census_report(
            lake.root,
            census_path,
            query_hash=result["query_hash"],
            provider_expected_objects=int(connector.provider_record_count or 0),
            found_page_count=int(result["found_count"]),
            cache_dir=context.cache_dir / "copernicus-crossref-audit",
            crossref_report_path=crossref_posted_report,
        )
        census_report = json.loads(census_path.read_text())
    if not census_report["passes"]:
        raise RuntimeError("Copernicus population/subtype acceptance report failed")
    raw_audit = verify_raw_manifests(raw.root, source_ids={connector.source_id})
    lake_audit = lake.verify(source_id=connector.source_id)
    if not raw_audit["passes"] or not lake_audit["passes"]:
        raise RuntimeError(json.dumps({"raw": raw_audit, "lake": lake_audit}, sort_keys=True))
    stage = run_root / "staging" / connector.source_id / result["query_hash"][:16]
    if stage.exists():
        shutil.rmtree(stage)
    receipt = {
        "status": result["status"],
        "source_id": result["source_id"],
        "query_hash": result["query_hash"],
        "found_count": result["found_count"],
        "tables": result["tables"],
        "coverage_count": result["coverage_count"],
        "resource_estimate": result["resource_estimate"],
        "raw_audit": {"passes": True, "checked": raw_audit["checked"]},
        "lake_audit": {"passes": True, "checked": lake_audit["checked"]},
        "census_report": str(census_path),
        "volume": VOLUME_NAME,
    }
    receipt_path = run_root / "modal_copernicus_receipt.json"
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    volume.commit()
    return receipt


@app.function(
    image=image,
    cpu=4.0,
    memory=16_384,
    timeout=21_600,
    volumes={"/volume": volume},
)
def harvest_copernicus_crossref(*, restart: bool = False, estimate_only: bool = False) -> dict:
    """Harvest the exact Crossref posted-content population for prefix 10.5194."""
    import json
    import shutil
    import sys
    from pathlib import Path

    sys.path.insert(0, "/opt/observatory/src")
    from observatory.adapters.copernicus_crossref import (
        CopernicusCrossrefPostedConnector,
    )
    from observatory.connectors.base import ConnectorContext
    from observatory.connectors.runner import (
        RunOptions,
        estimate_resources,
        run_connector,
    )
    from observatory.copernicus_crossref_census import (
        write_copernicus_crossref_report,
    )
    from observatory.integrity import verify_raw_manifests
    from observatory.storage import NormalizedLake, RawStore

    root = Path("/volume/workspace")
    connector = CopernicusCrossrefPostedConnector(rows=1000)
    context = ConnectorContext(
        workspace=root,
        fixture_dir=Path("/opt/observatory/fixtures"),
        cache_dir=root / "data" / "observatory" / "cache",
    )
    estimate = connector.count(context)
    resource = estimate_resources(connector, context, estimate, limit=None)
    if resource["estimated_modal_cost_usd"] is None or resource["estimated_modal_cost_usd"] > MODAL_BUDGET_USD:
        raise RuntimeError(f"estimated cost violates ${MODAL_BUDGET_USD:.0f} Modal ceiling")
    if not resource["proof_passes"]:
        raise RuntimeError("Copernicus Crossref proof-of-access is absent, stale, or failing")
    if estimate_only:
        return {"estimate": estimate.__dict__, "resource_estimate": resource}

    raw = RawStore(root / "data" / "observatory" / "raw")
    lake = NormalizedLake(root / "data" / "observatory" / "normalized")
    run_root = root / "results" / "observatory"
    result = run_connector(
        connector,
        context,
        raw_store=raw,
        lake=lake,
        run_root=run_root,
        options=RunOptions(restart=restart, estimate_storage=True, estimate_modal_cost=True),
    )
    report_path = write_copernicus_crossref_report(
        lake.root,
        run_root / "copernicus_crossref_posted_census.json",
        query_hash=result["query_hash"],
        provider_total_results=int(connector.provider_total or 0),
        expected_page_bundles=int(result["estimate"]["expected_objects"]),
        found_page_bundles=int(result["found_count"]),
    )
    report = json.loads(report_path.read_text())
    if not report["passes"]:
        volume.commit()
        raise RuntimeError("Copernicus Crossref population acceptance report failed")
    raw_audit = verify_raw_manifests(raw.root, source_ids={connector.source_id})
    lake_audit = lake.verify(source_id=connector.source_id)
    if not raw_audit["passes"] or not lake_audit["passes"]:
        raise RuntimeError(json.dumps({"raw": raw_audit, "lake": lake_audit}, sort_keys=True))
    stage = run_root / "staging" / connector.source_id / result["query_hash"][:16]
    if stage.exists():
        shutil.rmtree(stage)
    receipt = {
        "status": result["status"],
        "source_id": result["source_id"],
        "query_hash": result["query_hash"],
        "found_page_bundles": result["found_count"],
        "provider_total_results": connector.provider_total,
        "tables": result["tables"],
        "resource_estimate": result["resource_estimate"],
        "raw_audit": {"passes": True, "checked": raw_audit["checked"]},
        "lake_audit": {"passes": True, "checked": lake_audit["checked"]},
        "population_report": str(report_path),
        "volume": VOLUME_NAME,
    }
    receipt_path = run_root / "modal_copernicus_crossref_receipt.json"
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    volume.commit()
    return receipt


@app.function(
    image=image,
    cpu=4.0,
    memory=16_384,
    timeout=86_400,
    volumes={"/volume": volume},
)
def finalize_copernicus_outcomes(*, restart: bool = False) -> dict:
    """Join the full Crossref graph to provider-visible Copernicus outcomes."""
    import json
    import sys
    from pathlib import Path

    sys.path.insert(0, "/opt/observatory/src")
    from observatory.copernicus_outcomes import crawl_copernicus_outcomes
    from observatory.copernicus_relations import write_copernicus_chains
    from observatory.ids import content_hash

    root = Path("/volume/workspace")
    run_root = root / "results" / "observatory"
    required = (
        run_root / "modal_copernicus_receipt.json",
        run_root / "modal_crossref_receipt.json",
    )
    if not all(path.exists() for path in required):
        raise RuntimeError("completed Copernicus and Crossref receipts are required")
    crossref_profile = json.loads((run_root / "crossref_peer_review_profile.json").read_text())
    if not crossref_profile.get("snapshot_manifest", {}).get("complete"):
        raise RuntimeError("complete Crossref cursor snapshot is required")
    chains_path = run_root / "copernicus_public_review_chains.json"
    chain_report = json.loads(chains_path.read_text()) if chains_path.exists() else {}
    posted_report = json.loads((run_root / "copernicus_crossref_posted_census.json").read_text())
    expected_scanned = int(crossref_profile["snapshot_manifest"]["provider_total_results_max"]) + int(
        posted_report["provider_total_results"]
    )
    retained_hash = chain_report.pop("report_hash", None)
    retained_valid = bool(
        retained_hash
        and retained_hash == content_hash(json.dumps(chain_report, sort_keys=True))
        and int(chain_report.get("objects_scanned") or 0) == expected_scanned
        and int(chain_report.get("crossref_posted_preprint_population") or 0)
        == int(posted_report["discussion_candidate_count"])
    )
    if retained_hash:
        chain_report["report_hash"] = retained_hash
    if not retained_valid:
        chains_path = write_copernicus_chains(root / "data" / "observatory" / "raw", chains_path)
        chain_report = json.loads(chains_path.read_text())
    outcomes_path = crawl_copernicus_outcomes(
        chain_report,
        staging_dir=run_root / "staging" / "copernicus_outcome_pages",
        output=run_root / "copernicus_provider_outcome_audit.json",
        restart=restart,
        repair_transient_errors=False,
    )
    report = json.loads(outcomes_path.read_text())
    relation_summary = {key: value for key, value in report["relation_audit"].items() if key != "rows"}
    receipt = {
        "status": "complete" if report.get("passes") else "failed_acceptance_gate",
        "source_id": "copernicus_outcomes",
        "chain_report": str(chains_path),
        "chain_report_hash": chain_report["report_hash"],
        "outcome_report": str(outcomes_path),
        "outcome_report_hash": report["report_hash"],
        "chain_count": report["chain_count"],
        "outcome_count": report["outcome_count"],
        "outcome_states": report["outcome_states"],
        "affirmative_rejected_non_ml_count": report["affirmative_rejected_non_ml_count"],
        "relation_audit": relation_summary,
        "provider_page_fetch_scope": report["provider_page_fetch_scope"],
        "passes": report["passes"],
        "volume": VOLUME_NAME,
    }
    receipt_path = run_root / "modal_copernicus_outcomes_receipt.json"
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    volume.commit()
    return {key: value for key, value in report.items() if key not in {"outcomes", "relation_audit"}} | {
        "relation_audit": relation_summary,
        "receipt": str(receipt_path),
    }


@app.function(
    image=image,
    cpu=2.0,
    memory=4_096,
    timeout=3_600,
    volumes={"/volume": volume},
)
def audit_copernicus_relation_redirects() -> dict:
    """Audit sampled Crossref relations through independent DOI redirects."""
    import json
    import sys
    from pathlib import Path

    sys.path.insert(0, "/opt/observatory/src")
    from observatory.copernicus_relation_audit import (
        write_redirect_relation_audit,
    )

    root = Path("/volume/workspace")
    run_root = root / "results" / "observatory"
    outcome_path = run_root / "copernicus_provider_outcome_audit.json"
    output = write_redirect_relation_audit(
        outcome_path,
        run_root / "copernicus_doi_relation_audit.json",
    )
    relation = json.loads(output.read_text())
    outcome = json.loads(outcome_path.read_text())
    receipt = json.loads((run_root / "modal_copernicus_outcomes_receipt.json").read_text())
    combined_passes = bool(
        int(outcome["affirmative_rejected_non_ml_count"]) >= 2_000
        and relation["passes"]
        and outcome["hidden_stage"]
        and not outcome["absence_of_final_relation_means_rejection"]
    )
    receipt.update(
        {
            "status": "complete" if combined_passes else "failed_acceptance_gate",
            "passes": combined_passes,
            "relation_audit": {key: value for key, value in relation.items() if key != "rows"},
            "relation_audit_path": str(output),
        }
    )
    receipt_path = run_root / "modal_copernicus_outcomes_receipt.json"
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    volume.commit()
    return {
        "passes": combined_passes,
        "affirmative_rejected_non_ml_count": outcome["affirmative_rejected_non_ml_count"],
        "relation_audit": {key: value for key, value in relation.items() if key != "rows"},
        "receipt": str(receipt_path),
    }


@app.function(
    image=image,
    cpu=4.0,
    memory=16_384,
    timeout=21_600,
    volumes={"/volume": volume},
)
def harvest_elife_process(*, restart: bool = False, estimate_only: bool = False) -> dict:
    """Harvest the complete provider-native eLife Reviewed Preprint process."""
    import json
    import shutil
    import sys
    from pathlib import Path

    sys.path.insert(0, "/opt/observatory/src")
    from observatory.adapters.elife import ELifeProcessConnector
    from observatory.connectors.base import ConnectorContext
    from observatory.connectors.runner import RunOptions, estimate_resources, run_connector
    from observatory.elife_cohorts import write_elife_cohort_report
    from observatory.integrity import verify_raw_manifests
    from observatory.storage import NormalizedLake, RawStore

    root = Path("/volume/workspace")
    connector = ELifeProcessConnector(page_size=100)
    context = ConnectorContext(
        workspace=root,
        fixture_dir=Path("/opt/observatory/fixtures"),
        cache_dir=root / "data" / "observatory" / "cache",
    )
    estimate = connector.count(context)
    resource = estimate_resources(connector, context, estimate, limit=None)
    if resource["estimated_modal_cost_usd"] is None or resource["estimated_modal_cost_usd"] > MODAL_BUDGET_USD:
        raise RuntimeError(f"estimated cost violates ${MODAL_BUDGET_USD:.0f} Modal ceiling")
    if not resource["proof_passes"]:
        raise RuntimeError("eLife process proof-of-access is absent, stale, or failing")
    if estimate_only:
        return {"estimate": estimate.__dict__, "resource_estimate": resource}

    raw = RawStore(root / "data" / "observatory" / "raw")
    lake = NormalizedLake(root / "data" / "observatory" / "normalized")
    run_root = root / "results" / "observatory"
    result = run_connector(
        connector,
        context,
        raw_store=raw,
        lake=lake,
        run_root=run_root,
        options=RunOptions(restart=restart, estimate_storage=True, estimate_modal_cost=True),
    )
    cohort_path = write_elife_cohort_report(
        lake.root,
        run_root / "elife_editorial_model_cohorts.json",
        query_hash=result["query_hash"],
        provider_expected_objects=int(result["estimate"]["expected_objects"]),
        found_count=int(result["found_count"]),
    )
    cohort_report = json.loads(cohort_path.read_text())
    if not cohort_report["passes"]:
        raise RuntimeError("eLife cohort acceptance report failed")
    raw_audit = verify_raw_manifests(raw.root, source_ids={connector.source_id})
    lake_audit = lake.verify(source_id=connector.source_id)
    if not raw_audit["passes"] or not lake_audit["passes"]:
        raise RuntimeError(json.dumps({"raw": raw_audit, "lake": lake_audit}, sort_keys=True))
    stage = run_root / "staging" / connector.source_id / result["query_hash"][:16]
    if stage.exists():
        shutil.rmtree(stage)
    receipt = {
        "status": result["status"],
        "source_id": result["source_id"],
        "query_hash": result["query_hash"],
        "found_count": result["found_count"],
        "tables": result["tables"],
        "coverage_count": result["coverage_count"],
        "resource_estimate": result["resource_estimate"],
        "raw_audit": {"passes": True, "checked": raw_audit["checked"]},
        "lake_audit": {"passes": True, "checked": lake_audit["checked"]},
        "cohort_report": str(cohort_path),
        "volume": VOLUME_NAME,
    }
    receipt_path = run_root / "modal_elife_process_receipt.json"
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    volume.commit()
    return receipt


@app.function(
    image=image,
    cpu=4.0,
    memory=16_384,
    timeout=86_400,
    volumes={"/volume": volume},
)
def harvest_f1000_process(*, restart: bool = False, estimate_only: bool = False) -> dict:
    """Harvest every passing public F1000-family version/review corpus."""
    import json
    import shutil
    import sys
    from pathlib import Path

    sys.path.insert(0, "/opt/observatory/src")
    from observatory.adapters.f1000 import F1000ProcessConnector
    from observatory.connectors.base import ConnectorContext
    from observatory.connectors.runner import RunOptions, estimate_resources, run_connector
    from observatory.f1000_family import write_f1000_family_report
    from observatory.integrity import verify_raw_manifests
    from observatory.storage import NormalizedLake, RawStore

    root = Path("/volume/workspace")
    connector = F1000ProcessConnector(page_size=100)
    context = ConnectorContext(
        workspace=root,
        fixture_dir=Path("/opt/observatory/fixtures"),
        cache_dir=root / "data" / "observatory" / "cache",
        parameters={
            "enumeration_snapshot": str(root / "results" / "observatory" / "f1000_enumeration_snapshot_v6.json")
        },
    )
    estimate = connector.count(context)
    resource = estimate_resources(connector, context, estimate, limit=None)
    if resource["estimated_modal_cost_usd"] is None or resource["estimated_modal_cost_usd"] > MODAL_BUDGET_USD:
        raise RuntimeError(f"estimated cost violates ${MODAL_BUDGET_USD:.0f} Modal ceiling")
    if not resource["proof_passes"]:
        raise RuntimeError("F1000-family proof-of-access is absent, stale, or failing")
    if estimate_only:
        return {
            "estimate": estimate.__dict__,
            "resource_estimate": resource,
            "platform_census": connector.platform_census,
        }

    raw = RawStore(root / "data" / "observatory" / "raw")
    lake = NormalizedLake(root / "data" / "observatory" / "normalized")
    run_root = root / "results" / "observatory"
    result = run_connector(
        connector,
        context,
        raw_store=raw,
        lake=lake,
        run_root=run_root,
        options=RunOptions(restart=restart, estimate_storage=True, estimate_modal_cost=True),
    )
    report_path = write_f1000_family_report(
        lake.root,
        run_root / "f1000_family_process_report.json",
        query_hash=result["query_hash"],
        provider_expected_objects=int(result["estimate"]["expected_objects"]),
        found_count=int(result["found_count"]),
        platform_census=connector.platform_census,
    )
    report = json.loads(report_path.read_text())
    if not report["passes"]:
        raise RuntimeError("F1000-family process acceptance report failed")
    raw_audit = verify_raw_manifests(raw.root, source_ids={connector.source_id})
    lake_audit = lake.verify(source_id=connector.source_id)
    if not raw_audit["passes"] or not lake_audit["passes"]:
        raise RuntimeError(json.dumps({"raw": raw_audit, "lake": lake_audit}, sort_keys=True))
    stage = run_root / "staging" / connector.source_id / result["query_hash"][:16]
    if stage.exists():
        shutil.rmtree(stage)
    receipt = {
        "status": result["status"],
        "source_id": result["source_id"],
        "query_hash": result["query_hash"],
        "found_count": result["found_count"],
        "tables": result["tables"],
        "coverage_count": result["coverage_count"],
        "resource_estimate": result["resource_estimate"],
        "platform_census": connector.platform_census,
        "raw_audit": {"passes": True, "checked": raw_audit["checked"]},
        "lake_audit": {"passes": True, "checked": lake_audit["checked"]},
        "process_report": str(report_path),
        "volume": VOLUME_NAME,
    }
    receipt_path = run_root / "modal_f1000_process_receipt.json"
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    volume.commit()
    return receipt


@app.function(
    image=image,
    cpu=2.0,
    memory=8_192,
    timeout=86_400,
    volumes={"/volume": volume},
)
def harvest_f1000_platform(platform_id: str, *, restart: bool = False, estimate_only: bool = False) -> dict:
    """Harvest one independent F1000-family host without cross-host serialization."""
    import json
    import sys
    from pathlib import Path

    sys.path.insert(0, "/opt/observatory/src")
    from observatory.adapters.f1000 import PLATFORMS, F1000ProcessConnector
    from observatory.connectors.base import ConnectorContext
    from observatory.connectors.runner import RunOptions, estimate_resources, run_connector
    from observatory.storage import NormalizedLake, RawStore

    if platform_id not in PLATFORMS:
        raise ValueError(f"unsupported F1000-family platform: {platform_id}")
    root = Path("/volume/workspace")
    connector = F1000ProcessConnector(page_size=100)
    context = ConnectorContext(
        workspace=root,
        fixture_dir=Path("/opt/observatory/fixtures"),
        cache_dir=root / "data" / "observatory" / "cache",
        parameters={
            "enumeration_snapshot": str(root / "results" / "observatory" / "f1000_enumeration_snapshot_v6.json"),
            "platform_ids": [platform_id],
            "recovery_manifest": str(Path("/opt/observatory/configs/observatory/f1000_broken_versions.json")),
        },
    )
    estimate = connector.count(context)
    resource = estimate_resources(connector, context, estimate, limit=None)
    if resource["estimated_modal_cost_usd"] is None or resource["estimated_modal_cost_usd"] > MODAL_BUDGET_USD:
        raise RuntimeError(f"estimated cost violates ${MODAL_BUDGET_USD:.0f} Modal ceiling")
    if not resource["proof_passes"]:
        raise RuntimeError("F1000-family proof-of-access is absent, stale, or failing")
    if estimate_only:
        return {
            "platform_id": platform_id,
            "estimate": estimate.__dict__,
            "resource_estimate": resource,
        }

    run_root = root / "results" / "observatory"
    result = run_connector(
        connector,
        context,
        raw_store=RawStore(root / "data" / "observatory" / "raw"),
        lake=NormalizedLake(root / "data" / "observatory" / "normalized"),
        run_root=run_root,
        options=RunOptions(restart=restart, estimate_storage=True, estimate_modal_cost=True),
    )
    receipt = {
        "schema": "observatory.modal-f1000-platform-shard/1",
        "status": result["status"],
        "source_id": result["source_id"],
        "platform_id": platform_id,
        "query_hash": result["query_hash"],
        "found_count": result["found_count"],
        "expected_count": int(estimate.expected_objects or 0),
        "tables": result["tables"],
        "coverage_count": result["coverage_count"],
        "resource_estimate": result["resource_estimate"],
        "volume": VOLUME_NAME,
    }
    if receipt["found_count"] != receipt["expected_count"]:
        raise RuntimeError(json.dumps(receipt, sort_keys=True))
    receipt_path = run_root / f"modal_f1000_{platform_id}_shard_receipt.json"
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    volume.commit()
    return receipt


@app.function(
    image=image,
    cpu=1.0,
    memory=4_096,
    timeout=3_600,
    volumes={"/volume": volume},
)
def migrate_f1000_research_checkpoint(old_query_hash: str) -> dict:
    """Clone a proven monolithic prefix into the F1000Research host shard."""
    import sys
    from pathlib import Path

    sys.path.insert(0, "/opt/observatory/src")
    from observatory.f1000_shards import migrate_f1000_prefix_checkpoint

    root = Path("/volume/workspace")
    parameters = {
        "enumeration_snapshot": str(root / "results" / "observatory" / "f1000_enumeration_snapshot_v6.json"),
        "platform_ids": ["f1000research"],
        "recovery_manifest": str(Path("/opt/observatory/configs/observatory/f1000_broken_versions.json")),
    }
    receipt = migrate_f1000_prefix_checkpoint(
        root / "results" / "observatory",
        enumeration_snapshot=Path(parameters["enumeration_snapshot"]),
        old_query_hash=old_query_hash,
        new_parameters=parameters,
        platform_id="f1000research",
    )
    volume.commit()
    return receipt


@app.function(
    image=image,
    cpu=4.0,
    memory=16_384,
    timeout=86_400,
    volumes={"/volume": volume},
)
def finalize_f1000_platforms() -> dict:
    """Audit and freeze the union of the four independent host shards."""
    import json
    import shutil
    import sys
    from pathlib import Path

    sys.path.insert(0, "/opt/observatory/src")
    from observatory.adapters.f1000 import F1000ProcessConnector
    from observatory.connectors.base import ConnectorContext
    from observatory.f1000_family import write_f1000_family_report
    from observatory.ids import content_hash
    from observatory.integrity import verify_raw_manifests
    from observatory.storage import NormalizedLake, RawStore

    root = Path("/volume/workspace")
    run_root = root / "results" / "observatory"
    included_platforms = (
        "f1000research",
        "wellcome_open_research",
        "gates_open_research",
        "nihr_open_research",
    )
    shard_receipts = []
    for platform_id in included_platforms:
        path = run_root / f"modal_f1000_{platform_id}_shard_receipt.json"
        if not path.exists():
            raise FileNotFoundError(f"missing completed F1000 shard: {platform_id}")
        row = json.loads(path.read_text())
        if (
            row.get("schema") != "observatory.modal-f1000-platform-shard/1"
            or row.get("platform_id") != platform_id
            or row.get("status") != "complete"
            or int(row.get("found_count") or 0) != int(row.get("expected_count") or -1)
        ):
            raise RuntimeError(f"F1000 shard receipt failed validation: {platform_id}")
        shard_receipts.append(row)

    connector = F1000ProcessConnector(page_size=100)
    context = ConnectorContext(
        workspace=root,
        fixture_dir=Path("/opt/observatory/fixtures"),
        cache_dir=root / "data" / "observatory" / "cache",
        parameters={"enumeration_snapshot": str(run_root / "f1000_enumeration_snapshot_v6.json")},
    )
    estimate = connector.count(context)
    expected = int(estimate.expected_objects or 0)
    found = sum(int(row["found_count"]) for row in shard_receipts)
    if found != expected:
        raise RuntimeError(f"F1000 shard union {found} does not match census {expected}")
    union_hash = content_hash(json.dumps([row["query_hash"] for row in shard_receipts], sort_keys=True))
    lake = NormalizedLake(root / "data" / "observatory" / "normalized")
    report_path = write_f1000_family_report(
        lake.root,
        run_root / "f1000_family_process_report.json",
        query_hash=union_hash,
        query_hashes=[row["query_hash"] for row in shard_receipts],
        provider_expected_objects=expected,
        found_count=found,
        platform_census=connector.platform_census,
        acquired_by_platform={row["platform_id"]: int(row["found_count"]) for row in shard_receipts},
    )
    report = json.loads(report_path.read_text())
    if not report.get("passes"):
        raise RuntimeError("F1000-family sharded process acceptance report failed")
    raw = RawStore(root / "data" / "observatory" / "raw")
    raw_audit = verify_raw_manifests(raw.root, source_ids={connector.source_id})
    lake_audit = lake.verify(source_id=connector.source_id)
    if not raw_audit["passes"] or not lake_audit["passes"]:
        raise RuntimeError(json.dumps({"raw": raw_audit, "lake": lake_audit}, sort_keys=True))

    removed_stage_dirs = []
    for row in shard_receipts:
        stage = run_root / "staging" / connector.source_id / row["query_hash"][:16]
        if stage.exists():
            shutil.rmtree(stage)
            removed_stage_dirs.append(str(stage))
    migration_path = run_root / "f1000_checkpoint_migration_receipt.json"
    retired_monolithic_checkpoint = None
    retired_monolithic_stage = None
    if migration_path.exists():
        migration = json.loads(migration_path.read_text())
        old_hash = str(migration["old_query_hash"])
        old_checkpoint = run_root / "checkpoints" / f"f1000_process-{old_hash[:12]}.json"
        old_stage = run_root / "staging" / connector.source_id / old_hash[:16]
        if old_checkpoint.exists():
            old_checkpoint.unlink()
            retired_monolithic_checkpoint = str(old_checkpoint)
        if old_stage.exists():
            shutil.rmtree(old_stage)
            retired_monolithic_stage = str(old_stage)

    receipt = {
        "schema": "observatory.modal-f1000-process-receipt/2",
        "status": "complete",
        "source_id": connector.source_id,
        "query_hash": union_hash,
        "found_count": found,
        "expected_count": expected,
        "normalized_version_count": report["normalized_version_count"],
        "normalization_ratio": report["normalization_ratio"],
        "unresolved_after_acquisition": report["unresolved_after_acquisition"],
        "shards": shard_receipts,
        "platform_census": connector.platform_census,
        "process_report": str(report_path),
        "raw_audit": {"passes": True, "checked": raw_audit["checked"]},
        "lake_audit": {"passes": True, "checked": lake_audit["checked"]},
        "removed_stage_dirs": removed_stage_dirs,
        "retired_monolithic_checkpoint": retired_monolithic_checkpoint,
        "retired_monolithic_stage": retired_monolithic_stage,
        "volume": VOLUME_NAME,
    }
    receipt_path = run_root / "modal_f1000_process_receipt.json"
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    volume.commit()
    return receipt


@app.function(
    image=image,
    cpu=4.0,
    memory=16_384,
    timeout=86_400,
    volumes={"/volume": volume},
)
def harvest_scipost_process(*, restart: bool = False, estimate_only: bool = False) -> dict:
    """Harvest the exact dated current-public SciPost process graph."""
    import json
    import shutil
    import sys
    from pathlib import Path

    sys.path.insert(0, "/opt/observatory/src")
    from observatory.adapters.scipost import SciPostProcessConnector
    from observatory.connectors.base import ConnectorContext
    from observatory.connectors.runner import RunOptions, estimate_resources, run_connector
    from observatory.integrity import verify_raw_manifests
    from observatory.scipost_process import write_scipost_process_report
    from observatory.storage import NormalizedLake, RawStore

    root = Path("/volume/workspace")
    connector = SciPostProcessConnector(page_size=20)
    context = ConnectorContext(
        workspace=root,
        fixture_dir=Path("/opt/observatory/fixtures"),
        cache_dir=root / "data" / "observatory" / "cache",
        parameters={"reuse_existing_raw_bundles": True},
    )
    estimate = connector.count(context)
    resource = estimate_resources(connector, context, estimate, limit=None)
    if resource["estimated_modal_cost_usd"] is None or resource["estimated_modal_cost_usd"] > MODAL_BUDGET_USD:
        raise RuntimeError(f"estimated cost violates ${MODAL_BUDGET_USD:.0f} Modal ceiling")
    if not resource["proof_passes"]:
        raise RuntimeError("SciPost process proof-of-access is absent, stale, or failing")
    if estimate_only:
        return {
            "estimate": estimate.__dict__,
            "resource_estimate": resource,
            "discovery": list(connector.discover(context)),
        }

    raw = RawStore(root / "data" / "observatory" / "raw")
    lake = NormalizedLake(root / "data" / "observatory" / "normalized")
    run_root = root / "results" / "observatory"
    result = run_connector(
        connector,
        context,
        raw_store=raw,
        lake=lake,
        run_root=run_root,
        options=RunOptions(restart=restart, estimate_storage=True, estimate_modal_cost=True),
    )
    report_path = write_scipost_process_report(
        lake.root,
        run_root / "scipost_process_report.json",
        query_hash=result["query_hash"],
        provider_expected_series=int(result["estimate"]["expected_objects"]),
        found_series=int(result["found_count"]),
    )
    report = json.loads(report_path.read_text())
    if not report["passes"]:
        raise RuntimeError("SciPost process acceptance report failed")
    raw_audit = verify_raw_manifests(raw.root, source_ids={connector.source_id})
    lake_audit = lake.verify(source_id=connector.source_id)
    if not raw_audit["passes"] or not lake_audit["passes"]:
        raise RuntimeError(json.dumps({"raw": raw_audit, "lake": lake_audit}, sort_keys=True))
    stage = run_root / "staging" / connector.source_id / result["query_hash"][:16]
    if stage.exists():
        shutil.rmtree(stage)
    receipt = {
        "status": result["status"],
        "source_id": result["source_id"],
        "query_hash": result["query_hash"],
        "found_count": result["found_count"],
        "tables": result["tables"],
        "coverage_count": result["coverage_count"],
        "resource_estimate": result["resource_estimate"],
        "raw_audit": {"passes": True, "checked": raw_audit["checked"]},
        "lake_audit": {"passes": True, "checked": lake_audit["checked"]},
        "process_report": str(report_path),
        "volume": VOLUME_NAME,
    }
    receipt_path = run_root / "modal_scipost_process_receipt.json"
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    volume.commit()
    return receipt


@app.local_entrypoint()
def main(
    source: str = "openreview",
    restart: bool = False,
    estimate_only: bool = False,
    platform: str = "",
    old_query_hash: str = "",
    snapshot_label: str = "current",
):
    import json

    if source == "openreview":
        result = normalize_openreview.remote(restart=restart, estimate_only=estimate_only)
    elif source == "openreview-audit":
        result = audit_openreview_public_invitations.remote(
            snapshot_label=snapshot_label,
            restart=restart,
        )
    elif source == "openreview-probe":
        result = probe_openreview_api.remote(restart=restart)
    elif source == "openreview-process":
        result = harvest_openreview_process.remote(restart=restart, estimate_only=estimate_only)
    elif source == "openreview-domain":
        result = harvest_openreview_domain_edits.remote(restart=restart, estimate_only=estimate_only)
    elif source == "crossref":
        result = harvest_crossref.remote(restart=restart, estimate_only=estimate_only)
    elif source == "fulltext":
        result = benchmark_fulltext.remote(estimate_only=estimate_only)
    elif source == "copernicus":
        result = harvest_copernicus.remote(restart=restart, estimate_only=estimate_only)
    elif source == "copernicus-crossref":
        result = harvest_copernicus_crossref.remote(restart=restart, estimate_only=estimate_only)
    elif source == "copernicus-outcomes":
        result = finalize_copernicus_outcomes.remote(restart=restart)
    elif source == "elife":
        result = harvest_elife_process.remote(restart=restart, estimate_only=estimate_only)
    elif source == "f1000":
        result = harvest_f1000_process.remote(restart=restart, estimate_only=estimate_only)
    elif source == "f1000-platform":
        if not platform:
            raise ValueError("--platform is required for f1000-platform")
        result = harvest_f1000_platform.remote(platform, restart=restart, estimate_only=estimate_only)
    elif source == "f1000-migrate":
        if not old_query_hash:
            raise ValueError("--old-query-hash is required for f1000-migrate")
        result = migrate_f1000_research_checkpoint.remote(old_query_hash)
    elif source == "f1000-finalize":
        result = finalize_f1000_platforms.remote()
    elif source == "scipost":
        result = harvest_scipost_process.remote(restart=restart, estimate_only=estimate_only)
    else:
        raise ValueError(f"unsupported source: {source}")
    print(json.dumps(result, indent=2, sort_keys=True))
