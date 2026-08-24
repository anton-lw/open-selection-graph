"""End-to-end connector orchestration with checkpoints and manifests."""

from __future__ import annotations

import gzip
import inspect
import json
import os
import shutil
import tempfile
from contextlib import ExitStack
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..ids import content_hash, stable_id
from ..provenance import ProvenanceIndex, make_event
from ..schema import PRIMARY_KEYS
from ..storage import NormalizedLake, RawStore
from ..storage_guard import storage_preflight
from .base import (
    Connector,
    ConnectorContext,
    CoverageEvidence,
    FetchBatch,
    RawItem,
    coverage_observation_id,
)
from .checkpoint import CheckpointStore, query_hash


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, sort_keys=True, default=str)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def _atomic_gzip_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "wb") as handle:
            payload = json.dumps(value, sort_keys=True, default=str).encode() + b"\n"
            handle.write(gzip.compress(payload, compresslevel=6, mtime=0))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def _stage_files(stage_dir: Path) -> list[Path]:
    return sorted((*stage_dir.glob("batch-*.json"), *stage_dir.glob("batch-*.json.gz")))


def _read_stage(path: Path) -> dict[str, Any]:
    if path.suffix == ".gz":
        return json.loads(gzip.decompress(path.read_bytes()))
    return json.loads(path.read_text())


def _deduplicate_table_rows(table: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse repeats while retaining genuine cross-entity alias conflicts.

    A provider can exceptionally assign one canonical identifier to two native
    entities.  That is evidence to preserve for identity resolution, not a
    reason to discard either mapping or abort an otherwise complete harvest.
    All other primary-key semantic conflicts remain fatal.
    """
    key = PRIMARY_KEYS[table]
    volatile = {"source_object_id", "provenance_event_id", "observed_at", "record_version"}
    grouped: dict[Any, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(row[key], []).append(row)

    output: list[dict[str, Any]] = []
    for pk, group in grouped.items():
        semantic: dict[str, dict[str, Any]] = {}
        for row in group:
            digest = content_hash(
                json.dumps(
                    {name: value for name, value in row.items() if name not in volatile},
                    sort_keys=True,
                    default=str,
                )
            )
            semantic.setdefault(digest, row)
        if len(semantic) == 1:
            output.append(next(iter(semantic.values())))
            continue
        distinct = list(semantic.values())
        if table == "candidate":
            without_first_observed = {
                content_hash(
                    json.dumps(
                        {name: value for name, value in row.items() if name not in volatile | {"first_observed_at"}},
                        sort_keys=True,
                        default=str,
                    )
                )
                for row in distinct
            }
            if len(without_first_observed) == 1:
                # Version-local provider rows may rediscover one candidate in
                # separate resume batches. Its canonical first observation is
                # the earliest declared version date, independent of batch
                # order or the process that happened to emit the candidate.
                chosen = min(
                    distinct,
                    key=lambda row: (
                        str(row.get("first_observed_at") or "9999"),
                        str(row.get("source_object_id") or ""),
                    ),
                )
                output.append(chosen)
                continue
            without_mutable_snapshot_fields = {
                content_hash(
                    json.dumps(
                        {
                            name: value
                            for name, value in row.items()
                            if name
                            not in volatile
                            | {"first_observed_at", "canonical_title", "status"}
                        },
                        sort_keys=True,
                        default=str,
                    )
                )
                for row in distinct
            }
            if len(without_mutable_snapshot_fields) == 1:
                # A candidate is an entity, while title and status are
                # provider snapshots. Population-scale connectors can resume
                # in a fresh process and therefore rediscover the same entity
                # from a later version. Keep the earliest observation and the
                # latest non-null mutable values deterministically; every
                # contributing field-level provenance row remains in the
                # parallel field_provenance table.
                chronological = sorted(
                    distinct,
                    key=lambda row: (
                        str(row.get("observed_at") or ""),
                        str(row.get("source_object_id") or ""),
                    ),
                )
                chosen = dict(chronological[-1])
                first_observations = [
                    row["first_observed_at"]
                    for row in distinct
                    if row.get("first_observed_at") is not None
                ]
                chosen["first_observed_at"] = (
                    min(first_observations, key=str) if first_observations else None
                )
                for field in ("canonical_title", "status"):
                    latest = next(
                        (
                            row.get(field)
                            for row in reversed(chronological)
                            if row.get(field) is not None
                        ),
                        None,
                    )
                    chosen[field] = latest
                output.append(chosen)
                continue
        if table == "evaluation":
            without_embedding_forum = {
                content_hash(
                    json.dumps(
                        {name: value for name, value in row.items() if name not in volatile | {"forum_native_id"}},
                        sort_keys=True,
                        default=str,
                    )
                )
                for row in distinct
            }
            if len(without_embedding_forum) == 1:
                # F1000-family JATS repeats an unchanged review sub-article in
                # later article versions. The review DOI and explicit
                # reply-to version are authoritative; forum_native_id merely
                # identifies the wrapper in which it was rediscovered.
                output.append(
                    min(
                        distinct,
                        key=lambda row: (
                            str(row.get("observed_at") or "9999"),
                            str(row.get("source_object_id") or ""),
                        ),
                    )
                )
                continue
        if table == "content_artifact":
            without_wrapper_locator = {
                content_hash(
                    json.dumps(
                        {
                            name: value
                            for name, value in row.items()
                            if name not in volatile | {"size_bytes", "local_pointer", "source_url"}
                        },
                        sort_keys=True,
                        default=str,
                    )
                )
                for row in distinct
            }
            if len(without_wrapper_locator) == 1:
                embedded_process_artifact = any(
                    str(row.get("object_type") or "")
                    in {"reviewer-report", "response", "author-comment"}
                    for row in distinct
                )
                if embedded_process_artifact:
                    # F1000-family JATS repeats unchanged process sub-articles
                    # in later article versions. Canonicalize to the first
                    # wrapper in which the sub-article was observed.
                    chosen = min(
                        distinct,
                        key=lambda row: (
                            str(row.get("observed_at") or "9999"),
                            str(row.get("source_object_id") or ""),
                        ),
                    )
                else:
                    # Metadata feeds can repeat one DOI in wrappers with
                    # different byte sizes or page indices. Prefer the richer
                    # wrapper and, for equal-size rows, the later pointer.
                    chosen = max(
                        distinct,
                        key=lambda row: (
                            int(row.get("size_bytes") or 0),
                            str(row.get("local_pointer") or ""),
                            str(row.get("source_object_id") or ""),
                        ),
                    )
                output.append(chosen)
                continue
        alias_values = {
            (
                row.get("scheme"),
                row.get("canonical_value") or row.get("value"),
            )
            for row in distinct
        }
        entities = {(row.get("entity_kind"), row.get("entity_id")) for row in distinct}
        if table != "identifier_alias" or len(alias_values) != 1 or len(entities) < 2:
            raise ValueError(f"conflicting {table}.{key}: {pk}")
        for row in distinct:
            preserved = dict(row)
            preserved[key] = stable_id(
                "identifier_alias",
                str(row.get("source_id") or "unknown"),
                "|".join(
                    (
                        str(row.get("entity_kind") or "unknown"),
                        str(row.get("entity_id") or "unknown"),
                        str(row.get("scheme") or "unknown"),
                        str(row.get("canonical_value") or row.get("value") or "unknown"),
                    )
                ),
            )
            preserved["conflict_status"] = "provider_alias_maps_multiple_entities"
            output.append(preserved)
    if len({row[key] for row in output}) != len(output):
        raise ValueError(f"derived conflict keys are not unique for {table}.{key}")
    return output


def _load_stages(stage_dir: Path) -> dict[str, list[dict[str, Any]]]:
    buffers: dict[str, list[dict[str, Any]]] = {}
    for path in _stage_files(stage_dir):
        stage = _read_stage(path)
        for table, rows in (stage.get("tables") or {}).items():
            buffers.setdefault(table, []).extend(rows)
    return buffers


def _coverage_map(rows: list[CoverageEvidence]) -> dict[str, CoverageEvidence]:
    return {f"{row.gate_cycle_id}|{row.object_type}": row for row in rows}


def _coverage_deltas(before: dict[str, CoverageEvidence], after: dict[str, CoverageEvidence]) -> list[dict[str, Any]]:
    deltas = []
    for key, row in sorted(after.items()):
        prior = before.get(key)
        delta = row.found_count - (prior.found_count if prior else 0)
        if delta or prior is None:
            deltas.append({**asdict(row), "found_count_delta": delta})
    return deltas


def _aggregate_coverage(stage_dir: Path, fallback: list[CoverageEvidence]) -> list[CoverageEvidence]:
    combined: dict[str, dict[str, Any]] = {}
    saw_deltas = False
    for path in _stage_files(stage_dir):
        for row in _read_stage(path).get("coverage_deltas") or []:
            saw_deltas = True
            key = f"{row['gate_cycle_id']}|{row['object_type']}"
            current = combined.setdefault(
                key,
                {
                    **{k: v for k, v in row.items() if k not in {"found_count", "found_count_delta"}},
                    "found_count": 0,
                },
            )
            current.update({k: v for k, v in row.items() if k not in {"found_count", "found_count_delta"}})
            current["found_count"] = int(current.get("found_count") or 0) + int(row["found_count_delta"])
    if not saw_deltas:
        return fallback
    return [CoverageEvidence(**row) for _, row in sorted(combined.items())]


def _compile_stages_streaming(
    stage_dir: Path,
    lake: NormalizedLake,
    *,
    source_id: str,
    qhash: str,
    buckets: int = 128,
) -> tuple[dict[str, int], dict[str, list[str]]]:
    """Disk-bound deterministic deduplication for population-scale runs.

    Buckets live on container-local scratch, never on a network/object-backed
    mounted volume.  Some such volumes do not implement append faithfully; a
    population OpenReview compile exposed a valid gzip header being replaced by
    only its final empty member.  Immutable stage inputs and final Parquet
    outputs remain on durable storage, while this wholly derivable scratch is
    removed automatically.
    """
    legacy_bucket_root = stage_dir / "compile-buckets"
    if legacy_bucket_root.exists():
        shutil.rmtree(legacy_bucket_root)
    with tempfile.TemporaryDirectory(prefix="observatory-compile-") as scratch:
        bucket_root = Path(scratch)
        # Opening and closing every table/bucket gzip stream once per upstream
        # page made the derivable compile phase dominate full-population runs
        # (for example 1,090 Crossref pages x 128 buckets).  Scratch lives on
        # the container's local filesystem, so keeping the lazily-created
        # writers open for this bounded table x bucket set is both safe and
        # orders of magnitude cheaper. ExitStack guarantees complete gzip
        # trailers before any bucket is read.
        with ExitStack() as stack:
            writers: dict[tuple[str, int], Any] = {}
            for stage_path in _stage_files(stage_dir):
                stage = _read_stage(stage_path)
                for table, rows in (stage.get("tables") or {}).items():
                    key_name = PRIMARY_KEYS[table]
                    for row in rows:
                        bucket = int(content_hash(str(row[key_name]))[:8], 16) % buckets
                        writer_key = (table, bucket)
                        handle = writers.get(writer_key)
                        if handle is None:
                            target = bucket_root / table / f"{bucket:03d}.jsonl.gz"
                            target.parent.mkdir(parents=True, exist_ok=True)
                            handle = stack.enter_context(
                                gzip.open(
                                    target,
                                    "wt",
                                    encoding="utf-8",
                                    compresslevel=3,
                                )
                            )
                            writers[writer_key] = handle
                        handle.write(json.dumps(row, sort_keys=True, default=str, separators=(",", ":")) + "\n")

        table_counts: dict[str, int] = {}
        written: dict[str, list[str]] = {}
        prefix = f"run-{qhash[:16]}-"
        tables = sorted(path.name for path in bucket_root.iterdir())
        for table in tables:
            old = {path for path in lake.files(table) if path.name.startswith(prefix)}
            new: set[Path] = set()
            key_name = PRIMARY_KEYS[table]
            for bucket_path in sorted((bucket_root / table).glob("*.jsonl.gz")):
                bucket_rows = []
                with gzip.open(bucket_path, "rt", encoding="utf-8") as handle:
                    for line in handle:
                        bucket_rows.append(json.loads(line))
                deduplicated = _deduplicate_table_rows(table, bucket_rows)
                unique = {row[key_name]: row for row in deduplicated}
                if not unique:
                    continue
                shard = lake.write(
                    table,
                    [unique[key] for key in sorted(unique, key=str)],
                    partition={"source_id": source_id},
                    shard_name=f"{prefix}b{bucket_path.stem.split('.')[0]}.parquet",
                )
                new.add(shard)
                written.setdefault(table, []).append(str(shard))
                table_counts[table] = table_counts.get(table, 0) + len(unique)
            for stale in sorted(old - new):
                lake.remove_shard(stale)
        return table_counts, written


def _fixture_batches(
    connector: Connector,
    context: ConnectorContext,
    raw_store: RawStore,
    *,
    cursor: str | None,
    limit: int | None,
) -> list[FetchBatch]:
    """Replay the committed proof manifest without contacting the provider."""
    proof_path = context.fixture_dir / connector.source_id / "probe_manifest.json"
    if not proof_path.exists():
        raise ValueError(f"fixture replay unavailable: {proof_path} does not exist")
    proof = json.loads(proof_path.read_text())
    if not proof.get("passes") or int(proof.get("object_count") or 0) < 1:
        raise ValueError("fixture replay blocked: proof manifest does not pass")
    wanted = list(zip(proof["native_ids"], proof["object_hashes"]))
    if limit is not None:
        wanted = wanted[:limit]
    start = int(cursor or 0)
    manifest_path = raw_store.manifests / f"{connector.source_id}.jsonl"
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for line in manifest_path.read_text().splitlines():
        if line.strip():
            row = json.loads(line)
            latest[(str(row["native_id"]), str(row["byte_hash"]))] = row
    items: list[RawItem] = []
    for native_id, byte_hash in wanted[start:]:
        receipt = latest.get((str(native_id), str(byte_hash)))
        if receipt is None:
            raise ValueError(f"fixture raw object missing: {native_id} {byte_hash}")
        metadata = dict(receipt.get("metadata") or {})
        items.append(
            RawItem(
                native_id=str(native_id),
                object_type=str(receipt["object_type"]),
                payload=raw_store.get(str(byte_hash)),
                source_url=metadata.get("source_url"),
                licence=metadata.get("licence"),
                release_class="pointer_hash",
                metadata=metadata,
            )
        )
    return [
        FetchBatch(tuple(items), None, True, f"fixture:{connector.source_id}:{proof.get('query_hash')}", len(wanted))
    ]


@dataclass(frozen=True)
class RunOptions:
    limit: int | None = None
    fixture: bool = False
    dry_run: bool = False
    no_text: bool = False
    restart: bool = False
    estimate_storage: bool = False
    estimate_modal_cost: bool = False


def estimate_resources(
    connector: Connector, context: ConnectorContext, estimate, *, limit: int | None
) -> dict[str, Any]:
    planned_limit_objects = max(int(limit * estimate.objects_per_limit_unit), 1) if limit is not None else None
    objects = (
        min(estimate.expected_objects, planned_limit_objects)
        if planned_limit_objects is not None and estimate.expected_objects is not None
        else (planned_limit_objects if planned_limit_objects is not None else estimate.expected_objects)
    )
    proof = context.fixture_dir / connector.source_id / "probe_manifest.json"
    average_bytes = 4096
    proof_passes = False
    proof_connector_version = None
    if proof.exists():
        row = json.loads(proof.read_text())
        proof_connector_version = str(row.get("connector_version"))
        proof_passes = bool(row.get("passes")) and proof_connector_version == connector.connector_version
        sizes = row.get("object_size_bytes") or []
        if sizes:
            average_bytes = max(sum(sizes) // len(sizes), 1)
    expected_bytes = estimate.expected_bytes or (objects * average_bytes if objects is not None else None)
    requests = estimate.expected_requests
    if limit is not None and estimate.requests_per_limit_unit is not None:
        requests = max(int(limit * estimate.requests_per_limit_unit + 0.999999), 1)
    if requests is None and objects is not None:
        requests = max((objects + 199) // 200, 1)
    cpu_hours = (expected_bytes / 1_000_000_000 * 0.25) if expected_bytes is not None else None
    modal_cost = cpu_hours * 0.20 if cpu_hours is not None else None
    return {
        "objects": objects,
        "average_fixture_bytes": average_bytes,
        "expected_bytes": expected_bytes,
        "expected_requests": requests,
        "estimated_cpu_hours": cpu_hours,
        "estimated_modal_cost_usd": modal_cost,
        "modal_budget_usd": 30.0,
        "within_modal_budget": modal_cost is not None and modal_cost <= 30.0,
        "proof_manifest": str(proof),
        "proof_passes": proof_passes,
        "proof_connector_version": proof_connector_version,
        "assumptions": "fixture mean size; 0.25 CPU-hours/GB; $0.20/CPU-hour ceiling",
    }


def run_connector(
    connector: Connector,
    context: ConnectorContext,
    *,
    raw_store: RawStore,
    lake: NormalizedLake,
    run_root: Path,
    options: RunOptions | None = None,
) -> dict[str, Any]:
    opts = options or RunOptions()
    context.no_text = opts.no_text
    params = {
        "source_id": connector.source_id,
        "version": connector.connector_version,
        "parameters": context.parameters,
        "since": context.since,
        "until": context.until,
        "limit": opts.limit,
        "fixture": opts.fixture,
        "no_text": opts.no_text,
    }
    qhash = query_hash(params)
    checkpoint_path = run_root / "checkpoints" / f"{connector.source_id}-{qhash[:12]}.json"
    checkpoint_store = CheckpointStore(checkpoint_path)
    # A completed immutable run is a frozen acquisition result. Returning it
    # must not depend on a fresh provider count request: that is both slower and
    # less reproducible when an endpoint is temporarily unavailable. Dry runs
    # and explicit restarts still perform a live estimate by design.
    if not opts.restart and not opts.dry_run:
        frozen = checkpoint_store.load(
            source_id=connector.source_id,
            expected_query_hash=qhash,
        )
        if frozen.complete:
            completed_manifest = run_root / "runs" / f"{connector.source_id}-{qhash[:12]}.json"
            if completed_manifest.exists():
                return json.loads(completed_manifest.read_text())
            frozen.complete = False
            frozen.fetch_complete = True
            checkpoint_store.save(frozen)
    estimate = connector.count(context)
    resource_estimate = estimate_resources(connector, context, estimate, limit=opts.limit)
    if opts.dry_run:
        return {
            "status": "dry_run",
            "source_id": connector.source_id,
            "estimate": asdict(estimate),
            "discovered": list(connector.discover(context)),
            "fixture_validation": connector.validate_fixture(context),
            "resource_estimate": resource_estimate,
        }
    projected_bytes = int(resource_estimate.get("expected_bytes") or 0)
    storage_receipt = storage_preflight(
        run_root,
        projected_input_bytes=projected_bytes,
        projected_output_bytes=projected_bytes,
    )
    if not opts.fixture and opts.limit is None and (estimate.expected_objects or 0) > 100:
        fixture = connector.validate_fixture(context)
        if not fixture.get("passes") or not resource_estimate["proof_passes"]:
            raise ValueError("full harvest blocked: successful fixture and 100-object smoke manifest required")
        if resource_estimate["estimated_modal_cost_usd"] is not None and not resource_estimate["within_modal_budget"]:
            raise ValueError("full harvest blocked: estimated Modal cost exceeds constitutional $30 ceiling")
    try:
        connector_code_hash = content_hash(inspect.getsource(connector.__class__))
    except (OSError, TypeError):
        connector_code_hash = content_hash(f"{connector.__class__.__module__}.{connector.__class__.__qualname__}")
    stage_dir = run_root / "staging" / connector.source_id / qhash[:16]
    projected_objects = opts.limit if opts.limit is not None else estimate.expected_objects
    streaming = bool(not opts.fixture and (connector.force_streaming or (projected_objects or 0) > 25_000))
    if opts.restart and checkpoint_path.exists():
        checkpoint_path.unlink()
    if opts.restart and stage_dir.exists():
        for path in _stage_files(stage_dir):
            path.unlink()
        bucket_root = stage_dir / "compile-buckets"
        if bucket_root.exists():
            shutil.rmtree(bucket_root)
    checkpoint = checkpoint_store.load(source_id=connector.source_id, expected_query_hash=qhash)

    provenance = ProvenanceIndex(run_root / "provenance" / f"{connector.source_id}.jsonl")
    table_buffers = {} if streaming else _load_stages(stage_dir)
    if checkpoint.started_at is None:
        checkpoint.started_at = datetime.now(timezone.utc).isoformat(timespec="microseconds")
        checkpoint_store.save(checkpoint)
    # A restarted run retains one retrieval timestamp.  This makes normalized
    # output independent of where an interruption happened.
    retrieved_at = checkpoint.started_at
    batches = (
        []
        if checkpoint.fetch_complete
        else (
            _fixture_batches(connector, context, raw_store, cursor=checkpoint.cursor, limit=opts.limit)
            if opts.fixture
            else connector.fetch(context, cursor=checkpoint.cursor, limit=opts.limit)
        )
    )
    for batch in batches:
        stage_path = stage_dir / (
            f"batch-{checkpoint.batch_count + 1:08d}.json.gz"
            if streaming
            else f"batch-{checkpoint.batch_count + 1:08d}.json"
        )
        if stage_path.exists():
            staged = _read_stage(stage_path)
            if staged.get("request_fingerprint") != batch.request_fingerprint:
                raise ValueError("upstream cursor/resorting drift: staged request fingerprint changed")
            checkpoint.cursor = staged.get("cursor")
            checkpoint.batch_count += 1
            checkpoint.found_count += int(staged.get("found_delta") or 0)
            checkpoint.last_native_id = staged.get("last_native_id")
            checkpoint.last_batch_hash = staged.get("last_batch_hash")
            checkpoint.fetch_complete = bool(staged.get("done"))
            checkpoint_store.save(checkpoint)
            continue
        before_coverage = _coverage_map(list(connector.emit_coverage(context, found_count=checkpoint.found_count)))
        batch_buffers: dict[str, list[dict[str, Any]]] = {}
        batch_hash_parts: list[str] = []
        for item in batch.items:
            receipt = raw_store.put(
                source_id=connector.source_id,
                native_id=item.native_id,
                object_type=item.object_type,
                payload=item.payload,
                metadata={**dict(item.metadata), "source_url": item.source_url},
                retrieved_at=retrieved_at,
            )
            event = make_event(
                source_id=connector.source_id,
                source_object_id=receipt.source_object_id,
                event_type="normalize",
                parser_name=connector.__class__.__name__,
                parser_version=connector.connector_version,
                code_hash=connector_code_hash,
                input_hash=receipt.byte_hash,
                parameters=params,
                occurred_at=retrieved_at,
            )
            normalized = list(
                connector.normalize(
                    item,
                    source_object_id=receipt.source_object_id,
                    provenance_event_id=event.provenance_event_id,
                )
            )
            output_hash = content_hash(json.dumps([dict(n.row) for n in normalized], sort_keys=True, default=str))
            event = make_event(
                source_id=connector.source_id,
                source_object_id=receipt.source_object_id,
                event_type="normalize",
                parser_name=connector.__class__.__name__,
                parser_version=connector.connector_version,
                code_hash=connector_code_hash,
                input_hash=receipt.byte_hash,
                output_hash=output_hash,
                parameters=params,
                occurred_at=event.occurred_at,
            )
            provenance.append(event)
            source_row = {
                "source_object_id": receipt.source_object_id,
                "source_id": connector.source_id,
                "native_id": item.native_id,
                "object_type": item.object_type,
                "source_url": item.source_url,
                "created_at": item.created_at,
                "modified_at": item.modified_at,
                "retrieved_at": receipt.retrieved_at,
                "deleted_at": None,
                "byte_hash": receipt.byte_hash,
                "raw_pointer": receipt.raw_pointer,
                "http_status": item.http_status,
                "etag": item.etag,
                "last_modified": item.last_modified,
                "licence": item.licence,
                "release_class": item.release_class,
                "status": "active",
            }
            batch_buffers.setdefault("source_object", []).append(source_row)
            batch_buffers.setdefault("provenance_event", []).append(asdict(event))
            for record in normalized:
                row = dict(record.row)
                row.setdefault("source_id", connector.source_id)
                row.setdefault("source_object_id", receipt.source_object_id)
                row.setdefault("provenance_event_id", event.provenance_event_id)
                if row.get("observed_at") is None:
                    row["observed_at"] = retrieved_at
                row.setdefault("record_version", 1)
                batch_buffers.setdefault(record.table, []).append(row)
                if record.table != "field_provenance":
                    record_id = str(row[PRIMARY_KEYS[record.table]])
                    ignored = {
                        PRIMARY_KEYS[record.table],
                        "source_id",
                        "source_object_id",
                        "provenance_event_id",
                        "observed_at",
                        "record_version",
                    }
                    for field_name, value in row.items():
                        if field_name in ignored or value is None:
                            continue
                        batch_buffers.setdefault("field_provenance", []).append(
                            {
                                "field_provenance_id": stable_id(
                                    "field_provenance",
                                    connector.source_id,
                                    f"{record.table}|{record_id}|{field_name}|{receipt.source_object_id}",
                                ),
                                "table_name": record.table,
                                "record_id": record_id,
                                "field_name": field_name,
                                "source_object_id": receipt.source_object_id,
                                "provenance_event_id": event.provenance_event_id,
                                "source_selector": f"adapter-output:{field_name}",
                                "confidence": 1.0,
                                "override_reason": None,
                                "observed_at": row["observed_at"],
                            }
                        )
            batch_hash_parts.append(receipt.byte_hash)
        batch_hash = content_hash("".join(batch_hash_parts))
        stage = {
            "request_fingerprint": batch.request_fingerprint,
            "cursor": batch.cursor,
            "done": batch.done,
            "found_delta": len(batch.items),
            "last_native_id": batch.items[-1].native_id if batch.items else checkpoint.last_native_id,
            "last_batch_hash": batch_hash,
            "tables": batch_buffers,
            "coverage_deltas": _coverage_deltas(
                before_coverage,
                _coverage_map(
                    list(connector.emit_coverage(context, found_count=checkpoint.found_count + len(batch.items)))
                ),
            ),
        }
        if streaming:
            _atomic_gzip_json(stage_path, stage)
        else:
            _atomic_json(stage_path, stage)
            for table, rows in batch_buffers.items():
                table_buffers.setdefault(table, []).extend(rows)
        checkpoint.cursor = batch.cursor
        checkpoint.batch_count += 1
        checkpoint.found_count += len(batch.items)
        checkpoint.last_native_id = stage["last_native_id"]
        checkpoint.last_batch_hash = batch_hash
        checkpoint.fetch_complete = batch.done
        checkpoint_store.save(checkpoint)

    # Streaming runs otherwise decompress every durable stage twice: once for
    # coverage aggregation and again for compile fan-out. Modal Volumes have
    # substantially higher per-file latency than container-local scratch, so
    # cache bounded stage sets once and reuse them for both derivable passes.
    # The durable originals remain authoritative and untouched. Keeping the
    # ceiling conservative leaves room for the compile buckets on ephemeral
    # disk; larger runs fall back to direct streaming.
    working_stage_dir = stage_dir
    local_stage_cache: tempfile.TemporaryDirectory[str] | None = None
    local_stage_cache_bytes = 0
    if streaming:
        durable_stage_files = _stage_files(stage_dir)
        local_stage_cache_bytes = sum(path.stat().st_size for path in durable_stage_files)
        if local_stage_cache_bytes <= 4 * 1024**3:
            local_stage_cache = tempfile.TemporaryDirectory(prefix="observatory-stage-cache-")
            working_stage_dir = Path(local_stage_cache.name)
            for path in durable_stage_files:
                shutil.copyfile(path, working_stage_dir / path.name)

    coverage_rows = []
    evidence_rows = _aggregate_coverage(
        working_stage_dir,
        list(connector.emit_coverage(context, found_count=checkpoint.found_count)),
    )
    coverage_payload = json.dumps({"estimate": asdict(estimate), "parameters": params}, sort_keys=True)
    coverage_receipt = raw_store.put(
        source_id=connector.source_id,
        native_id=f"coverage:{qhash}",
        object_type="coverage_assessment",
        payload=coverage_payload,
        metadata={"synthetic": True},
        retrieved_at=retrieved_at,
    )
    coverage_stub_source = coverage_receipt.source_object_id
    coverage_event = make_event(
        source_id=connector.source_id,
        source_object_id=coverage_stub_source,
        event_type="coverage_assessment",
        parser_name=connector.__class__.__name__,
        parser_version=connector.connector_version,
        code_hash=connector_code_hash,
        parameters=params,
        occurred_at=retrieved_at,
    )
    provenance.append(coverage_event)
    coverage_buffers: dict[str, list[dict[str, Any]]] = {} if streaming else table_buffers
    coverage_buffers.setdefault("source_object", []).append(
        {
            "source_object_id": coverage_stub_source,
            "source_id": connector.source_id,
            "native_id": f"coverage:{qhash}",
            "object_type": "coverage_assessment",
            "source_url": None,
            "created_at": retrieved_at,
            "modified_at": None,
            "retrieved_at": retrieved_at,
            "deleted_at": None,
            "byte_hash": coverage_receipt.byte_hash,
            "raw_pointer": coverage_receipt.raw_pointer,
            "http_status": 200,
            "etag": None,
            "last_modified": None,
            "licence": "generated-metadata",
            "release_class": "redistribute",
            "status": "active",
        }
    )
    coverage_buffers.setdefault("provenance_event", []).append(asdict(coverage_event))
    for evidence in evidence_rows:
        ratio = evidence.found_count / evidence.expected_count if evidence.expected_count not in (None, 0) else None
        coverage_rows.append(
            {
                "coverage_observation_id": coverage_observation_id(
                    connector.source_id, evidence.gate_cycle_id, evidence.object_type
                ),
                "gate_cycle_id": evidence.gate_cycle_id,
                "object_type": evidence.object_type,
                "earliest_public_stage": evidence.earliest_public_stage,
                "observability_grade": evidence.observability_grade,
                "expected_count": evidence.expected_count,
                "found_count": evidence.found_count,
                "coverage_ratio": ratio,
                "expected_count_method": evidence.expected_count_method,
                "query_or_invitation": evidence.query_or_invitation,
                "known_hidden_stages": list(evidence.known_hidden_stages),
                "known_exclusions": list(evidence.known_exclusions),
                "missing_reason": evidence.missing_reason,
                "audit_status": evidence.audit_status,
                "valid_from": retrieved_at,
                "valid_to": None,
                "source_id": connector.source_id,
                "source_object_id": coverage_stub_source,
                "provenance_event_id": coverage_event.provenance_event_id,
                "observed_at": retrieved_at,
                "record_version": 1,
            }
        )
    if coverage_rows:
        coverage_buffers.setdefault("coverage_observation", []).extend(coverage_rows)

    if streaming:
        coverage_stage = {"tables": coverage_buffers}
        _atomic_gzip_json(stage_dir / "batch-coverage.json.gz", coverage_stage)
        if working_stage_dir != stage_dir:
            _atomic_gzip_json(working_stage_dir / "batch-coverage.json.gz", coverage_stage)
        table_counts, written = _compile_stages_streaming(
            working_stage_dir,
            lake,
            source_id=connector.source_id,
            qhash=qhash,
            buckets=int(getattr(connector, "compile_buckets", 128)),
        )
    else:
        written: dict[str, str] = {}
        for table, rows in sorted(table_buffers.items()):
            if not rows:
                continue
            # Connectors may encounter the same entity through more than one raw
            # object. Exact semantic repeats collapse; disagreements are fatal.
            rows = _deduplicate_table_rows(table, rows)
            table_buffers[table] = rows
            path = lake.write(
                table,
                rows,
                partition={"source_id": connector.source_id},
                shard_name=f"run-{qhash[:16]}.parquet",
            )
            written[table] = str(path)
        table_counts = {table: len(rows) for table, rows in table_buffers.items()}
    checkpoint.complete = True
    checkpoint_store.save(checkpoint)
    result = {
        "status": "complete",
        "source_id": connector.source_id,
        "connector_version": connector.connector_version,
        "query_hash": qhash,
        "estimate": asdict(estimate),
        "resource_estimate": resource_estimate,
        "storage_preflight": storage_receipt,
        "connector_code_hash": connector_code_hash,
        "found_count": checkpoint.found_count,
        "batch_count": checkpoint.batch_count,
        "tables": table_counts,
        "written": written,
        "coverage": [asdict(row) for row in evidence_rows[:100]],
        "coverage_count": len(evidence_rows),
        "coverage_truncated_in_manifest": len(evidence_rows) > 100,
        "streaming_compile": streaming,
        "local_stage_cache_bytes": (local_stage_cache_bytes if local_stage_cache is not None else 0),
        "completed_at": datetime.now(timezone.utc).isoformat(timespec="microseconds"),
    }
    output_hashes: dict[str, str | list[str]] = {}
    for table, value in sorted(written.items()):
        paths = value if isinstance(value, list) else [value]
        hashes = [content_hash(Path(path).read_bytes()) for path in paths if Path(path).is_file()]
        output_hashes[table] = hashes if isinstance(value, list) else hashes[0]
    result["output_hashes"] = output_hashes
    result["run_manifest_hash"] = content_hash(json.dumps(result, sort_keys=True))
    result_path = run_root / "runs" / f"{connector.source_id}-{qhash[:12]}.json"
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result
