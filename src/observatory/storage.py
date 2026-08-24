"""Immutable raw store, normalized Parquet lake, and DuckDB catalog."""

from __future__ import annotations

import gzip
import json
import os
import sqlite3
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from .ids import content_hash, stable_id
from .schema import PRIMARY_KEYS, TABLE_SCHEMAS, pyarrow_schema, validate_records


def _stamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _atomic_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


@dataclass(frozen=True)
class RawReceipt:
    source_object_id: str
    source_id: str
    native_id: str
    object_type: str
    byte_hash: str
    raw_pointer: str
    size_bytes: int
    retrieved_at: str
    created: bool


class RawStore:
    def __init__(self, root: Path):
        self.root = root
        self.objects = root / "objects"
        self.manifests = root / "manifests"
        self.packs = root / "packs"
        self.pack_index = self.packs / "index.sqlite3"
        self._pack_connection: sqlite3.Connection | None = None

    def _packed_location(self, byte_hash: str) -> tuple[Path, int, int] | None:
        if not self.pack_index.exists():
            return None
        if self._pack_connection is None:
            self._pack_connection = sqlite3.connect(
                f"file:{self.pack_index}?mode=ro", uri=True
            )
        row = self._pack_connection.execute(
            "SELECT pack_name, byte_offset, byte_length FROM packed_object "
            "WHERE byte_hash = ?",
            [byte_hash],
        ).fetchone()
        if row is None:
            return None
        pack_name = str(row[0])
        if Path(pack_name).name != pack_name:
            raise ValueError("unsafe raw pack index path")
        return self.packs / pack_name, int(row[1]), int(row[2])

    def put(
        self,
        *,
        source_id: str,
        native_id: str,
        object_type: str,
        payload: bytes | str,
        metadata: Mapping[str, Any] | None = None,
        retrieved_at: str | None = None,
    ) -> RawReceipt:
        data = payload.encode("utf-8") if isinstance(payload, str) else payload
        digest = content_hash(data)
        path = self.objects / digest[:2] / f"{digest}.gz"
        packed = self._packed_location(digest)
        created = not path.exists() and packed is None
        if created:
            _atomic_bytes(path, gzip.compress(data, compresslevel=6, mtime=0))
        raw_pointer = (
            f"{packed[0]}#{packed[1]}:{packed[2]}" if packed else str(path)
        )
        stamp = retrieved_at or _stamp()
        # A native identifier can be mutated or deleted by a provider.  Binding
        # the source-object identity to the retrieved bytes preserves every
        # point-in-time version instead of silently reusing an old identity.
        source_object_id = stable_id("source_object", source_id, f"{native_id}|{digest}")
        receipt = RawReceipt(
            source_object_id=source_object_id,
            source_id=source_id,
            native_id=native_id,
            object_type=object_type,
            byte_hash=digest,
            raw_pointer=raw_pointer,
            size_bytes=len(data),
            retrieved_at=stamp,
            created=created,
        )
        manifest = {**asdict(receipt), "metadata": dict(metadata or {})}
        self._append_manifest(source_id, manifest)
        return receipt

    def get(self, byte_hash: str) -> bytes:
        path = self.objects / byte_hash[:2] / f"{byte_hash}.gz"
        if path.exists():
            return gzip.decompress(path.read_bytes())
        packed = self._packed_location(byte_hash)
        if packed is None:
            raise FileNotFoundError(path)
        pack_path, offset, length = packed
        with pack_path.open("rb") as handle:
            handle.seek(offset)
            compressed = handle.read(length)
        if len(compressed) != length:
            raise OSError(f"truncated raw pack member: {byte_hash}")
        return gzip.decompress(compressed)

    def pack_source(self, source_id: str) -> dict[str, Any]:
        """Compact one inactive source's immutable objects without changing hashes.

        Manifests stay untouched. The pack is committed and indexed before loose
        objects are removed, so interruption can at worst leave safe duplicates.
        """
        manifest = self.manifests / f"{source_id}.jsonl"
        if not manifest.exists():
            raise FileNotFoundError(manifest)
        manifest_lines = manifest.read_text().splitlines()
        hashes = sorted({
            str(json.loads(line)["byte_hash"])
            for line in manifest_lines
            if line.strip()
        })
        self.packs.mkdir(parents=True, exist_ok=True)
        pack_name = f"{source_id}-{content_hash(json.dumps(hashes))[:16]}.pack"
        pack_path = self.packs / pack_name
        loose = [
            (digest, self.objects / digest[:2] / f"{digest}.gz")
            for digest in hashes
            if (self.objects / digest[:2] / f"{digest}.gz").exists()
        ]
        indexed_hashes: set[str] = set()
        if pack_path.exists() and self.pack_index.exists():
            connection = sqlite3.connect(self.pack_index)
            try:
                indexed_hashes = {
                    str(row[0])
                    for row in connection.execute(
                        "SELECT byte_hash FROM packed_object WHERE pack_name = ?",
                        [pack_name],
                    )
                }
            finally:
                connection.close()
        indexed_complete = set(hashes) <= indexed_hashes
        if indexed_complete:
            probes = (
                [hashes[0], hashes[len(hashes) // 2], hashes[-1]] if hashes else []
            )
            if not all(self.verify(digest) for digest in probes):
                raise ValueError("existing raw pack failed post-commit probes")
            for _digest, path in loose:
                path.unlink()
            return {
                "source_id": source_id,
                "manifest_receipts": len(manifest_lines),
                "unique_hashes": len(hashes),
                "packed_loose_objects": 0,
                "removed_loose_objects": len(loose),
                "pack_path": str(pack_path),
                "pack_size_bytes": pack_path.stat().st_size,
                "verified_members": len(hashes),
                "post_removal_probe_hashes": probes,
                "resumed_committed_pack": bool(loose),
                "passes": True,
            }
        if not loose:
            raise ValueError("raw objects are neither loose nor completely indexed")
        temporary = self.packs / f".{pack_name}.tmp"
        locations: list[tuple[str, str, int, int]] = []
        with temporary.open("wb") as target:
            for digest, path in loose:
                compressed = path.read_bytes()
                if content_hash(gzip.decompress(compressed)) != digest:
                    raise ValueError(f"corrupt loose raw object: {digest}")
                offset = target.tell()
                target.write(compressed)
                locations.append((digest, pack_name, offset, len(compressed)))
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, pack_path)
        connection = sqlite3.connect(self.pack_index)
        try:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS packed_object ("
                "byte_hash TEXT PRIMARY KEY, pack_name TEXT NOT NULL, "
                "byte_offset INTEGER NOT NULL, byte_length INTEGER NOT NULL)"
            )
            connection.executemany(
                "INSERT OR REPLACE INTO packed_object "
                "(byte_hash, pack_name, byte_offset, byte_length) VALUES (?, ?, ?, ?)",
                locations,
            )
            connection.commit()
        finally:
            connection.close()
        if self._pack_connection is not None:
            self._pack_connection.close()
            self._pack_connection = None
        # Verify the committed pack sequentially. This checks every compressed
        # member and its declared offset without hundreds of thousands of
        # random opens against a mounted volume.
        with pack_path.open("rb") as packed_handle:
            for digest, _pack_name, offset, length in locations:
                packed_handle.seek(offset)
                compressed = packed_handle.read(length)
                if (
                    len(compressed) != length
                    or content_hash(gzip.decompress(compressed)) != digest
                ):
                    raise ValueError(f"raw pack verification failed: {digest}")
        for _digest, path in loose:
            path.unlink()
        for prefix in sorted(self.objects.iterdir()):
            if prefix.is_dir():
                try:
                    prefix.rmdir()
                except OSError:
                    pass
        probes = [hashes[0], hashes[len(hashes) // 2], hashes[-1]]
        passes = all(self.verify(digest) for digest in probes)
        return {
            "source_id": source_id,
            "manifest_receipts": len(manifest_lines),
            "unique_hashes": len(hashes),
            "packed_loose_objects": len(loose),
            "removed_loose_objects": len(loose),
            "pack_path": str(pack_path),
            "pack_size_bytes": pack_path.stat().st_size,
            "verified_members": len(locations),
            "post_removal_probe_hashes": probes,
            "passes": passes,
        }

    def verify(self, byte_hash: str) -> bool:
        try:
            return content_hash(self.get(byte_hash)) == byte_hash
        except (FileNotFoundError, OSError):
            return False

    def _append_manifest(self, source_id: str, row: Mapping[str, Any]) -> None:
        path = self.manifests / f"{source_id}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        # O_APPEND keeps each compact JSON line atomic for normal local files.
        fd = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
        try:
            os.write(fd, (json.dumps(row, sort_keys=True) + "\n").encode("utf-8"))
        finally:
            os.close(fd)


class NormalizedLake:
    def __init__(self, root: Path):
        self.root = root

    def write(
        self,
        table: str,
        records: Iterable[Mapping[str, Any]],
        *,
        partition: Mapping[str, str] | None = None,
        shard_name: str | None = None,
    ) -> Path:
        if table not in TABLE_SCHEMAS:
            raise KeyError(table)
        rows = list(records)
        validate_records(table, rows)
        import pyarrow as pa
        import pyarrow.parquet as pq

        target = self.root / table
        for key, value in sorted((partition or {}).items()):
            if not key.replace("_", "").isalnum() or "/" in value or ".." in value:
                raise ValueError(f"unsafe partition {key}={value}")
            target /= f"{key}={value}"
        target.mkdir(parents=True, exist_ok=True)
        name = shard_name or f"part-{content_hash(json.dumps(rows, sort_keys=True, default=str))[:16]}.parquet"
        if not name.endswith(".parquet") or Path(name).name != name:
            raise ValueError("shard_name must be a plain .parquet filename")
        path = target / name
        schema = pyarrow_schema(table)
        normalized = []
        for row in rows:
            item = dict(row)
            for field in schema:
                if field.name not in item:
                    item[field.name] = None
                value = item[field.name]
                if str(field.type) in {"large_string", "string"} and isinstance(value, (dict, list)):
                    item[field.name] = json.dumps(value, sort_keys=True)
                elif str(field.type) in {"large_string", "string"} and value is not None and not isinstance(value, str):
                    item[field.name] = str(value)
                elif str(field.type).startswith("timestamp") and isinstance(value, str):
                    # PyArrow intentionally does not parse ISO strings when an
                    # explicit timestamp schema is supplied.  Parsing here
                    # keeps the public connector contract JSON-serializable.
                    year_token = value.split("-", 1)[0]
                    if year_token and set(year_token) == {"0"} and field.nullable:
                        # Some bibliographic providers use year zero as an
                        # unknown-date sentinel, sometimes serialized as
                        # ``0`` and sometimes as ``0000``. Keep the exact
                        # string in raw provenance, but never fabricate a
                        # Gregorian date. Other malformed dates remain fatal.
                        item[field.name] = None
                    else:
                        item[field.name] = datetime.fromisoformat(
                            value.replace("Z", "+00:00")
                        )
            normalized.append(item)
        data = pa.Table.from_pylist(normalized, schema=schema)
        temp = path.with_suffix(".tmp.parquet")
        pq.write_table(data, temp, compression="zstd", use_dictionary=True)
        os.replace(temp, path)
        digest = content_hash(path.read_bytes())
        _atomic_bytes(path.with_suffix(".parquet.sha256"), (digest + "\n").encode())
        # Keep append-only manifests inside the table/source partition. A
        # single root manifest is unsafe on network-backed volumes when
        # independent source harvests compile concurrently.
        manifest_path = target / "_shard_manifest.jsonl"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(manifest_path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
        try:
            row = {
                "table": table, "path": str(path.relative_to(self.root)), "sha256": digest,
                "rows": len(rows), "written_at": _stamp(),
            }
            os.write(fd, (json.dumps(row, sort_keys=True) + "\n").encode())
        finally:
            os.close(fd)
        return path

    def files(self, table: str) -> list[Path]:
        return sorted((self.root / table).glob("**/*.parquet"))

    def remove_shard(self, path: Path) -> None:
        """Remove one exactly resolved generated shard and tombstone its manifest row."""
        resolved = path.resolve()
        root = self.root.resolve()
        if root not in resolved.parents or resolved.suffix != ".parquet":
            raise ValueError(f"refusing to remove non-lake shard: {path}")
        if resolved.exists():
            resolved.unlink()
        sidecar = resolved.with_suffix(".parquet.sha256")
        if sidecar.exists():
            sidecar.unlink()
        manifest_path = resolved.parent / "_shard_manifest.jsonl"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        row = {
            "table": resolved.parts[-3] if len(resolved.parts) >= 3 else "unknown",
            "path": str(resolved.relative_to(root)), "sha256": None,
            "rows": 0, "written_at": _stamp(), "deleted": True,
        }
        fd = os.open(manifest_path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
        try:
            os.write(fd, (json.dumps(row, sort_keys=True) + "\n").encode())
        finally:
            os.close(fd)

    def read(self, table: str):
        import pyarrow.dataset as ds

        files = self.files(table)
        if not files:
            raise FileNotFoundError(f"no normalized shards for {table}")
        return ds.dataset([str(p) for p in files], format="parquet", partitioning="hive").to_table()

    def verify(self, *, source_id: str | None = None) -> dict[str, Any]:
        """Verify finalized shards from their atomic hash sidecars.

        Sidecars are authoritative because they are co-located and atomically
        replaced with each shard. Partition manifests remain provenance logs,
        not a cross-source coordination primitive.
        """
        files = sorted(self.root.glob("**/*.parquet"))
        if source_id is not None:
            marker = f"source_id={source_id}"
            files = [path for path in files if marker in path.parts]
        from concurrent.futures import ThreadPoolExecutor

        def verify_file(path: Path) -> dict[str, str] | None:
            relative = str(path.relative_to(self.root))
            sidecar = path.with_suffix(".parquet.sha256")
            if not sidecar.exists():
                return {"path": relative, "issue": "sidecar_missing"}
            expected = sidecar.read_text().strip()
            if content_hash(path.read_bytes()) != expected:
                return {"path": relative, "issue": "hash_mismatch"}
            return None

        # Population shards are independently immutable. A small bounded pool
        # overlaps mounted-volume latency without creating unbounded I/O or
        # changing deterministic issue ordering.
        with ThreadPoolExecutor(max_workers=min(4, max(len(files), 1))) as executor:
            issues = [issue for issue in executor.map(verify_file, files) if issue]
        return {"passes": not issues, "checked": len(files), "issues": issues}


class ObservatoryCatalog:
    def __init__(self, lake_root: Path, database: Path | None = None):
        try:
            import duckdb
        except ImportError as exc:  # pragma: no cover - dependency guard
            raise RuntimeError("install duckdb to build the OSG catalog") from exc
        self.duckdb = duckdb
        self.lake_root = lake_root
        self.database = database or Path(":memory:")

    def connect(self):
        con = self.duckdb.connect(str(self.database))
        for table in TABLE_SCHEMAS:
            glob = self.lake_root / table / "**" / "*.parquet"
            if list((self.lake_root / table).glob("**/*.parquet")):
                quoted = str(glob).replace("'", "''")
                key = PRIMARY_KEYS[table]
                field_names = {field.name for field in TABLE_SCHEMAS[table]}
                if "observed_at" in field_names:
                    order = "observed_at DESC NULLS LAST"
                elif "retrieved_at" in field_names:
                    order = "retrieved_at DESC NULLS LAST"
                elif "occurred_at" in field_names:
                    order = "occurred_at DESC NULLS LAST"
                else:  # pragma: no cover - every current schema has temporal truth
                    order = f'"{key}" DESC'
                if "record_version" in field_names:
                    order = f"record_version DESC NULLS LAST, {order}"
                con.execute(
                    f'CREATE OR REPLACE VIEW "{table}_history" AS '
                    f"SELECT * FROM read_parquet('{quoted}', hive_partitioning=true, union_by_name=true)"
                )
                con.execute(
                    f'CREATE OR REPLACE VIEW "{table}" AS '
                    f'SELECT * FROM "{table}_history" '
                    f'QUALIFY row_number() OVER (PARTITION BY "{key}" ORDER BY {order}, source_id DESC) = 1'
                )
        return con
