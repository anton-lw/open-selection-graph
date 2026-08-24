# Raw storage lifecycle

The observatory raw tier is immutable and content-addressed. A loose object is
stored as `raw/objects/<hash-prefix>/<sha256>.gz`; source manifests retain the
native identifier, object type, retrieval time, byte hash, and acquisition
metadata.

## Indexed packs

When a completed, inactive source would exhaust a filesystem inode quota,
`RawStore.pack_source(source_id)` may compact its loose gzip members into one
append-only `.pack` file. `raw/packs/index.sqlite3` maps every byte hash to the
pack name, byte offset, and byte length. `RawStore.get()` and
`RawStore.verify()` resolve loose and packed objects through the same hash API.

Packing does not alter source manifests, normalized records, byte hashes, or
uncompressed source bytes. The operation:

1. resolves an exact source manifest rather than a broad directory target;
2. verifies every loose object's uncompressed SHA-256 while writing;
3. fsyncs and commits the pack;
4. transactionally records every offset in the index;
5. re-reads and verifies every packed member; and only then
6. removes the corresponding loose files.

An interruption before step 6 leaves only recoverable duplicate storage. An
interruption during step 6 leaves a mix of loose and indexed packed objects,
which the public read path handles deterministically.

Run an explicit compaction with:

```bash
PYTHONPATH=src python -m observatory.compact_raw \
  --root /exact/path/to/data/observatory/raw \
  --source exact_source_id \
  --receipt /exact/path/to/results/observatory/raw_pack_receipt.json
```

The receipt records the exact source, manifest receipt count, unique hashes,
verified member count, removed loose-object count, pack size, and post-removal
probes. Bulk public inputs remain refetchable from their source cards; pack and
manifest checksums are the preferred recovery path for a frozen snapshot.
