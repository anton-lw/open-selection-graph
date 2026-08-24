"""Streaming reader for the free quarterly OpenAlex snapshot/local shards."""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Iterable, Iterator

from .ids import canonical_doi


def iter_snapshot(
    paths: Iterable[Path],
    *,
    openalex_ids: set[str] | None = None,
    dois: set[str] | None = None,
) -> Iterator[dict]:
    wanted_ids = {value.rsplit("/", 1)[-1].upper() for value in (openalex_ids or set())}
    wanted_dois = {doi for value in (dois or set()) if (doi := canonical_doi(value))}
    for path in paths:
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                row = json.loads(line)
                oid = str(row.get("id") or "").rsplit("/", 1)[-1].upper()
                doi = canonical_doi(row.get("doi"))
                if not wanted_ids and not wanted_dois:
                    yield row
                elif oid in wanted_ids or (doi and doi in wanted_dois):
                    yield row
