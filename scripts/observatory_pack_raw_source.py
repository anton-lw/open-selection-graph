"""Compact explicitly named inactive observatory raw sources on a Modal volume.

This operational helper is deliberately explicit: callers must list every
source, and an active source can therefore be kept out of the compaction set.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from observatory.storage import RawStore


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source_id", nargs="+")
    parser.add_argument(
        "--raw-root",
        type=Path,
        default=Path("/volume/workspace/data/observatory/raw"),
    )
    args = parser.parse_args()
    raw = RawStore(args.raw_root)
    results = [raw.pack_source(source_id) for source_id in args.source_id]
    print(json.dumps(results, sort_keys=True))


if __name__ == "__main__":
    main()
