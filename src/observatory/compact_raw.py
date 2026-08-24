"""Compact one inactive raw source into an indexed append-only byte pack."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .storage import RawStore


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--receipt", type=Path)
    args = parser.parse_args()
    result = RawStore(args.root).pack_source(args.source)
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.receipt:
        args.receipt.parent.mkdir(parents=True, exist_ok=True)
        args.receipt.write_text(rendered)
    print(rendered, end="")
    return 0 if result["passes"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
