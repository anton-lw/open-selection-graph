"""Lossless native-to-normalized mapping registry."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class MappingResult:
    source_id: str
    vocabulary: str
    native_value: str
    normalized_value: str | None
    mapping_version: str
    mapped: bool


class MappingRegistry:
    def __init__(self, path: Path):
        data = yaml.safe_load(path.read_text())
        self.version = str(data["version"])
        self.mappings: dict[str, Any] = data.get("mappings") or {}

    def map(self, source_id: str, vocabulary: str, native_value: object) -> MappingResult:
        native = str(native_value)
        values = (((self.mappings.get(source_id) or {}).get(vocabulary) or {}).get("values") or {})
        normalized = values.get(native)
        return MappingResult(source_id, vocabulary, native, normalized, self.version, normalized is not None)

