"""Conservative language identification and derivative provenance."""

from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

from .ids import content_hash


@dataclass(frozen=True)
class LanguageGuess:
    language: str | None
    confidence: float
    method: str


@dataclass(frozen=True)
class TranslationDerivative:
    source_language: str
    target_language: str
    model_name: str
    model_version: str
    source_text_hash: str
    translated_text_hash: str
    validated: bool = False

    @property
    def primary_construct_eligible(self) -> bool:
        return self.validated


def validate_translation_derivative(row: Mapping[str, object]) -> TranslationDerivative:
    """Require provenance that prevents a translation being mistaken for source text."""
    required = (
        "source_language", "target_language", "model_name", "model_version",
        "source_text_hash", "translated_text_hash",
    )
    missing = [key for key in required if not str(row.get(key) or "").strip()]
    if missing:
        raise ValueError(f"translation derivative lacks provenance: {', '.join(missing)}")
    if row["source_text_hash"] == row["translated_text_hash"]:
        raise ValueError("translation derivative must not overwrite or alias the source text hash")
    return TranslationDerivative(
        source_language=str(row["source_language"]),
        target_language=str(row["target_language"]),
        model_name=str(row["model_name"]), model_version=str(row["model_version"]),
        source_text_hash=str(row["source_text_hash"]),
        translated_text_hash=str(row["translated_text_hash"]),
        validated=bool(row.get("validated", False)),
    )


_MARKERS = {
    "en": ("the", "and", "of", "in", "with"),
    "de": ("der", "die", "und", "von", "mit"),
    "fr": ("le", "la", "et", "de", "avec"),
    "es": ("el", "la", "y", "de", "con"),
}


def detect_language(text: str) -> LanguageGuess:
    words = re.findall(r"\b[^\W\d_]+\b", (text or "").lower())
    if len(words) < 20:
        return LanguageGuess(None, 0.0, "insufficient-text")
    counts = {lang: sum(words.count(marker) for marker in markers) for lang, markers in _MARKERS.items()}
    ordered = sorted(counts.items(), key=lambda item: item[1], reverse=True)
    if not ordered or ordered[0][1] < 2 or (len(ordered) > 1 and ordered[0][1] == ordered[1][1]):
        return LanguageGuess(None, 0.0, "marker-abstention/1")
    total = sum(counts.values()) or 1
    return LanguageGuess(ordered[0][0], ordered[0][1] / total, "marker-abstention/1")


def benchmark_language_detection(rows: Iterable[Mapping[str, str]]) -> dict[str, object]:
    """Evaluate accuracy and abstention without scoring abstentions as guesses."""
    total = correct = abstained = 0
    per_language: dict[str, dict[str, int]] = {}
    for row in rows:
        expected = str(row["language"])
        guess = detect_language(str(row["text"]))
        total += 1
        bucket = per_language.setdefault(expected, {"total": 0, "correct": 0, "abstained": 0})
        bucket["total"] += 1
        if guess.language is None:
            abstained += 1
            bucket["abstained"] += 1
        elif guess.language == expected:
            correct += 1
            bucket["correct"] += 1
    decided = total - abstained
    return {
        "schema": "observatory.language-benchmark/1", "total": total,
        "correct": correct, "abstained": abstained,
        "coverage": decided / total if total else None,
        "accuracy_when_decided": correct / decided if decided else None,
        "per_language": per_language,
    }


def write_fwf_parallel_language_benchmark(source_csv: Path, output: Path) -> Path:
    """Benchmark source-declared German/English FWF pairs without translating them."""
    rows = []
    pair_ids = []
    with source_csv.open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            german = str(row.get("text_de") or "").strip()
            english = str(row.get("text_en") or "").strip()
            if not german or not english:
                continue
            pair_ids.append(str(row.get("grant_id") or len(pair_ids)))
            rows.extend((
                {"language": "de", "text": german},
                {"language": "en", "text": english},
            ))
    report = benchmark_language_detection(rows)
    report.update({
        "source": str(source_csv), "source_declared_parallel_pair_count": len(pair_ids),
        "translation_performed": False,
        "pair_manifest_hash": content_hash(json.dumps(pair_ids, sort_keys=True)),
        "acceptance_accuracy_when_decided": 0.99,
    })
    accuracy = report["accuracy_when_decided"]
    report["passes"] = bool(
        len(pair_ids) >= 1000 and accuracy is not None and float(accuracy) >= 0.99
    )
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return output
