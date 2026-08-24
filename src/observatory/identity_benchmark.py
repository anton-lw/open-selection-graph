"""Large labelled DOI canonicalization fixture built from public OpenAlex IDs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from .ids import canonical_doi, content_hash


def benchmark_doi_canonicalization(
    real_dois: Iterable[str], *, maximum_unique: int = 2000
) -> dict[str, Any]:
    canonical = []
    seen = set()
    for value in real_dois:
        doi = canonical_doi(value)
        if doi and doi not in seen:
            seen.add(doi)
            canonical.append(doi)
        if len(canonical) >= maximum_unique:
            break
    if len(canonical) < 1000:
        raise ValueError("DOI benchmark requires at least 1000 distinct real identifiers")
    cases: list[tuple[str, str | None]] = []
    for doi in canonical:
        cases.extend((
            (doi, doi), (f"https://doi.org/{doi}", doi),
            (f"https://DX.DOI.ORG/{doi.upper()}", doi), (f"doi: {doi}.)", doi),
        ))
    negatives = [
        "", "not-a-doi", "11.1234/example", "10.123/x", "https://example.org/10.1234/x",
        "10.1234/has whitespace", "doi:", "10.1234", "10.1234/", "PMC12345",
    ]
    for index in range(len(canonical)):
        cases.append((negatives[index % len(negatives)], None))
    true_positive = false_positive = false_negative = true_negative = 0
    errors = []
    for value, expected in cases:
        observed = canonical_doi(value)
        if expected is None:
            if observed is None:
                true_negative += 1
            else:
                false_positive += 1
        elif observed == expected:
            true_positive += 1
        else:
            false_negative += 1
        if observed != expected and len(errors) < 100:
            errors.append({"value": value, "expected": expected, "observed": observed})
    precision = true_positive / (true_positive + false_positive) if true_positive + false_positive else None
    recall = true_positive / (true_positive + false_negative) if true_positive + false_negative else None
    report: dict[str, Any] = {
        "schema": "observatory.doi-canonicalization-benchmark/1",
        "real_unique_dois": len(canonical), "case_count": len(cases),
        "true_positive": true_positive, "false_positive": false_positive,
        "false_negative": false_negative, "true_negative": true_negative,
        "precision": precision, "recall": recall,
        "acceptance_precision": 0.999, "passes": bool(precision is not None and precision >= 0.999),
        "errors": errors,
    }
    report["fixture_hash"] = content_hash(json.dumps(cases, sort_keys=True))
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    return report


def openalex_dois(path: Path) -> Iterable[str]:
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            value = json.loads(line).get("doi")
            if value:
                yield str(value)


def write_doi_benchmark(openalex_jsonl: Path, output: Path) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(
            benchmark_doi_canonicalization(openalex_dois(openalex_jsonl)),
            indent=2, sort_keys=True,
        ) + "\n"
    )
    return output
