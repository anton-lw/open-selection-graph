"""Structured-source gold benchmark for JATS reference extraction."""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any

from lxml import etree

from .connectors.formats import parse_jats
from .ids import canonical_doi, content_hash


def benchmark_jats_references(raw_root: Path, *, maximum_documents: int = 1000) -> dict[str, Any]:
    manifest = raw_root / "manifests" / "europe_pmc.jsonl"
    latest: dict[str, dict[str, Any]] = {}
    for line in manifest.read_text().splitlines():
        if line.strip():
            row = json.loads(line)
            if row.get("object_type") == "fulltext_xml":
                latest[str(row["native_id"])] = row
    receipts = list(latest.values())[:maximum_documents]
    documents = gold_references = extracted_references = 0
    doi_true_positive = doi_false_positive = doi_false_negative = 0
    malformed = []
    for receipt in receipts:
        payload = gzip.decompress(Path(receipt["raw_pointer"]).read_bytes())
        try:
            root = etree.fromstring(payload)
            extracted = parse_jats(payload).get("references") or []
        except Exception as exc:
            malformed.append({
                "native_id": receipt["native_id"], "error_class": type(exc).__name__
            })
            continue
        gold_nodes = root.xpath(".//*[local-name()='ref-list']/*[local-name()='ref']")
        gold_dois = {
            canonical_doi(" ".join(node.xpath("string(.//*[local-name()='pub-id'][@pub-id-type='doi'][1])").split()))
            for node in gold_nodes
        } - {None}
        extracted_dois = {canonical_doi(row.get("doi")) for row in extracted} - {None}
        documents += 1
        gold_references += len(gold_nodes)
        extracted_references += len(extracted)
        doi_true_positive += len(gold_dois & extracted_dois)
        doi_false_positive += len(extracted_dois - gold_dois)
        doi_false_negative += len(gold_dois - extracted_dois)
    reference_recall = (
        min(extracted_references, gold_references) / gold_references if gold_references else None
    )
    doi_precision = (
        doi_true_positive / (doi_true_positive + doi_false_positive)
        if doi_true_positive + doi_false_positive else None
    )
    doi_recall = (
        doi_true_positive / (doi_true_positive + doi_false_negative)
        if doi_true_positive + doi_false_negative else None
    )
    report: dict[str, Any] = {
        "schema": "observatory.jats-reference-benchmark/1",
        "document_count": documents, "malformed_count": len(malformed),
        "gold_reference_count": gold_references,
        "extracted_reference_count": extracted_references,
        "reference_count_recall": reference_recall,
        "gold_doi_count": doi_true_positive + doi_false_negative,
        "doi_true_positive": doi_true_positive, "doi_false_positive": doi_false_positive,
        "doi_false_negative": doi_false_negative, "doi_precision": doi_precision,
        "doi_recall": doi_recall, "malformed": malformed,
        "unresolved_policy": "references without exact identifiers remain raw-citation hashes",
    }
    report["passes"] = bool(
        documents >= 100 and reference_recall is not None and reference_recall >= 0.99
        and (doi_precision is None or doi_precision >= 0.99)
        and (doi_recall is None or doi_recall >= 0.99)
    )
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    return report


def write_jats_reference_benchmark(raw_root: Path, output: Path) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(benchmark_jats_references(raw_root), indent=2, sort_keys=True) + "\n"
    )
    return output
