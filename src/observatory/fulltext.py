"""Bounded, resumable routing for structured text, HTML, PDFs, and OCR."""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
import time
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

from .connectors.formats import extract_pdf_text, parse_html, parse_jats, parse_tei
from .ids import content_hash
from .storage_guard import storage_preflight


@dataclass(frozen=True)
class TextJob:
    native_id: str
    media_type: str
    payload_path: str
    expected_reference_count: int | None = None
    ocr_permitted: bool = False
    max_ocr_pages: int = 50


@dataclass(frozen=True)
class TextResult:
    native_id: str
    route: str
    success: bool
    text_hash: str | None
    character_count: int
    extracted_reference_count: int
    reference_recall_proxy: float | None
    elapsed_seconds: float
    failure: str | None


class FullTextOrchestrator:
    def __init__(self, output_root: Path):
        self.output_root = output_root

    def process(self, job: TextJob) -> TextResult:
        start = time.monotonic()
        payload = Path(job.payload_path).read_bytes()
        route = "unknown"
        try:
            if job.media_type in {"application/xml", "application/jats+xml"}:
                route = "jats"
                parsed = parse_jats(payload)
                text = "\n".join(filter(None, (parsed.get("title"), parsed.get("abstract"), parsed.get("body_text"))))
                refs = len(parsed.get("references") or [])
            elif job.media_type == "application/tei+xml":
                route = "tei"
                parsed = parse_tei(payload)
                text = "\n".join(filter(None, (parsed.get("title"), parsed.get("abstract"), parsed.get("body_text"))))
                refs = len(parsed.get("references") or [])
            elif job.media_type == "text/html":
                route = "html"
                parsed = parse_html(payload)
                text, refs = parsed.get("text") or "", 0
            elif job.media_type == "application/pdf":
                route = "born_digital_pdf"
                parsed = extract_pdf_text(payload)
                text = "\n".join(parsed["pages"])
                refs = 0
                if parsed["ocr_likely_required"]:
                    if not job.ocr_permitted:
                        raise ValueError("ocr required but object licence/policy does not permit OCR")
                    text = self._ocr(payload, max_pages=job.max_ocr_pages)
                    route = "ocr_pdf"
            else:
                raise ValueError(f"unsupported media type: {job.media_type}")
            normalized = " ".join(text.split())
            recall = None
            if job.expected_reference_count:
                recall = min(refs / job.expected_reference_count, 1.0)
            return TextResult(
                job.native_id, route, True, content_hash(normalized), len(normalized), refs,
                recall, time.monotonic() - start, None,
            )
        except Exception as exc:
            return TextResult(
                job.native_id, route, False, None, 0, 0, None,
                time.monotonic() - start, f"{type(exc).__name__}: {exc}",
            )

    def _ocr(self, payload: bytes, *, max_pages: int) -> str:
        tesseract = shutil.which("tesseract")
        pdftoppm = shutil.which("pdftoppm")
        if not tesseract or not pdftoppm:
            raise RuntimeError("bounded PDF OCR requires pdftoppm and tesseract")
        with tempfile.TemporaryDirectory() as directory:
            pdf = Path(directory) / "input.pdf"
            pdf.write_bytes(payload)
            prefix = Path(directory) / "page"
            subprocess.run(
                [
                    pdftoppm, "-f", "1", "-l", str(max_pages), "-r", "200",
                    "-png", str(pdf), str(prefix),
                ],
                capture_output=True, timeout=300, check=True,
            )
            pages = []
            for image in sorted(Path(directory).glob("page-*.png")):
                completed = subprocess.run(
                    [tesseract, str(image), "stdout"],
                    capture_output=True, text=True, timeout=120, check=True,
                )
                pages.append(completed.stdout)
            if not pages:
                raise RuntimeError("pdftoppm produced no OCR pages")
            return "\n".join(pages)

    def benchmark(
        self,
        jobs: Iterable[TextJob],
        output: Path,
        *,
        shard_index: int = 0,
        shard_count: int = 1,
        resume: bool = True,
        cpu_cost_usd_per_hour: float = 0.0,
    ) -> dict:
        if shard_count < 1 or not 0 <= shard_index < shard_count:
            raise ValueError("invalid shard index/count")
        checkpoint = output.with_suffix(".results.jsonl")
        prior: dict[str, TextResult] = {}
        if resume and checkpoint.exists():
            for line in checkpoint.read_text().splitlines():
                if line.strip():
                    row = json.loads(line)
                    prior[str(row["native_id"])] = TextResult(**row)
        selected = [
            job for index, job in enumerate(jobs)
            if index % shard_count == shard_index
        ]
        projected_output = sum(Path(job.payload_path).stat().st_size for job in selected) * 2
        storage_receipt = storage_preflight(
            output.parent,
            projected_input_bytes=0,
            projected_output_bytes=projected_output,
        )
        results = []
        checkpoint.parent.mkdir(parents=True, exist_ok=True)
        for job in selected:
            result = prior.get(job.native_id)
            if result is None:
                result = self.process(job)
                with checkpoint.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(asdict(result), sort_keys=True) + "\n")
            results.append(result)
        failures = Counter((row.failure or "").split(":", 1)[0] for row in results if not row.success)
        recalls = [row.reference_recall_proxy for row in results if row.reference_recall_proxy is not None]
        report = {
            "schema": "observatory.fulltext-benchmark/1",
            "document_count": len(results),
            "success_count": sum(row.success for row in results),
            "success_rate": sum(row.success for row in results) / len(results) if results else None,
            "routes": dict(Counter(row.route for row in results)),
            "reference_recall_proxy_mean": sum(recalls) / len(recalls) if recalls else None,
            "elapsed_seconds": sum(row.elapsed_seconds for row in results),
            "estimated_compute_cost_usd": (
                sum(row.elapsed_seconds for row in results) / 3600 * cpu_cost_usd_per_hour
            ),
            "shard_index": shard_index, "shard_count": shard_count,
            "checkpoint": str(checkpoint), "resumed_count": sum(
                row.native_id in prior for row in results
            ),
            "failure_taxonomy": dict(failures),
            "storage_preflight": storage_receipt,
            "results": [asdict(row) for row in results],
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        return report
