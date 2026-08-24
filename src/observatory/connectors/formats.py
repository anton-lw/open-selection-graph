"""Pure parsers for the OSG's public source formats."""

from __future__ import annotations

import csv
import io
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup
from lxml import etree


def _xml_root(payload: bytes | str, *, recover: bool = False):
    data = payload if isinstance(payload, bytes) else payload.encode()
    parser = etree.XMLParser(
        resolve_entities=False,
        no_network=True,
        recover=recover,
        huge_tree=False,
    )
    return etree.fromstring(data, parser=parser)


def parse_json(payload: bytes | str) -> Any:
    return json.loads(payload.decode("utf-8") if isinstance(payload, bytes) else payload)


def parse_jsonl(payload: bytes | str) -> list[dict[str, Any]]:
    text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def parse_csv(payload: bytes | str, *, delimiter: str = ",") -> list[dict[str, str]]:
    text = payload.decode("utf-8-sig") if isinstance(payload, bytes) else payload
    return list(csv.DictReader(io.StringIO(text), delimiter=delimiter))


def parse_excel(payload: bytes) -> dict[str, list[dict[str, Any]]]:
    import pandas as pd

    sheets = pd.read_excel(io.BytesIO(payload), sheet_name=None)
    return {name: frame.where(frame.notna(), None).to_dict("records") for name, frame in sheets.items()}


def parse_oai(
    payload: bytes | str, *, recover: bool = False
) -> tuple[list[dict[str, Any]], str | None]:
    """Parse OAI-PMH, with provider-defect recovery explicitly opt-in."""
    root = _xml_root(payload, recover=recover)
    ns = {"oai": "http://www.openarchives.org/OAI/2.0/", "dc": "http://purl.org/dc/elements/1.1/"}
    rows = []
    for record in root.xpath(".//oai:record", namespaces=ns):
        header = record.find("oai:header", ns)
        metadata = record.find("oai:metadata", ns)
        rows.append({
            "identifier": header.findtext("oai:identifier", namespaces=ns) if header is not None else None,
            "datestamp": header.findtext("oai:datestamp", namespaces=ns) if header is not None else None,
            "sets": header.xpath("oai:setSpec/text()", namespaces=ns) if header is not None else [],
            "deleted": bool(header is not None and header.get("status") == "deleted"),
            "metadata_xml": etree.tostring(metadata[0], encoding="unicode") if metadata is not None and len(metadata) else None,
        })
    token = root.findtext(".//oai:resumptionToken", namespaces=ns)
    return rows, token.strip() if token and token.strip() else None


def parse_jats(payload: bytes | str) -> dict[str, Any]:
    root = _xml_root(payload)
    def text(xp: str) -> str | None:
        return " ".join(root.xpath(f"string({xp})").split()) or None
    refs = []
    for ref in root.xpath(".//ref-list/ref"):
        refs.append({
            "id": ref.get("id"),
            "doi": " ".join(ref.xpath("string(.//pub-id[@pub-id-type='doi'])").split()) or None,
            "text": " ".join("".join(ref.itertext()).split()),
        })
    sub_articles = []
    for node in root.xpath(".//sub-article"):
        sub_articles.append({
            "id": node.get("id"), "article_type": node.get("article-type"),
            "title": " ".join(node.xpath("string(.//article-title[1])").split()) or None,
            "body_text": "\n".join(
                " ".join("".join(section.itertext()).split())
                for section in node.xpath(".//body/sec")
            ),
        })
    related_articles = [
        {
            "relation_type": node.get("related-article-type"),
            "href": node.get("{http://www.w3.org/1999/xlink}href"),
            "ext_link_type": node.get("ext-link-type"),
        }
        for node in root.xpath(".//related-article")
    ]
    licences = [
        {
            "href": node.get("{http://www.w3.org/1999/xlink}href"),
            "text": " ".join("".join(node.itertext()).split()),
        }
        for node in root.xpath(".//license")
    ]
    return {
        "title": text(".//article-title"),
        "abstract": text(".//abstract"),
        "doi": text(".//article-id[@pub-id-type='doi']"),
        "pmcid": text(".//article-id[@pub-id-type='pmcid']"),
        "body_text": "\n".join(
            " ".join("".join(section.itertext()).split()) for section in root.xpath(".//body/sec")
        ),
        "references": refs,
        "sub_articles": sub_articles,
        "related_articles": related_articles,
        "licences": licences,
    }


def parse_html(payload: bytes | str) -> dict[str, Any]:
    soup = BeautifulSoup(payload, "lxml")
    jsonld = []
    for node in soup.select("script[type='application/ld+json']"):
        try:
            jsonld.append(json.loads(node.get_text()))
        except json.JSONDecodeError:
            continue
    return {
        "title": soup.title.get_text(" ", strip=True) if soup.title else None,
        "meta": {
            node.get("name") or node.get("property"): node.get("content")
            for node in soup.select("meta[name], meta[property]")
            if (node.get("name") or node.get("property")) and node.get("content")
        },
        "jsonld": jsonld,
        "text": re.sub(r"\n{3,}", "\n\n", soup.get_text("\n", strip=True)),
    }


def extract_pdf_text(payload: bytes) -> dict[str, Any]:
    """Extract born-digital text and report whether OCR is likely required."""

    try:
        from pypdf import PdfReader
    except ImportError as exc:  # pragma: no cover - optional dependency guard
        raise RuntimeError("install the observatory optional dependencies for PDF extraction") from exc
    reader = PdfReader(io.BytesIO(payload))
    pages = [(page.extract_text() or "") for page in reader.pages]
    nonspace = sum(len(re.sub(r"\s+", "", page)) for page in pages)
    return {
        "pages": pages,
        "n_pages": len(pages),
        "nonspace_characters": nonspace,
        "ocr_likely_required": bool(pages and nonspace / len(pages) < 50),
    }


def parse_tei(payload: bytes | str) -> dict[str, Any]:
    root = _xml_root(payload)
    ns = {"tei": "http://www.tei-c.org/ns/1.0"}
    return {
        "title": " ".join(root.xpath("string(.//tei:titleStmt/tei:title[1])", namespaces=ns).split()) or None,
        "abstract": " ".join(root.xpath("string(.//tei:profileDesc/tei:abstract)", namespaces=ns).split()) or None,
        "body_text": "\n".join(
            " ".join("".join(node.itertext()).split())
            for node in root.xpath(".//tei:text/tei:body//tei:div", namespaces=ns)
        ),
        "references": [
            " ".join("".join(node.itertext()).split())
            for node in root.xpath(".//tei:listBibl/tei:biblStruct", namespaces=ns)
        ],
    }


def parse_uspto_xml(payload: bytes | str) -> dict[str, Any]:
    root = _xml_root(payload)
    def values(name: str) -> list[str]:
        return [
            " ".join("".join(node.itertext()).split())
            for node in root.xpath(f".//*[local-name()='{name}']")
        ]
    return {
        "application_numbers": values("application-reference") or values("doc-number"),
        "claims": values("claim"),
        "citations": values("citation"),
        "office_actions": values("office-action"),
    }


def parse_tabular_pdf(payload: bytes) -> dict[str, Any]:
    import pdfplumber

    tables = []
    with pdfplumber.open(io.BytesIO(payload)) as document:
        for page_number, page in enumerate(document.pages, 1):
            for table in page.extract_tables() or []:
                tables.append({"page": page_number, "rows": table})
    return {"tables": tables, "table_count": len(tables)}


@dataclass(frozen=True)
class ParseOutcome:
    success: bool
    parser: str
    value: Any | None
    error_class: str | None
    error: str | None
    quarantine_path: str | None


def safe_parse(
    parser_name: str,
    payload: bytes | str,
    *,
    quarantine_root: Path | None = None,
    native_id: str = "unknown",
) -> ParseOutcome:
    parsers = {
        "json": parse_json, "jsonl": parse_jsonl, "csv": parse_csv, "oai": parse_oai,
        "jats": parse_jats, "tei": parse_tei, "html": parse_html,
        "pdf_text": extract_pdf_text, "pdf_tables": parse_tabular_pdf, "uspto_xml": parse_uspto_xml,
    }
    if parser_name not in parsers:
        raise KeyError(parser_name)
    try:
        return ParseOutcome(True, parser_name, parsers[parser_name](payload), None, None, None)
    except Exception as exc:
        pointer = None
        if quarantine_root is not None:
            from ..ids import content_hash

            data = payload if isinstance(payload, bytes) else payload.encode()
            target = quarantine_root / parser_name / f"{content_hash(data)}.bin"
            target.parent.mkdir(parents=True, exist_ok=True)
            if not target.exists():
                target.write_bytes(data)
            meta = target.with_suffix(".json")
            meta.write_text(json.dumps({
                "native_id": native_id, "parser": parser_name,
                "error_class": type(exc).__name__, "error": str(exc),
            }, indent=2, sort_keys=True) + "\n")
            pointer = str(target)
        return ParseOutcome(False, parser_name, None, type(exc).__name__, str(exc), pointer)
