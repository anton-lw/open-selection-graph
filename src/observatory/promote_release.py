"""Fail-closed promotion of completed OpenReview evidence and the final atlas."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import yaml

from .atlas import write_source_atlas
from .ids import content_hash
from .registry import source_cards
from .ticket_evidence import write_ticket_evidence_audit


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _verify_self_hash(body: dict[str, Any], key: str, *, label: str) -> None:
    declared = body.get(key)
    unhashed = {name: value for name, value in body.items() if name != key}
    if not declared or declared != content_hash(json.dumps(unhashed, sort_keys=True)):
        raise RuntimeError(f"{label} self-hash failed")


def promote_observatory_release(workspace: Path) -> dict[str, Any]:
    results = workspace / "results" / "observatory"
    receipt = _load(results / "modal_openreview_process_receipt.json")
    report = _load(results / "openreview_process_audit.json")
    forum_sample = _load(results / "openreview_forum_count_sample.json")
    forum_manifest = _load(results / "openreview_forum_manifest.json")
    state_manifest = _load(results / "openreview_passing_state_manifest.json")
    coverage_export = _load(results / "openreview_api_population_coverage.json")
    if (
        receipt.get("schema") != "observatory.modal-openreview-process-receipt/4"
        or receipt.get("status") != "complete"
        or not report.get("passes")
        or not forum_sample.get("passes")
    ):
        raise RuntimeError("OpenReview population evidence has not passed")
    for name, value in (("process report", report), ("forum sample", forum_sample)):
        _verify_self_hash(value, "report_hash", label=f"OpenReview {name}")
    _verify_self_hash(forum_manifest, "manifest_hash", label="OpenReview forum manifest")
    _verify_self_hash(state_manifest, "manifest_hash", label="OpenReview state manifest")
    hashes = receipt.get("domain_edit_query_hashes") or []
    shards = receipt.get("shards") or []
    covered = [index for shard in shards for index in shard.get("domain_indices") or []]
    if len(hashes) != 5 or len(set(hashes)) != 5 or sorted(covered) != list(range(176)):
        raise RuntimeError("OpenReview receipt does not prove the exhaustive five-shard partition")
    note_hashes = receipt.get("forum_note_query_hashes") or []
    note_shards = receipt.get("forum_note_shards") or []
    note_ranges = sorted(
        (int(shard.get("forum_start", -1)), int(shard.get("forum_stop", -1)))
        for shard in note_shards
    )
    forum_count = int(report.get("forum_count") or 0)
    contiguous_note_ranges = bool(note_ranges) and note_ranges[0][0] == 0
    contiguous_note_ranges = contiguous_note_ranges and all(
        current[1] == following[0] and current[0] < current[1]
        for current, following in zip(note_ranges, note_ranges[1:])
    )
    contiguous_note_ranges = (
        contiguous_note_ranges
        and note_ranges[-1][0] < note_ranges[-1][1]
        and note_ranges[-1][1] == forum_count
    )
    if (
        len(note_hashes) != 5
        or len(set(note_hashes)) != 5
        or len(note_ranges) != 5
        or not contiguous_note_ranges
    ):
        raise RuntimeError(
            "OpenReview receipt does not prove the exhaustive five-range current-Note partition"
        )
    union_hash = content_hash(
        json.dumps(
            {
                "domain_edit_query_hashes": sorted(hashes),
                "forum_note_query_hashes": sorted(note_hashes),
            },
            sort_keys=True,
        )
    )
    if receipt.get("union_hash") != union_hash:
        raise RuntimeError("OpenReview receipt union hash failed")
    if (
        set(report.get("forum_query_hash") or []) != set(hashes)
        or set(report.get("note_query_hash") or []) != set(note_hashes)
        or report.get("state_query_hash") != receipt.get("state_query_hash")
        or forum_manifest.get("state_query_hash") != receipt.get("state_query_hash")
        or int(forum_manifest.get("forum_count") or 0) != forum_count
        or int(state_manifest.get("passing_cycle_count") or 0) != 176
    ):
        raise RuntimeError("OpenReview receipt/report/manifest provenance chain failed")
    for query_value in (*hashes, *note_hashes):
        run_manifest = _load(results / "runs" / f"openreview_api-{query_value[:12]}.json")
        if (
            run_manifest.get("source_id") != "openreview_api"
            or run_manifest.get("status") != "complete"
            or run_manifest.get("query_hash") != query_value
        ):
            raise RuntimeError(f"OpenReview run manifest failed: {query_value}")
    if report.get("passing_cycle_count") != 176:
        raise RuntimeError("OpenReview report does not cover all 176 passing cycles")
    if (
        not report.get("domain_edit_count_reconciliation_passes")
        or not report.get("current_note_count_reconciliation_passes")
        or not report.get("current_note_forum_partition_passes")
        or float(report.get("state_root_current_note_overlap_ratio") or 0) < 0.95
        or len(report.get("domain_reconciliation") or []) != 176
        or len(report.get("current_note_cycle_reconciliation") or []) != 176
    ):
        raise RuntimeError("OpenReview current-Note/Edit population reconciliation is incomplete")
    coverage_unhashed = {
        key: value for key, value in coverage_export.items() if key != "export_hash"
    }
    if (
        coverage_export.get("schema") != "observatory.population-coverage/1"
        or coverage_export.get("source_id") != "openreview_api"
        or coverage_export.get("process_report_hash") != report.get("report_hash")
        or coverage_export.get("export_hash")
        != content_hash(json.dumps(coverage_unhashed, sort_keys=True))
    ):
        raise RuntimeError("OpenReview full coverage export failed provenance checks")
    coverage_rows = coverage_export.get("rows") or []
    coverage_keys = {
        (str(row.get("venue_id")), str(row.get("object_type")))
        for row in coverage_rows
    }
    expected_coverage_keys = {
        (str(row["venue_id"]), object_type)
        for row in report["state_cycle_reconciliation"]
        for object_type in (
            "candidate_state",
            "current_note_graph",
            "note_edit_history",
        )
    }
    if (
        int(coverage_export.get("row_count") or 0) != 528
        or len(coverage_rows) != 528
        or coverage_keys != expected_coverage_keys
        or any(
            row.get("observability_grade") != "B"
            or float(row.get("coverage_ratio") or 0) < 0.95
            for row in coverage_rows
        )
    ):
        raise RuntimeError("OpenReview full coverage export is not exhaustive and release-grade")

    ticket_path = workspace / "configs" / "observatory" / "ticket_evidence.yaml"
    tickets = yaml.safe_load(ticket_path.read_text())
    by_id = {str(row["id"]): row for row in tickets["tickets"]}
    common_evidence = [
        "results/observatory/modal_openreview_process_receipt.json",
        "results/observatory/openreview_process_audit.json",
        "results/observatory/openreview_forum_count_sample.json",
        "results/observatory/openreview_passing_state_manifest.json",
        "results/observatory/openreview_api_population_coverage.json",
        "src/observatory/adapters/openreview_api.py",
        "src/observatory/openreview_process.py",
    ]
    for ticket_id in ("E2", "E3"):
        by_id[ticket_id].update(status="complete", evidence=common_evidence, gap=None)

    source_path = workspace / "configs" / "observatory" / "sources.yaml"
    sources = yaml.safe_load(source_path.read_text())
    for row in sources["sources"]:
        if row["source_id"] != "openreview_api":
            continue
        row["status"] = "included"
        row["provisional_grade"] = "B"
        row["earliest_public_stage"] = "provider-audited public submission invitation"
        row["notes"] = (
            "Grade B applies only to the 176 cycles frozen in the passing-state manifest; "
            "unreadable Notes/Edits and confidential screening remain explicit hidden stages."
        )

    coverage_evidence = [
        str(path.relative_to(workspace))
        for path in sorted(results.glob("*_population_coverage.json"))
    ]
    by_id["E12"].update(
        status="complete",
        evidence=[
            "results/observatory/source_coverage_atlas.json",
            "src/observatory/atlas.py",
            *coverage_evidence,
        ],
        gap=None,
    )
    rendered_tickets = yaml.safe_dump(tickets, sort_keys=False, width=1_000)
    rendered_sources = yaml.safe_dump(sources, sort_keys=False, width=1_000)
    # Validate the exact candidate source registry before either live file is
    # changed. The two small replacements then occur with rollback protection.
    candidate_source_path = results / ".sources.promote.yaml"
    candidate_source_path.write_text(rendered_sources)
    source_cards(candidate_source_path)
    old_tickets = ticket_path.read_bytes()
    old_sources = source_path.read_bytes()
    try:
        ticket_path.write_text(rendered_tickets)
        source_path.write_text(rendered_sources)
        ticket_audit_path = write_ticket_evidence_audit(
            workspace, results / "ticket_evidence_audit.json"
        )
        ticket_audit = _load(ticket_audit_path)
        if not ticket_audit.get("all_acceptance_complete"):
            completed = (ticket_audit.get("status_counts") or {}).get("complete", 0)
            expected = ticket_audit.get("expected_ticket_count", 0)
            raise RuntimeError(
                f"ticket evidence did not reach the full ticketbook: {completed}/{expected} complete"
            )
        atlas_path = write_source_atlas(results, results / "source_coverage_atlas.json")
        atlas = _load(atlas_path)
        if not atlas.get("frozen") or not atlas.get("release_snapshot_id"):
            raise RuntimeError("release atlas did not freeze")
    except Exception:
        ticket_path.write_bytes(old_tickets)
        source_path.write_bytes(old_sources)
        raise
    finally:
        candidate_source_path.unlink(missing_ok=True)
    return {
        "status": "complete",
        "release_snapshot_id": atlas["release_snapshot_id"],
        "atlas_hash": atlas["atlas_hash"],
        "ticket_status_counts": ticket_audit["status_counts"],
        "all_acceptance_complete": ticket_audit["all_acceptance_complete"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    args = parser.parse_args()
    print(json.dumps(promote_observatory_release(args.workspace.resolve()), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
