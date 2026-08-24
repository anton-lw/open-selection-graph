"""Constitution-enforcing DuckDB analysis views."""

from __future__ import annotations

import re

VIEW_SQL: dict[str, str] = {
    "entry_selection": """
        SELECT e.*, c.observability_grade, c.earliest_public_stage
        FROM candidate_gate_event e
        JOIN coverage_observation c ON e.coverage_observation_id = c.coverage_observation_id
        WHERE c.observability_grade = 'A'
    """,
    "stage_selection": """
        SELECT e.*, c.observability_grade, c.earliest_public_stage
        FROM candidate_gate_event e
        JOIN coverage_observation c ON e.coverage_observation_id = c.coverage_observation_id
        WHERE c.observability_grade IN ('A', 'B')
    """,
    "evaluation_descriptive": """
        SELECT e.*, c.observability_grade
        FROM evaluation e
        JOIN candidate_gate_event ge
          ON e.candidate_version_id = ge.candidate_version_id
         AND e.gate_cycle_id = ge.gate_cycle_id
        JOIN coverage_observation c
          ON ge.coverage_observation_id = c.coverage_observation_id
        WHERE c.observability_grade IN ('A', 'B', 'C')
    """,
    "portfolio_descriptive": """
        SELECT ge.*, c.observability_grade
        FROM candidate_gate_event ge
        JOIN coverage_observation c ON ge.coverage_observation_id = c.coverage_observation_id
        WHERE c.observability_grade IN ('A', 'B', 'C', 'D')
    """,
    "stage_transitions": """
        SELECT ge.*, d.stage_native, d.stage_normalized, d.outcome_native,
               d.outcome_normalized, d.decided_at, c.observability_grade
        FROM candidate_gate_event ge
        JOIN coverage_observation c ON ge.coverage_observation_id = c.coverage_observation_id
        LEFT JOIN decision_event d
          ON ge.candidate_version_id = d.candidate_version_id
         AND ge.gate_cycle_id = d.gate_cycle_id
        WHERE c.observability_grade IN ('A', 'B', 'C', 'D')
    """,
    "lineage": """
        SELECT l.*, sv.created_at AS source_created_at, tv.created_at AS target_created_at
        FROM lineage_edge l
        LEFT JOIN candidate_version sv ON l.source_version_id = sv.candidate_version_id
        LEFT JOIN candidate_version tv ON l.target_version_id = tv.candidate_version_id
        WHERE l.declared OR (l.linkage_tier = 'analysis' AND l.confidence >= 0.97)
    """,
    "afterlife": """
        SELECT o.*, c.canonical_title, c.candidate_type
        FROM downstream_outcome o JOIN candidate c ON o.candidate_id = c.candidate_id
        WHERE o.censoring_date IS NOT NULL
    """,
    "funding_evaluability": """
        SELECT ge.*, c.observability_grade
        FROM candidate_gate_event ge
        JOIN coverage_observation c ON ge.coverage_observation_id = c.coverage_observation_id
        JOIN gate_cycle gc ON ge.gate_cycle_id = gc.gate_cycle_id
        WHERE gc.architecture = 'fundable_band_lottery'
          AND c.observability_grade IN ('A', 'B', 'C', 'D')
    """,
    "patent_examination": """
        SELECT ge.*, c.observability_grade
        FROM candidate_gate_event ge
        JOIN coverage_observation c ON ge.coverage_observation_id = c.coverage_observation_id
        JOIN gate_cycle gc ON ge.gate_cycle_id = gc.gate_cycle_id
        WHERE gc.architecture = 'prosecution_examination'
          AND c.observability_grade IN ('A', 'B')
    """,
    "licence_safe_content": """
        SELECT * FROM content_artifact WHERE release_class = 'redistribute'
    """,
}


def install_views(connection) -> None:
    existing = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
    for name, query in VIEW_SQL.items():
        needed = {
            token for token in (
                "candidate_gate_event", "coverage_observation", "evaluation", "decision_event",
                "lineage_edge", "candidate_version", "downstream_outcome", "candidate",
                "gate_cycle", "content_artifact",
            ) if re.search(rf"\b{re.escape(token)}\b", query)
        }
        if needed <= existing:
            connection.execute(f'CREATE OR REPLACE VIEW "analysis_{name}" AS {query}')
