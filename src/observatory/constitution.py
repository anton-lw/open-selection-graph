"""Constitutional types and executable claim-admissibility rules.

This module is the implementation counterpart of
``docs/observatory/CONSTITUTION.md``.  Analysis code should call
``evaluate_estimand`` instead of inferring that a populated table is a valid
choice set.  The firewall is intentionally small, deterministic, and easy to
audit.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterable, Mapping, Sequence


class ObservabilityGrade(str, Enum):
    """What population a source-cycle-object observation represents."""

    A = "A"  # entry-complete
    B = "B"  # stage-complete after a named hidden screen
    C = "C"  # selected/accepted/opt-in history
    D = "D"  # outcome registry without a comparable candidate pool
    U = "U"  # unresolved; quarantined from substantive analysis


class SourceStatus(str, Enum):
    INCLUDED = "included"
    POINTER_ONLY = "pointer_only"
    DERIVED_ONLY = "derived_only"
    QUARANTINED = "quarantined"
    BLOCKED_BY_TERMS = "blocked_by_terms"
    BLOCKED_BY_COVERAGE = "blocked_by_coverage"
    RETIRED = "retired"


class AnalysisClass(str, Enum):
    ENTRY_SELECTION = "entry_selection"
    STAGE_SELECTION = "stage_selection"
    EVALUATION_DESCRIPTION = "evaluation_description"
    PORTFOLIO_DESCRIPTION = "portfolio_description"
    TRAJECTORY = "trajectory"
    POLICY_DESCRIPTION = "policy_description"
    BOUNDS = "bounds"


class Architecture(str, Enum):
    COMPETITIVE_QUOTA = "competitive_quota"
    ROLLING_THRESHOLD = "rolling_threshold"
    ACCESS_PUBLIC_DISCUSSION = "access_public_discussion"
    PUBLISH_REVIEW_CURATE = "publish_review_curate"
    POST_PUBLICATION_REVIEW = "post_publication_review"
    FUNDABLE_BAND_LOTTERY = "fundable_band_lottery"
    PROSECUTION_EXAMINATION = "prosecution_examination"
    UNKNOWN = "unknown"


_ADMISSIBLE: Mapping[AnalysisClass, frozenset[ObservabilityGrade]] = {
    AnalysisClass.ENTRY_SELECTION: frozenset({ObservabilityGrade.A}),
    AnalysisClass.STAGE_SELECTION: frozenset({ObservabilityGrade.A, ObservabilityGrade.B}),
    AnalysisClass.EVALUATION_DESCRIPTION: frozenset(
        {ObservabilityGrade.A, ObservabilityGrade.B, ObservabilityGrade.C}
    ),
    AnalysisClass.PORTFOLIO_DESCRIPTION: frozenset(
        {ObservabilityGrade.A, ObservabilityGrade.B, ObservabilityGrade.C, ObservabilityGrade.D}
    ),
    AnalysisClass.TRAJECTORY: frozenset(
        {ObservabilityGrade.A, ObservabilityGrade.B, ObservabilityGrade.C, ObservabilityGrade.D}
    ),
    AnalysisClass.POLICY_DESCRIPTION: frozenset(
        {ObservabilityGrade.A, ObservabilityGrade.B, ObservabilityGrade.C, ObservabilityGrade.D}
    ),
    AnalysisClass.BOUNDS: frozenset(
        {ObservabilityGrade.A, ObservabilityGrade.B, ObservabilityGrade.C, ObservabilityGrade.D}
    ),
}


def admissible(grade: ObservabilityGrade | str, analysis: AnalysisClass | str) -> bool:
    """Return whether ``grade`` may support ``analysis`` under the constitution."""

    g = grade if isinstance(grade, ObservabilityGrade) else ObservabilityGrade(grade)
    a = analysis if isinstance(analysis, AnalysisClass) else AnalysisClass(analysis)
    return g in _ADMISSIBLE[a]


@dataclass(frozen=True)
class EstimandVerdict:
    estimand_id: str
    verdict: str
    missing_fields: tuple[str, ...]
    reason: str

    @property
    def identified(self) -> bool:
        return self.verdict == "identified"


def evaluate_estimand(
    *,
    estimand_id: str,
    analysis_class: AnalysisClass | str,
    admissible_grades: Sequence[ObservabilityGrade | str],
    required_fields: Iterable[str],
    observed_fields: Mapping[str, object] | Iterable[str],
    grade: ObservabilityGrade | str,
    partial_if_missing: bool = False,
) -> EstimandVerdict:
    """Evaluate identification from declared requirements, never proxy presence.

    ``observed_fields`` may be a mapping, in which case ``None`` and the empty
    string are treated as absent, or an iterable of populated field names.
    ``partial_if_missing`` is reserved for estimands with an explicit bounding
    strategy; it never converts an inadmissible observability grade.
    """

    g = grade if isinstance(grade, ObservabilityGrade) else ObservabilityGrade(grade)
    a = analysis_class if isinstance(analysis_class, AnalysisClass) else AnalysisClass(analysis_class)
    declared_grades = {
        x if isinstance(x, ObservabilityGrade) else ObservabilityGrade(x) for x in admissible_grades
    }
    if not admissible(g, a) or g not in declared_grades:
        return EstimandVerdict(
            estimand_id,
            "not_identified",
            (),
            f"observability grade {g.value} is inadmissible for {a.value}",
        )

    if isinstance(observed_fields, Mapping):
        present = {
            key
            for key, value in observed_fields.items()
            if value is not None and value != "" and value != [] and value != {}
        }
    else:
        present = set(observed_fields)
    missing = tuple(sorted(set(required_fields) - present))
    if not missing:
        return EstimandVerdict(estimand_id, "identified", (), "all declared fields are present")
    if partial_if_missing:
        return EstimandVerdict(
            estimand_id,
            "partially_identified",
            missing,
            "one or more required point-identification fields are absent; use registered bounds",
        )
    return EstimandVerdict(
        estimand_id,
        "not_identified",
        missing,
        "required fields are absent",
    )


def allowed_grades(analysis: AnalysisClass | str) -> tuple[str, ...]:
    a = analysis if isinstance(analysis, AnalysisClass) else AnalysisClass(analysis)
    return tuple(sorted(g.value for g in _ADMISSIBLE[a]))
