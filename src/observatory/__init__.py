"""Open Selection Graph (OSG).

The package implements the public-data process graph specified in
``TICKETBOOK_GATEKEEPING_OBSERVATORY.md``.  Its invariants are deliberately
stricter than the individual paper pipelines: every normalized record carries
source provenance, an observability boundary, temporal truth, and an
object-level release decision.
"""

from .constitution import (
    AnalysisClass,
    Architecture,
    ObservabilityGrade,
    SourceStatus,
    admissible,
    evaluate_estimand,
)
from .ids import stable_id

__all__ = [
    "AnalysisClass",
    "Architecture",
    "ObservabilityGrade",
    "SourceStatus",
    "admissible",
    "evaluate_estimand",
    "stable_id",
]

__version__ = "0.1.0"
