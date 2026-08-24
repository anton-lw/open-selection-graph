"""Provider/dual-method count reconciliation and automatic grade discipline."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CountReconciliation:
    expected_count: int | None
    found_count: int
    coverage_ratio: float | None
    methods: tuple[str, ...]
    independent_methods: int
    requested_grade: str
    final_grade: str
    passes: bool
    reason: str


def reconcile_count(
    *,
    found_count: int,
    expected_counts: dict[str, int | None],
    requested_grade: str,
    threshold: float = 0.95,
) -> CountReconciliation:
    known = {name: int(value) for name, value in expected_counts.items() if value is not None}
    expected = max(known.values()) if known else None
    ratio = min(found_count / expected, 1.0) if expected else None
    enough_evidence = bool(known) and (len(known) >= 2 or any("provider" in name.lower() for name in known))
    passes = bool(ratio is not None and ratio >= threshold and enough_evidence)
    final = requested_grade
    reason = "reconciled"
    if requested_grade in {"A", "B"} and not passes:
        final = "U"
        reason = "A/B requires >=95% provider or dual-method count reconciliation"
    return CountReconciliation(
        expected, found_count, ratio, tuple(sorted(known)), len(known), requested_grade,
        final, passes, reason,
    )

