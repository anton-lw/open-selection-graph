# Open Selection Graph (OSG) constitution

**Version:** 0.1.0  
**Effective:** 2026-08-10  
**Authority:** `TICKETBOOK_OPEN_SELECTION_GRAPH.md`, Epics A–Q

## Purpose

OSG represents public selection processes in science and adjacent expert gates. Its contribution is the joined process layer: observable candidate pools, institutional rules, evaluations, decisions, versions, capacity, trajectories, and downstream outcomes.

The primary unit is a **gate cycle**: the smallest venue/track/call/policy-stable period for which rules, capacity, and the observable pool have a coherent meaning. Candidate documents are subordinate units.

## Hard constraints

1. No bespoke partnership, private data agreement, or newly recruited human subject is required.
2. No paid data/model API or service that can silently incur usage charges is permitted.
3. Public access is not permission to deanonymize reviewers, expose contact details, rank individuals, or redistribute content without an affirmative licence.
4. Every normalized value is traceable to a source object, retrieval event, and transformation version.
5. Native institutional stages, labels, rubrics, and policies are retained before normalisation.
6. Missing choice sets, treatment labels, or stage boundaries cause `not_identified`, a bound, or a narrower description—not proxy substitution.
7. Credentials never enter Git, source cards, logs, manifests, notebooks, command arguments, or releases.

## Observability grades

- **A — entry-complete:** the earliest submitted pool is verified, including public withdrawals and rejections.
- **B — stage-complete:** the pool is verified after a named hidden screen.
- **C — selected/opt-in history:** evaluation history exists only for selected, accepted, or opted-in works.
- **D — outcome registry:** visible outcomes/winners lack a comparable candidate pool.
- **U — unresolved:** the observable population cannot yet be established.

Grades attach to `source × gate_cycle × object_type`; they are not permanent provider labels. Entry-selection estimands require A. Conditional stage-selection estimands permit A/B. C/D support process/portfolio description only. U is quarantined.

## Architecture preservation

Competitive quotas, rolling thresholds, public discussion, reviewed-preprint publication, post-publication review, fundable-band lotteries, and patent prosecution retain their native stages. A generic `accepted` flag may be derived for a specific analysis but may not replace the source state machine.

## Temporal truth

OSG distinguishes source creation, source modification, retrieval, policy effectiveness, decision time, and public-identity disclosure time. Decision models may use only fields available to the relevant decision maker by the decision date. Later versions, citations, and revealed authorship are outcomes unless an estimand states otherwise.

## Release and amendment

Dataset, schema, linkage, feature, and source snapshots are versioned separately. Historical releases are immutable. A constitutional amendment requires a dated entry in `docs/observatory/DECISIONS.md`, the changed text, affected tickets/estimands/releases, and a version increment. No amendment may relax the no-paid-API, no-new-subjects, secret-safety, or identification-firewall constraints.
