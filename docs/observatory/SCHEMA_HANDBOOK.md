# OSG schema handbook

## Reading the schema

The normative field definitions are generated in `schemas/observatory/*.schema.json` and `schemas/observatory/observatory.sql`. Every normalized fact links to a `source_object_id` and `provenance_event_id`; field-level facts additionally link through `field_provenance`.

The DuckDB catalog exposes two views for each canonical table:

- `<table>_history` is the append-only union of immutable shards and revisions.
- `<table>` is the deterministic current record per primary key, ordered by record version and observation time.

Analyses normally use the current view and name the source snapshot. Historical/revision studies use the history view and an explicit as-of cutoff.

## Core relationships

`gate` owns `gate_cycle`; each cycle names an architecture and a policy observation. `candidate` owns `candidate_version`. `candidate_gate_event` joins a candidate/version to a cycle and a coverage observation. `evaluation` and `decision_event` join to the exact candidate version and cycle when knowable. `lineage_edge` links candidates/versions without forcing uncertain descendants into one canonical work. `downstream_outcome` records eligibility, occurrence, windows, and censoring.

## Native and normalized values

Native values are never overwritten by normalized values. Examples include:

- `decision_event.outcome_native` and `outcome_normalized`;
- `evaluation.criterion_native`, `criterion_value`, `criterion_value_numeric`, and `scale_json`;
- `field_assignment.native_label` and `normalized_label`;
- `policy_version.criteria_json`, `rubric_json`, and `stage_rules_json`.

An unsupported outcome mapping remains native-only. A scale change receives a new policy/rubric version. An ambiguous construct mapping is multi-valued or unmapped.

## Temporal fields

`created_at` and `modified_at` describe the source object/entity where declared. `retrieved_at` is acquisition time. `observed_at` is when the normalized fact became observable in the snapshot. `effective_at`/`valid_to` describe policy validity. `visible_from`/`visible_to` describe identity visibility. `censoring_date` closes a downstream observation window.

Decision-time predictors must be available no later than the decision. Final versions, future citations, and identities revealed after the decision are not admissible predictors. Reference corpora require `reference.created_at < target cutoff`.

## Identifiers

Canonical IDs are namespaced deterministic UUIDv5 values such as `obs:candidate:*`. Native IDs and aliases remain in `native_id` or `identifier_alias`. A canonical ID is not evidence that two uncertain records are the same work; probabilistic relations belong in `lineage_edge` with confidence and linkage tier.

## Release version columns

Release views name schema version, source snapshot version, normalized-data version, linkage-model version, feature version, and release-package version. A source refetch always creates a new source snapshot. A feature formula/reference/model change always creates a new feature version. Breaking changes receive migration notes and a new semantic version.

## Correct query examples

Entry selection: join `candidate_gate_event` to `coverage_observation`, restrict to grade A, and use all entry events as the denominator. Do not use review rows as the denominator.

Stage-conditional selection: restrict to grades A/B and state the earliest public stage. The estimand is conditional on reaching that stage.

Evaluation description: join `evaluation` to the exact candidate version and cycle; describe only visible evaluations. `official_status=unspecified` is not official, and a public comment is not silently a review.

Selected review history: grade C is valid only for the selected/opt-in population. Do not compare it to all submissions unless an independently observed parent denominator and selection model are supplied.

