# OSG release migrations

Release diffs enumerate row/object additions and removals, corrections, observability-grade changes, ID merges/splits, schema changes, and feature-version changes. Analyses must match `schema_version`, `source_snapshot_version`, `normalized_data_version`, `linkage_model_version`, `feature_version`, and `release_package_version` before joining.

Breaking changes never overwrite an artifact. The migration tool refuses ambiguous ID changes unless a one-to-many or many-to-one mapping and reason are supplied. Removed objects become versioned tombstones. The 1.0 migration establishes the integrated R1–R5 registry; its machine-readable diff is generated in `results/observatory/r5/release_diff_0.1.0-r1_to_1.0.0.json`.

