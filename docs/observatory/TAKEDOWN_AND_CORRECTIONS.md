# Takedown, correction, and source-removal protocol

Requests enter through the repository security/contact channel. A maintainer verifies that the request identifies a source object or a demonstrable disclosure risk, records an impact assessment, and assigns a request ID. The request must not include additional personal data beyond what is necessary to authenticate the claim.

The response is versioned and ordered: quarantine the raw-access pointer; tombstone normalized rows; invalidate dependent evaluations, features, lineages, and outcomes; exclude the object from analysis views; then append release errata. Released identifiers are not silently reused. A tombstone states only the minimum provenance needed to prevent accidental resurrection. Immutable third-party archives are outside OSG's control and are disclosed rather than promised deleted.

The executable synthetic drill is `results/observatory/r5/removal_propagation_simulation.parquet`. It exercises every layer without deleting any live record.

