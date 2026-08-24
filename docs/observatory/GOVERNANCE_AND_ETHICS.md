# OSG data governance and ethics plan

**Plan version:** 0.2.0  
**Constitutional baseline effective:** 2026-08-10  
**Field-catalogue amendment:** 2026-08-20

OSG is public-data research about institutional processes. It does not recruit, contact, intervene upon, or form bespoke data agreements with human subjects. Public availability is treated as a necessary acquisition condition, not as blanket permission to identify people, redistribute content, or support decisions about individuals.

Its purposes are to document observable gate cycles, preserve their architecture and temporal state, quantify coverage, and enable qualified research on selection and institutional dynamics. Maintainers may access encrypted raw objects and restricted fields when source terms allow; research views remove secrets and purpose-irrelevant identity; public releases are generated only through the release policy in `configs/observatory/governance.yaml`.

## Data categories and release tiers

The canonical categories are institutional rules, candidates and versions, evaluations, decisions, lineage, capacity, coverage, outcomes, content metadata, identifiers, identity visibility, and provenance. The machine-readable plan assigns a purpose to every canonical table. `release_field_catalogue()` expands those table purposes over every schema field and resolves exactly one tier for each field:

- `public`: redistributable normalized facts and non-disclosive aggregates;
- `pointer_hash`: public pointers/hashes where content redistribution is not licensed;
- `aggregate_only`: released only after disclosure control;
- `restricted`: absent from public row-level releases.

The catalogue is executable: an unknown table, field, tier, or unclassified schema addition fails validation. Native/raw identifiers, evaluator identity fields, person identifiers, signatures/readers, and local/raw pointers are restricted. Text remains licence-governed; a hash never grants permission to reconstruct or redistribute the source.

## Risk groups and prohibited uses

Anonymous or pseudonymous evaluators face the highest re-identification risk. Named authors, applicants, editors, and decision makers face ranking, employment, harassment, and targeting risk. Small institutional cells face singling-out risk. Accordingly, the project does not support reviewer deanonymization, identity-inference joins, individual productivity or quality rankings, employment/funding evaluation, harassment, surveillance, automated targeting, or restricted-text reconstruction. See `PROHIBITED_USES.md` for the public release contract.

## Retention, security, and takedown

Raw data are retained only as required for reproducible normalisation and only while terms permit. Normalised releases are immutable; deletions produce tombstones and amended releases instead of silent history rewrites. Credentials are ephemeral inputs from operating-system or Modal secret stores and are never required to reproduce public outputs.

Takedown or correction requests use the repository security/contact channel. Maintainers quarantine the affected object or release field, preserve only a non-disclosive tombstone where necessary for provenance, document the decision, and publish a versioned amendment. The plan and its resolved field catalogue are audited by `gate-observatory validate`.
