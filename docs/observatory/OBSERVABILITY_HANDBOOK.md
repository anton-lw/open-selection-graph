# OSG observability handbook

## Why observability is data

A portal can expose thousands of records while hiding the gate entry population. OSG therefore stores expected/found counts, count method, query/invitation, earliest public stage, known hidden stages, exclusions, missing reason, audit status, and validity interval as `coverage_observation` rows. Coverage is not a footnote attached after analysis.

## Grades

- A — gate-entry population is observable. Entry-selection estimands may be identified subject to count validation.
- B — public observation begins after a documented access/desk screen. Only stage-conditional selection estimands are admissible.
- C — selected, published, or opt-in history. Descriptive evaluation/process claims only.
- D — winner/output registry. Portfolio description only; failed/unsuccessful applications are not inferred.
- U — denominator, stage, or source behavior is unresolved.

An advertised A/B grade is downgraded to U when independently reconciled count coverage is below 0.95 or a stricter source threshold. Flow identities can also force downgrade.

## Denominator checklist

Before computing a rate, record:

1. the gate and gate cycle;
2. the event stage at which units enter the denominator;
3. the candidate/version/event unit;
4. expected and found counts and their methods;
5. hidden stages and exclusions;
6. grade and validity interval;
7. censoring/maturity rule;
8. whether linkage or text availability further selects the sample.

If any item is unknown, use a descriptive count, bound, or explicit `not_identified` result rather than silently choosing a convenient denominator.

## Source examples

OpenReview cycles with complete public state enumeration can support grade A or B only after invitation-specific provider counts and readable public Note checks pass. Public configuration alone does not establish submission coverage.

Copernicus public discussion starts at public preprint/discussion-paper deposit. It does not observe manuscripts never reaching that stage.

Transparent-review journal histories and Crossref review relations are selected-only unless an independently observed candidate pool exists. Their correct denominator is the selected/published parent population.

Funding winner registries are grade D. A success rate, unsuccessful-applicant comparison, lottery allocation effect, or repeat-application trajectory is `not_identified` without the relevant applicant/eligible-band records.

Patent public application records are conditioned on public filing/publication and document availability. They cannot represent all filed or abandoned confidential applications without external bounds.

## Missingness

Metadata rows remain even when text, reviews, references, outcomes, or identities are absent. A feature table must name its minimum text/reference coverage, and its public analysis view must include a missingness table. “Not found” is never automatically “unpublished,” “abandoned,” “no review,” or “no later influence.”

## Invalid queries

- Dividing accepted decisions by visible reviews and calling it acceptance rate.
- Treating a selected transparent-review corpus as all submissions.
- Using final-version novelty to predict an earlier gate decision.
- Treating an unmatched rejected candidate as abandoned.
- Inferring failed funding applications from a winner registry.
- Equating patent legal novelty/obviousness with scientific semantic novelty by name alone.

