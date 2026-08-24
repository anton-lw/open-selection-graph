---
pretty_name: Open Selection Graph (OSG)
license: other
language:
  - en
tags:
  - metascience
  - peer-review
  - research-funding
  - patents
  - scientific-selection
  - observability
size_categories:
  - 1M<n<10M
---

# Open Selection Graph (OSG)

Open Selection Graph is a dataset for studying how candidates move through observable selection processes in science. It brings together public evidence from peer review, research funding and patent examination. The common unit is an institutional cycle: venues, tracks, calls, panels, journal periods, or examination processes governed by a defined set of rules.

Public records begin at different points. Conferences may expose submissions after desk screening, funders may publish the proposals considered by a panel, and patent sources may begin with publication or a recorded office action. OSG records starting points for populations to make the available denominator, hidden stages, and admissible comparisons visible to researchers from the outset.

Version 2.0.0 is a frozen release with a source cutoff of 23 August 2026.

## What the dataset contains

OSG represents candidates, document versions, institutional policies, evaluations, decisions, lineage, and later outcomes in one relational graph. Native terminology remains attached to each record. An acceptance decision, a fundable-band outcome, and a notice of allowance therefore retain their domain-specific meanings.

| Resource | Contents | Scale in version 2.0.0 |
|---|---|---:|
| Observability census | Public population boundaries, earliest visible stages, source coverage, and policy surfaces | 4,872 cycle records |
| Process atlas | Reconstructed publication-selection cycles with coverage and rate eligibility | 350 cycles |
| Audited OpenReview cohorts | Public candidate populations, decisions, withdrawals, and official-review counts | 55,369 candidates and 31,992 reviews |
| Research funding panels | UKRI application-panel events and anonymised SNSF proposal votes | 21,883 UKRI events and 4,811 SNSF vote cells |
| Patent examination | PANORAMA office-action trajectories, claim-state transitions, prior-art links, and capacity cells | 8,143 application trajectories |
| Released HUPD comparison population | Privacy-minimised metadata for published English-language US utility applications filed from 2004 through 2018 | 4,518,254 applications |

Additional tables describe manuscript versions, source-declared lineage, fixed-window publication outcomes, institutional rules, evaluation criteria, and semantic or recombinatorial measurements. SPECTER2 and Qwen3 provide separate semantic rulers over 8,501 shared time-valid document versions. Their disagreement is released as a measurement result.

## Understanding population coverage

The observability grade states which population a source can support.

| Grade | Public evidence | Appropriate interpretation |
|---|---|---|
| A | The observable population begins at entry to the modelled process | Entry-selection and stage-transition analysis |
| B | The observable population begins after a named institutional screen | Analysis conditional on reaching the public stage |
| C | Review histories are selected, published, or opt-in | Description of visible evaluation and revision histories |
| D | Awards, publications, or other outcomes form the public registry | Portfolio and outcome description |
| U | The public population boundary remains unresolved | Source auditing, disclosure research, and missingness analysis |

Grades belong to a source, cycle, and object type. They can change over time. Independent count evidence and stage-flow checks determine whether a cycle is eligible for denominator-based analysis. Version 2.0.0 contains 210 Grade A or B process cycles, including 200 with an observed candidate population. Sixty-one cycles support observed selection rates and 197 support review-incidence measures.

## Research uses

OSG supports questions such as:

- Which parts of a selection process become public, and when?
- How do review intensity and selection shares vary across observable institutional architectures?
- How do policy changes coincide with shifts in evaluation, workload, or candidate flow?
- How do manuscripts change across versions, venues, and later publication states?
- How stable are novelty results across scientific-document encoders and recombination measures?
- How do panel-stage funding choices and patent-examination trajectories compare with publication review at the level of process architecture?
- Which findings remain stable when hidden screens, linkage uncertainty, and source coverage are varied?

The dataset is suited to metascience, research evaluation, institutional comparison, measurement research, source auditing, and reproducible method development.

## Files and access

The release is organised into six licence-separated components.

| Component | Main contents | Licence |
|---|---|---|
| `metadata` | Coverage atlas, process flows, policies, rubrics, schemas, documentation, and benchmark definitions | CC0 1.0 |
| `derived_features` | Evaluations, novelty measures, lineage, outcome windows, construct benchmarks, and transport diagnostics | CC BY 4.0 |
| `funding_open_panels` | UKRI application-panel events and anonymised SNSF proposals, votes, and outcomes | CC BY 4.0 and OGL 3.0; source notices apply |
| `funding_noncommercial` | Funding opportunity, panel, portfolio, recurrence, and grant-output tables | CC BY-NC-SA 4.0 |
| `patent_noncommercial` | PANORAMA process tables and the full privacy-minimised HUPD boundary census for 2004 through 2018 | CC BY-NC-SA 4.0 |
| `validation` | Machine-readable coverage, leakage, linkage, privacy, licensing, and recovery reports | CC0 1.0 |

Each component includes a `LICENCE.json` file. The release root contains `VERSION.json`, `PACKAGE.json`, `MANIFEST.json`, `VALIDATION.json`, `TABLE_COUNTS.csv`, an analytical DuckDB database, and machine-readable schemas. The HUPD census remains a standalone Parquet table because of its size. Its row count and hash are verified with the rest of the package.

## Quick start

Query the analytical database with DuckDB:

```python
import duckdb

con = duckdb.connect("observatory.duckdb", read_only=True)

cycles = con.sql("""
    SELECT platform_label,
           effective_observability_grade,
           count(*) AS cycles
    FROM gate_cycle_descriptive_atlas
    GROUP BY 1, 2
    ORDER BY 1, 2
""").df()
```

Query the released HUPD comparison population directly:

```python
hupd_count = con.sql("""
    SELECT count(*) AS applications
    FROM read_parquet(
      'components/patent_noncommercial/hupd_application_population.parquet'
    )
""").fetchone()[0]
```

For selection-rate research, begin with `gate_cycle_descriptive_atlas`. Filter on `descriptive_rate_allowed`, confirm a positive `observable_count`, and report `earliest_public_stage` with the result. Evaluation research should join `evaluation_objects` to the exact candidate version and retain the native rubric and scale.

## Data sources

The dataset was assembled from public APIs, OAI-PMH services, bulk files, structured publisher records, public workbooks, and openly available research corpora. Every source has a versioned card describing its role, public stage, access method, identifiers, licensing, expected-count evidence, and realised release state.

The catalogue uses three reader-facing states:

- **Released** means that the source contributes records or computational dependencies to version 2.0.0.
- **Pointer** means that OSG supplies provenance, retrieval instructions, or validation evidence while the upstream object remains with its provider.
- **Catalogued** means that the source was assessed and retained in the source census. Its current coverage, access, or licensing state prevents row-level inclusion.

<details>
<summary><strong>Complete source catalogue: all 53 registered source cards</strong></summary>

<!-- source-card-order
openreview_surface
openreview_api
openreview
copernicus
copernicus_crossref
copernicus_outcomes
elife
elife_process
f1000research
f1000_process
wellcome_open_research_process
gates_open_research_process
nihr_open_research_process
hrb_open_research_process
scipost
scipost_process
peerj
plos_review_history
embo_transparent_review
royal_society_review
bmc_open_review
qeios
prereview
reviewarena
re2
nlpeer
researcharcade
peerread
moprd
arr_review_arcade
crossref
openalex
arxiv
kaggle_arxiv_snapshot
europe_pmc
ukri_demystifying
fwf
snsf
ukri_public_outcomes
lottery_instrument_web
snsf_individual_votes
uspto_oard
uspto_patex
uspto_claims
uspto_odp
panorama
patent_cr
patre
hupd
specter2
qwen3_embedding_0_6b
peer_review_analyze_1_0
preprint_to_paper_gray_zone
-->

### Publication platforms and process records

| Source | Role | State |
|---|---|---|
| OpenReview public configuration | Venue groups, invitations, permissions, rubrics, and policy surfaces | Released |
| OpenReview audited API cohorts | Public submission-state populations, decisions, withdrawals, and reviews | Released |
| OpenReview legacy venue connector | Earlier venue enumeration pathway retained for provenance | Catalogued |
| Copernicus public discussion | Discussion papers, referee comments, replies, and versions | Released |
| Crossref/Copernicus denominator layer | Independent discussion-population reconciliation | Released |
| Copernicus outcomes | Later article and discussion relations | Released |
| eLife Reviewed Preprint census | Editorial-model source census | Catalogued |
| eLife process records | Reviewed Preprints, reviews, responses, assessments, and policy cohorts | Released |
| F1000Research records | Published articles, versions, reports, and responses | Released |
| F1000 platform-family process records | Shared provider-native process representation | Released |
| Wellcome Open Research | Public post-publication review process | Released |
| Gates Open Research | Public post-publication review process | Released |
| NIHR Open Research | Public post-publication review process | Released |
| HRB Open Research | Platform-family source census | Catalogued |
| SciPost source census | Public submissions and refereeing pages | Catalogued |
| SciPost process records | Editor-assigned public submissions, reports, replies, and decisions | Released |
| PeerJ review histories | Published review histories | Pointer |
| PLOS review histories | Author opt-in published histories | Pointer |
| EMBO Press transparent review | Published process files | Pointer |
| Royal Society review histories | Published process files | Pointer |
| BMC open review | Open-review journal histories | Pointer |
| Qeios | Public preprints and evaluation surface | Catalogued |
| PREreview | Community reviews of public preprints | Catalogued |

### Derived peer-review corpora

| Source | Role | State |
|---|---|---|
| ReviewArena | Derived review corpus | Pointer |
| Re2 | Derived review corpus | Pointer |
| NLPeer | Multi-domain peer-review corpus | Pointer |
| ResearchArcade | Derived OpenReview research corpus | Pointer |
| PeerRead | Papers, reviews, and decisions from selected venues | Pointer |
| MOPRD | Transparent-review histories | Catalogued |
| Review Arcade / ACL ARR consent programme | Consented peer-review research corpus | Pointer |

### Scholarly metadata and content

| Source | Role | State |
|---|---|---|
| Crossref | Bibliographic metadata and work relations | Released |
| OpenAlex | Scholarly identities, works, citations, and concepts | Released |
| arXiv | Preprint metadata and version history | Released |
| Cornell/Kaggle arXiv snapshot | Bulk mirror assessed for recovery | Catalogued |
| Europe PMC | Biomedical identifiers, metadata, JATS, references, and corrections | Released |

### Research funding

| Source | Role | State |
|---|---|---|
| Demystifying Funding / UKRI public sources | Panel and outcome evidence | Pointer |
| Austrian Science Fund Open API | Public funded-project registry | Released |
| Swiss National Science Foundation Data Portal | Public funded-project registry | Released |
| UKRI public outcomes | Council and call outcome workbooks | Catalogued |
| Public lottery-instrument pages | Funding-policy and allocation-mechanism census | Catalogued |
| SNSF individual-vote dataset | Anonymised proposal, panel, grade, conflict, and outcome cells | Pointer |

### Patent examination

| Source | Role | State |
|---|---|---|
| USPTO Office Action Research Dataset | Public prosecution events | Released |
| USPTO PatEx | Public application and examination records | Released |
| USPTO Patent Claims Research Dataset | Published and granted claim records | Released |
| USPTO Open Data Portal | Application-level retrieval pathway | Catalogued |
| PANORAMA | Public rejection-to-allowance process trajectories | Pointer |
| Patent-CR | Rejected-to-granted claim-revision pairs | Pointer |
| PatRe | Public prosecution cases and revision evidence | Pointer |
| Harvard USPTO Patent Dataset | Public US utility-application population | Pointer |

### Models and human benchmarks

| Source | Role | State |
|---|---|---|
| SPECTER2 | Scientific-document embedding ruler | Released |
| Qwen3-Embedding-0.6B | Independent text-embedding ruler | Released |
| Peer Review Analyze 1.0 | Human-labelled free-form review benchmark | Pointer |
| PreprintToPaper Gray Zone | Human-adjudicated lineage benchmark | Pointer |

</details>

Normalised facts retain links to source and provenance records. Content governed by source-specific terms is represented through identifiers, retrieval recipes, and verification hashes.

## Construction and measurement

Semantic measurements use revision-pinned SPECTER2 and Qwen3 encoders. Each target year is compared with earlier documents. Citation and textual recombination are calculated as separate measures because they describe different opportunity spaces. Source-declared relations form the analysis-grade lineage layer; probabilistic candidates are supplied for discovery and sensitivity analysis.

Policy, evaluation, and outcome fields preserve provider terminology before normalisation. Point-in-time views align document versions, policy states, and downstream records with the date of the modelled decision.

## Validation

Validation operates at the population, record, temporal, and package levels.

- Five OpenReview shards reconcile with provider-reported public candidate counts for all 176 audited cycles.
- Stage-flow identities check observable, reviewed, withdrawn, rejected, selected, and unresolved counts.
- Temporal tests exclude later document versions, future citations, and identities revealed after a decision.
- Native outcomes and rubric values are preserved through normalisation.
- Funding workbooks reconcile panel totals, outcomes, repeated appearances, and duplicate rows.
- Every PANORAMA case has one HUPD reconciliation state. The states account for all 8,143 cases.
- Every pointer row has an absolute HTTPS retrieval target, an object locator, and at least one payload-derived SHA-256 verification hash.
- The release scanner checks schemas and packaged paths for protected identity fields, contacts, raw objects, and restricted content.
- Clean-room tests rebuild representative public fixtures in an isolated environment.
- Package verification checks 72 Parquet tables, the DuckDB tables, the external HUPD table, and every file hash.

Machine-readable reports for these checks are included in the `validation` component.

## Scope and limitations

OSG describes public evidence. Hidden editorial, administrative, and legal stages create selection into observation. The coverage grade identifies the earliest visible stage, while partial-identification and sensitivity tables describe compatible hidden populations.

Institutional populations differ in field, calendar period, architecture, size, selection share, and review intensity. Pairwise transport diagnostics measure overlap in the recorded dimensions. Cycle-level domain, policy, and disclosure modifiers have incomplete coverage and remain required inputs for stronger cross-source transport claims.

Publication outcomes use fixed observation windows. Exact publication dates are unidentified for much of the linked panel, and unmatched candidates retain an unknown state. Free-form review constructs and probabilistic lineage are external benchmarks or discovery aids. Validated source-declared evidence governs canonical links.

The funding tables begin at the observed panel stage. Pre-panel applicants and randomised allocation arms fall outside the available public choice sets. The HUPD comparison population covers published English-language US utility applications filed from 2004 through 2018. PANORAMA supplies a separate 8,143-case process sample. Its reconciliation is complete: 3,416 cases match HUPD exactly, 3,532 fall outside HUPD's filing-year boundary, and 1,195 fall within the boundary but are absent from the frozen HUPD population. Confidential applications and applicant response documents are outside the released evidence.

## Privacy, licensing, and responsible use

The public package minimises person-level information. Evaluation identifiers, protected authorship identifiers, direct contact fields, source full text, proposal text, claim text, and raw acquisition objects are excluded. Funding and reviewer-supply measures are aggregated or de-identified according to their source conditions.

OSG is intended for research on institutions, populations, and measurement. The release policy prohibits reviewer deanonymisation, identity inference aimed at anonymous people, individual employment or funding evaluation, personnel ranking, harassment, surveillance, automated targeting, and reconstruction of restricted text.

Licensing is component-specific. Consult the `LICENCE.json` file beside each component and the source attribution records before reuse. A verification hash establishes provenance; access and redistribution remain governed by the applicable source terms.

## Citation and contact

Please cite the dataset and its Data Descriptor:

> Waagaard, A. *Open Selection Graph (OSG): a dataset for mapping observable populations and selection processes in science*. Version 2.0.0 (2026).

Creator: Anton Waagaard  
Affiliation: Dimensional Impact Lab  
Contact: anton@dimpactlab.org

Corrections and takedown requests are documented in `TAKEDOWN_AND_CORRECTIONS.md`. Versioned manifests record every amended release.
