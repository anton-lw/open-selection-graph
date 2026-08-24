# Open Selection Graph

Open Selection Graph (OSG) is a research dataset for studying observable populations and selection processes in science. It represents candidates, versions, evaluations, decisions, institutional rules, capacity, provenance and downstream outcomes while preserving the distinctions between editorial, conference, open-review, funding and patent processes.

This repository contains the source code, schemas, source registry, validation rules, governance documents and reproducible examples for OSG release 2.0.0. The licence-separated data components are distributed through the accompanying dataset deposits. The release contains a 4,518,254-row published US patent-application census and a separately identified 8,143-case PANORAMA process sample. The crosswalk assigns every PANORAMA case one reconciliation state and preserves the sources' different temporal and coverage boundaries.

## Repository contents

- `src/observatory/`: ingestion, normalisation, validation, feature and release code
- `configs/observatory/`: source, estimand, observability, licence and release registries
- `schemas/observatory/`: JSON Schema and DuckDB schema definitions
- `docs/observatory/`: data card, methods paper, governance and use guidance
- `tests/`: unit and integrity tests with public fixtures
- `modal_observatory.py` and `modal_validity.py`: optional large-scale execution entry points

## Installation

OSG supports Python 3.11 to 3.13.

```bash
python -m pip install -e ".[dev]"
PYTHONPATH=src python -m pytest tests/test_observatory_core.py tests/test_observatory_hardening.py tests/test_observatory_limitations.py -m "not release_assets" -q
PYTHONPATH=src python -m observatory.cli policy
```

The `release_assets` tests exercise the complete licence-separated data package and run against the accompanying dataset deposit. The remaining repository tests use public fixtures and execute from a code-only checkout.

The data card in `docs/observatory/DATA_CARD.md` describes the six release components, every registered source, population boundaries, linkage classes, observability grades, licences and recommended uses. `docs/observatory/SCHEMA_HANDBOOK.md` provides table-level guidance.

## Citation and contact

Citation metadata are provided in `CITATION.cff`. Questions, corrections and takedown requests may be sent to Anton Waagaard, Dimensional Impact Lab, at [anton@dimpactlab.org](mailto:anton@dimpactlab.org).

## Licence

Source code is released under the MIT Licence. Dataset components and incorporated source material retain the licences recorded in their component manifests and source cards. Those component-level terms take precedence for data reuse.
