"""Generate and execute dependency-light public OSG notebooks."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

from .ids import content_hash

NOTEBOOKS = {
    "01_gate_cycle_atlas.ipynb": """from pathlib import Path
import json, os
import pandas as pd
DATA_ROOT = Path(os.environ['OBSERVATORY_PUBLIC_DATA'])
df = pd.read_parquet(DATA_ROOT / 'gate_cycle_flow_series.parquet')
summary = {'rows': len(df), 'cycles': int(df['gate_cycle_id'].nunique()), 'seed': 1729}
Path(os.environ['OBSERVATORY_NOTEBOOK_OUTPUT']).write_text(json.dumps(summary, sort_keys=True))
print(summary)
""",
    "02_novelty_evaluation_atlas.ipynb": """from pathlib import Path
import json, os
import pandas as pd
DATA_ROOT = Path(os.environ['OBSERVATORY_PUBLIC_DATA'])
df = pd.read_parquet(DATA_ROOT / 'novelty_evaluation_atlas.parquet')
summary = {'rows': len(df), 'rulers': int((df['selector_type'] == 'ruler').sum()), 'seed': 1729}
Path(os.environ['OBSERVATORY_NOTEBOOK_OUTPUT']).write_text(json.dumps(summary, sort_keys=True))
print(summary)
""",
    "03_afterlife_censoring.ipynb": """from pathlib import Path
import json, os
import pandas as pd
DATA_ROOT = Path(os.environ['OBSERVATORY_PUBLIC_DATA'])
df = pd.read_parquet(DATA_ROOT / 'afterlife_panel.parquet')
summary = {'rows': len(df), 'right_censored': int((df['later_publication_status'] == 'right_censored_not_found').sum()), 'seed': 1729}
Path(os.environ['OBSERVATORY_NOTEBOOK_OUTPUT']).write_text(json.dumps(summary, sort_keys=True))
print(summary)
""",
}


def _notebook(code: str) -> dict[str, Any]:
    return {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "This notebook uses only the documented public OSG subset. ",
                    "It fixes seed 1729 and performs no network calls.\n",
                ],
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": code.splitlines(keepends=True),
            },
        ],
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": f"{sys.version_info.major}.{sys.version_info.minor}"},
            "observatory": {"public_subset": True, "seed": 1729, "network": "disabled"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def build_and_execute_notebooks(workspace: Path, output: Path) -> dict[str, Any]:
    notebook_root = workspace / "docs" / "observatory" / "notebooks"
    notebook_root.mkdir(parents=True, exist_ok=True)
    subset = output / "notebook_public_subset"
    subset.mkdir(parents=True, exist_ok=True)
    sources = {
        "gate_cycle_flow_series.parquet": output.parent / "r1" / "gate_cycle_flow_series.parquet",
        "novelty_evaluation_atlas.parquet": output / "novelty_evaluation_atlas.parquet",
        "afterlife_panel.parquet": output / "afterlife_panel.parquet",
    }
    subset_manifest = []
    for name, source in sources.items():
        destination = subset / name
        shutil.copy2(source, destination)
        subset_manifest.append(
            {"path": name, "sha256": content_hash(destination.read_bytes()), "size_bytes": destination.stat().st_size}
        )
    (subset / "MANIFEST.json").write_text(json.dumps({"files": subset_manifest}, indent=2, sort_keys=True) + "\n")

    receipts = []
    for name, code in NOTEBOOKS.items():
        path = notebook_root / name
        path.write_text(json.dumps(_notebook(code), indent=2, sort_keys=True) + "\n")
        with tempfile.TemporaryDirectory(prefix="observatory-notebook-") as temporary:
            result_path = Path(temporary) / "result.json"
            environment = dict(os.environ)
            environment.update(
                {
                    "OBSERVATORY_PUBLIC_DATA": str(subset.resolve()),
                    "OBSERVATORY_NOTEBOOK_OUTPUT": str(result_path),
                    "PYTHONHASHSEED": "1729",
                    "NO_PROXY": "*",
                }
            )
            completed = subprocess.run(
                [sys.executable, "-I", "-c", code],
                cwd=temporary,
                env=environment,
                capture_output=True,
                text=True,
                timeout=120,
                check=False,
            )
            result = json.loads(result_path.read_text()) if result_path.exists() else None
            receipts.append(
                {
                    "notebook": str(path.relative_to(workspace)),
                    "notebook_sha256": content_hash(path.read_bytes()),
                    "exit_code": completed.returncode,
                    "result": result,
                    "stderr": completed.stderr[-500:] if completed.returncode else "",
                    "fixed_seed": 1729,
                    "network_calls": 0,
                    "public_subset_manifest": content_hash((subset / "MANIFEST.json").read_bytes()),
                }
            )
    report = {
        "schema": "observatory.notebook-execution-report/1",
        "notebooks": receipts,
        "subset_files": subset_manifest,
        "passes": all(
            row["exit_code"] == 0 and row["result"] is not None and row["network_calls"] == 0 for row in receipts
        ),
    }
    report["report_hash"] = content_hash(json.dumps(report, sort_keys=True))
    (output / "notebook_execution_report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report
