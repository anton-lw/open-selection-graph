"""Reader-facing descriptive tables and figures for the OSG data paper."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from .ids import content_hash

GRADE_ORDER = ["A", "B", "C", "D", "U"]
GRADE_COLORS = {
    "A": "#0072B2",
    "B": "#56B4E9",
    "C": "#E69F00",
    "D": "#CC79A7",
    "U": "#7F7F7F",
}


def _platform(source_id: str, platform: Any) -> str:
    if str(platform) == "OpenReview" or source_id in {"openreview", "openreview_api", "openreview_surface"}:
        return "OpenReview"
    if source_id.startswith("f1000"):
        return "F1000 family"
    labels = {
        "elife": "eLife",
        "elife_process": "eLife",
        "bmc_open_review": "BMC",
        "copernicus": "Copernicus",
        "embo_transparent_review": "EMBO",
        "peerj": "PeerJ",
        "plos_review_history": "PLOS",
        "royal_society_review": "Royal Society",
        "scipost": "SciPost",
        "scipost_process": "SciPost",
        "qeios": "Qeios",
        "crossref": "Crossref",
    }
    return labels.get(source_id, source_id.replace("_", " ").title())


def _latex_escape(value: Any) -> str:
    text = str(value)
    for before, after in (
        ("&", r"\&"),
        ("%", r"\%"),
        ("_", r"\_"),
        ("#", r"\#"),
    ):
        text = text.replace(before, after)
    return text


def build_descriptive_outputs(workspace: Path) -> dict[str, Any]:
    results = workspace / "results" / "observatory"
    atlas = pd.read_parquet(results / "r3" / "gate_cycle_descriptive_atlas.parquet")
    census = pd.read_parquet(results / "r3" / "gate_cycle_observability_census.parquet")
    verified = pd.read_parquet(results / "openreview_verified_cycle_metrics.parquet")
    atlas["platform_label"] = [
        _platform(str(source), platform)
        for source, platform in zip(atlas["source_id"], atlas["platform"], strict=True)
    ]
    census["platform_label"] = [
        _platform(str(source), platform)
        for source, platform in zip(census["source_id"], census["platform"], strict=True)
    ]
    platform_rows: list[dict[str, Any]] = []
    for platform, frame in atlas.groupby("platform_label"):
        grades = frame["effective_observability_grade"].fillna("U").value_counts().to_dict()
        platform_rows.append(
            {
                "platform": platform,
                "process_cycles": len(frame),
                **{f"grade_{grade}": int(grades.get(grade, 0)) for grade in GRADE_ORDER},
                "verified_denominators": int(frame["denominator_admissible"].sum()),
                "selection_rate_cycles": int(frame["descriptive_rate_allowed"].sum()),
                "review_rate_cycles": int(frame["review_rate_allowed"].sum()),
            }
        )
    platform_rows.sort(key=lambda row: (-row["process_cycles"], row["platform"]))
    review_counts = verified["official_review_count"].astype(float)
    summary: dict[str, Any] = {
        "schema": "observatory.data-paper-descriptive-summary/1",
        "observability_census_cycles": len(census),
        "policy_surface_cycles": int((~census["analytical_cycle"]).sum()),
        "analytical_process_cycles": len(atlas),
        "verified_denominator_cycles": int(atlas["denominator_admissible"].sum()),
        "populated_verified_denominator_cycles": int(
            (atlas["denominator_admissible"] & atlas["observable_count"].gt(0)).sum()
        ),
        "selection_rate_cycles": int(atlas["descriptive_rate_allowed"].sum()),
        "review_rate_cycles": int(atlas["review_rate_allowed"].sum()),
        "openreview": {
            "audited_cycles": len(verified),
            "populated_cycles": int(verified["observable_count"].gt(0).sum()),
            "observable_candidates": int(verified["observable_count"].sum()),
            "official_reviews": int(verified["official_review_count"].sum()),
            "cycles_with_official_reviews": int(verified["official_review_count"].gt(0).sum()),
            "mean_reviews_per_cycle": float(review_counts.mean()),
            "median_reviews_per_cycle": float(review_counts.median()),
            "reviews_per_cycle_q1": float(review_counts.quantile(0.25)),
            "reviews_per_cycle_q3": float(review_counts.quantile(0.75)),
            "mean_reviews_per_populated_cycle": float(
                verified.loc[verified["observable_count"].gt(0), "official_review_count"].mean()
            ),
            "fields": verified["field_of_study"].value_counts().sort_index().to_dict(),
            "selection_contexts": verified["selection_context"].value_counts().sort_index().to_dict(),
        },
        "platform_observability": platform_rows,
    }

    figure_dir = workspace / "docs" / "observatory" / "figures"
    generated_dir = workspace / "docs" / "observatory" / "generated"
    figure_dir.mkdir(parents=True, exist_ok=True)
    generated_dir.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update(
        {
            "font.family": "sans-serif",
            "font.sans-serif": ["DejaVu Sans", "Liberation Sans", "Arial"],
            "font.size": 8.5,
            "axes.titlesize": 9.5,
            "axes.titleweight": "bold",
            "axes.labelsize": 8.5,
            "axes.labelcolor": "#333333",
            "axes.edgecolor": "none",
            "axes.facecolor": "#F2F2F2",
            "axes.axisbelow": True,
            "xtick.labelsize": 7.5,
            "ytick.labelsize": 7.5,
            "xtick.color": "#444444",
            "ytick.color": "#444444",
            "figure.facecolor": "white",
            "savefig.facecolor": "white",
            "figure.dpi": 150,
        }
    )
    figure, axes = plt.subplots(2, 2, figsize=(10.2, 7.0), constrained_layout=True)

    platform_frame = pd.DataFrame(platform_rows).head(10).sort_values("process_cycles")
    left = np.zeros(len(platform_frame))
    for grade in GRADE_ORDER:
        values = platform_frame[f"grade_{grade}"].to_numpy()
        axes[0, 0].barh(
            platform_frame["platform"],
            values,
            left=left,
            label=f"Grade {grade}",
            color=GRADE_COLORS[grade],
            edgecolor="white",
            linewidth=0.3,
        )
        left += values
    axes[0, 0].set_title("A   Observability grades by platform", loc="left", pad=8)
    axes[0, 0].set_xlabel("Process cycles")
    axes[0, 0].legend(frameon=False, ncol=3, fontsize=7, loc="lower right")

    contexts = verified["selection_context"].value_counts().sort_values()
    axes[0, 1].barh(contexts.index.str.replace("_", " "), contexts.values, color="#0072B2")
    axes[0, 1].set_title(
        "B   Audited OpenReview cycles by selection context", loc="left", pad=8
    )
    axes[0, 1].set_xlabel("Cycles")

    fields = verified["field_of_study"].value_counts().sort_values()
    axes[1, 0].barh(fields.index, fields.values, color=["#999999" if x == "unclassified" else "#009E73" for x in fields.index])
    axes[1, 0].set_title(
        "C   Field coverage of verified OpenReview denominators", loc="left", pad=8
    )
    axes[1, 0].set_xlabel("Cycles")
    axes[1, 0].text(
        0.98,
        0.05,
        "166/176 cycles are classified as machine learning",
        transform=axes[1, 0].transAxes,
        ha="right",
        va="bottom",
        fontsize=7.5,
    )

    review_by_context = (
        verified.groupby("selection_context")["official_review_count"]
        .agg(["mean", "median", "count"])
        .sort_values("mean")
    )
    axes[1, 1].barh(
        review_by_context.index.str.replace("_", " "),
        review_by_context["mean"],
        color="#D55E00",
    )
    axes[1, 1].scatter(
        review_by_context["median"],
        np.arange(len(review_by_context)),
        color="black",
        s=13,
        label="median",
        zorder=3,
    )
    axes[1, 1].set_title("D   Official reviews per audited cycle", loc="left", pad=8)
    axes[1, 1].set_xlabel("Mean (bar) and median (dot)")
    axes[1, 1].set_xscale("symlog", linthresh=1)
    axes[1, 1].set_xticks([0, 1, 10, 100, 1_000])
    axes[1, 1].set_xticklabels(["0", "1", "10", "100", "1,000"])
    axes[1, 1].legend(frameon=False, fontsize=7, loc="lower right")

    for axis in axes.flat:
        axis.spines[["top", "right", "bottom", "left"]].set_visible(False)
        axis.grid(axis="x", color="white", linewidth=0.9)
        axis.grid(axis="y", visible=False)
        axis.tick_params(axis="both", length=0, pad=3)
    png_path = figure_dir / "osg_descriptive_overview.png"
    pdf_path = figure_dir / "osg_descriptive_overview.pdf"
    figure.savefig(png_path, dpi=300, bbox_inches="tight")
    figure.savefig(pdf_path, bbox_inches="tight")
    plt.close(figure)

    table_lines = [
        r"\begin{table}[H]",
        r"\centering",
        r"\caption{Process-level scope and denominator-dependent analytical eligibility. Policy-only configuration rows are retained in a separate observability census.}",
        r"\label{tab:descriptive-summary}",
        r"\small",
        r"\begin{tabular}{lrrrrr}",
        r"\toprule",
        r"Platform & Process cycles & Grade A/B & Populated A/B & Selection-rate & Review-rate \\",
        r"\midrule",
    ]
    for row in platform_rows:
        platform_atlas = atlas[atlas["platform_label"] == row["platform"]]
        populated = int(
            (platform_atlas["denominator_admissible"] & platform_atlas["observable_count"].gt(0)).sum()
        )
        table_lines.append(
            f"{_latex_escape(row['platform'])} & {row['process_cycles']:,} & "
            f"{row['verified_denominators']:,} & {populated:,} & "
            f"{row['selection_rate_cycles']:,} & {row['review_rate_cycles']:,} \\\\"
        )
    table_lines.extend(
        [
            r"\midrule",
            f"Total & {len(atlas):,} & {summary['verified_denominator_cycles']:,} & "
            f"{summary['populated_verified_denominator_cycles']:,} & "
            f"{summary['selection_rate_cycles']:,} & {summary['review_rate_cycles']:,} \\\\ ",
            r"\bottomrule",
            r"\end{tabular}",
            r"\end{table}",
            "",
            r"\begin{table}[H]",
            r"\centering",
            r"\caption{Verified OpenReview public-process cohort. Review counts are complete for the audited public Note graph; they do not include confidential or unreadable objects.}",
            r"\label{tab:openreview-summary}",
            r"\small",
            r"\begin{tabular}{lr}",
            r"\toprule",
            r"Quantity & Value \\",
            r"\midrule",
            f"Audited cycles & {len(verified):,} \\\\ ",
            f"Populated cycles & {int(verified['observable_count'].gt(0).sum()):,} \\\\ ",
            f"Observable candidates & {int(verified['observable_count'].sum()):,} \\\\ ",
            f"Official reviews & {int(verified['official_review_count'].sum()):,} \\\\ ",
            f"Cycles with at least one official review & {int(verified['official_review_count'].gt(0).sum()):,} \\\\ ",
            f"Mean reviews per cycle & {review_counts.mean():.1f} \\\\ ",
            f"Median reviews per cycle & {review_counts.median():.1f} \\\\ ",
            r"\bottomrule",
            r"\end{tabular}",
            r"\end{table}",
            "",
        ]
    )
    table_path = generated_dir / "descriptive_tables.tex"
    table_path.write_text("\n".join(table_lines))
    report_path = results / "r3" / "descriptive_summary.json"
    summary["artifacts"] = {
        "figure_pdf": str(pdf_path.relative_to(workspace)),
        "figure_png": str(png_path.relative_to(workspace)),
        "latex_tables": str(table_path.relative_to(workspace)),
    }
    summary["artifact_hashes"] = {
        key: content_hash((workspace / value).read_bytes())
        for key, value in summary["artifacts"].items()
    }
    summary["passes"] = (
        summary["verified_denominator_cycles"] >= 200
        and summary["openreview"]["observable_candidates"] == 55_369
        and summary["openreview"]["official_reviews"] == 31_992
    )
    summary["report_hash"] = content_hash(json.dumps(summary, sort_keys=True))
    report_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    return summary


def main() -> None:
    workspace = Path.cwd().resolve()
    print(json.dumps(build_descriptive_outputs(workspace), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
