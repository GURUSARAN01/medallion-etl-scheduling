from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "analysis" / "outputs"

RUN_LAYER_PATH = OUTPUT_DIR / "run_layer_metrics.csv"
WILCOXON_PATH = OUTPUT_DIR / "wilcoxon_results.csv"

FIG_RUNTIME_BAR = OUTPUT_DIR / "runtime_by_strategy_layer.png"
FIG_RUNTIME_BOXPLOT = OUTPUT_DIR / "runtime_boxplot_by_layer.png"
FIG_IMPROVEMENT_HEATMAP = OUTPUT_DIR / "improvement_heatmap.png"

TABLE_RUNTIME_CSV = OUTPUT_DIR / "table_runtime_summary.csv"
TABLE_RUNTIME_TEX = OUTPUT_DIR / "table_runtime_summary.tex"
TABLE_WILCOXON_RUNTIME_CSV = OUTPUT_DIR / "table_wilcoxon_runtime.csv"
TABLE_WILCOXON_RUNTIME_TEX = OUTPUT_DIR / "table_wilcoxon_runtime.tex"
TABLE_IMPROVEMENT_CSV = OUTPUT_DIR / "table_runtime_improvement.csv"
TABLE_IMPROVEMENT_TEX = OUTPUT_DIR / "table_runtime_improvement.tex"

LAYER_ORDER = ["bronze", "silver", "gold"]
STRATEGY_ORDER = ["static", "heft", "xgboost"]
LAYER_LABELS = {"bronze": "Bronze", "silver": "Silver", "gold": "Gold"}
STRATEGY_LABELS = {"static": "Static", "heft": "HEFT", "xgboost": "XGBoost"}


def require_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Required file not found: {path}\nRun analysis/statistical_tests.py first.")


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    require_file(RUN_LAYER_PATH)
    require_file(WILCOXON_PATH)
    run_df = pd.read_csv(RUN_LAYER_PATH)
    wilcoxon_df = pd.read_csv(WILCOXON_PATH)
    run_df["layer"] = pd.Categorical(run_df["layer"], categories=LAYER_ORDER, ordered=True)
    run_df["strategy"] = pd.Categorical(run_df["strategy"], categories=STRATEGY_ORDER, ordered=True)
    return run_df, wilcoxon_df


def build_runtime_summary(run_df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        run_df.groupby(["layer", "strategy"], observed=True)
        .agg(
            runs=("run_id", "count"),
            avg_runtime_s=("runtime_s", "mean"),
            std_runtime_s=("runtime_s", "std"),
            avg_delay_s=("avg_delay_s", "mean"),
            avg_task_duration_s=("avg_task_duration_s", "mean"),
        )
        .reset_index()
        .sort_values(["layer", "strategy"])
    )
    summary["layer_label"] = summary["layer"].map(LAYER_LABELS)
    summary["strategy_label"] = summary["strategy"].map(STRATEGY_LABELS)
    return summary


def build_improvement_table(summary: pd.DataFrame) -> pd.DataFrame:
    pivot = summary.pivot(index="layer", columns="strategy", values="avg_runtime_s")
    rows = []
    for layer in LAYER_ORDER:
        static = float(pivot.loc[layer, "static"])
        heft = float(pivot.loc[layer, "heft"])
        xgb = float(pivot.loc[layer, "xgboost"])
        rows.append({
            "layer": LAYER_LABELS[layer],
            "static_runtime_s": static,
            "heft_runtime_s": heft,
            "xgboost_runtime_s": xgb,
            "heft_vs_static_improvement_pct": (static - heft) / static * 100,
            "xgboost_vs_static_improvement_pct": (static - xgb) / static * 100,
            "xgboost_vs_heft_improvement_pct": (heft - xgb) / heft * 100,
        })
    return pd.DataFrame(rows)


def plot_runtime_bar(summary: pd.DataFrame) -> None:
    layers = LAYER_ORDER
    strategies = STRATEGY_ORDER
    x = np.arange(len(layers))
    width = 0.25
    fig, ax = plt.subplots(figsize=(8.5, 5.0))
    for idx, strategy in enumerate(strategies):
        subset = summary[summary["strategy"] == strategy].set_index("layer").loc[layers]
        offsets = x + (idx - 1) * width
        ax.bar(offsets, subset["avg_runtime_s"].to_numpy(), width, yerr=subset["std_runtime_s"].to_numpy(), capsize=4, label=STRATEGY_LABELS[strategy])
        for xpos, value in zip(offsets, subset["avg_runtime_s"].to_numpy()):
            ax.text(xpos, value + 0.12, f"{value:.2f}", ha="center", va="bottom", fontsize=8)
    ax.set_title("Average Layer Runtime by Strategy")
    ax.set_ylabel("Runtime (seconds)")
    ax.set_xlabel("Medallion layer")
    ax.set_xticks(x)
    ax.set_xticklabels([LAYER_LABELS[layer] for layer in layers])
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(FIG_RUNTIME_BAR, dpi=300)
    plt.close(fig)


def plot_runtime_boxplot(run_df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(9.0, 5.0))
    data, labels, positions = [], [], []
    pos = 1
    for layer in LAYER_ORDER:
        for strategy in STRATEGY_ORDER:
            values = run_df[(run_df["layer"] == layer) & (run_df["strategy"] == strategy)]["runtime_s"].astype(float).to_numpy()
            data.append(values)
            labels.append(STRATEGY_LABELS[strategy])
            positions.append(pos)
            pos += 1
        pos += 0.7
    ax.boxplot(data, positions=positions, widths=0.55, showmeans=True)
    group_centres = []
    for group_idx in range(len(LAYER_ORDER)):
        start = group_idx * 3 + 1 + group_idx * 0.7
        group_centres.append(start + 1)
    ax.set_xticks(group_centres)
    ax.set_xticklabels([LAYER_LABELS[layer] for layer in LAYER_ORDER])
    ax.set_title("Runtime Distribution by Layer and Strategy")
    ax.set_ylabel("Runtime (seconds)")
    ax.set_xlabel("Medallion layer")
    ax.grid(axis="y", alpha=0.3)
    ymin, ymax = ax.get_ylim()
    label_y = ymin - (ymax - ymin) * 0.08
    for p, label in zip(positions, labels):
        ax.text(p, label_y, label, ha="right", va="top", rotation=45, fontsize=8)
    ax.set_ylim(label_y - (ymax - ymin) * 0.08, ymax)
    fig.tight_layout()
    fig.savefig(FIG_RUNTIME_BOXPLOT, dpi=300)
    plt.close(fig)


def plot_improvement_heatmap(improvement: pd.DataFrame) -> None:
    cols = ["heft_vs_static_improvement_pct", "xgboost_vs_static_improvement_pct", "xgboost_vs_heft_improvement_pct"]
    labels = ["HEFT vs Static", "XGBoost vs Static", "XGBoost vs HEFT"]
    matrix = improvement[cols].to_numpy(dtype=float)
    fig, ax = plt.subplots(figsize=(8.5, 4.7))
    image = ax.imshow(matrix, aspect="auto")
    fig.colorbar(image, ax=ax, label="Runtime improvement (%)")
    ax.set_title("Runtime Improvement by Layer and Strategy Comparison")
    ax.set_yticks(np.arange(len(improvement)))
    ax.set_yticklabels(improvement["layer"].tolist())
    ax.set_xticks(np.arange(len(labels)))
    ax.set_xticklabels(labels, rotation=25, ha="right")
    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            ax.text(j, i, f"{matrix[i, j]:.2f}%", ha="center", va="center", fontsize=9)
    fig.tight_layout()
    fig.savefig(FIG_IMPROVEMENT_HEATMAP, dpi=300)
    plt.close(fig)


def write_tables(summary: pd.DataFrame, improvement: pd.DataFrame, wilcoxon_df: pd.DataFrame) -> None:
    runtime_table = summary[["layer_label", "strategy_label", "runs", "avg_runtime_s", "std_runtime_s", "avg_delay_s", "avg_task_duration_s"]].copy()
    runtime_table = runtime_table.rename(columns={"layer_label": "Layer", "strategy_label": "Strategy", "runs": "Runs", "avg_runtime_s": "Avg. runtime (s)", "std_runtime_s": "Std. runtime (s)", "avg_delay_s": "Avg. delay (s)", "avg_task_duration_s": "Avg. task duration (s)"})
    runtime_table.to_csv(TABLE_RUNTIME_CSV, index=False)
    runtime_table.to_latex(TABLE_RUNTIME_TEX, index=False, float_format="%.3f", caption="Average runtime, scheduling delay, and task duration by layer and scheduling strategy.", label="tab:runtime-summary", escape=True)

    improvement_table = improvement.rename(columns={"layer": "Layer", "static_runtime_s": "Static runtime (s)", "heft_runtime_s": "HEFT runtime (s)", "xgboost_runtime_s": "XGBoost runtime (s)", "heft_vs_static_improvement_pct": "HEFT vs Static (%)", "xgboost_vs_static_improvement_pct": "XGBoost vs Static (%)", "xgboost_vs_heft_improvement_pct": "XGBoost vs HEFT (%)"})
    improvement_table.to_csv(TABLE_IMPROVEMENT_CSV, index=False)
    improvement_table.to_latex(TABLE_IMPROVEMENT_TEX, index=False, float_format="%.3f", caption="Runtime improvement percentages by layer. Positive values indicate lower runtime for the candidate strategy.", label="tab:runtime-improvement", escape=True)

    runtime_wilcoxon = wilcoxon_df[wilcoxon_df["metric"] == "runtime_s"].copy()
    runtime_wilcoxon["Layer"] = runtime_wilcoxon["layer"].map(LAYER_LABELS)
    runtime_wilcoxon["Comparison"] = runtime_wilcoxon["comparison"].str.replace("_", " ", regex=False)
    runtime_wilcoxon["Significant"] = runtime_wilcoxon["significant_0_05"].map({True: "Yes", False: "No"})
    runtime_wilcoxon_table = runtime_wilcoxon[["Layer", "Comparison", "n_pairs", "baseline_mean", "candidate_mean", "candidate_improvement_pct", "p_value", "Significant"]].rename(columns={"n_pairs": "Pairs", "baseline_mean": "Baseline mean", "candidate_mean": "Candidate mean", "candidate_improvement_pct": "Improvement (%)", "p_value": "p-value"})
    runtime_wilcoxon_table.to_csv(TABLE_WILCOXON_RUNTIME_CSV, index=False)
    runtime_wilcoxon_table.to_latex(TABLE_WILCOXON_RUNTIME_TEX, index=False, float_format="%.3f", caption="Wilcoxon signed-rank test results for layer runtime comparisons.", label="tab:wilcoxon-runtime", escape=True)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    run_df, wilcoxon_df = load_data()
    summary = build_runtime_summary(run_df)
    improvement = build_improvement_table(summary)
    plot_runtime_bar(summary)
    plot_runtime_boxplot(run_df)
    plot_improvement_heatmap(improvement)
    write_tables(summary, improvement, wilcoxon_df)
    print("\nRuntime summary:")
    print(summary[["layer_label", "strategy_label", "runs", "avg_runtime_s", "std_runtime_s", "avg_delay_s", "avg_task_duration_s"]].to_string(index=False))
    print("\nRuntime improvement table:")
    print(improvement.to_string(index=False))
    print("\nFigures and tables written to:", OUTPUT_DIR)


if __name__ == "__main__":
    main()
