from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import psycopg2
from scipy.stats import wilcoxon


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "analysis" / "outputs"

RUN_LAYER_PATH = OUTPUT_DIR / "run_layer_metrics.csv"
WILCOXON_PATH = OUTPUT_DIR / "wilcoxon_results.csv"


STRATEGIES = ["static", "heft", "xgboost"]

COMPARISONS = [
    ("static", "heft"),
    ("static", "xgboost"),
    ("heft", "xgboost"),
]

METRICS = [
    "runtime_s",
    "avg_delay_s",
    "avg_task_duration_s",
]


def get_postgres_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "airflow"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
    )


def load_run_layer_metrics() -> pd.DataFrame:
    query = """
        SELECT
            strategy,
            layer,
            dag_id,
            run_id,
            MIN(start_date) AS run_start,
            MAX(observed_end_date) AS run_end,
            EXTRACT(EPOCH FROM MAX(observed_end_date) - MIN(start_date)) AS runtime_s,
            AVG(scheduling_delay_s) AS avg_delay_s,
            AVG(duration_s) AS avg_task_duration_s,
            SUM(duration_s) AS total_task_duration_s,
            COUNT(*) AS task_count
        FROM etl_task_run_metrics
        WHERE strategy IN ('static', 'heft', 'xgboost')
        GROUP BY strategy, layer, dag_id, run_id
        ORDER BY layer, strategy, run_start;
    """

    with get_postgres_connection() as conn:
        df = pd.read_sql_query(query, conn)

    if df.empty:
        raise ValueError("No benchmark rows found in etl_task_run_metrics.")

    df["run_start"] = pd.to_datetime(df["run_start"])
    df["run_end"] = pd.to_datetime(df["run_end"])

    df = df.sort_values(["layer", "strategy", "run_start"]).copy()

    df["run_index"] = (
        df.groupby(["layer", "strategy"])
        .cumcount()
        .add(1)
    )

    return df


def validate_counts(df: pd.DataFrame) -> None:
    counts = (
        df.groupby(["strategy", "layer"])
        .agg(runs=("run_id", "count"), tasks=("task_count", "sum"))
        .reset_index()
        .sort_values(["strategy", "layer"])
    )

    print("\nRun/task counts:")
    print(counts.to_string(index=False))

    expected_runs = 10
    problems = counts[counts["runs"] != expected_runs]

    if not problems.empty:
        print(f"\nWARNING: Some strategy/layer combinations do not have {expected_runs} runs:")
        print(problems.to_string(index=False))


def run_wilcoxon_tests(df: pd.DataFrame) -> pd.DataFrame:
    results = []

    layers = sorted(df["layer"].unique())

    for layer in layers:
        for metric in METRICS:
            for baseline, candidate in COMPARISONS:
                baseline_values = (
                    df[
                        (df["layer"] == layer)
                        & (df["strategy"] == baseline)
                    ]
                    .sort_values("run_index")[metric]
                    .astype(float)
                    .tolist()
                )

                candidate_values = (
                    df[
                        (df["layer"] == layer)
                        & (df["strategy"] == candidate)
                    ]
                    .sort_values("run_index")[metric]
                    .astype(float)
                    .tolist()
                )

                n_pairs = min(len(baseline_values), len(candidate_values))

                baseline_values = baseline_values[:n_pairs]
                candidate_values = candidate_values[:n_pairs]

                baseline_series = pd.Series(baseline_values)
                candidate_series = pd.Series(candidate_values)

                baseline_mean = baseline_series.mean()
                candidate_mean = candidate_series.mean()

                mean_diff = candidate_mean - baseline_mean

                # Lower is better for runtime and delay metrics.
                improvement_pct = (
                    (baseline_mean - candidate_mean) / baseline_mean * 100
                    if baseline_mean != 0
                    else 0.0
                )

                try:
                    if all(
                        abs(candidate_values[i] - baseline_values[i]) < 1e-12
                        for i in range(n_pairs)
                    ):
                        statistic = 0.0
                        p_value = 1.0
                    else:
                        test_result = wilcoxon(
                            baseline_values,
                            candidate_values,
                            alternative="two-sided",
                            zero_method="wilcox",
                        )
                        statistic = float(test_result.statistic)
                        p_value = float(test_result.pvalue)

                except ValueError:
                    statistic = None
                    p_value = None

                if p_value is None:
                    significant = False
                else:
                    significant = p_value < 0.05

                if improvement_pct > 0:
                    direction = f"{candidate} faster/lower than {baseline}"
                elif improvement_pct < 0:
                    direction = f"{candidate} slower/higher than {baseline}"
                else:
                    direction = "no difference"

                results.append(
                    {
                        "layer": layer,
                        "metric": metric,
                        "baseline": baseline,
                        "candidate": candidate,
                        "comparison": f"{baseline}_vs_{candidate}",
                        "n_pairs": n_pairs,
                        "baseline_mean": round(float(baseline_mean), 6),
                        "candidate_mean": round(float(candidate_mean), 6),
                        "mean_diff_candidate_minus_baseline": round(float(mean_diff), 6),
                        "candidate_improvement_pct": round(float(improvement_pct), 3),
                        "wilcoxon_statistic": statistic,
                        "p_value": None if p_value is None else round(float(p_value), 6),
                        "significant_0_05": significant,
                        "direction": direction,
                    }
                )

    return pd.DataFrame(results)


def print_runtime_summary(df: pd.DataFrame) -> None:
    summary = (
        df.groupby(["layer", "strategy"])
        .agg(
            runs=("run_id", "count"),
            avg_runtime_s=("runtime_s", "mean"),
            avg_delay_s=("avg_delay_s", "mean"),
            avg_task_duration_s=("avg_task_duration_s", "mean"),
        )
        .reset_index()
        .sort_values(["layer", "strategy"])
    )

    print("\nRuntime summary:")
    print(
        summary.to_string(
            index=False,
            formatters={
                "avg_runtime_s": "{:.3f}".format,
                "avg_delay_s": "{:.4f}".format,
                "avg_task_duration_s": "{:.3f}".format,
            },
        )
    )


def print_wilcoxon_summary(results: pd.DataFrame) -> None:
    runtime_results = results[results["metric"] == "runtime_s"].copy()

    print("\nWilcoxon results for runtime_s:")
    print(
        runtime_results[
            [
                "layer",
                "comparison",
                "n_pairs",
                "baseline_mean",
                "candidate_mean",
                "candidate_improvement_pct",
                "p_value",
                "significant_0_05",
                "direction",
            ]
        ].to_string(index=False)
    )


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = load_run_layer_metrics()

    validate_counts(df)
    print_runtime_summary(df)

    df.to_csv(RUN_LAYER_PATH, index=False)

    results = run_wilcoxon_tests(df)
    results.to_csv(WILCOXON_PATH, index=False)

    print_wilcoxon_summary(results)

    print("\nFiles written:")
    print(f"- {RUN_LAYER_PATH}")
    print(f"- {WILCOXON_PATH}")


if __name__ == "__main__":
    main()