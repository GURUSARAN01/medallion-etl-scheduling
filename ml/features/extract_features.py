from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, Iterable, Tuple

import pandas as pd
import psycopg2


PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "ml" / "features"
TRAINING_DATA_PATH = OUTPUT_DIR / "training_data.csv"
FEATURE_SUMMARY_PATH = OUTPUT_DIR / "feature_summary.csv"


Dependency = Tuple[str, str]


TASK_METADATA = {
    # Bronze
    ("bronze", "ingest_date_dim"): {
        "task_type": "ingest",
        "base_duration_s": 0.5,
        "row_count": 73_000,
        "size_mb": 5.0,
    },
    ("bronze", "ingest_item"): {
        "task_type": "ingest",
        "base_duration_s": 1.2,
        "row_count": 18_000,
        "size_mb": 2.0,
    },
    ("bronze", "ingest_customer"): {
        "task_type": "ingest",
        "base_duration_s": 1.8,
        "row_count": 100_000,
        "size_mb": 10.0,
    },
    ("bronze", "ingest_web_sales"): {
        "task_type": "ingest",
        "base_duration_s": 3.2,
        "row_count": 720_000,
        "size_mb": 72.0,
    },
    ("bronze", "ingest_store_sales"): {
        "task_type": "ingest",
        "base_duration_s": 4.5,
        "row_count": 2_800_000,
        "size_mb": 280.0,
    },
    ("bronze", "ingest_catalog_sales"): {
        "task_type": "ingest",
        "base_duration_s": 5.1,
        "row_count": 1_400_000,
        "size_mb": 140.0,
    },

    # Silver
    ("silver", "clean_sales"): {
        "task_type": "clean_union",
        "base_duration_s": 6.0,
        "row_count": 5_000_000,
        "size_mb": 500.0,
    },
    ("silver", "clean_customer"): {
        "task_type": "clean_dimension",
        "base_duration_s": 2.5,
        "row_count": 100_000,
        "size_mb": 10.0,
    },
    ("silver", "clean_item"): {
        "task_type": "clean_dimension",
        "base_duration_s": 1.8,
        "row_count": 18_000,
        "size_mb": 2.0,
    },
    ("silver", "join_fact_sales"): {
        "task_type": "join",
        "base_duration_s": 9.5,
        "row_count": 5_000_000,
        "size_mb": 500.0,
    },

    # Gold
    ("gold", "agg_top_items"): {
        "task_type": "aggregation",
        "base_duration_s": 5.5,
        "row_count": 18_000,
        "size_mb": 2.0,
    },
    ("gold", "agg_sales_by_region"): {
        "task_type": "aggregation",
        "base_duration_s": 7.0,
        "row_count": 5_000_000,
        "size_mb": 500.0,
    },
    ("gold", "agg_customer_ltv"): {
        "task_type": "aggregation",
        "base_duration_s": 8.5,
        "row_count": 100_000,
        "size_mb": 10.0,
    },
}


LAYER_DEPENDENCIES: Dict[str, list[Dependency]] = {
    "bronze": [
        ("ingest_date_dim", "ingest_store_sales"),
        ("ingest_date_dim", "ingest_web_sales"),
        ("ingest_date_dim", "ingest_catalog_sales"),
        ("ingest_item", "ingest_store_sales"),
        ("ingest_item", "ingest_web_sales"),
        ("ingest_item", "ingest_catalog_sales"),
        ("ingest_customer", "ingest_store_sales"),
    ],
    "silver": [
        ("clean_sales", "join_fact_sales"),
        ("clean_customer", "join_fact_sales"),
        ("clean_item", "join_fact_sales"),
    ],
    "gold": [],
}


def get_postgres_connection():
    """
    Defaults work when running locally because docker-compose exposes Postgres on localhost:5432.
    Environment variables can override these values later.
    """
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "airflow"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
    )


def load_metrics() -> pd.DataFrame:
    query = """
        SELECT
            dag_id,
            run_id,
            task_id,
            layer,
            strategy,
            dependency_count,
            priority_weight,
            queued_dttm,
            start_date,
            observed_end_date,
            duration_s,
            scheduling_delay_s,
            extra_json,
            created_at
        FROM etl_task_run_metrics
        WHERE strategy IN ('static', 'heft')
        ORDER BY strategy, layer, run_id, task_id;
    """

    with get_postgres_connection() as conn:
        df = pd.read_sql_query(query, conn)

    if df.empty:
        raise ValueError(
            "No rows found in etl_task_run_metrics for strategies static/heft."
        )

    return df


def compute_upward_ranks(
    task_durations: Dict[str, float],
    dependencies: Iterable[Dependency],
) -> Dict[str, float]:
    successors: Dict[str, list[str]] = {task_id: [] for task_id in task_durations}

    for upstream, downstream in dependencies:
        successors[upstream].append(downstream)

    cache: Dict[str, float] = {}

    def rank(task_id: str) -> float:
        if task_id in cache:
            return cache[task_id]

        own_cost = task_durations[task_id]

        if not successors[task_id]:
            cache[task_id] = own_cost
            return own_cost

        cache[task_id] = own_cost + max(rank(child) for child in successors[task_id])
        return cache[task_id]

    return {task_id: round(rank(task_id), 6) for task_id in task_durations}


def build_graph_features() -> pd.DataFrame:
    rows = []

    for layer in ["bronze", "silver", "gold"]:
        task_ids = [
            task_id
            for (task_layer, task_id) in TASK_METADATA.keys()
            if task_layer == layer
        ]

        durations = {
            task_id: TASK_METADATA[(layer, task_id)]["base_duration_s"]
            for task_id in task_ids
        }

        dependencies = LAYER_DEPENDENCIES[layer]
        ranks = compute_upward_ranks(durations, dependencies)

        upstream_count = {task_id: 0 for task_id in task_ids}
        downstream_count = {task_id: 0 for task_id in task_ids}

        for upstream, downstream in dependencies:
            downstream_count[upstream] += 1
            upstream_count[downstream] += 1

        for task_id in task_ids:
            meta = TASK_METADATA[(layer, task_id)]

            rows.append(
                {
                    "layer": layer,
                    "task_id": task_id,
                    "task_type": meta["task_type"],
                    "base_duration_s": meta["base_duration_s"],
                    "row_count": meta["row_count"],
                    "size_mb": meta["size_mb"],
                    "upstream_count": upstream_count[task_id],
                    "downstream_count": downstream_count[task_id],
                    "is_source_task": int(upstream_count[task_id] == 0),
                    "is_sink_task": int(downstream_count[task_id] == 0),
                    "heft_rank": ranks[task_id],
                    "heft_priority_weight": max(1, int(round(ranks[task_id] * 100))),
                }
            )

    return pd.DataFrame(rows)


def add_historical_features(df: pd.DataFrame) -> pd.DataFrame:
    historical = (
        df.groupby(["layer", "task_id"])
        .agg(
            historical_mean_duration_s=("duration_s", "mean"),
            historical_std_duration_s=("duration_s", "std"),
            historical_mean_delay_s=("scheduling_delay_s", "mean"),
            historical_std_delay_s=("scheduling_delay_s", "std"),
            historical_run_count=("duration_s", "count"),
        )
        .reset_index()
    )

    historical["historical_std_duration_s"] = historical[
        "historical_std_duration_s"
    ].fillna(0.0)
    historical["historical_std_delay_s"] = historical[
        "historical_std_delay_s"
    ].fillna(0.0)

    return df.merge(historical, on=["layer", "task_id"], how="left")


def add_extra_json_features(df: pd.DataFrame) -> pd.DataFrame:
    parsed_rows = []

    for value in df["extra_json"].fillna("{}"):
        try:
            parsed_rows.append(json.loads(value))
        except json.JSONDecodeError:
            parsed_rows.append({})

    extra_df = pd.json_normalize(parsed_rows)

    # Prefix avoids accidental column name collisions.
    extra_df = extra_df.add_prefix("extra_")

    return pd.concat([df.reset_index(drop=True), extra_df.reset_index(drop=True)], axis=1)


def normalise_by_layer(df: pd.DataFrame, column: str, output_column: str) -> pd.DataFrame:
    df[output_column] = 0.0

    for layer, layer_df in df.groupby("layer"):
        min_value = layer_df[column].min()
        max_value = layer_df[column].max()

        if max_value == min_value:
            df.loc[df["layer"] == layer, output_column] = 1.0
        else:
            mask = df["layer"] == layer
            df.loc[mask, output_column] = (
                (df.loc[mask, column] - min_value) / (max_value - min_value)
            )

    return df


def add_priority_target(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a supervised learning target for XGBoost.

    The target is a scheduling-priority proxy. It combines:
    - critical-path importance from HEFT rank
    - task duration
    - fan-out / downstream unlock effect
    - short-unlock advantage for small tasks that release many downstream tasks

    This is intentionally richer than pure HEFT because the preliminary HEFT result
    showed that short fan-out tasks such as ingest_date_dim can be under-prioritised.
    """
    df = normalise_by_layer(df, "heft_rank", "heft_rank_norm")
    df = normalise_by_layer(df, "base_duration_s", "base_duration_norm")
    df = normalise_by_layer(df, "downstream_count", "downstream_count_norm")
    df = normalise_by_layer(df, "size_mb", "size_mb_norm")

    df["short_unlock_score"] = (
        df["downstream_count_norm"] * (1.0 - df["base_duration_norm"])
    )

    df["priority_target"] = 1000.0 * (
        0.35 * df["heft_rank_norm"]
        + 0.25 * df["base_duration_norm"]
        + 0.25 * df["short_unlock_score"]
        + 0.15 * df["downstream_count_norm"]
    )

    df["priority_target"] = df["priority_target"].round(3)

    return df


def build_training_data() -> pd.DataFrame:
    metrics_df = load_metrics()
    graph_df = build_graph_features()

    df = metrics_df.merge(
        graph_df,
        on=["layer", "task_id"],
        how="left",
        validate="many_to_one",
    )

    missing_meta = df[df["base_duration_s"].isna()][["layer", "task_id"]].drop_duplicates()

    if not missing_meta.empty:
        raise ValueError(
            "Missing task metadata for:\n"
            + missing_meta.to_string(index=False)
        )

    df = add_historical_features(df)
    df = add_extra_json_features(df)
    df = add_priority_target(df)

    # Helpful ordering for readability.
    ordered_columns = [
        "strategy",
        "dag_id",
        "run_id",
        "layer",
        "task_id",
        "task_type",
        "duration_s",
        "scheduling_delay_s",
        "priority_target",
        "priority_weight",
        "heft_priority_weight",
        "heft_rank",
        "heft_rank_norm",
        "base_duration_s",
        "base_duration_norm",
        "row_count",
        "size_mb",
        "size_mb_norm",
        "upstream_count",
        "downstream_count",
        "downstream_count_norm",
        "short_unlock_score",
        "is_source_task",
        "is_sink_task",
        "historical_mean_duration_s",
        "historical_std_duration_s",
        "historical_mean_delay_s",
        "historical_std_delay_s",
        "historical_run_count",
        "queued_dttm",
        "start_date",
        "observed_end_date",
        "created_at",
    ]

    remaining_columns = [col for col in df.columns if col not in ordered_columns]
    df = df[ordered_columns + remaining_columns]

    return df


def write_outputs(df: pd.DataFrame) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df.to_csv(TRAINING_DATA_PATH, index=False)

    summary = (
        df.groupby(["strategy", "layer"])
        .agg(
            rows=("task_id", "count"),
            avg_duration_s=("duration_s", "mean"),
            avg_delay_s=("scheduling_delay_s", "mean"),
            avg_priority_target=("priority_target", "mean"),
        )
        .reset_index()
    )

    summary.to_csv(FEATURE_SUMMARY_PATH, index=False)

    print("\nFeature extraction complete.")
    print(f"Training data written to: {TRAINING_DATA_PATH}")
    print(f"Feature summary written to: {FEATURE_SUMMARY_PATH}")
    print("\nRows by strategy/layer:")
    print(summary.to_string(index=False))


def main() -> None:
    df = build_training_data()

    print(f"Loaded and engineered {len(df)} task-level rows.")

    expected_min_rows = 130
    if len(df) < expected_min_rows:
        print(
            f"WARNING: Expected at least {expected_min_rows} rows "
            f"(65 static + 65 HEFT), but found {len(df)}."
        )

    write_outputs(df)


if __name__ == "__main__":
    main()