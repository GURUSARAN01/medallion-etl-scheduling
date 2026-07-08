from datetime import datetime
import time
import random

from airflow import DAG
from airflow.operators.python import PythonOperator

from metrics_utils import log_task_metrics
from heft_scheduler import calculate_heft_priority_weights, format_heft_table


STRATEGY = "heft"
LAYER = "bronze"


TASK_DURATIONS = {
    "ingest_date_dim": 0.5,
    "ingest_item": 1.2,
    "ingest_customer": 1.8,
    "ingest_web_sales": 3.2,
    "ingest_store_sales": 4.5,
    "ingest_catalog_sales": 5.1,
}

DEPENDENCIES = [
    ("ingest_date_dim", "ingest_store_sales"),
    ("ingest_date_dim", "ingest_web_sales"),
    ("ingest_date_dim", "ingest_catalog_sales"),
    ("ingest_item", "ingest_store_sales"),
    ("ingest_item", "ingest_web_sales"),
    ("ingest_item", "ingest_catalog_sales"),
    ("ingest_customer", "ingest_store_sales"),
]

HEFT_RANKS, HEFT_PRIORITY_WEIGHTS = calculate_heft_priority_weights(
    TASK_DURATIONS,
    DEPENDENCIES,
)

print("[HEFT][BRONZE] Priority table:")
print(format_heft_table(HEFT_RANKS, HEFT_PRIORITY_WEIGHTS))


def ingest_table(
    table_name: str,
    base_duration_s: float,
    dependency_count: int,
    row_count: int,
    size_mb: float,
    heft_rank: float,
    **context,
):
    started = time.perf_counter()

    simulated_duration = max(
        0.1,
        base_duration_s + random.uniform(-0.3, 0.3),
    )

    time.sleep(simulated_duration)

    duration_s = time.perf_counter() - started

    log_task_metrics(
        context,
        layer=LAYER,
        strategy=STRATEGY,
        duration_s=duration_s,
        dependency_count=dependency_count,
        extra={
            "table_name": table_name,
            "base_duration_s": base_duration_s,
            "simulated_duration_s": round(simulated_duration, 6),
            "row_count": row_count,
            "size_mb": size_mb,
            "heft_rank": heft_rank,
        },
    )

    print(
        f"[BRONZE][{STRATEGY}] Ingested {table_name} "
        f"in {duration_s:.2f}s | HEFT rank={heft_rank:.3f}"
    )


with DAG(
    dag_id="bronze_ingestion_heft",
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=2,
    tags=["bronze", "medallion", "thesis", "heft"],
) as dag:

    table_config = {
        "date_dim": {
            "task_id": "ingest_date_dim",
            "base_duration_s": 0.5,
            "dependency_count": 0,
            "row_count": 73_000,
            "size_mb": 5.0,
        },
        "item": {
            "task_id": "ingest_item",
            "base_duration_s": 1.2,
            "dependency_count": 0,
            "row_count": 18_000,
            "size_mb": 2.0,
        },
        "customer": {
            "task_id": "ingest_customer",
            "base_duration_s": 1.8,
            "dependency_count": 0,
            "row_count": 100_000,
            "size_mb": 10.0,
        },
        "web_sales": {
            "task_id": "ingest_web_sales",
            "base_duration_s": 3.2,
            "dependency_count": 2,
            "row_count": 720_000,
            "size_mb": 72.0,
        },
        "store_sales": {
            "task_id": "ingest_store_sales",
            "base_duration_s": 4.5,
            "dependency_count": 3,
            "row_count": 2_800_000,
            "size_mb": 280.0,
        },
        "catalog_sales": {
            "task_id": "ingest_catalog_sales",
            "base_duration_s": 5.1,
            "dependency_count": 2,
            "row_count": 1_400_000,
            "size_mb": 140.0,
        },
    }

    tasks = {}

    for table_name, config in table_config.items():
        task_id = config["task_id"]

        tasks[table_name] = PythonOperator(
            task_id=task_id,
            python_callable=ingest_table,
            priority_weight=HEFT_PRIORITY_WEIGHTS[task_id],
            weight_rule="absolute",
            op_kwargs={
                "table_name": table_name,
                "base_duration_s": config["base_duration_s"],
                "dependency_count": config["dependency_count"],
                "row_count": config["row_count"],
                "size_mb": config["size_mb"],
                "heft_rank": HEFT_RANKS[task_id],
            },
        )

    tasks["date_dim"] >> [
        tasks["store_sales"],
        tasks["web_sales"],
        tasks["catalog_sales"],
    ]

    tasks["item"] >> [
        tasks["store_sales"],
        tasks["web_sales"],
        tasks["catalog_sales"],
    ]

    tasks["customer"] >> tasks["store_sales"]