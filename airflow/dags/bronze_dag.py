from datetime import datetime
import time
import random

from airflow import DAG
from airflow.operators.python import PythonOperator

from metrics_utils import log_task_metrics


STRATEGY = "static"
LAYER = "bronze"


def ingest_table(
    table_name: str,
    base_duration_s: float,
    dependency_count: int,
    row_count: int,
    size_mb: float,
    **context,
):
    """
    Simulates Bronze layer ingestion for one TPC-DS table.

    T5 addition:
    Logs duration_s, strategy, task_name, dependency_count,
    and scheduling_delay_s into PostgreSQL.
    """
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
        },
    )

    print(
        f"[BRONZE][{STRATEGY}] Ingested {table_name} "
        f"in {duration_s:.2f}s"
    )


with DAG(
    dag_id="bronze_ingestion",
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=2,
    tags=["bronze", "medallion", "thesis", "static"],
) as dag:

    table_config = {
        "date_dim": {
            "base_duration_s": 0.5,
            "dependency_count": 0,
            "row_count": 73_000,
            "size_mb": 5.0,
        },
        "item": {
            "base_duration_s": 1.2,
            "dependency_count": 0,
            "row_count": 18_000,
            "size_mb": 2.0,
        },
        "customer": {
            "base_duration_s": 1.8,
            "dependency_count": 0,
            "row_count": 100_000,
            "size_mb": 10.0,
        },
        "web_sales": {
            "base_duration_s": 3.2,
            "dependency_count": 2,
            "row_count": 720_000,
            "size_mb": 72.0,
        },
        "store_sales": {
            "base_duration_s": 4.5,
            "dependency_count": 3,
            "row_count": 2_800_000,
            "size_mb": 280.0,
        },
        "catalog_sales": {
            "base_duration_s": 5.1,
            "dependency_count": 2,
            "row_count": 1_400_000,
            "size_mb": 140.0,
        },
    }

    tasks = {
        table_name: PythonOperator(
            task_id=f"ingest_{table_name}",
            python_callable=ingest_table,
            op_kwargs={
                "table_name": table_name,
                **config,
            },
        )
        for table_name, config in table_config.items()
    }

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