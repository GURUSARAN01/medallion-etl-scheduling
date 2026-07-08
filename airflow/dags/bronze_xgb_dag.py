from datetime import datetime
import time
import random

from airflow import DAG
from airflow.operators.python import PythonOperator

from metrics_utils import log_task_metrics
from xgb_scheduler import predict_xgb_priority_weights, format_xgb_table


STRATEGY = "xgboost"
LAYER = "bronze"


XGB_SCORES, XGB_PRIORITY_WEIGHTS, XGB_FEATURES = predict_xgb_priority_weights(LAYER)

print("[XGBOOST][BRONZE] Priority table:")
print(format_xgb_table(XGB_SCORES, XGB_PRIORITY_WEIGHTS))


def ingest_table(
    table_name: str,
    base_duration_s: float,
    dependency_count: int,
    row_count: int,
    size_mb: float,
    xgb_score: float,
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
            "xgb_score": xgb_score,
        },
    )

    print(
        f"[BRONZE][{STRATEGY}] Ingested {table_name} "
        f"in {duration_s:.2f}s | XGB score={xgb_score:.3f}"
    )


with DAG(
    dag_id="bronze_ingestion_xgb",
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=2,
    tags=["bronze", "medallion", "thesis", "xgboost"],
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
            priority_weight=XGB_PRIORITY_WEIGHTS[task_id],
            weight_rule="absolute",
            op_kwargs={
                "table_name": table_name,
                "base_duration_s": config["base_duration_s"],
                "dependency_count": config["dependency_count"],
                "row_count": config["row_count"],
                "size_mb": config["size_mb"],
                "xgb_score": XGB_SCORES[task_id],
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