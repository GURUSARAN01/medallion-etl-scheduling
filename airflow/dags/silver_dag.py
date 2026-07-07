from datetime import datetime
import time
import random

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.db import provide_session
from airflow.models import DagRun
from airflow.utils.state import State

from metrics_utils import log_task_metrics


STRATEGY = "static"
LAYER = "silver"


@provide_session
def check_bronze_success(session=None, **context):
    """
    Returns True if bronze_ingestion has at least one successful run.
    This avoids ExternalTaskSensor execution_date mismatch for manual triggers.
    """
    run = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == "bronze_ingestion",
            DagRun.state == State.SUCCESS,
        )
        .order_by(DagRun.execution_date.desc())
        .first()
    )

    return run is not None


def transform_task(
    task_name: str,
    base_duration_s: float,
    dependency_count: int,
    operation_type: str,
    **context,
):
    """
    Simulates Silver layer CPU-heavy transformations.

    T5 addition:
    Logs scheduling_delay_s and strategy='static' into PostgreSQL.
    """
    started = time.perf_counter()

    simulated_duration = max(
        0.1,
        base_duration_s + random.uniform(-0.5, 0.5),
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
            "task_name": task_name,
            "base_duration_s": base_duration_s,
            "simulated_duration_s": round(simulated_duration, 6),
            "operation_type": operation_type,
        },
    )

    print(
        f"[SILVER][{STRATEGY}] {task_name} completed "
        f"in {duration_s:.2f}s"
    )


with DAG(
    dag_id="silver_transformation",
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=2,
    tags=["silver", "medallion", "thesis", "static"],
) as dag:

    wait_bronze = PythonSensor(
        task_id="wait_for_bronze",
        python_callable=check_bronze_success,
        timeout=300,
        poke_interval=10,
        mode="poke",
    )

    clean_sales = PythonOperator(
        task_id="clean_sales",
        python_callable=transform_task,
        op_kwargs={
            "task_name": "clean_sales",
            "base_duration_s": 6.0,
            "dependency_count": 0,
            "operation_type": "union_all_sales_tables",
        },
    )

    clean_customer = PythonOperator(
        task_id="clean_customer",
        python_callable=transform_task,
        op_kwargs={
            "task_name": "clean_customer",
            "base_duration_s": 2.5,
            "dependency_count": 0,
            "operation_type": "deduplicate_and_fill_nulls",
        },
    )

    clean_item = PythonOperator(
        task_id="clean_item",
        python_callable=transform_task,
        op_kwargs={
            "task_name": "clean_item",
            "base_duration_s": 1.8,
            "dependency_count": 0,
            "operation_type": "standardise_categories",
        },
    )

    join_fact_sales = PythonOperator(
        task_id="join_fact_sales",
        python_callable=transform_task,
        op_kwargs={
            "task_name": "join_fact_sales",
            "base_duration_s": 9.5,
            "dependency_count": 3,
            "operation_type": "four_way_join",
        },
    )

    wait_bronze >> [clean_sales, clean_customer, clean_item]

    [clean_sales, clean_customer, clean_item] >> join_fact_sales