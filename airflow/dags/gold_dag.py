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
LAYER = "gold"


@provide_session
def check_silver_success(session=None, **context):
    """
    Returns True if silver_transformation has at least one successful run.
    This avoids ExternalTaskSensor execution_date mismatch for manual triggers.
    """
    run = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == "silver_transformation",
            DagRun.state == State.SUCCESS,
        )
        .order_by(DagRun.execution_date.desc())
        .first()
    )

    return run is not None


def aggregate_task(
    task_name: str,
    base_duration_s: float,
    dependency_count: int,
    aggregation_type: str,
    **context,
):
    """
    Simulates Gold layer aggregation tasks.

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
            "aggregation_type": aggregation_type,
        },
    )

    print(
        f"[GOLD][{STRATEGY}] {task_name} completed "
        f"in {duration_s:.2f}s"
    )


with DAG(
    dag_id="gold_aggregation",
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=2,
    tags=["gold", "medallion", "thesis", "static"],
) as dag:

    wait_silver = PythonSensor(
        task_id="wait_for_silver",
        python_callable=check_silver_success,
        timeout=300,
        poke_interval=10,
        mode="poke",
    )

    agg_top_items = PythonOperator(
        task_id="agg_top_items",
        python_callable=aggregate_task,
        op_kwargs={
            "task_name": "agg_top_items",
            "base_duration_s": 5.5,
            "dependency_count": 0,
            "aggregation_type": "top_100_items_by_revenue_per_month",
        },
    )

    agg_sales_by_region = PythonOperator(
        task_id="agg_sales_by_region",
        python_callable=aggregate_task,
        op_kwargs={
            "task_name": "agg_sales_by_region",
            "base_duration_s": 7.0,
            "dependency_count": 0,
            "aggregation_type": "daily_sales_by_store_region",
        },
    )

    agg_customer_ltv = PythonOperator(
        task_id="agg_customer_ltv",
        python_callable=aggregate_task,
        op_kwargs={
            "task_name": "agg_customer_ltv",
            "base_duration_s": 8.5,
            "dependency_count": 0,
            "aggregation_type": "customer_lifetime_value",
        },
    )

    wait_silver >> [
        agg_top_items,
        agg_sales_by_region,
        agg_customer_ltv,
    ]