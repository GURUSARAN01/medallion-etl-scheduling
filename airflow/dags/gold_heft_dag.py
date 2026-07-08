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
from heft_scheduler import calculate_heft_priority_weights, format_heft_table


STRATEGY = "heft"
LAYER = "gold"


TASK_DURATIONS = {
    "agg_top_items": 5.5,
    "agg_sales_by_region": 7.0,
    "agg_customer_ltv": 8.5,
}

DEPENDENCIES = []

HEFT_RANKS, HEFT_PRIORITY_WEIGHTS = calculate_heft_priority_weights(
    TASK_DURATIONS,
    DEPENDENCIES,
)

print("[HEFT][GOLD] Priority table:")
print(format_heft_table(HEFT_RANKS, HEFT_PRIORITY_WEIGHTS))


@provide_session
def check_silver_heft_success(session=None, **context):
    run = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == "silver_transformation_heft",
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
    heft_rank: float,
    **context,
):
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
            "heft_rank": heft_rank,
        },
    )

    print(
        f"[GOLD][{STRATEGY}] {task_name} completed "
        f"in {duration_s:.2f}s | HEFT rank={heft_rank:.3f}"
    )


with DAG(
    dag_id="gold_aggregation_heft",
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=2,
    tags=["gold", "medallion", "thesis", "heft"],
) as dag:

    wait_silver = PythonSensor(
        task_id="wait_for_silver_heft",
        python_callable=check_silver_heft_success,
        timeout=300,
        poke_interval=10,
        mode="poke",
    )

    agg_top_items = PythonOperator(
        task_id="agg_top_items",
        python_callable=aggregate_task,
        priority_weight=HEFT_PRIORITY_WEIGHTS["agg_top_items"],
        weight_rule="absolute",
        op_kwargs={
            "task_name": "agg_top_items",
            "base_duration_s": 5.5,
            "dependency_count": 0,
            "aggregation_type": "top_100_items_by_revenue_per_month",
            "heft_rank": HEFT_RANKS["agg_top_items"],
        },
    )

    agg_sales_by_region = PythonOperator(
        task_id="agg_sales_by_region",
        python_callable=aggregate_task,
        priority_weight=HEFT_PRIORITY_WEIGHTS["agg_sales_by_region"],
        weight_rule="absolute",
        op_kwargs={
            "task_name": "agg_sales_by_region",
            "base_duration_s": 7.0,
            "dependency_count": 0,
            "aggregation_type": "daily_sales_by_store_region",
            "heft_rank": HEFT_RANKS["agg_sales_by_region"],
        },
    )

    agg_customer_ltv = PythonOperator(
        task_id="agg_customer_ltv",
        python_callable=aggregate_task,
        priority_weight=HEFT_PRIORITY_WEIGHTS["agg_customer_ltv"],
        weight_rule="absolute",
        op_kwargs={
            "task_name": "agg_customer_ltv",
            "base_duration_s": 8.5,
            "dependency_count": 0,
            "aggregation_type": "customer_lifetime_value",
            "heft_rank": HEFT_RANKS["agg_customer_ltv"],
        },
    )

    wait_silver >> [
        agg_top_items,
        agg_sales_by_region,
        agg_customer_ltv,
    ]