"""
Controlled Azure Gold concurrency-sensitivity experiment.

The priority order is fixed across all conditions:

1. agg_customer_ltv
2. agg_top_items
3. agg_sales_by_region

Only ``max_active_tasks`` changes between 1, 2, and 3. This isolates
the effect of permitted concurrency while reusing the same Databricks
notebook, input tables, cluster, task parameters, and output metrics.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunOperator,
)

DATABRICKS_CONN_ID = "databricks_azure"

TASK_IDS = (
    "agg_top_items",
    "agg_sales_by_region",
    "agg_customer_ltv",
)

# This order reproduces the best controlled opening pair observed in the
# previous Gold mechanism study when max_active_tasks=2:
# customer_ltv + top_items first, sales_by_region second.
FIXED_PRIORITIES = {
    "agg_customer_ltv": 300,
    "agg_top_items": 200,
    "agg_sales_by_region": 100,
}


def build_gold_concurrency_dag(*, concurrency: int) -> DAG:
    if concurrency not in {1, 2, 3}:
        raise ValueError("concurrency must be one of {1, 2, 3}")

    cluster_id = Variable.get(
        "databricks_azure_cluster_id",
        default_var="SET_DATABRICKS_CLUSTER_ID",
    )
    notebook_path = Variable.get(
        "databricks_gold_notebook_path",
        default_var="/Shared/06_gold_task",
    )
    target_catalog = Variable.get(
        "databricks_target_catalog",
        default_var="dbw_thesis_etl_scheduling",
    )
    target_schema = Variable.get(
        "databricks_target_schema",
        default_var="thesis_etl_scheduling",
    )

    dag_id = f"gold_azure_concurrency_c{concurrency}"
    strategy = f"concurrency_{concurrency}"

    dag = DAG(
        dag_id=dag_id,
        description=(
            "Azure Gold concurrency-sensitivity experiment with "
            f"max_active_tasks={concurrency}"
        ),
        start_date=datetime(2026, 7, 1),
        schedule=None,
        catchup=False,
        max_active_tasks=concurrency,
        max_active_runs=1,
        default_args={
            "owner": "gurusaran",
            "depends_on_past": False,
            "retries": 0,
            "execution_timeout": timedelta(minutes=30),
        },
        tags=[
            "azure",
            "gold",
            "concurrency-sensitivity",
            strategy,
            "tpcds-sf1",
        ],
    )

    with dag:
        for task_id in TASK_IDS:
            priority = FIXED_PRIORITIES[task_id]

            DatabricksSubmitRunOperator(
                task_id=task_id,
                databricks_conn_id=DATABRICKS_CONN_ID,
                existing_cluster_id=cluster_id,
                run_name=(
                    f"{dag_id}-"
                    "{{ dag_run.run_id if dag_run else run_id }}-"
                    f"{task_id}"
                ),
                notebook_task={
                    "notebook_path": notebook_path,
                    "source": "WORKSPACE",
                    "base_parameters": {
                        "task_id": task_id,
                        "strategy": strategy,
                        "run_id": (
                            "{{ dag_run.run_id if dag_run else run_id }}"
                        ),
                        "priority_weight": str(priority),
                        "target_catalog": target_catalog,
                        "target_schema": target_schema,
                    },
                },
                polling_period_seconds=10,
                wait_for_termination=True,
                do_xcom_push=True,
                priority_weight=priority,
                weight_rule="absolute",
            )

    return dag
