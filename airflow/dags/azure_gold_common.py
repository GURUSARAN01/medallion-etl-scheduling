"""
Shared factory for Azure Gold scheduling DAGs.

The three Gold tasks are independent. With max_active_tasks=2, priority
weights determine which two tasks run first.

Static and Azure HEFT both start:
- agg_top_items
- agg_sales_by_region

XGBoost starts:
- agg_customer_ltv
- agg_sales_by_region
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

STATIC_PRIORITIES = {
    "agg_top_items": 300,
    "agg_sales_by_region": 200,
    "agg_customer_ltv": 100,
}

# Azure Gold Static pilot durations:
# agg_sales_by_region = 47.668 s
# agg_top_items       = 47.663 s
# agg_customer_ltv    = 32.974 s
#
# Gold tasks are independent, so each HEFT upward rank equals its own
# measured computation cost. Multiplication by 1000 preserves the small
# difference between the first two tasks.
AZURE_HEFT_PRIORITIES = {
    "agg_sales_by_region": 47668,
    "agg_top_items": 47663,
    "agg_customer_ltv": 32974,
}

# Existing offline XGBoost scores from the local benchmark.
XGBOOST_PRIORITIES = {
    "agg_customer_ltv": 750,
    "agg_sales_by_region": 575,
    "agg_top_items": 400,
}


def build_azure_gold_dag(
    *,
    dag_id: str,
    strategy: str,
    priorities: dict[str, int],
) -> DAG:
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

    dag = DAG(
        dag_id=dag_id,
        description=f"Azure Gold TPC-DS SF1 benchmark using {strategy}",
        start_date=datetime(2026, 7, 1),
        schedule=None,
        catchup=False,
        max_active_tasks=2,
        max_active_runs=1,
        default_args={
            "owner": "gurusaran",
            "depends_on_past": False,
            "retries": 0,
            "execution_timeout": timedelta(minutes=30),
        },
        tags=["azure", "gold", strategy, "tpcds-sf1"],
    )

    with dag:
        for task_id in TASK_IDS:
            priority = priorities[task_id]

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
