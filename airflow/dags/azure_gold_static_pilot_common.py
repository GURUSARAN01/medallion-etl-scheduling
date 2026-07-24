"""
Shared factory for the Azure Gold Static pilot DAG.

The deterministic Static order reproduces the local Gold DAG creation order:
1. agg_top_items
2. agg_sales_by_region
3. agg_customer_ltv

With max_active_tasks=2, the first two tasks start while customer LTV waits.
The pilot measures real Azure task durations for later HEFT ranking.
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


def build_azure_gold_static_pilot_dag() -> DAG:
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
        dag_id="gold_azure_static_pilot",
        description="Azure Gold TPC-DS SF1 Static duration pilot",
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
        tags=["azure", "gold", "static", "pilot", "tpcds-sf1"],
    )

    with dag:
        for task_id in TASK_IDS:
            priority = STATIC_PRIORITIES[task_id]

            DatabricksSubmitRunOperator(
                task_id=task_id,
                databricks_conn_id=DATABRICKS_CONN_ID,
                existing_cluster_id=cluster_id,
                run_name=(
                    "gold-static-pilot-"
                    "{{ dag_run.run_id if dag_run else run_id }}-"
                    f"{task_id}"
                ),
                notebook_task={
                    "notebook_path": notebook_path,
                    "source": "WORKSPACE",
                    "base_parameters": {
                        "task_id": task_id,
                        "strategy": "static",
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
