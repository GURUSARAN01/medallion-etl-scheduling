"""
Shared factory for Azure Bronze scheduling DAGs.

Airflow controls task eligibility and priority. Each Airflow task submits the
same parameterised Databricks notebook to one existing Databricks cluster.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunOperator,
)

DATABRICKS_CONN_ID = "databricks_azure"

SOURCE_TABLES = {
    "ingest_date_dim": "date_dim",
    "ingest_item": "item",
    "ingest_customer": "customer",
    "ingest_web_sales": "web_sales",
    "ingest_store_sales": "store_sales",
    "ingest_catalog_sales": "catalog_sales",
}

TARGET_TABLES = {
    task_id: f"bronze_{source_table}"
    for task_id, source_table in SOURCE_TABLES.items()
}

STATIC_PRIORITIES = {
    "ingest_date_dim": 600,
    "ingest_item": 500,
    "ingest_customer": 400,
    "ingest_web_sales": 300,
    "ingest_store_sales": 200,
    "ingest_catalog_sales": 100,
}

# Computed from the Azure TPC-DS SF1 pilot durations and the Bronze DAG.
AZURE_HEFT_PRIORITIES = {
    "ingest_customer": 1886,
    "ingest_date_dim": 1868,
    "ingest_item": 1858,
    "ingest_store_sales": 1622,
    "ingest_web_sales": 813,
    "ingest_catalog_sales": 797,
}

# Existing offline XGBoost priorities from the local benchmark.
# Keeping these fixed tests transferability to the Azure workload.
XGBOOST_PRIORITIES = {
    "ingest_item": 750,
    "ingest_date_dim": 671,
    "ingest_customer": 530,
    "ingest_catalog_sales": 465,
    "ingest_store_sales": 364,
    "ingest_web_sales": 147,
}


def build_azure_bronze_dag(
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
        "databricks_bronze_notebook_path",
        default_var="/Shared/03_bronze_task",
    )
    target_catalog = Variable.get(
        "databricks_target_catalog",
        default_var="dbw_thesis_etl_scheduling",
    )
    target_schema = Variable.get(
        "databricks_target_schema",
        default_var="thesis_etl_scheduling",
    )

    default_args = {
        "owner": "gurusaran",
        "depends_on_past": False,
        "retries": 0,
        "execution_timeout": timedelta(minutes=20),
    }

    dag = DAG(
        dag_id=dag_id,
        description=f"Azure Bronze TPC-DS SF1 benchmark using {strategy}",
        start_date=datetime(2026, 7, 1),
        schedule=None,
        catchup=False,
        max_active_tasks=2,
        max_active_runs=1,
        default_args=default_args,
        tags=["azure", "bronze", strategy, "tpcds-sf1"],
    )

    with dag:
        tasks: dict[str, DatabricksSubmitRunOperator] = {}

        for task_id, source_table in SOURCE_TABLES.items():
            priority = priorities[task_id]

            tasks[task_id] = DatabricksSubmitRunOperator(
                task_id=task_id,
                databricks_conn_id=DATABRICKS_CONN_ID,
                existing_cluster_id=cluster_id,
                run_name=(
                    f"{dag_id}-"
                    f"{{{{ dag_run.run_id if dag_run else run_id }}}}-"
                    f"{task_id}"
                ),
                notebook_task={
                    "notebook_path": notebook_path,
                    "source": "WORKSPACE",
                    "base_parameters": {
                        "task_id": task_id,
                        "source_table": source_table,
                        "target_table": TARGET_TABLES[task_id],
                        "strategy": strategy,
                        "run_id": (
                            "{{ dag_run.run_id if dag_run else run_id }}"
                        ),
                        "priority_weight": str(priority),
                        "source_catalog": "samples",
                        "source_schema": "tpcds_sf1",
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

        # Preserve the same Bronze graph used in the local benchmark.
        [tasks["ingest_date_dim"], tasks["ingest_item"]] >> tasks[
            "ingest_web_sales"
        ]
        [
            tasks["ingest_date_dim"],
            tasks["ingest_item"],
            tasks["ingest_customer"],
        ] >> tasks["ingest_store_sales"]
        [tasks["ingest_date_dim"], tasks["ingest_item"]] >> tasks[
            "ingest_catalog_sales"
        ]

    return dag
