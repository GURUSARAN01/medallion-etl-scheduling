from airflow import DAG  # Required for Airflow safe-mode DAG discovery

from azure_bronze_common import (
    XGBOOST_PRIORITIES,
    build_azure_bronze_dag,
)

dag = build_azure_bronze_dag(
    dag_id="bronze_azure_xgboost",
    strategy="xgboost",
    priorities=XGBOOST_PRIORITIES,
)
