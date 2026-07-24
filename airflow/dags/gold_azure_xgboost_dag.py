from airflow import DAG  # Required for Airflow safe-mode discovery

from azure_gold_common import (
    XGBOOST_PRIORITIES,
    build_azure_gold_dag,
)

dag = build_azure_gold_dag(
    dag_id="gold_azure_xgboost",
    strategy="xgboost",
    priorities=XGBOOST_PRIORITIES,
)
