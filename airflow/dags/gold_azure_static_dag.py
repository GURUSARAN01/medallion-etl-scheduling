from airflow import DAG  # Required for Airflow safe-mode discovery

from azure_gold_common import (
    STATIC_PRIORITIES,
    build_azure_gold_dag,
)

dag = build_azure_gold_dag(
    dag_id="gold_azure_static",
    strategy="static",
    priorities=STATIC_PRIORITIES,
)
