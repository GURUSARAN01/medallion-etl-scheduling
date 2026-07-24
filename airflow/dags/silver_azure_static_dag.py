from airflow import DAG  # Required for Airflow safe-mode discovery

from azure_silver_common import (
    STATIC_PRIORITIES,
    build_azure_silver_dag,
)

dag = build_azure_silver_dag(
    dag_id="silver_azure_static",
    strategy="static",
    priorities=STATIC_PRIORITIES,
)
