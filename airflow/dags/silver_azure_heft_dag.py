from airflow import DAG  # Required for Airflow safe-mode discovery

from azure_silver_common import (
    AZURE_HEFT_PRIORITIES,
    build_azure_silver_dag,
)

dag = build_azure_silver_dag(
    dag_id="silver_azure_heft",
    strategy="heft",
    priorities=AZURE_HEFT_PRIORITIES,
)
