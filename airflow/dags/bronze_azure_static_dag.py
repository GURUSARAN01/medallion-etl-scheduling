from airflow import DAG  # Required for Airflow safe-mode DAG discovery

from azure_bronze_common import (
    STATIC_PRIORITIES,
    build_azure_bronze_dag,
)

dag = build_azure_bronze_dag(
    dag_id="bronze_azure_static",
    strategy="static",
    priorities=STATIC_PRIORITIES,
)
