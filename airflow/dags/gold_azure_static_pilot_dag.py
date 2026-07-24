from airflow import DAG  # Required for Airflow safe-mode discovery

from azure_gold_static_pilot_common import (
    build_azure_gold_static_pilot_dag,
)

dag = build_azure_gold_static_pilot_dag()
