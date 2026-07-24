from airflow import DAG  # Required for Airflow safe-mode discovery
from azure_silver_static_pilot_common import build_azure_silver_static_pilot_dag
dag = build_azure_silver_static_pilot_dag()
