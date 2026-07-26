from airflow import DAG  # Required for Airflow safe-mode discovery

from azure_bronze_unlock_common import build_bronze_unlock_dag

dag = build_bronze_unlock_dag(variant="early")
