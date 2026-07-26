from airflow import DAG  # Required for Airflow safe-mode discovery

from azure_gold_concurrency_common import build_gold_concurrency_dag

dag = build_gold_concurrency_dag(concurrency=1)
