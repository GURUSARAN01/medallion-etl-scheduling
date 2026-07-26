from airflow import DAG  # Required for Airflow safe-mode discovery

from azure_gold_pairing_common import build_gold_pairing_dag

dag = build_gold_pairing_dag(pair_name="ltv_region")
