"""
Controlled Azure Bronze dependency-unlocking experiment.

Both variants reuse the production Bronze notebook and DAG topology.
Only the ordering of the three initially eligible root tasks changes.

unlock_early:
    date_dim + item start first.

unlock_delayed:
    customer + date_dim start first; item waits.

Priorities for downstream sales tasks remain unchanged between variants.
"""

from __future__ import annotations

from azure_bronze_common import build_azure_bronze_dag

UNLOCK_EARLY_PRIORITIES = {
    "ingest_date_dim": 600,
    "ingest_item": 500,
    "ingest_customer": 400,
    "ingest_store_sales": 300,
    "ingest_web_sales": 200,
    "ingest_catalog_sales": 100,
}

UNLOCK_DELAYED_PRIORITIES = {
    "ingest_customer": 600,
    "ingest_date_dim": 500,
    "ingest_item": 400,
    "ingest_store_sales": 300,
    "ingest_web_sales": 200,
    "ingest_catalog_sales": 100,
}


def build_bronze_unlock_dag(*, variant: str):
    if variant == "early":
        priorities = UNLOCK_EARLY_PRIORITIES
        strategy = "unlock_early"
    elif variant == "delayed":
        priorities = UNLOCK_DELAYED_PRIORITIES
        strategy = "unlock_delayed"
    else:
        raise ValueError("variant must be 'early' or 'delayed'")

    return build_azure_bronze_dag(
        dag_id=f"bronze_azure_unlock_{variant}",
        strategy=strategy,
        priorities=priorities,
    )
