"""
Controlled Azure Gold pairing experiment.

The production Gold notebook and existing Azure Gold DAG factory are reused.
Only the priority weights change so that each of the three possible
two-task opening pairs is evaluated with max_active_tasks=2.
"""

from __future__ import annotations

from azure_gold_common import build_azure_gold_dag

GOLD_PAIR_PRIORITIES = {
    "top_region": {
        "agg_top_items": 300,
        "agg_sales_by_region": 200,
        "agg_customer_ltv": 100,
    },
    "ltv_region": {
        "agg_customer_ltv": 300,
        "agg_sales_by_region": 200,
        "agg_top_items": 100,
    },
    "ltv_top": {
        "agg_customer_ltv": 300,
        "agg_top_items": 200,
        "agg_sales_by_region": 100,
    },
}


def build_gold_pairing_dag(*, pair_name: str):
    if pair_name not in GOLD_PAIR_PRIORITIES:
        raise ValueError(
            f"Unknown pair_name={pair_name!r}. "
            f"Expected one of {sorted(GOLD_PAIR_PRIORITIES)}."
        )

    return build_azure_gold_dag(
        dag_id=f"gold_azure_pair_{pair_name}",
        strategy=f"pair_{pair_name}",
        priorities=GOLD_PAIR_PRIORITIES[pair_name],
    )
