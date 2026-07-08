from __future__ import annotations

from typing import Dict, Tuple


# XGBoost predicted priority scores.
# These scores come from the trained offline XGBoost priority model.
# Airflow uses them as priority_weight values through weight_rule="absolute".

XGB_PREDICTED_SCORES: Dict[str, Dict[str, float]] = {
    "bronze": {
        "ingest_item": 750.000,
        "ingest_date_dim": 670.968,
        "ingest_customer": 530.435,
        "ingest_catalog_sales": 464.516,
        "ingest_store_sales": 364.165,
        "ingest_web_sales": 146.739,
    },
    "silver": {
        "clean_sales": 750.000,
        "clean_customer": 545.833,
        "clean_item": 505.000,
        "join_fact_sales": 250.000,
    },
    "gold": {
        "agg_customer_ltv": 750.000,
        "agg_sales_by_region": 575.000,
        "agg_top_items": 400.000,
    },
}


def predict_xgb_priority_weights(
    layer: str,
) -> Tuple[Dict[str, float], Dict[str, int], Dict[str, Dict[str, float]]]:
    """
    Returns offline XGBoost-predicted priority scores for one Medallion layer.

    The XGBoost model was trained outside Airflow using:
    ml/training/train_xgboost.py

    The resulting predicted priority scores are exported here so that the Airflow
    scheduler can use them without requiring pandas, scikit-learn, or xgboost
    inside the Docker container.
    """
    if layer not in XGB_PREDICTED_SCORES:
        raise ValueError(
            f"Unknown layer '{layer}'. Expected one of: "
            f"{list(XGB_PREDICTED_SCORES.keys())}"
        )

    predicted_scores = XGB_PREDICTED_SCORES[layer]

    priority_weights = {
        task_id: max(1, int(round(score)))
        for task_id, score in predicted_scores.items()
    }

    return predicted_scores, priority_weights, XGB_PREDICTED_SCORES


def format_xgb_table(
    predicted_scores: Dict[str, float],
    priority_weights: Dict[str, int],
) -> str:
    rows = []

    for task_id in sorted(predicted_scores, key=predicted_scores.get, reverse=True):
        rows.append(
            f"{task_id:30s} "
            f"xgb_score={predicted_scores[task_id]:8.3f} "
            f"priority_weight={priority_weights[task_id]}"
        )

    return "\n".join(rows)