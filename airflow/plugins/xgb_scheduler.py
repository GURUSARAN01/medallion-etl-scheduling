from __future__ import annotations

from typing import Dict, Tuple


# Historical fixed task priorities for the XGBoost-labelled policy.
# These scores equal the engineered training targets, not the saved model's
# exact predictions. Offline verification confirms the same within-layer
# task ranking for all 13 tasks (Bronze 6, Silver 4, Gold 3).
# The constant name is retained to preserve compatibility with existing DAGs.
# Airflow uses rounded integers with weight_rule="absolute".

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
    Return historical, frozen priority scores for one Medallion layer.

    A separate offline XGBoost model was trained with
    ml/training/train_xgboost.py. These stored values reproduce the engineered
    priority targets; they are not numerical exports of the saved model's
    predictions. The trained model induces the same within-layer ranking on
    the 13 evaluated tasks (see ml/verification/).

    Airflow uses these fixed values without importing pandas, scikit-learn,
    or xgboost into the scheduler process. This function performs no inference.
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