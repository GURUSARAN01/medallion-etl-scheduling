from __future__ import annotations

import json
from pathlib import Path

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import KFold, cross_val_score, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from xgboost import XGBRegressor


PROJECT_ROOT = Path(__file__).resolve().parents[2]

TRAINING_DATA_PATH = PROJECT_ROOT / "ml" / "features" / "training_data.csv"
OUTPUT_DIR = PROJECT_ROOT / "ml" / "training" / "outputs"

MODEL_PATH = OUTPUT_DIR / "xgboost_priority_model.joblib"
PREDICTIONS_PATH = OUTPUT_DIR / "xgb_predictions.csv"
FEATURE_IMPORTANCE_PATH = OUTPUT_DIR / "xgb_feature_importance.csv"
METRICS_PATH = OUTPUT_DIR / "xgb_metrics.json"


TARGET_COLUMN = "priority_target"

CATEGORICAL_FEATURES = [
    "layer",
    "task_id",
    "task_type",
]

NUMERIC_FEATURES = [
    "base_duration_s",
    "row_count",
    "size_mb",
    "upstream_count",
    "downstream_count",
    "is_source_task",
    "is_sink_task",
    "heft_rank",
    "heft_rank_norm",
    "base_duration_norm",
    "size_mb_norm",
    "downstream_count_norm",
    "short_unlock_score",
    "historical_mean_duration_s",
    "historical_std_duration_s",
    "historical_mean_delay_s",
    "historical_std_delay_s",
    "historical_run_count",
]


def load_training_data() -> pd.DataFrame:
    if not TRAINING_DATA_PATH.exists():
        raise FileNotFoundError(
            f"Training data not found: {TRAINING_DATA_PATH}. "
            "Run ml/features/extract_features.py first."
        )

    df = pd.read_csv(TRAINING_DATA_PATH)

    required_columns = CATEGORICAL_FEATURES + NUMERIC_FEATURES + [TARGET_COLUMN]
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    return df


def build_pipeline() -> Pipeline:
    preprocessor = ColumnTransformer(
        transformers=[
            (
                "categorical",
                OneHotEncoder(handle_unknown="ignore"),
                CATEGORICAL_FEATURES,
            ),
            (
                "numeric",
                "passthrough",
                NUMERIC_FEATURES,
            ),
        ]
    )

    model = XGBRegressor(
        objective="reg:squarederror",
        n_estimators=120,
        max_depth=3,
        learning_rate=0.05,
        subsample=0.9,
        colsample_bytree=0.9,
        random_state=42,
        n_jobs=1,
    )

    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", model),
        ]
    )


def get_feature_names(pipeline: Pipeline) -> list[str]:
    preprocessor = pipeline.named_steps["preprocessor"]

    categorical_names = (
        preprocessor
        .named_transformers_["categorical"]
        .get_feature_names_out(CATEGORICAL_FEATURES)
        .tolist()
    )

    return categorical_names + NUMERIC_FEATURES


def evaluate_model(df: pd.DataFrame) -> tuple[Pipeline, pd.DataFrame, dict]:
    X = df[CATEGORICAL_FEATURES + NUMERIC_FEATURES]
    y = df[TARGET_COLUMN]

    # Shuffle split is acceptable here because the target is task-level priority,
    # not a time-series forecast.
    X_train, X_test, y_train, y_test, df_train, df_test = train_test_split(
        X,
        y,
        df,
        test_size=0.25,
        random_state=42,
        shuffle=True,
    )

    pipeline = build_pipeline()
    pipeline.fit(X_train, y_train)

    y_pred = pipeline.predict(X_test)

    mse = mean_squared_error(y_test, y_pred)
    rmse = mse ** 0.5
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    cv = KFold(n_splits=5, shuffle=True, random_state=42)
    cv_scores = cross_val_score(
        pipeline,
        X,
        y,
        cv=cv,
        scoring="neg_mean_absolute_error",
    )

    metrics = {
        "rows": int(len(df)),
        "train_rows": int(len(X_train)),
        "test_rows": int(len(X_test)),
        "mae": round(float(mae), 4),
        "rmse": round(float(rmse), 4),
        "r2": round(float(r2), 4),
        "cv_mae_mean": round(float(-cv_scores.mean()), 4),
        "cv_mae_std": round(float(cv_scores.std()), 4),
        "target_min": round(float(y.min()), 4),
        "target_max": round(float(y.max()), 4),
        "target_mean": round(float(y.mean()), 4),
    }

    predictions = df_test[
        [
            "strategy",
            "layer",
            "task_id",
            "task_type",
            "priority_target",
            "heft_rank",
            "downstream_count",
            "short_unlock_score",
        ]
    ].copy()

    predictions["xgb_predicted_priority"] = y_pred.round(3)
    predictions["absolute_error"] = (
        predictions["priority_target"] - predictions["xgb_predicted_priority"]
    ).abs().round(3)

    predictions = predictions.sort_values(
        by=["layer", "xgb_predicted_priority"],
        ascending=[True, False],
    )

    return pipeline, predictions, metrics


def write_feature_importance(pipeline: Pipeline) -> pd.DataFrame:
    model = pipeline.named_steps["model"]
    feature_names = get_feature_names(pipeline)

    importance_df = pd.DataFrame(
        {
            "feature": feature_names,
            "importance": model.feature_importances_,
        }
    )

    importance_df = importance_df.sort_values(
        by="importance",
        ascending=False,
    )

    importance_df.to_csv(FEATURE_IMPORTANCE_PATH, index=False)

    return importance_df


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = load_training_data()

    pipeline, predictions, metrics = evaluate_model(df)

    # Refit on full dataset for final saved scheduler model.
    X_full = df[CATEGORICAL_FEATURES + NUMERIC_FEATURES]
    y_full = df[TARGET_COLUMN]
    pipeline.fit(X_full, y_full)

    joblib.dump(pipeline, MODEL_PATH)

    predictions.to_csv(PREDICTIONS_PATH, index=False)

    importance_df = write_feature_importance(pipeline)

    with open(METRICS_PATH, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=4)

    print("\nXGBoost training complete.")
    print(f"Model saved to: {MODEL_PATH}")
    print(f"Predictions saved to: {PREDICTIONS_PATH}")
    print(f"Feature importance saved to: {FEATURE_IMPORTANCE_PATH}")
    print(f"Metrics saved to: {METRICS_PATH}")

    print("\nMetrics:")
    for key, value in metrics.items():
        print(f"{key}: {value}")

    print("\nTop 15 feature importances:")
    print(importance_df.head(15).to_string(index=False))

    print("\nSample predictions:")
    print(predictions.head(20).to_string(index=False))


if __name__ == "__main__":
    main()