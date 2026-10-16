#!/usr/bin/env python3
"""Read-only verification of offline XGBoost priority rankings against archived Airflow tables.

Usage:
    python verify_xgb_priority_ranking.py --project-root /path/to/medallion-etl-scheduling

Only load your own trusted .joblib model. Loading pickled models can execute code.
This script never changes the historical priority tables or experiment results.
"""
from __future__ import annotations

import argparse
import ast
import csv
import warnings
from pathlib import Path

import joblib
import pandas as pd


def literal_assignment(path: Path, name: str):
    """Read constant Python dict/list without importing Airflow code."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(isinstance(t, ast.Name) and t.id == name for t in targets):
                return ast.literal_eval(node.value)
    raise ValueError(f"Missing {name} in {path}")


def verify(project_root: Path, output_csv: Path):
    training_script = project_root / "ml/training/train_xgboost.py"
    scheduler_path = project_root / "airflow/plugins/xgb_scheduler.py"
    model_path = project_root / "ml/training/outputs/xgboost_priority_model.joblib"
    training_csv = project_root / "ml/features/training_data.csv"
    category = literal_assignment(training_script, "CATEGORICAL_FEATURES")
    numeric = literal_assignment(training_script, "NUMERIC_FEATURES")
    local_float = literal_assignment(scheduler_path, "XGB_PREDICTED_SCORES")
    azure = {
        "bronze": literal_assignment(project_root / "airflow/dags/azure_bronze_common.py", "XGBOOST_PRIORITIES"),
        "silver": literal_assignment(project_root / "airflow/dags/azure_silver_common.py", "XGBOOST_PRIORITIES"),
        "gold": literal_assignment(project_root / "airflow/dags/azure_gold_common.py", "XGBOOST_PRIORITIES"),
    }
    df = pd.read_csv(training_csv)
    missing = set(category + numeric + ["layer", "task_id", "priority_target"]) - set(df.columns)
    if missing:
        raise RuntimeError(f"Training input columns missing: {sorted(missing)}")
    # Load only an explicitly supplied, trusted model artifact; no refitting occurs.
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        pipeline = joblib.load(model_path)
        df["model_prediction"] = pipeline.predict(df[category + numeric])
    groups = df.groupby(["layer", "task_id"], sort=True)
    if (groups["model_prediction"].nunique() > 1).any():
        raise RuntimeError("Some task identities have more than one model prediction across recorded rows")
    tasks = groups.first().reset_index()
    rows = []
    rank_ok = {}
    for layer, ldf in tasks.groupby("layer", sort=True):
        fixed_scores = local_float[layer]
        official_weights = azure[layer]
        ids = set(ldf.task_id)
        if ids != set(fixed_scores) or ids != set(official_weights):
            raise RuntimeError(f"Task-ID set mismatch in {layer}")
        model_order = ldf.sort_values("model_prediction", ascending=False)["task_id"].tolist()
        deployed_order = sorted(ids, key=lambda t: fixed_scores[t], reverse=True)
        azure_order = sorted(ids, key=lambda t: official_weights[t], reverse=True)
        rank_ok[layer] = model_order == deployed_order == azure_order
        for _, task in ldf.iterrows():
            name = task.task_id
            pred = float(task.model_prediction)
            target = float(task.priority_target)
            configured = float(fixed_scores[name])
            azure_int = int(official_weights[name])
            rows.append({
                "layer": layer,
                "task_id": name,
                "engineered_priority_target": f"{target:.6f}",
                "saved_model_prediction": f"{pred:.6f}",
                "historical_local_float_score": f"{configured:.6f}",
                "historical_azure_integer_weight": azure_int,
                "rounded_saved_model_prediction": max(1, round(pred)),
                "same_numeric_score": abs(pred-configured) < 1e-5,
                "target_equals_configured_score": abs(target-configured) < 1e-5,
                "local_float_matches_azure_integer": max(1, round(configured)) == azure_int,
                "all_three_rankings_agree_in_layer": rank_ok[layer],
            })
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
    print(f"Compared {len(rows)} distinct task identities across {len(rank_ok)} layers")
    for layer, ok in sorted(rank_ok.items()):
        print(f"  {layer}: identical complete task ordering = {ok}")
    print("All historical fixed floats match engineered target:", all(r["target_equals_configured_score"] for r in rows))
    print("All Azure integers match rounded historical fixed floats:", all(r["local_float_matches_azure_integer"] for r in rows))
    print("All model numeric predictions match historical fixed floats:", all(r["same_numeric_score"] for r in rows))
    print("Rounded integer weights differ for:", ", ".join(r["task_id"] for r in rows if r["rounded_saved_model_prediction"] != r["historical_azure_integer_weight"]) or "none")
    print("Output:", output_csv)
    if captured:
        print("NOTICE: model-persistence/version warnings were emitted; validate with the original training environment for maximum reproducibility.")
    return rows, rank_ok


if __name__ == "__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--project-root", type=Path, default=Path.cwd())
    p.add_argument("--out", type=Path, default=Path("xgb_deployment_rank_check.csv"))
    a=p.parse_args()
    _, ranked = verify(a.project_root, a.out)
    if not all(ranked.values()):
        raise SystemExit("FAILED: model and fixed priority rankings differ")
