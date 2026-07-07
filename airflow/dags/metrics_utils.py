from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

from airflow import settings
from airflow.models.taskinstance import TaskInstance
from sqlalchemy import text


def _seconds_between(later: Optional[datetime], earlier: Optional[datetime]) -> Optional[float]:
    """
    Calculates seconds between two datetimes.
    Returns None if Airflow has not stored queued/start timestamp.
    """
    if later is None or earlier is None:
        return None

    value = (later - earlier).total_seconds()
    return round(max(value, 0.0), 6)


def _to_iso(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return value.isoformat()


def _ensure_metrics_table(session) -> None:
    """
    Creates one clean thesis benchmark table inside the Airflow PostgreSQL DB.
    This avoids needing to reconstruct everything from raw XCom tables later.
    """
    session.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS etl_task_run_metrics (
                id BIGSERIAL PRIMARY KEY,
                dag_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                layer TEXT NOT NULL,
                strategy TEXT NOT NULL,
                dependency_count INTEGER NOT NULL,
                priority_weight INTEGER,
                queued_dttm TIMESTAMPTZ,
                start_date TIMESTAMPTZ,
                observed_end_date TIMESTAMPTZ,
                duration_s DOUBLE PRECISION,
                scheduling_delay_s DOUBLE PRECISION,
                extra_json TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (dag_id, run_id, task_id, strategy)
            );
            """
        )
    )

    session.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS idx_etl_metrics_strategy_layer
            ON etl_task_run_metrics (strategy, layer);
            """
        )
    )

    session.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS idx_etl_metrics_dag_run
            ON etl_task_run_metrics (dag_id, run_id);
            """
        )
    )


def log_task_metrics(
    context: dict[str, Any],
    *,
    layer: str,
    strategy: str,
    duration_s: float,
    dependency_count: int,
    extra: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """
    Logs metrics for one ETL task.

    Main new T5 metric:
        scheduling_delay_s = Airflow task start_date - queued_dttm

    Writes:
    1. XCom keys for Airflow inspection.
    2. etl_task_run_metrics table for thesis analysis.
    """
    ti = context["ti"]
    observed_end_date = datetime.now(timezone.utc)

    session = settings.Session()

    try:
        _ensure_metrics_table(session)

        fresh_ti = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == ti.dag_id,
                TaskInstance.task_id == ti.task_id,
                TaskInstance.run_id == ti.run_id,
            )
            .one_or_none()
        )

        queued_dttm = getattr(fresh_ti, "queued_dttm", None) or getattr(ti, "queued_dttm", None)
        start_date = getattr(fresh_ti, "start_date", None) or getattr(ti, "start_date", None)
        scheduling_delay_s = _seconds_between(start_date, queued_dttm)

        priority_weight = getattr(getattr(ti, "task", None), "priority_weight", None)

        db_payload = {
            "dag_id": ti.dag_id,
            "run_id": ti.run_id,
            "task_id": ti.task_id,
            "layer": layer,
            "strategy": strategy,
            "dependency_count": int(dependency_count),
            "priority_weight": priority_weight,
            "queued_dttm": queued_dttm,
            "start_date": start_date,
            "observed_end_date": observed_end_date,
            "duration_s": round(float(duration_s), 6),
            "scheduling_delay_s": scheduling_delay_s,
            "extra_json": json.dumps(extra or {}),
        }

        session.execute(
            text(
                """
                INSERT INTO etl_task_run_metrics (
                    dag_id,
                    run_id,
                    task_id,
                    layer,
                    strategy,
                    dependency_count,
                    priority_weight,
                    queued_dttm,
                    start_date,
                    observed_end_date,
                    duration_s,
                    scheduling_delay_s,
                    extra_json
                )
                VALUES (
                    :dag_id,
                    :run_id,
                    :task_id,
                    :layer,
                    :strategy,
                    :dependency_count,
                    :priority_weight,
                    :queued_dttm,
                    :start_date,
                    :observed_end_date,
                    :duration_s,
                    :scheduling_delay_s,
                    :extra_json
                )
                ON CONFLICT (dag_id, run_id, task_id, strategy)
                DO UPDATE SET
                    dependency_count = EXCLUDED.dependency_count,
                    priority_weight = EXCLUDED.priority_weight,
                    queued_dttm = EXCLUDED.queued_dttm,
                    start_date = EXCLUDED.start_date,
                    observed_end_date = EXCLUDED.observed_end_date,
                    duration_s = EXCLUDED.duration_s,
                    scheduling_delay_s = EXCLUDED.scheduling_delay_s,
                    extra_json = EXCLUDED.extra_json;
                """
            ),
            db_payload,
        )

        session.commit()

        xcom_payload = {
            "dag_id": ti.dag_id,
            "run_id": ti.run_id,
            "task_id": ti.task_id,
            "task_name": ti.task_id,
            "layer": layer,
            "strategy": strategy,
            "dependency_count": int(dependency_count),
            "priority_weight": priority_weight,
            "queued_dttm": _to_iso(queued_dttm),
            "start_date": _to_iso(start_date),
            "observed_end_date": _to_iso(observed_end_date),
            "duration_s": round(float(duration_s), 6),
            "scheduling_delay_s": scheduling_delay_s,
            "extra": extra or {},
        }

        ti.xcom_push(key="task_metrics", value=xcom_payload)
        ti.xcom_push(key="duration_s", value=xcom_payload["duration_s"])
        ti.xcom_push(key="layer", value=layer)
        ti.xcom_push(key="task_name", value=ti.task_id)
        ti.xcom_push(key="dependency_count", value=int(dependency_count))
        ti.xcom_push(key="strategy", value=strategy)
        ti.xcom_push(key="scheduling_delay_s", value=scheduling_delay_s)
        ti.xcom_push(key="priority_weight", value=priority_weight)

        return xcom_payload

    except Exception:
        session.rollback()
        raise

    finally:
        session.close()