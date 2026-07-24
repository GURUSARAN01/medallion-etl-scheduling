-- Official Azure Silver benchmark: run-level summary
WITH official_runs AS (
    SELECT
        run_id,
        strategy,
        MIN(start_time) AS processing_start,
        MAX(end_time) AS processing_end,
        TIMESTAMPDIFF(
            MICROSECOND,
            MIN(start_time),
            MAX(end_time)
        ) / 1000000.0 AS processing_window_s,
        SUM(duration_s) AS sum_task_duration_s,
        COUNT(*) AS task_count,
        SUM(records_out) AS total_records_out
    FROM dbw_thesis_etl_scheduling.thesis_etl_scheduling.etl_task_run_metrics_azure
    WHERE run_id RLIKE '^azure_(static|heft|xgboost)_silver_official_r[1-3]_[0-9]{8}_[0-9]{6}$'
    GROUP BY run_id, strategy
)
SELECT
    run_id,
    strategy,
    ROUND(processing_window_s, 3) AS processing_window_s,
    ROUND(sum_task_duration_s, 3) AS sum_task_duration_s,
    task_count,
    total_records_out,
    processing_start,
    processing_end
FROM official_runs
ORDER BY run_id;


-- Official Azure Silver benchmark: task-level metrics
SELECT
    run_id,
    strategy,
    task_id,
    priority_weight,
    ROUND(duration_s, 3) AS duration_s,
    records_in,
    records_out,
    start_time,
    end_time
FROM dbw_thesis_etl_scheduling.thesis_etl_scheduling.etl_task_run_metrics_azure
WHERE run_id RLIKE '^azure_(static|heft|xgboost)_silver_official_r[1-3]_[0-9]{8}_[0-9]{6}$'
ORDER BY run_id, start_time;
