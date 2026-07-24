-- Silver strategy validation: task-level metrics
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
WHERE run_id RLIKE '^azure_(static|heft|xgboost)_silver_validation_[0-9]{8}_[0-9]{6}$'
ORDER BY run_id, start_time;


-- Silver strategy validation: run-level summary
SELECT
    run_id,
    strategy,
    ROUND(
        TIMESTAMPDIFF(
            MICROSECOND,
            MIN(start_time),
            MAX(end_time)
        ) / 1000000.0,
        3
    ) AS processing_window_s,
    ROUND(SUM(duration_s), 3) AS sum_task_duration_s,
    COUNT(*) AS task_count,
    SUM(records_out) AS total_records_out,
    MIN(start_time) AS processing_start,
    MAX(end_time) AS processing_end
FROM dbw_thesis_etl_scheduling.thesis_etl_scheduling.etl_task_run_metrics_azure
WHERE run_id RLIKE '^azure_(static|heft|xgboost)_silver_validation_[0-9]{8}_[0-9]{6}$'
GROUP BY run_id, strategy
ORDER BY run_id;
