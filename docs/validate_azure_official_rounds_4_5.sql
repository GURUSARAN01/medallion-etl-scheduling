-- ================================================================
-- Azure official rounds 4 and 5 validation
-- Metrics table:
-- dbw_thesis_etl_scheduling.thesis_etl_scheduling.etl_task_run_metrics_azure
-- ================================================================

-- Query 1: run-level summaries for only rounds 4 and 5.
-- Expected result: 18 rows.
WITH new_runs AS (
    SELECT
        LOWER(layer) AS layer,
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
    WHERE run_id RLIKE
        '^azure_(static|heft|xgboost)_(bronze|silver|gold)_official_r[45]_[0-9]{8}_[0-9]{6}$'
    GROUP BY LOWER(layer), run_id, strategy
)
SELECT
    layer,
    run_id,
    strategy,
    ROUND(processing_window_s, 3) AS processing_window_s,
    ROUND(sum_task_duration_s, 3) AS sum_task_duration_s,
    task_count,
    total_records_out,
    processing_start,
    processing_end
FROM new_runs
ORDER BY
    CASE layer
        WHEN 'bronze' THEN 1
        WHEN 'silver' THEN 2
        WHEN 'gold' THEN 3
    END,
    run_id;


-- Query 2: task-level rows for only rounds 4 and 5.
-- Expected result: 78 rows.
SELECT
    LOWER(layer) AS layer,
    run_id,
    strategy,
    task_id,
    priority_weight,
    ROUND(duration_s, 3) AS duration_s,
    records_in,
    records_out,
    source_size_bytes,
    start_time,
    end_time
FROM dbw_thesis_etl_scheduling.thesis_etl_scheduling.etl_task_run_metrics_azure
WHERE run_id RLIKE
    '^azure_(static|heft|xgboost)_(bronze|silver|gold)_official_r[45]_[0-9]{8}_[0-9]{6}$'
ORDER BY
    CASE LOWER(layer)
        WHEN 'bronze' THEN 1
        WHEN 'silver' THEN 2
        WHEN 'gold' THEN 3
    END,
    run_id,
    start_time;


-- Query 3: integrity check for only rounds 4 and 5.
-- Expected per strategy:
-- Bronze: 2 runs and 12 task rows
-- Silver: 2 runs and 8 task rows
-- Gold:   2 runs and 6 task rows
SELECT
    LOWER(layer) AS layer,
    strategy,
    COUNT(DISTINCT run_id) AS new_run_count,
    COUNT(*) AS new_task_row_count,
    COUNT(DISTINCT total_records_out_per_run) AS distinct_output_totals
FROM (
    SELECT
        layer,
        strategy,
        run_id,
        task_id,
        SUM(records_out) OVER (PARTITION BY run_id) AS total_records_out_per_run
    FROM dbw_thesis_etl_scheduling.thesis_etl_scheduling.etl_task_run_metrics_azure
    WHERE run_id RLIKE
        '^azure_(static|heft|xgboost)_(bronze|silver|gold)_official_r[45]_[0-9]{8}_[0-9]{6}$'
)
GROUP BY LOWER(layer), strategy
ORDER BY
    CASE LOWER(layer)
        WHEN 'bronze' THEN 1
        WHEN 'silver' THEN 2
        WHEN 'gold' THEN 3
    END,
    strategy;


-- Query 4: full official n=5 integrity check.
-- Expected:
-- Each layer-strategy combination has 5 runs.
-- Bronze has 30 task rows per strategy.
-- Silver has 20 task rows per strategy.
-- Gold has 15 task rows per strategy.
SELECT
    LOWER(layer) AS layer,
    strategy,
    COUNT(DISTINCT run_id) AS official_run_count,
    COUNT(*) AS official_task_row_count,
    COUNT(DISTINCT total_records_out_per_run) AS distinct_output_totals
FROM (
    SELECT
        layer,
        strategy,
        run_id,
        task_id,
        SUM(records_out) OVER (PARTITION BY run_id) AS total_records_out_per_run
    FROM dbw_thesis_etl_scheduling.thesis_etl_scheduling.etl_task_run_metrics_azure
    WHERE run_id RLIKE
        '^azure_(static|heft|xgboost)_(bronze|silver|gold)_official_r[1-5]_[0-9]{8}_[0-9]{6}$'
)
GROUP BY LOWER(layer), strategy
ORDER BY
    CASE LOWER(layer)
        WHEN 'bronze' THEN 1
        WHEN 'silver' THEN 2
        WHEN 'gold' THEN 3
    END,
    strategy;


-- Query 5: final n=5 descriptive processing-window statistics.
WITH official_run_summary AS (
    SELECT
        LOWER(layer) AS layer,
        run_id,
        strategy,
        TIMESTAMPDIFF(
            MICROSECOND,
            MIN(start_time),
            MAX(end_time)
        ) / 1000000.0 AS processing_window_s,
        SUM(duration_s) AS sum_task_duration_s,
        COUNT(*) AS task_count,
        SUM(records_out) AS total_records_out
    FROM dbw_thesis_etl_scheduling.thesis_etl_scheduling.etl_task_run_metrics_azure
    WHERE run_id RLIKE
        '^azure_(static|heft|xgboost)_(bronze|silver|gold)_official_r[1-5]_[0-9]{8}_[0-9]{6}$'
    GROUP BY LOWER(layer), run_id, strategy
)
SELECT
    layer,
    strategy,
    COUNT(*) AS repetitions,
    ROUND(AVG(processing_window_s), 3) AS mean_processing_window_s,
    ROUND(
        PERCENTILE_APPROX(processing_window_s, 0.5),
        3
    ) AS median_processing_window_s,
    ROUND(STDDEV_SAMP(processing_window_s), 3) AS sd_processing_window_s,
    ROUND(MIN(processing_window_s), 3) AS min_processing_window_s,
    ROUND(MAX(processing_window_s), 3) AS max_processing_window_s,
    ROUND(
        100.0 * STDDEV_SAMP(processing_window_s)
        / NULLIF(AVG(processing_window_s), 0),
        2
    ) AS coefficient_of_variation_pct
FROM official_run_summary
GROUP BY layer, strategy
ORDER BY
    CASE layer
        WHEN 'bronze' THEN 1
        WHEN 'silver' THEN 2
        WHEN 'gold' THEN 3
    END,
    CASE strategy
        WHEN 'static' THEN 1
        WHEN 'heft' THEN 2
        WHEN 'xgboost' THEN 3
    END;


-- Query 6: final n=5 run-level export.
-- Expected result: 45 rows.
WITH official_runs AS (
    SELECT
        LOWER(layer) AS layer,
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
    WHERE run_id RLIKE
        '^azure_(static|heft|xgboost)_(bronze|silver|gold)_official_r[1-5]_[0-9]{8}_[0-9]{6}$'
    GROUP BY LOWER(layer), run_id, strategy
)
SELECT
    layer,
    run_id,
    strategy,
    ROUND(processing_window_s, 3) AS processing_window_s,
    ROUND(sum_task_duration_s, 3) AS sum_task_duration_s,
    task_count,
    total_records_out,
    processing_start,
    processing_end
FROM official_runs
ORDER BY
    CASE layer
        WHEN 'bronze' THEN 1
        WHEN 'silver' THEN 2
        WHEN 'gold' THEN 3
    END,
    run_id;


-- Query 7: final n=5 task-level export.
-- Expected result: 195 rows.
SELECT
    LOWER(layer) AS layer,
    run_id,
    strategy,
    task_id,
    priority_weight,
    ROUND(duration_s, 3) AS duration_s,
    records_in,
    records_out,
    source_size_bytes,
    start_time,
    end_time
FROM dbw_thesis_etl_scheduling.thesis_etl_scheduling.etl_task_run_metrics_azure
WHERE run_id RLIKE
    '^azure_(static|heft|xgboost)_(bronze|silver|gold)_official_r[1-5]_[0-9]{8}_[0-9]{6}$'
ORDER BY
    CASE LOWER(layer)
        WHEN 'bronze' THEN 1
        WHEN 'silver' THEN 2
        WHEN 'gold' THEN 3
    END,
    run_id,
    start_time;
