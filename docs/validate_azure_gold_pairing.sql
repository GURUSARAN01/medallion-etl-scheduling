-- ================================================================
-- Gold pairing mechanism experiment
-- Expected: 15 runs and 45 task rows.
-- ================================================================

-- Query 1: run-level results.
WITH run_summary AS (
    SELECT
        run_id,
        strategy AS pairing,
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
        '^azure_gold_pair_(top_region|ltv_region|ltv_top)_r[1-5]_[0-9]{8}_[0-9]{6}$'
    GROUP BY run_id, strategy
)
SELECT
    run_id,
    pairing,
    ROUND(processing_window_s, 3) AS processing_window_s,
    ROUND(sum_task_duration_s, 3) AS sum_task_duration_s,
    task_count,
    total_records_out,
    processing_start,
    processing_end
FROM run_summary
ORDER BY run_id;


-- Query 2: task-level results with execution context.
WITH task_context AS (
    SELECT
        run_id,
        strategy AS pairing,
        task_id,
        priority_weight,
        duration_s,
        records_in,
        records_out,
        start_time,
        end_time,
        CASE
            WHEN start_time = MAX(start_time) OVER (PARTITION BY run_id)
                THEN 'second_wave_alone'
            ELSE 'first_wave_concurrent'
        END AS execution_context
    FROM dbw_thesis_etl_scheduling.thesis_etl_scheduling.etl_task_run_metrics_azure
    WHERE run_id RLIKE
        '^azure_gold_pair_(top_region|ltv_region|ltv_top)_r[1-5]_[0-9]{8}_[0-9]{6}$'
)
SELECT
    run_id,
    pairing,
    task_id,
    priority_weight,
    execution_context,
    ROUND(duration_s, 3) AS duration_s,
    records_in,
    records_out,
    start_time,
    end_time
FROM task_context
ORDER BY run_id, start_time;


-- Query 3: descriptive processing-window statistics.
WITH run_summary AS (
    SELECT
        run_id,
        strategy AS pairing,
        TIMESTAMPDIFF(
            MICROSECOND,
            MIN(start_time),
            MAX(end_time)
        ) / 1000000.0 AS processing_window_s
    FROM dbw_thesis_etl_scheduling.thesis_etl_scheduling.etl_task_run_metrics_azure
    WHERE run_id RLIKE
        '^azure_gold_pair_(top_region|ltv_region|ltv_top)_r[1-5]_[0-9]{8}_[0-9]{6}$'
    GROUP BY run_id, strategy
)
SELECT
    pairing,
    COUNT(*) AS repetitions,
    ROUND(AVG(processing_window_s), 3) AS mean_window_s,
    ROUND(PERCENTILE_APPROX(processing_window_s, 0.5), 3) AS median_window_s,
    ROUND(STDDEV_SAMP(processing_window_s), 3) AS sd_window_s,
    ROUND(MIN(processing_window_s), 3) AS min_window_s,
    ROUND(MAX(processing_window_s), 3) AS max_window_s
FROM run_summary
GROUP BY pairing
ORDER BY pairing;


-- Query 4: mean duration of every task in every pairing/context.
WITH task_context AS (
    SELECT
        run_id,
        strategy AS pairing,
        task_id,
        duration_s,
        CASE
            WHEN start_time = MAX(start_time) OVER (PARTITION BY run_id)
                THEN 'second_wave_alone'
            ELSE 'first_wave_concurrent'
        END AS execution_context
    FROM dbw_thesis_etl_scheduling.thesis_etl_scheduling.etl_task_run_metrics_azure
    WHERE run_id RLIKE
        '^azure_gold_pair_(top_region|ltv_region|ltv_top)_r[1-5]_[0-9]{8}_[0-9]{6}$'
)
SELECT
    pairing,
    task_id,
    execution_context,
    COUNT(*) AS observations,
    ROUND(AVG(duration_s), 3) AS mean_duration_s,
    ROUND(STDDEV_SAMP(duration_s), 3) AS sd_duration_s
FROM task_context
GROUP BY pairing, task_id, execution_context
ORDER BY task_id, execution_context, pairing;


-- Query 5: integrity check.
SELECT
    strategy AS pairing,
    COUNT(DISTINCT run_id) AS run_count,
    COUNT(*) AS task_row_count,
    COUNT(DISTINCT total_records_out_per_run) AS distinct_output_totals
FROM (
    SELECT
        strategy,
        run_id,
        SUM(records_out) OVER (PARTITION BY run_id) AS total_records_out_per_run
    FROM dbw_thesis_etl_scheduling.thesis_etl_scheduling.etl_task_run_metrics_azure
    WHERE run_id RLIKE
        '^azure_gold_pair_(top_region|ltv_region|ltv_top)_r[1-5]_[0-9]{8}_[0-9]{6}$'
)
GROUP BY strategy
ORDER BY strategy;
