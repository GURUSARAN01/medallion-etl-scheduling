-- ================================================================
-- Bronze dependency-unlocking mechanism experiment
-- Expected: 10 runs and 60 task rows.
-- ================================================================

-- Query 1: run-level results and mechanism timings.
WITH per_run AS (
    SELECT
        run_id,
        strategy AS variant,
        MIN(start_time) AS processing_start,
        MAX(end_time) AS processing_end,
        TIMESTAMPDIFF(
            MICROSECOND,
            MIN(start_time),
            MAX(end_time)
        ) / 1000000.0 AS processing_window_s,
        SUM(duration_s) AS sum_task_duration_s,
        COUNT(*) AS task_count,
        SUM(records_out) AS total_records_out,
        MAX(
            CASE WHEN task_id = 'ingest_item' THEN end_time END
        ) AS item_end,
        MIN(
            CASE
                WHEN task_id IN (
                    'ingest_web_sales',
                    'ingest_store_sales',
                    'ingest_catalog_sales'
                )
                THEN start_time
            END
        ) AS first_downstream_start,
        MAX(
            CASE
                WHEN task_id = 'ingest_store_sales'
                THEN start_time
            END
        ) AS store_sales_start
    FROM dbw_thesis_etl_scheduling.thesis_etl_scheduling.etl_task_run_metrics_azure
    WHERE run_id RLIKE
        '^azure_bronze_unlock_(early|delayed)_r[1-5]_[0-9]{8}_[0-9]{6}$'
    GROUP BY run_id, strategy
)
SELECT
    run_id,
    variant,
    ROUND(processing_window_s, 3) AS processing_window_s,
    ROUND(sum_task_duration_s, 3) AS sum_task_duration_s,
    ROUND(
        TIMESTAMPDIFF(
            MICROSECOND,
            processing_start,
            item_end
        ) / 1000000.0,
        3
    ) AS item_completion_from_start_s,
    ROUND(
        TIMESTAMPDIFF(
            MICROSECOND,
            processing_start,
            first_downstream_start
        ) / 1000000.0,
        3
    ) AS first_downstream_start_delay_s,
    ROUND(
        TIMESTAMPDIFF(
            MICROSECOND,
            processing_start,
            store_sales_start
        ) / 1000000.0,
        3
    ) AS store_sales_start_delay_s,
    task_count,
    total_records_out,
    processing_start,
    processing_end
FROM per_run
ORDER BY run_id;


-- Query 2: task-level results.
SELECT
    run_id,
    strategy AS variant,
    task_id,
    priority_weight,
    ROUND(duration_s, 3) AS duration_s,
    records_in,
    records_out,
    start_time,
    end_time
FROM dbw_thesis_etl_scheduling.thesis_etl_scheduling.etl_task_run_metrics_azure
WHERE run_id RLIKE
    '^azure_bronze_unlock_(early|delayed)_r[1-5]_[0-9]{8}_[0-9]{6}$'
ORDER BY run_id, start_time;


-- Query 3: descriptive mechanism statistics.
WITH per_run AS (
    SELECT
        run_id,
        strategy AS variant,
        MIN(start_time) AS processing_start,
        MAX(end_time) AS processing_end,
        TIMESTAMPDIFF(
            MICROSECOND,
            MIN(start_time),
            MAX(end_time)
        ) / 1000000.0 AS processing_window_s,
        MAX(
            CASE WHEN task_id = 'ingest_item' THEN end_time END
        ) AS item_end,
        MIN(
            CASE
                WHEN task_id IN (
                    'ingest_web_sales',
                    'ingest_store_sales',
                    'ingest_catalog_sales'
                )
                THEN start_time
            END
        ) AS first_downstream_start
    FROM dbw_thesis_etl_scheduling.thesis_etl_scheduling.etl_task_run_metrics_azure
    WHERE run_id RLIKE
        '^azure_bronze_unlock_(early|delayed)_r[1-5]_[0-9]{8}_[0-9]{6}$'
    GROUP BY run_id, strategy
)
SELECT
    variant,
    COUNT(*) AS repetitions,
    ROUND(AVG(processing_window_s), 3) AS mean_window_s,
    ROUND(STDDEV_SAMP(processing_window_s), 3) AS sd_window_s,
    ROUND(
        AVG(
            TIMESTAMPDIFF(
                MICROSECOND,
                processing_start,
                item_end
            ) / 1000000.0
        ),
        3
    ) AS mean_item_completion_from_start_s,
    ROUND(
        AVG(
            TIMESTAMPDIFF(
                MICROSECOND,
                processing_start,
                first_downstream_start
            ) / 1000000.0
        ),
        3
    ) AS mean_first_downstream_start_delay_s
FROM per_run
GROUP BY variant
ORDER BY variant;


-- Query 4: integrity check.
SELECT
    strategy AS variant,
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
        '^azure_bronze_unlock_(early|delayed)_r[1-5]_[0-9]{8}_[0-9]{6}$'
)
GROUP BY strategy
ORDER BY strategy;
