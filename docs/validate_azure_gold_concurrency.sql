-- ================================================================
-- Gold concurrency-sensitivity experiment
--
-- Fixed priority order:
--   1. agg_customer_ltv
--   2. agg_top_items
--   3. agg_sales_by_region
--
-- Only max_active_tasks changes: 1, 2, or 3.
--
-- Expected:
--   15 run summaries
--   45 task rows
--   5 runs and 15 task rows per concurrency condition
-- ================================================================


-- Query 1: integrity check.
SELECT
    CAST(
        REGEXP_EXTRACT(strategy, '([123])$', 1)
        AS INT
    ) AS max_active_tasks,
    COUNT(DISTINCT run_id) AS run_count,
    COUNT(*) AS task_row_count,
    COUNT(DISTINCT total_records_out_per_run) AS distinct_output_totals
FROM (
    SELECT
        strategy,
        run_id,
        SUM(records_out) OVER (
            PARTITION BY run_id
        ) AS total_records_out_per_run
    FROM dbw_thesis_etl_scheduling.thesis_etl_scheduling.etl_task_run_metrics_azure
    WHERE run_id RLIKE
        '^azure_gold_concurrency_c[123]_r[1-5]_[0-9]{8}_[0-9]{6}$'
)
GROUP BY
    CAST(
        REGEXP_EXTRACT(strategy, '([123])$', 1)
        AS INT
    )
ORDER BY max_active_tasks;


-- Query 2: run-level summary.
WITH run_summary AS (
    SELECT
        run_id,
        strategy,
        CAST(
            REGEXP_EXTRACT(strategy, '([123])$', 1)
            AS INT
        ) AS max_active_tasks,
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
        '^azure_gold_concurrency_c[123]_r[1-5]_[0-9]{8}_[0-9]{6}$'
    GROUP BY run_id, strategy
),
candidate_times AS (
    SELECT
        run_id,
        start_time AS candidate_time
    FROM dbw_thesis_etl_scheduling.thesis_etl_scheduling.etl_task_run_metrics_azure
    WHERE run_id RLIKE
        '^azure_gold_concurrency_c[123]_r[1-5]_[0-9]{8}_[0-9]{6}$'
),
observed_concurrency AS (
    SELECT
        run_id,
        MAX(active_tasks) AS observed_max_concurrency
    FROM (
        SELECT
            c.run_id,
            c.candidate_time,
            COUNT(*) AS active_tasks
        FROM candidate_times c
        INNER JOIN dbw_thesis_etl_scheduling.thesis_etl_scheduling.etl_task_run_metrics_azure t
            ON c.run_id = t.run_id
           AND t.start_time <= c.candidate_time
           AND t.end_time > c.candidate_time
        GROUP BY c.run_id, c.candidate_time
    ) concurrency_counts
    GROUP BY run_id
)
SELECT
    r.max_active_tasks,
    r.run_id,
    ROUND(r.processing_window_s, 3) AS processing_window_s,
    ROUND(r.sum_task_duration_s, 3) AS sum_task_duration_s,
    ROUND(
        r.sum_task_duration_s / NULLIF(r.processing_window_s, 0),
        3
    ) AS duration_to_window_ratio,
    o.observed_max_concurrency,
    r.task_count,
    r.total_records_out,
    r.processing_start,
    r.processing_end
FROM run_summary r
LEFT JOIN observed_concurrency o
    ON r.run_id = o.run_id
ORDER BY r.max_active_tasks, r.run_id;


-- Query 3: descriptive statistics by concurrency condition.
WITH run_summary AS (
    SELECT
        run_id,
        CAST(
            REGEXP_EXTRACT(strategy, '([123])$', 1)
            AS INT
        ) AS max_active_tasks,
        TIMESTAMPDIFF(
            MICROSECOND,
            MIN(start_time),
            MAX(end_time)
        ) / 1000000.0 AS processing_window_s,
        SUM(duration_s) AS sum_task_duration_s
    FROM dbw_thesis_etl_scheduling.thesis_etl_scheduling.etl_task_run_metrics_azure
    WHERE run_id RLIKE
        '^azure_gold_concurrency_c[123]_r[1-5]_[0-9]{8}_[0-9]{6}$'
    GROUP BY run_id, strategy
)
SELECT
    max_active_tasks,
    COUNT(*) AS repetitions,
    ROUND(AVG(processing_window_s), 3) AS mean_window_s,
    ROUND(
        PERCENTILE_APPROX(processing_window_s, 0.5),
        3
    ) AS median_window_s,
    ROUND(STDDEV_SAMP(processing_window_s), 3) AS sd_window_s,
    ROUND(MIN(processing_window_s), 3) AS min_window_s,
    ROUND(MAX(processing_window_s), 3) AS max_window_s,
    ROUND(
        100.0 * STDDEV_SAMP(processing_window_s)
        / NULLIF(AVG(processing_window_s), 0),
        2
    ) AS coefficient_of_variation_pct,
    ROUND(AVG(sum_task_duration_s), 3) AS mean_sum_task_duration_s,
    ROUND(
        AVG(
            sum_task_duration_s
            / NULLIF(processing_window_s, 0)
        ),
        3
    ) AS mean_duration_to_window_ratio
FROM run_summary
GROUP BY max_active_tasks
ORDER BY max_active_tasks;


-- Query 4: task-level duration statistics by concurrency condition.
SELECT
    CAST(
        REGEXP_EXTRACT(strategy, '([123])$', 1)
        AS INT
    ) AS max_active_tasks,
    task_id,
    COUNT(*) AS observations,
    ROUND(AVG(duration_s), 3) AS mean_duration_s,
    ROUND(
        PERCENTILE_APPROX(duration_s, 0.5),
        3
    ) AS median_duration_s,
    ROUND(STDDEV_SAMP(duration_s), 3) AS sd_duration_s,
    ROUND(MIN(duration_s), 3) AS min_duration_s,
    ROUND(MAX(duration_s), 3) AS max_duration_s
FROM dbw_thesis_etl_scheduling.thesis_etl_scheduling.etl_task_run_metrics_azure
WHERE run_id RLIKE
    '^azure_gold_concurrency_c[123]_r[1-5]_[0-9]{8}_[0-9]{6}$'
GROUP BY
    CAST(
        REGEXP_EXTRACT(strategy, '([123])$', 1)
        AS INT
    ),
    task_id
ORDER BY max_active_tasks, task_id;


-- Query 5: task start offsets within each run.
WITH task_offsets AS (
    SELECT
        run_id,
        CAST(
            REGEXP_EXTRACT(strategy, '([123])$', 1)
            AS INT
        ) AS max_active_tasks,
        task_id,
        priority_weight,
        duration_s,
        start_time,
        end_time,
        MIN(start_time) OVER (
            PARTITION BY run_id
        ) AS run_start
    FROM dbw_thesis_etl_scheduling.thesis_etl_scheduling.etl_task_run_metrics_azure
    WHERE run_id RLIKE
        '^azure_gold_concurrency_c[123]_r[1-5]_[0-9]{8}_[0-9]{6}$'
)
SELECT
    max_active_tasks,
    run_id,
    task_id,
    priority_weight,
    ROUND(
        TIMESTAMPDIFF(
            MICROSECOND,
            run_start,
            start_time
        ) / 1000000.0,
        3
    ) AS start_offset_s,
    ROUND(duration_s, 3) AS duration_s,
    start_time,
    end_time
FROM task_offsets
ORDER BY max_active_tasks, run_id, start_time;


-- Query 6: full task-level export.
SELECT
    CAST(
        REGEXP_EXTRACT(strategy, '([123])$', 1)
        AS INT
    ) AS max_active_tasks,
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
    '^azure_gold_concurrency_c[123]_r[1-5]_[0-9]{8}_[0-9]{6}$'
ORDER BY max_active_tasks, run_id, start_time;
