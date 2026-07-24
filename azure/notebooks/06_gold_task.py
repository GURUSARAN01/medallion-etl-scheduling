# Databricks notebook source
# MAGIC %md
# MAGIC # Parameterised Gold Task
# MAGIC
# MAGIC Runs exactly one Gold aggregation task for the Azure scheduling benchmark.
# MAGIC Airflow supplies the task, strategy, run ID, and priority weight.

# COMMAND ----------

from datetime import datetime, timezone
import json
import re

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
    LongType,
)
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("task_id", "")
dbutils.widgets.text("strategy", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("priority_weight", "1")
dbutils.widgets.text("target_catalog", "dbw_thesis_etl_scheduling")
dbutils.widgets.text("target_schema", "thesis_etl_scheduling")

task_id = dbutils.widgets.get("task_id").strip()
strategy = dbutils.widgets.get("strategy").strip().lower()
run_id = dbutils.widgets.get("run_id").strip()
priority_weight = float(dbutils.widgets.get("priority_weight"))
target_catalog = dbutils.widgets.get("target_catalog").strip()
target_schema = dbutils.widgets.get("target_schema").strip()

required_values = {
    "task_id": task_id,
    "strategy": strategy,
    "run_id": run_id,
}
missing = [name for name, value in required_values.items() if not value]
if missing:
    raise ValueError("Missing required parameters: " + ", ".join(missing))

allowed_tasks = {
    "agg_top_items",
    "agg_sales_by_region",
    "agg_customer_ltv",
}
if task_id not in allowed_tasks:
    raise ValueError(
        f"Unsupported task_id={task_id!r}. "
        f"Expected one of {sorted(allowed_tasks)}."
    )

identifier_pattern = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def validate_identifier(value: str, label: str) -> str:
    if not identifier_pattern.fullmatch(value):
        raise ValueError(
            f"{label} must contain only letters, numbers, and underscores "
            f"and may not start with a number. Received: {value!r}"
        )
    return value

validate_identifier(target_catalog, "target_catalog")
validate_identifier(target_schema, "target_schema")

namespace = f"`{target_catalog}`.`{target_schema}`"
metrics_table = f"{namespace}.`etl_task_run_metrics_azure`"

def table(name: str) -> str:
    validate_identifier(name, "table")
    return f"{namespace}.`{name}`"

source_table_name = "silver_fact_sales"
source_table = table(source_table_name)

print(f"task_id={task_id}")
print(f"strategy={strategy}")
print(f"run_id={run_id}")
print(f"priority_weight={priority_weight}")
print(f"source_table={source_table}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")

try:
    spark.table(source_table).limit(1).collect()
except Exception as exc:
    raise RuntimeError(
        "Required Silver input table is missing: silver_fact_sales"
    ) from exc

metric_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("strategy", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("task_id", StringType(), False),
    StructField("start_time", TimestampType(), False),
    StructField("end_time", TimestampType(), False),
    StructField("duration_s", DoubleType(), False),
    StructField("records_in", LongType(), True),
    StructField("records_out", LongType(), True),
    StructField("source_size_bytes", LongType(), True),
    StructField("priority_weight", DoubleType(), True),
    StructField("extra_json", StringType(), True),
])

def get_source_size_bytes() -> int | None:
    try:
        detail = spark.sql(
            f"DESCRIBE DETAIL {source_table}"
        ).first().asDict()
        value = detail.get("sizeInBytes")
        return int(value) if value is not None else None
    except Exception as exc:
        print(f"Source-size metadata unavailable: {exc}")
        return None

def build_top_items():
    source_df = spark.table(source_table)

    aggregated = (
        source_df
        .filter(F.col("item_sk").isNotNull())
        .groupBy(
            "item_sk",
            "item_id",
            "product_name",
            "category",
            "item_class",
            "brand",
        )
        .agg(
            F.sum("quantity").alias("total_quantity"),
            F.sum("ext_sales_price").alias("total_revenue"),
            F.sum("gross_margin").alias("total_gross_margin"),
            F.sum("net_profit").alias("total_net_profit"),
            F.count(F.lit(1)).alias("sales_line_count"),
            F.countDistinct("customer_sk").alias("unique_customers"),
        )
    )

    ranking_window = Window.orderBy(
        F.col("total_revenue").desc_nulls_last()
    )

    result_df = (
        aggregated
        .withColumn(
            "revenue_rank",
            F.dense_rank().over(ranking_window),
        )
        .filter(F.col("revenue_rank") <= 1000)
    )

    return result_df, "gold_top_items"

def build_sales_by_region():
    source_df = spark.table(source_table)

    result_df = (
        source_df
        .withColumn(
            "customer_region",
            F.coalesce(
                F.col("birth_country"),
                F.lit("UNKNOWN"),
            ),
        )
        .groupBy(
            "customer_region",
            "sales_channel",
            "sold_year",
            "sold_month",
        )
        .agg(
            F.sum("quantity").alias("total_quantity"),
            F.sum("ext_sales_price").alias("total_revenue"),
            F.sum("gross_margin").alias("total_gross_margin"),
            F.sum("net_profit").alias("total_net_profit"),
            F.count(F.lit(1)).alias("sales_line_count"),
            F.countDistinct("customer_sk").alias("unique_customers"),
            F.countDistinct("item_sk").alias("unique_items"),
        )
        .withColumn(
            "average_revenue_per_line",
            F.when(
                F.col("sales_line_count") > 0,
                F.col("total_revenue") / F.col("sales_line_count"),
            ),
        )
    )

    return result_df, "gold_sales_by_region"

def build_customer_ltv():
    source_df = spark.table(source_table)

    result_df = (
        source_df
        .filter(F.col("customer_sk").isNotNull())
        .groupBy(
            "customer_sk",
            "customer_id",
            "first_name",
            "last_name",
            "birth_country",
            "is_preferred_customer",
        )
        .agg(
            F.sum("quantity").alias("lifetime_quantity"),
            F.sum("ext_sales_price").alias("lifetime_revenue"),
            F.sum("gross_margin").alias("lifetime_gross_margin"),
            F.sum("net_profit").alias("lifetime_net_profit"),
            F.count(F.lit(1)).alias("sales_line_count"),
            F.countDistinct("sold_date").alias("active_sales_days"),
            F.countDistinct("item_sk").alias("unique_items_purchased"),
            F.min("sold_date").alias("first_sale_date"),
            F.max("sold_date").alias("latest_sale_date"),
            F.avg("gross_margin_ratio").alias(
                "average_gross_margin_ratio"
            ),
        )
        .withColumn(
            "average_revenue_per_line",
            F.when(
                F.col("sales_line_count") > 0,
                F.col("lifetime_revenue") / F.col("sales_line_count"),
            ),
        )
        .withColumn(
            "customer_value_segment",
            F.when(
                F.col("lifetime_revenue") >= 10000,
                F.lit("HIGH"),
            )
            .when(
                F.col("lifetime_revenue") >= 2500,
                F.lit("MEDIUM"),
            )
            .otherwise(F.lit("STANDARD")),
        )
    )

    return result_df, "gold_customer_ltv"

builders = {
    "agg_top_items": build_top_items,
    "agg_sales_by_region": build_sales_by_region,
    "agg_customer_ltv": build_customer_ltv,
}

# COMMAND ----------

result_df, output_table_name = builders[task_id]()
source_size_bytes = get_source_size_bytes()

start_time = datetime.now(timezone.utc)

(
    result_df
    .withColumn("_gold_loaded_at", F.current_timestamp())
    .withColumn("_gold_run_id", F.lit(run_id))
    .withColumn("_gold_strategy", F.lit(strategy))
    .withColumn("_gold_priority_weight", F.lit(priority_weight))
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(table(output_table_name))
)

end_time = datetime.now(timezone.utc)

# Validation counts are deliberately outside the timed aggregation/write window.
records_in = spark.table(source_table).count()
records_out = spark.table(table(output_table_name)).count()
duration_s = (end_time - start_time).total_seconds()

metric_row = [(
    run_id,
    strategy,
    "Gold",
    task_id,
    start_time.replace(tzinfo=None),
    end_time.replace(tzinfo=None),
    float(duration_s),
    int(records_in),
    int(records_out),
    int(source_size_bytes) if source_size_bytes is not None else None,
    float(priority_weight),
    json.dumps({
        "input_table": source_table_name,
        "output_table": output_table_name,
        "dataset": "TPC-DS SF1 selected subset",
        "execution_mode": "Airflow submitted Databricks task",
    }),
)]

(
    spark.createDataFrame(metric_row, schema=metric_schema)
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(metrics_table)
)

print(
    f"Completed {task_id}: duration={duration_s:.3f}s, "
    f"records_in={records_in:,}, records_out={records_out:,}"
)

dbutils.notebook.exit(
    json.dumps({
        "task_id": task_id,
        "strategy": strategy,
        "run_id": run_id,
        "duration_s": duration_s,
        "records_in": records_in,
        "records_out": records_out,
        "priority_weight": priority_weight,
        "output_table": output_table_name,
    })
)
