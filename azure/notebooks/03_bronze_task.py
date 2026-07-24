# Databricks notebook source
# MAGIC %md
# MAGIC # Parameterised Bronze Task
# MAGIC
# MAGIC Runs exactly one Bronze ingestion task for the Azure scheduling benchmark.
# MAGIC Airflow supplies the task, strategy, run identifier, and priority weight.

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

# COMMAND ----------

dbutils.widgets.text("task_id", "")
dbutils.widgets.text("source_table", "")
dbutils.widgets.text("target_table", "")
dbutils.widgets.text("strategy", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("priority_weight", "1")
dbutils.widgets.text("source_catalog", "samples")
dbutils.widgets.text("source_schema", "tpcds_sf1")
dbutils.widgets.text("target_catalog", "dbw_thesis_etl_scheduling")
dbutils.widgets.text("target_schema", "thesis_etl_scheduling")

task_id = dbutils.widgets.get("task_id").strip()
source_table_name = dbutils.widgets.get("source_table").strip()
target_table_name = dbutils.widgets.get("target_table").strip()
strategy = dbutils.widgets.get("strategy").strip().lower()
run_id = dbutils.widgets.get("run_id").strip()
priority_weight = float(dbutils.widgets.get("priority_weight"))
source_catalog = dbutils.widgets.get("source_catalog").strip()
source_schema = dbutils.widgets.get("source_schema").strip()
target_catalog = dbutils.widgets.get("target_catalog").strip()
target_schema = dbutils.widgets.get("target_schema").strip()

required_values = {
    "task_id": task_id,
    "source_table": source_table_name,
    "target_table": target_table_name,
    "strategy": strategy,
    "run_id": run_id,
}
missing = [name for name, value in required_values.items() if not value]
if missing:
    raise ValueError("Missing required parameters: " + ", ".join(missing))

identifier_pattern = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def validate_identifier(value: str, label: str) -> str:
    if not identifier_pattern.fullmatch(value):
        raise ValueError(
            f"{label} must contain only letters, numbers, and underscores "
            f"and may not start with a number. Received: {value!r}"
        )
    return value

for value, label in [
    (source_catalog, "source_catalog"),
    (source_schema, "source_schema"),
    (source_table_name, "source_table"),
    (target_catalog, "target_catalog"),
    (target_schema, "target_schema"),
    (target_table_name, "target_table"),
]:
    validate_identifier(value, label)

source_table = f"`{source_catalog}`.`{source_schema}`.`{source_table_name}`"
target_table = f"`{target_catalog}`.`{target_schema}`.`{target_table_name}`"
metrics_table = (
    f"`{target_catalog}`.`{target_schema}`."
    f"`etl_task_run_metrics_azure`"
)

print(f"task_id={task_id}")
print(f"strategy={strategy}")
print(f"run_id={run_id}")
print(f"priority_weight={priority_weight}")
print(f"source={source_table}")
print(f"target={target_table}")

# COMMAND ----------

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS "
    f"`{target_catalog}`.`{target_schema}`"
)

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

def get_table_size_bytes(qualified_table: str):
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {qualified_table}").first().asDict()
        value = detail.get("sizeInBytes")
        return int(value) if value is not None else None
    except Exception as exc:
        print(f"Source-size metadata unavailable: {exc}")
        return None

# COMMAND ----------

source_size_bytes = get_table_size_bytes(source_table)
start_time = datetime.now(timezone.utc)

source_df = spark.table(source_table)
(
    source_df
    .withColumn("_bronze_loaded_at", F.current_timestamp())
    .withColumn("_bronze_run_id", F.lit(run_id))
    .withColumn("_bronze_strategy", F.lit(strategy))
    .withColumn("_bronze_priority_weight", F.lit(priority_weight))
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

end_time = datetime.now(timezone.utc)

# Perform row validation outside the timed write.
records_out = spark.table(target_table).count()

metric_row = [(
    run_id,
    strategy,
    "Bronze",
    task_id,
    start_time.replace(tzinfo=None),
    end_time.replace(tzinfo=None),
    float((end_time - start_time).total_seconds()),
    int(records_out),
    int(records_out),
    int(source_size_bytes) if source_size_bytes is not None else None,
    float(priority_weight),
    json.dumps({
        "source_table": source_table,
        "target_table": target_table,
        "dataset": "TPC-DS SF1",
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

duration_s = (end_time - start_time).total_seconds()
print(
    f"Completed {task_id}: duration={duration_s:.3f}s, "
    f"records={records_out:,}"
)

dbutils.notebook.exit(
    json.dumps({
        "task_id": task_id,
        "strategy": strategy,
        "run_id": run_id,
        "duration_s": duration_s,
        "records_out": records_out,
        "priority_weight": priority_weight,
    })
)
