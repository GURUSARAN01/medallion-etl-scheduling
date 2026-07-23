# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Static Smoke Test
# MAGIC
# MAGIC This notebook is the first Azure Databricks smoke test for the thesis.
# MAGIC It creates a small synthetic raw dataset, writes Bronze Delta tables, and logs task-level runtime metrics.
# MAGIC
# MAGIC Use this notebook before running any full Azure benchmark.

# COMMAND ----------

from datetime import datetime, timezone
from pathlib import Path
import time
import json
import uuid

from pyspark.sql import functions as F

# COMMAND ----------

# Databricks widgets make the notebook reusable from Jobs.
dbutils.widgets.text("strategy", "static")
dbutils.widgets.text("layer", "Bronze")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("base_path", "dbfs:/tmp/medallion_etl_scheduling")
dbutils.widgets.text("scale_rows", "10000")

strategy = dbutils.widgets.get("strategy")
layer = dbutils.widgets.get("layer")
run_id = dbutils.widgets.get("run_id") or f"azure_{strategy}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
base_path = dbutils.widgets.get("base_path").rstrip("/")
scale_rows = int(dbutils.widgets.get("scale_rows"))

raw_path = f"{base_path}/raw"
bronze_path = f"{base_path}/bronze"
metrics_path = f"{base_path}/metrics/task_run_metrics_delta"

print(f"strategy={strategy}")
print(f"layer={layer}")
print(f"run_id={run_id}")
print(f"base_path={base_path}")
print(f"scale_rows={scale_rows}")

# COMMAND ----------

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def log_metric(task_id, start_time, end_time, records_in, records_out, extra=None):
    """Append one task-level metric row to a Delta metrics table."""
    duration_s = (end_time - start_time).total_seconds()
    row = [{
        "run_id": run_id,
        "strategy": strategy,
        "layer": layer,
        "task_id": task_id,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_s": float(duration_s),
        "records_in": int(records_in) if records_in is not None else None,
        "records_out": int(records_out) if records_out is not None else None,
        "base_path": base_path,
        "extra_json": json.dumps(extra or {})
    }]
    (
        spark.createDataFrame(row)
        .write
        .format("delta")
        .mode("append")
        .save(metrics_path)
    )
    print(f"Logged {task_id}: {duration_s:.3f}s, out={records_out}")

def timed_task(task_id, func):
    start = datetime.now(timezone.utc)
    result = func()
    end = datetime.now(timezone.utc)
    records_in = result.get("records_in")
    records_out = result.get("records_out")
    extra = result.get("extra", {})
    log_metric(task_id, start, end, records_in, records_out, extra)
    return result

# COMMAND ----------

# Create a tiny synthetic source dataset inside DBFS.
# This avoids upload/storage complexity during the first smoke test.

def make_date_dim():
    df = (
        spark.range(0, 365)
        .withColumnRenamed("id", "date_id")
        .withColumn("year", F.lit(2026))
        .withColumn("month", (F.col("date_id") % 12) + 1)
        .withColumn("day", (F.col("date_id") % 28) + 1)
    )
    path = f"{raw_path}/date_dim"
    df.write.format("delta").mode("overwrite").save(path)
    return {"records_in": 365, "records_out": df.count(), "extra": {"path": path}}

def make_item():
    df = (
        spark.range(0, 1000)
        .withColumnRenamed("id", "item_id")
        .withColumn("category_id", (F.col("item_id") % 20))
        .withColumn("base_price", (F.col("item_id") % 200) + F.lit(1.0))
    )
    path = f"{raw_path}/item"
    df.write.format("delta").mode("overwrite").save(path)
    return {"records_in": 1000, "records_out": df.count(), "extra": {"path": path}}

def make_customer():
    df = (
        spark.range(0, 5000)
        .withColumnRenamed("id", "customer_id")
        .withColumn("region_id", (F.col("customer_id") % 10))
        .withColumn("segment_id", (F.col("customer_id") % 5))
    )
    path = f"{raw_path}/customer"
    df.write.format("delta").mode("overwrite").save(path)
    return {"records_in": 5000, "records_out": df.count(), "extra": {"path": path}}

def make_sales(table_name, multiplier):
    n = scale_rows * multiplier
    df = (
        spark.range(0, n)
        .withColumnRenamed("id", "sale_id")
        .withColumn("date_id", F.col("sale_id") % 365)
        .withColumn("item_id", F.col("sale_id") % 1000)
        .withColumn("customer_id", F.col("sale_id") % 5000)
        .withColumn("quantity", (F.col("sale_id") % 5) + 1)
        .withColumn("amount", ((F.col("sale_id") % 200) + 1) * F.col("quantity"))
        .withColumn("source", F.lit(table_name))
    )
    path = f"{raw_path}/{table_name}"
    df.write.format("delta").mode("overwrite").save(path)
    return {"records_in": n, "records_out": df.count(), "extra": {"path": path}}

# COMMAND ----------

# Static order: simple deterministic ingestion order.
# Later HEFT/XGBoost notebooks will use different ordering, but the same operations.

timed_task("ingest_date_dim", make_date_dim)
timed_task("ingest_item", make_item)
timed_task("ingest_customer", make_customer)
timed_task("ingest_web_sales", lambda: make_sales("web_sales", 1))
timed_task("ingest_store_sales", lambda: make_sales("store_sales", 1))
timed_task("ingest_catalog_sales", lambda: make_sales("catalog_sales", 1))

# COMMAND ----------

# Copy raw Delta data to Bronze Delta paths.
# This keeps the Bronze layer explicit and easy to inspect.

bronze_tables = ["date_dim", "item", "customer", "web_sales", "store_sales", "catalog_sales"]

for table_name in bronze_tables:
    start = datetime.now(timezone.utc)
    source = f"{raw_path}/{table_name}"
    target = f"{bronze_path}/{table_name}"
    df = spark.read.format("delta").load(source)
    records_in = df.count()
    (
        df.withColumn("_bronze_loaded_at", F.current_timestamp())
        .write
        .format("delta")
        .mode("overwrite")
        .save(target)
    )
    records_out = spark.read.format("delta").load(target).count()
    end = datetime.now(timezone.utc)
    log_metric(
        task_id=f"write_bronze_{table_name}",
        start_time=start,
        end_time=end,
        records_in=records_in,
        records_out=records_out,
        extra={"source": source, "target": target}
    )

# COMMAND ----------

# Display metrics from this run.
metrics_df = spark.read.format("delta").load(metrics_path).where(F.col("run_id") == run_id)
display(metrics_df.orderBy("start_time"))

# COMMAND ----------

summary_df = (
    metrics_df
    .agg(
        F.min("start_time").alias("layer_start_time"),
        F.max("end_time").alias("layer_end_time"),
        F.sum("duration_s").alias("sum_task_duration_s"),
        F.count("*").alias("logged_task_count")
    )
    .withColumn(
        "layer_runtime_s",
        F.unix_timestamp("layer_end_time") - F.unix_timestamp("layer_start_time")
    )
    .withColumn("run_id", F.lit(run_id))
    .withColumn("strategy", F.lit(strategy))
    .withColumn("layer", F.lit(layer))
)

display(summary_df)

print("Bronze static smoke test completed.")
