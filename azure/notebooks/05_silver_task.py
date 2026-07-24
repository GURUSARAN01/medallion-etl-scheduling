# Databricks notebook source
# MAGIC %md
# MAGIC # Parameterised Silver Task
# MAGIC Runs exactly one Silver task. Airflow supplies the task, strategy, run ID, and priority.

# COMMAND ----------
from datetime import datetime, timezone
import json, re
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, LongType

# COMMAND ----------
for name, default in [
    ("task_id", ""), ("strategy", ""), ("run_id", ""),
    ("priority_weight", "1"),
    ("target_catalog", "dbw_thesis_etl_scheduling"),
    ("target_schema", "thesis_etl_scheduling"),
]:
    dbutils.widgets.text(name, default)

task_id = dbutils.widgets.get("task_id").strip()
strategy = dbutils.widgets.get("strategy").strip().lower()
run_id = dbutils.widgets.get("run_id").strip()
priority_weight = float(dbutils.widgets.get("priority_weight"))
target_catalog = dbutils.widgets.get("target_catalog").strip()
target_schema = dbutils.widgets.get("target_schema").strip()

if not task_id or not strategy or not run_id:
    raise ValueError("task_id, strategy, and run_id are required")

allowed_tasks = {"clean_sales", "clean_customer", "clean_item", "join_fact_sales"}
if task_id not in allowed_tasks:
    raise ValueError(f"Unsupported task_id: {task_id}")

ident = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
for value, label in [(target_catalog, "target_catalog"), (target_schema, "target_schema")]:
    if not ident.fullmatch(value):
        raise ValueError(f"Invalid {label}: {value}")

ns = f"`{target_catalog}`.`{target_schema}`"
metrics_table = f"{ns}.`etl_task_run_metrics_azure`"

def q(name):
    if not ident.fullmatch(name): raise ValueError(f"Invalid table: {name}")
    return f"{ns}.`{name}`"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ns}")

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

def ensure(names):
    missing = []
    for name in names:
        try: spark.table(q(name)).limit(1).collect()
        except Exception: missing.append(name)
    if missing: raise RuntimeError("Missing input tables: " + ", ".join(missing))

def size_bytes(name):
    try:
        row = spark.sql(f"DESCRIBE DETAIL {q(name)}").first().asDict()
        return int(row["sizeInBytes"]) if row.get("sizeInBytes") is not None else None
    except Exception as exc:
        print(f"Size unavailable for {name}: {exc}")
        return None

def total_size(names):
    vals = [size_bytes(n) for n in names]
    vals = [v for v in vals if v is not None]
    return sum(vals) if vals else None

def build_clean_sales():
    inputs = ["bronze_store_sales", "bronze_web_sales", "bronze_catalog_sales"]
    ensure(inputs)
    store = spark.table(q(inputs[0])).select(
        F.lit("store").alias("sales_channel"), F.col("ss_sold_date_sk").cast("long").alias("sold_date_sk"),
        F.col("ss_item_sk").cast("long").alias("item_sk"), F.col("ss_customer_sk").cast("long").alias("customer_sk"),
        F.col("ss_quantity").cast("long").alias("quantity"), F.col("ss_sales_price").cast("double").alias("sales_price"),
        F.col("ss_ext_sales_price").cast("double").alias("ext_sales_price"),
        F.col("ss_ext_wholesale_cost").cast("double").alias("ext_wholesale_cost"),
        F.col("ss_net_profit").cast("double").alias("net_profit"))
    web = spark.table(q(inputs[1])).select(
        F.lit("web").alias("sales_channel"), F.col("ws_sold_date_sk").cast("long").alias("sold_date_sk"),
        F.col("ws_item_sk").cast("long").alias("item_sk"), F.col("ws_bill_customer_sk").cast("long").alias("customer_sk"),
        F.col("ws_quantity").cast("long").alias("quantity"), F.col("ws_sales_price").cast("double").alias("sales_price"),
        F.col("ws_ext_sales_price").cast("double").alias("ext_sales_price"),
        F.col("ws_ext_wholesale_cost").cast("double").alias("ext_wholesale_cost"),
        F.col("ws_net_profit").cast("double").alias("net_profit"))
    catalog = spark.table(q(inputs[2])).select(
        F.lit("catalog").alias("sales_channel"), F.col("cs_sold_date_sk").cast("long").alias("sold_date_sk"),
        F.col("cs_item_sk").cast("long").alias("item_sk"), F.col("cs_bill_customer_sk").cast("long").alias("customer_sk"),
        F.col("cs_quantity").cast("long").alias("quantity"), F.col("cs_sales_price").cast("double").alias("sales_price"),
        F.col("cs_ext_sales_price").cast("double").alias("ext_sales_price"),
        F.col("cs_ext_wholesale_cost").cast("double").alias("ext_wholesale_cost"),
        F.col("cs_net_profit").cast("double").alias("net_profit"))
    df = (store.unionByName(web).unionByName(catalog)
          .filter(F.col("sold_date_sk").isNotNull() & F.col("item_sk").isNotNull() & F.col("customer_sk").isNotNull() & (F.col("quantity") > 0))
          .withColumn("gross_margin", F.col("ext_sales_price") - F.col("ext_wholesale_cost"))
          .withColumn("unit_revenue", F.when(F.col("quantity") != 0, F.col("ext_sales_price") / F.col("quantity")))
          .dropDuplicates())
    return df, inputs, "silver_sales"

def build_clean_customer():
    inputs = ["bronze_customer"]; ensure(inputs)
    df = (spark.table(q(inputs[0])).select(
        F.col("c_customer_sk").cast("long").alias("customer_sk"), F.trim("c_customer_id").alias("customer_id"),
        F.initcap(F.trim("c_first_name")).alias("first_name"), F.initcap(F.trim("c_last_name")).alias("last_name"),
        F.col("c_birth_year").cast("int").alias("birth_year"), F.upper(F.trim("c_birth_country")).alias("birth_country"),
        F.when(F.upper(F.trim("c_preferred_cust_flag")) == "Y", True).otherwise(False).alias("is_preferred_customer"),
        F.lower(F.trim("c_email_address")).alias("email_address"))
        .filter(F.col("customer_sk").isNotNull()).dropDuplicates(["customer_sk"]))
    return df, inputs, "silver_customer"

def build_clean_item():
    inputs = ["bronze_item"]; ensure(inputs)
    df = (spark.table(q(inputs[0])).select(
        F.col("i_item_sk").cast("long").alias("item_sk"), F.trim("i_item_id").alias("item_id"),
        F.trim("i_product_name").alias("product_name"), F.trim("i_item_desc").alias("item_description"),
        F.initcap(F.trim("i_category")).alias("category"), F.initcap(F.trim("i_class")).alias("item_class"),
        F.trim("i_brand").alias("brand"), F.col("i_current_price").cast("double").alias("current_price"),
        F.col("i_wholesale_cost").cast("double").alias("wholesale_cost"))
        .filter(F.col("item_sk").isNotNull()).dropDuplicates(["item_sk"]))
    return df, inputs, "silver_item"

def build_join_fact_sales():
    inputs = ["silver_sales", "silver_customer", "silver_item", "bronze_date_dim"]; ensure(inputs)
    s = spark.table(q("silver_sales")).alias("s")
    c = spark.table(q("silver_customer")).alias("c")
    i = spark.table(q("silver_item")).alias("i")
    d = (spark.table(q("bronze_date_dim")).select(
        F.col("d_date_sk").cast("long").alias("sold_date_sk"), F.col("d_date").cast("date").alias("sold_date"),
        F.col("d_year").cast("int").alias("sold_year"), F.col("d_moy").cast("int").alias("sold_month"),
        F.col("d_qoy").cast("int").alias("sold_quarter")).dropDuplicates(["sold_date_sk"]).alias("d"))
    df = (s.join(c, F.col("s.customer_sk") == F.col("c.customer_sk"), "left")
          .join(i, F.col("s.item_sk") == F.col("i.item_sk"), "left")
          .join(d, F.col("s.sold_date_sk") == F.col("d.sold_date_sk"), "left")
          .select("s.*", "c.customer_id", "c.first_name", "c.last_name", "c.birth_year", "c.birth_country",
                  "c.is_preferred_customer", "i.item_id", "i.product_name", "i.category", "i.item_class", "i.brand",
                  "d.sold_date", "d.sold_year", "d.sold_month", "d.sold_quarter")
          .withColumn("gross_margin_ratio", F.when(F.col("ext_sales_price") != 0, F.col("gross_margin") / F.col("ext_sales_price")))
          .withColumn("customer_age_at_sale", F.when(F.col("birth_year").isNotNull() & F.col("sold_year").isNotNull(), F.col("sold_year") - F.col("birth_year"))))
    return df, inputs, "silver_fact_sales"

builders = {"clean_sales": build_clean_sales, "clean_customer": build_clean_customer, "clean_item": build_clean_item, "join_fact_sales": build_join_fact_sales}
result_df, input_tables, output_name = builders[task_id]()
source_size_bytes = total_size(input_tables)

start_time = datetime.now(timezone.utc)
(result_df.withColumn("_silver_loaded_at", F.current_timestamp())
 .withColumn("_silver_run_id", F.lit(run_id))
 .withColumn("_silver_strategy", F.lit(strategy))
 .withColumn("_silver_priority_weight", F.lit(priority_weight))
 .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(q(output_name)))
end_time = datetime.now(timezone.utc)

records_out = spark.table(q(output_name)).count()
records_in = spark.table(q("silver_sales")).count() if task_id == "join_fact_sales" else records_out
duration_s = (end_time - start_time).total_seconds()

row = [(run_id, strategy, "Silver", task_id, start_time.replace(tzinfo=None), end_time.replace(tzinfo=None), float(duration_s),
        int(records_in), int(records_out), int(source_size_bytes) if source_size_bytes is not None else None, float(priority_weight),
        json.dumps({"input_tables": input_tables, "output_table": output_name, "dataset": "TPC-DS SF1 selected subset"}))]
(spark.createDataFrame(row, schema=metric_schema).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(metrics_table))

print(f"Completed {task_id}: duration={duration_s:.3f}s, records_out={records_out:,}")
dbutils.notebook.exit(json.dumps({"task_id": task_id, "strategy": strategy, "run_id": run_id, "duration_s": duration_s,
                                  "records_in": records_in, "records_out": records_out, "priority_weight": priority_weight,
                                  "output_table": output_name}))
