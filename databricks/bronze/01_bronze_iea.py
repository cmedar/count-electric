# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — IEA Global EV Data
# MAGIC
# MAGIC **Source:** IEA Global EV Data Explorer
# MAGIC **S3 path:** `s3://count-electric/landing/raw/iea/`
# MAGIC **Target table:** `bronze.ev_iea_raw`
# MAGIC **Schema:** Append-only Delta table, one row per source CSV row + ingestion metadata
# MAGIC
# MAGIC ### Columns in source CSV
# MAGIC | Column | Description |
# MAGIC |---|---|
# MAGIC | `region` | Country or region name (e.g. "Romania", "World") |
# MAGIC | `category` | Always "Historical" |
# MAGIC | `parameter` | Metric type — "EV sales", "EV stock", "EV stock share" |
# MAGIC | `mode` | Vehicle mode — "Cars", "Buses", "Trucks" etc. |
# MAGIC | `powertrain` | BEV, PHEV, FCEV, or EV (aggregate of all) |
# MAGIC | `year` | Year (2010–2024) |
# MAGIC | `unit` | "Vehicles" or "Percent" |
# MAGIC | `value` | The numeric value |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType
)

LANDING_PATH = "s3://count-electric/landing/raw/iea/"
BRONZE_TABLE  = "bronze.ev_iea_raw"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Discover new files to ingest

# COMMAND ----------

# Files already recorded in the Bronze table (skip on first run)
already_ingested = set()
if spark.catalog.tableExists(BRONZE_TABLE):
    already_ingested = {
        row.source_file
        for row in spark.sql(f"SELECT DISTINCT source_file FROM {BRONZE_TABLE}").collect()
    }
    print(f"Already ingested: {len(already_ingested)} file(s)")
else:
    print("Table does not exist yet — will create on first load.")

landing_files = [f.path for f in dbutils.fs.ls(LANDING_PATH) if f.name.endswith(".csv")]
new_files = [f for f in landing_files if f not in already_ingested]

print(f"Found {len(landing_files)} file(s) in landing zone, {len(new_files)} new.")
for f in new_files:
    print(f"  → {f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Define schema

# COMMAND ----------

iea_schema = StructType([
    StructField("region",     StringType(),  True),
    StructField("category",   StringType(),  True),
    StructField("parameter",  StringType(),  True),
    StructField("mode",       StringType(),  True),
    StructField("powertrain", StringType(),  True),
    StructField("year",       IntegerType(), True),
    StructField("unit",       StringType(),  True),
    StructField("value",      DoubleType(),  True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Read, add metadata, append to Bronze table

# COMMAND ----------

if not new_files:
    print("No new files to ingest.")
    dbutils.notebook.exit("No new files.")

for file_path in new_files:
    print(f"\nIngesting: {file_path}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .schema(iea_schema)
        .csv(file_path)
    )

    df = (
        df
        .withColumn("source_file",  F.lit(file_path))
        .withColumn("ingested_at",  F.current_timestamp())
        .withColumn("source_name",  F.lit("iea_global_ev_data"))
    )

    print(f"  Rows read: {df.count()}")

    if spark.catalog.tableExists(BRONZE_TABLE):
        df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)
    else:
        df.write.format("delta").mode("overwrite").saveAsTable(BRONZE_TABLE)
        print(f"  Created table: {BRONZE_TABLE}")

    print(f"  Appended to: {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Validate

# COMMAND ----------

df_bronze = spark.table(BRONZE_TABLE)
print(f"Total rows in {BRONZE_TABLE}: {df_bronze.count()}")
print(f"Source files loaded:          {df_bronze.select('source_file').distinct().count()}")

# COMMAND ----------

print("Rows by powertrain:")
df_bronze.groupBy("powertrain").count().orderBy("powertrain").show()

# COMMAND ----------

print("Year range:")
df_bronze.agg(F.min("year").alias("from"), F.max("year").alias("to")).show()

# COMMAND ----------

print("Romania EV sales by year:")
display(
    df_bronze
    .filter(
        (F.col("region")    == "Romania") &
        (F.col("parameter") == "EV sales") &
        (F.col("mode")      == "Cars")
    )
    .select("year", "powertrain", "value", "unit")
    .orderBy("year", "powertrain")
)
