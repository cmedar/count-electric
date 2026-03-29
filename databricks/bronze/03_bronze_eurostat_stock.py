# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Eurostat Passenger Car Stock by Motor Energy
# MAGIC
# MAGIC **Source:** Eurostat dataset `ROAD_EQS_CARPDA`
# MAGIC **S3 path:** `s3://count-electric/landing/raw/eurostat_stock/`
# MAGIC **Target table:** `bronze.car_stock_eurostat_raw`
# MAGIC
# MAGIC **Stock vs Registrations:**
# MAGIC This is `ROAD_EQS` (equipment **stock**) — all cars currently on the road,
# MAGIC accumulated over time. Compare with `ROAD_EQR` (registrations) which counts
# MAGIC only new cars sold in a given year.
# MAGIC
# MAGIC ### Fuel type codes (`mot_nrg`)
# MAGIC | Code | Fuel type |
# MAGIC |---|---|
# MAGIC | `TOTAL` | All fuel types — total fleet size |
# MAGIC | `ELC` | Electric / BEV |
# MAGIC | `ELC_PET_PI` | Plug-in hybrid petrol (PHEV) |
# MAGIC | `ELC_DIE_PI` | Plug-in hybrid diesel (PHEV) |
# MAGIC | `ELC_PET_HYB` | Non-plug-in hybrid petrol |
# MAGIC | `ELC_DIE_HYB` | Non-plug-in hybrid diesel |
# MAGIC | `PET` | Petrol / gasoline |
# MAGIC | `DIE` | Diesel |
# MAGIC | `LPG` | LPG |
# MAGIC | `GAS` | Natural gas |
# MAGIC | `HYD_FCELL` | Hydrogen fuel cell |
# MAGIC
# MAGIC Romania geo code: **`RO`**

# COMMAND ----------

import json
from itertools import product

import boto3
from pyspark.sql import Row, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

LANDING_PATH  = "s3://count-electric/landing/raw/eurostat_stock/"
BRONZE_TABLE  = "bronze.car_stock_eurostat_raw"
S3_BUCKET     = "count-electric"
S3_PREFIX     = "landing/raw/eurostat_stock"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Discover new files

# COMMAND ----------

already_ingested = set()
if spark.catalog.tableExists(BRONZE_TABLE):
    already_ingested = {
        row.source_file
        for row in spark.sql(f"SELECT DISTINCT source_file FROM {BRONZE_TABLE}").collect()
    }
    print(f"Already ingested: {len(already_ingested)} file(s)")
else:
    print("Table does not exist yet — will create on first load.")

landing_files = [f.path for f in dbutils.fs.ls(LANDING_PATH) if f.name.endswith(".json")]
new_files = [f for f in landing_files if f not in already_ingested]

print(f"Found {len(landing_files)} file(s), {len(new_files)} new.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Parse Eurostat JSON-stat2 format

# COMMAND ----------

def parse_jsonstat2(raw: dict) -> list:
    """
    Convert a Eurostat JSON-stat2 response into a flat list of dicts.
    Each dict has one key per dimension plus a 'value' key.
    """
    dims   = raw["id"]
    sizes  = raw["size"]
    values = raw["value"]

    label_maps = {}
    for dim in dims:
        cats = raw["dimension"][dim]["category"]
        label_maps[dim] = {str(v): k for k, v in cats["index"].items()}

    rows = []
    for i, combo in enumerate(product(*[range(s) for s in sizes])):
        val = values.get(str(i))
        if val is None:
            continue
        row = {dim: label_maps[dim][str(idx)] for dim, idx in zip(dims, combo)}
        row["value"] = float(val)
        rows.append(row)

    return rows


def read_s3_json(bucket: str, key: str) -> dict:
    access_key = dbutils.secrets.get(scope="count-electric", key="aws-access-key-id")
    secret_key = dbutils.secrets.get(scope="count-electric", key="aws-secret-access-key")
    region     = dbutils.secrets.get(scope="count-electric", key="aws-region")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )
    response = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Read, parse, append to Bronze table

# COMMAND ----------

bronze_schema = StructType([
    StructField("freq",        StringType(), True),
    StructField("mot_nrg",     StringType(), True),   # fuel type code
    StructField("geo",         StringType(), True),   # country code e.g. "RO"
    StructField("time",        StringType(), True),   # year as string e.g. "2023"
    StructField("value",       DoubleType(), True),   # total stock count
    StructField("source_file", StringType(), False),
    StructField("source_name", StringType(), False),
])

if not new_files:
    print("No new files to ingest.")
    dbutils.notebook.exit("No new files.")

for file_path in new_files:
    print(f"\nIngesting: {file_path}")

    s3_key = file_path.replace(f"s3://{S3_BUCKET}/", "")

    raw  = read_s3_json(S3_BUCKET, s3_key)
    rows = parse_jsonstat2(raw)
    print(f"  Parsed {len(rows)} observations")

    spark_rows = [
        Row(
            freq=r["freq"],
            mot_nrg=r["mot_nrg"],
            geo=r["geo"],
            time=r["time"],
            value=r["value"],
            source_file=file_path,
            source_name="eurostat_road_eqs_carpda",
        )
        for r in rows
    ]

    df = (
        spark.createDataFrame(spark_rows, schema=bronze_schema)
        .withColumn("ingested_at", F.current_timestamp())
    )

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
print(f"Total rows: {df_bronze.count()}")

# COMMAND ----------

print("Rows by fuel type (mot_nrg):")
df_bronze.groupBy("mot_nrg").count().orderBy(F.col("count").desc()).show(20)

# COMMAND ----------

print("Romania — year range and fuel types available:")
(
    df_bronze
    .filter(F.col("geo") == "RO")
    .groupBy("mot_nrg")
    .agg(
        F.min("time").alias("from_year"),
        F.max("time").alias("to_year"),
        F.count("*").alias("rows"),
    )
    .orderBy("mot_nrg")
    .show()
)

# COMMAND ----------

print("Romania — Electric vs Petrol vs Diesel stock:")
display(
    df_bronze
    .filter(
        (F.col("geo")     == "RO") &
        (F.col("mot_nrg").isin("ELC", "PET", "DIE", "TOTAL", "ELC_PET_PI", "ELC_DIE_PI"))
    )
    .select("time", "mot_nrg", "value")
    .orderBy("mot_nrg", "time")
)
