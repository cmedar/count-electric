# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Car Stock Snapshot (Total Fleet on the Road)
# MAGIC
# MAGIC **Source table:** `silver.car_stock_eurostat`
# MAGIC **Target table:** `gold.car_stock_snapshot`
# MAGIC
# MAGIC ## What this table answers
# MAGIC | Question | Column |
# MAGIC |---|---|
# MAGIC | How many cars are on the road in total in country X? | `total_stock` |
# MAGIC | How many of those are Electric? | `electric_stock` |
# MAGIC | How many are Combustion (petrol + diesel)? | `combustion_stock` |
# MAGIC | What share of the total fleet is Electric? | `electric_share_pct` |
# MAGIC | What share is Combustion? | `combustion_share_pct` |
# MAGIC
# MAGIC ## Key difference from gold.ev_market_share
# MAGIC `ev_market_share` counts **new cars registered** each year (flow).
# MAGIC `car_stock_snapshot` counts **all cars on the road** (stock).
# MAGIC The fleet transitions much more slowly — EVs may be 10% of new sales
# MAGIC but only 1-2% of the total fleet.
# MAGIC
# MAGIC ## Transformations
# MAGIC | Step | Spark concept used |
# MAGIC |---|---|
# MAGIC | Aggregate by category | `groupBy` + `agg(sum)` |
# MAGIC | Reshape categories into columns | `pivot` |
# MAGIC | Fleet share % | Column arithmetic with null guard |
# MAGIC | Write Gold Delta table | `saveAsTable` with `overwrite` |
# MAGIC | Optimize for queries | `OPTIMIZE` + `ZORDER BY` |

# COMMAND ----------

from pyspark.sql import functions as F

SILVER_TABLE = "silver.car_stock_eurostat"
GOLD_TABLE   = "gold.car_stock_snapshot"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Aggregate stock by country, year, fuel category

# COMMAND ----------

df_silver = spark.table(SILVER_TABLE)

df_cat = (
    df_silver
    .groupBy("country_code", "year", "fuel_category")
    .agg(F.sum("stock_count").alias("stock"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Pivot: fuel categories become columns

# COMMAND ----------

df_pivot = (
    df_cat
    .groupBy("country_code", "year")
    .pivot("fuel_category", ["Electric", "Combustion", "Hybrid", "Other", "Total"])
    .agg(F.first("stock"))
    .withColumnRenamed("Electric",   "electric_stock")
    .withColumnRenamed("Combustion", "combustion_stock")
    .withColumnRenamed("Hybrid",     "hybrid_stock")
    .withColumnRenamed("Other",      "other_stock")
    .withColumnRenamed("Total",      "total_stock")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Calculate fleet share percentages

# COMMAND ----------

df_gold = (
    df_pivot
    .withColumn(
        "electric_share_pct",
        F.round(
            F.when(F.col("total_stock") > 0,
                   F.col("electric_stock") / F.col("total_stock") * 100),
            2
        )
    )
    .withColumn(
        "combustion_share_pct",
        F.round(
            F.when(F.col("total_stock") > 0,
                   F.col("combustion_stock") / F.col("total_stock") * 100),
            2
        )
    )
    .select(
        "country_code",
        "year",
        "total_stock",
        "electric_stock",
        "combustion_stock",
        "hybrid_stock",
        "other_stock",
        "electric_share_pct",
        "combustion_share_pct",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Write Gold table

# COMMAND ----------

(
    df_gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_TABLE)
)
print(f"Written {df_gold.count()} rows to {GOLD_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — Export to S3 for Streamlit (direct Parquet read, no SQL Warehouse needed)

# COMMAND ----------

S3_EXPORT = "s3://count-electric/gold/car_stock_snapshot/"
df_gold.coalesce(1).write.mode("overwrite").parquet(S3_EXPORT)
print(f"Exported Parquet to {S3_EXPORT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 — Optimize

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE gold.car_stock_snapshot ZORDER BY (country_code, year)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 — Validate

# COMMAND ----------

df = spark.table(GOLD_TABLE)
print(f"Total rows : {df.count()}")
print(f"Countries  : {df.select('country_code').distinct().count()}")
print(f"Year range : {df.agg(F.min('year'), F.max('year')).collect()[0]}")

# COMMAND ----------

print("Romania — total fleet snapshot:")
display(
    df.filter(F.col("country_code") == "RO")
    .select("year", "total_stock", "electric_stock", "combustion_stock",
            "hybrid_stock", "other_stock", "electric_share_pct", "combustion_share_pct")
    .orderBy("year")
)

# COMMAND ----------

print("Latest year — top 10 countries by Electric fleet share:")
latest_year = df.agg(F.max("year")).collect()[0][0]
display(
    df.filter(F.col("year") == latest_year)
    .filter(F.col("total_stock") > 10000)
    .orderBy(F.desc("electric_share_pct"))
    .limit(10)
    .select("country_code", "year", "total_stock", "electric_stock", "electric_share_pct")
)
