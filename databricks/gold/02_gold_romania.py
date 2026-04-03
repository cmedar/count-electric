# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Romania Deep-Dive
# MAGIC
# MAGIC **Source tables:**
# MAGIC - `gold.ev_market_share` — EV share & YoY (Eurostat-based, run 01 first)
# MAGIC - `silver.ev_registrations_iea` — EV stock + sales globally
# MAGIC
# MAGIC **Target table:** `gold.romania_ev_summary`
# MAGIC
# MAGIC ## What this table answers
# MAGIC | Question | Column |
# MAGIC |---|---|
# MAGIC | How does Romania's EV share compare to the EU average? | `ev_share_pct`, `eu_avg_ev_share_pct`, `vs_eu_avg_pp` |
# MAGIC | How many EVs are on Romanian roads (stock)? | `ev_stock` |
# MAGIC | How many new EVs were sold each year? | `ev_sales` |
# MAGIC | What is Romania's rank among EU countries? | `ev_share_rank` |
# MAGIC
# MAGIC ## Databricks concepts used
# MAGIC | Concept | Where |
# MAGIC |---|---|
# MAGIC | Join two Gold/Silver tables | Step 3 — Romania + EU average |
# MAGIC | Window rank function | Step 4 — country ranking |
# MAGIC | Subquery via temp view | Step 2 — EU average |
# MAGIC | `createOrReplaceTempView` | Expose a DataFrame as a SQL table name |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

GOLD_MARKET_SHARE = "gold.ev_market_share"
SILVER_IEA        = "silver.ev_registrations_iea"
GOLD_TABLE        = "gold.romania_ev_summary"

# EU member states (ISO codes) — used to compute EU average
EU_COUNTRIES = {
    "AT","BE","BG","HR","CY","CZ","DK","EE","FI","FR","DE","GR",
    "HU","IE","IT","LV","LT","LU","MT","NL","PL","PT","RO","SK",
    "SI","ES","SE"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Romania row from Gold market share table

# COMMAND ----------

df_ms = spark.table(GOLD_MARKET_SHARE)

df_ro = (
    df_ms
    .filter(F.col("country_code") == "RO")
    .select(
        "year",
        "electric_registrations",
        "total_registrations",
        "ev_market_share_pct",
        "ev_yoy_growth_pct",
        "ev_share_yoy_change_pp",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — EU average EV market share per year
# MAGIC
# MAGIC We register a DataFrame as a **temporary view** so we can query it
# MAGIC with Spark SQL in the next step. A temp view lives only for the duration
# MAGIC of the Spark session — it is not persisted to the catalog.

# COMMAND ----------

df_eu = (
    df_ms
    .filter(F.col("country_code").isin(EU_COUNTRIES))
    .filter(F.col("total_registrations") > 0)
    .groupBy("year")
    .agg(
        F.round(
            F.when(F.sum("total_registrations") > 0,
                   F.sum("electric_registrations") / F.sum("total_registrations") * 100),
            2
        ).alias("eu_avg_ev_share_pct"),
        F.countDistinct("country_code").alias("eu_country_count"),
    )
)

# Register as temp view so we can join it by name
df_eu.createOrReplaceTempView("eu_avg")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — IEA data: Romania EV sales and stock
# MAGIC
# MAGIC `silver.ev_registrations_iea` has two relevant `parameter` values:
# MAGIC - `EV sales` — new EV sales in that year
# MAGIC - `EV stock` — cumulative EVs on the road at year end
# MAGIC
# MAGIC We use another `pivot` to reshape parameter rows into columns,
# MAGIC then sum across BEV + PHEV powertrain types.

# COMMAND ----------

df_iea = spark.table(SILVER_IEA)

df_iea_ro = (
    df_iea
    .filter(
        (F.col("country_code") == "RO") &
        (F.col("parameter").isin(["EV sales", "EV stock"]))
    )
    .groupBy("country_code", "year", "parameter")
    .agg(F.sum("ev_count").alias("value"))   # sum BEV + PHEV
    .groupBy("country_code", "year")
    .pivot("parameter", ["EV sales", "EV stock"])
    .agg(F.first("value"))
    .withColumnRenamed("EV sales", "ev_sales_iea")
    .withColumnRenamed("EV stock", "ev_stock_iea")
    .drop("country_code")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Join Romania + EU average + IEA
# MAGIC
# MAGIC A standard DataFrame `join`. We use `left` join so Romania rows are
# MAGIC always kept even if a year is missing from one of the sources.

# COMMAND ----------

df_eu_spark = spark.table("eu_avg")

df_joined = (
    df_ro
    .join(df_eu_spark, on="year", how="left")
    .join(df_iea_ro,   on="year", how="left")
    .withColumn(
        "vs_eu_avg_pp",
        F.round(F.col("ev_market_share_pct") - F.col("eu_avg_ev_share_pct"), 2)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — Romania's rank among EU countries each year
# MAGIC
# MAGIC `Window.partitionBy("year")` groups all countries within the same year.
# MAGIC `rank()` assigns rank 1 to the highest EV share.
# MAGIC We then filter to keep only Romania's rank.

# COMMAND ----------

w_year = Window.partitionBy("year").orderBy(F.desc("ev_market_share_pct"))

df_rank = (
    df_ms
    .filter(F.col("country_code").isin(EU_COUNTRIES))
    .withColumn("ev_share_rank", F.rank().over(w_year))
    .filter(F.col("country_code") == "RO")
    .select("year", "ev_share_rank", F.lit(len(EU_COUNTRIES)).alias("eu_country_total"))
)

df_gold = (
    df_joined
    .join(df_rank, on="year", how="left")
    .select(
        "year",
        "electric_registrations",
        "total_registrations",
        "ev_market_share_pct",
        "eu_avg_ev_share_pct",
        "vs_eu_avg_pp",
        "ev_yoy_growth_pct",
        "ev_share_yoy_change_pp",
        "ev_sales_iea",
        "ev_stock_iea",
        "ev_share_rank",
        "eu_country_total",
        "eu_country_count",
    )
    .orderBy("year")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 — Write Gold table

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
# MAGIC ## 7 — Export to S3 for Streamlit (direct Parquet read, no SQL Warehouse needed)

# COMMAND ----------

S3_EXPORT = "s3://count-electric/gold/romania_ev_summary/"
df_gold.coalesce(1).write.mode("overwrite").parquet(S3_EXPORT)
print(f"Exported Parquet to {S3_EXPORT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE gold.romania_ev_summary ZORDER BY (year)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 — Validate

# COMMAND ----------

df = spark.table(GOLD_TABLE)
display(
    df.select(
        "year",
        "ev_market_share_pct",
        "eu_avg_ev_share_pct",
        "vs_eu_avg_pp",
        "ev_share_rank",
        "ev_yoy_growth_pct",
        "ev_sales_iea",
        "ev_stock_iea",
    )
    .orderBy("year")
)
