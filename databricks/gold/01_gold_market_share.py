# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — EV Market Share & YoY Growth
# MAGIC
# MAGIC **Source table:** `silver.car_registrations_eurostat`
# MAGIC **Target table:** `gold.ev_market_share`
# MAGIC
# MAGIC ## What this table answers
# MAGIC | Question | Column |
# MAGIC |---|---|
# MAGIC | What % of new cars registered in country X in year Y were electric? | `ev_market_share_pct` |
# MAGIC | How fast are EV registrations growing year-over-year? | `ev_yoy_growth_pct` |
# MAGIC | Is the EV share accelerating or decelerating? | `ev_share_yoy_change_pp` (percentage points) |
# MAGIC | How many ICE cars are being displaced? | `ice_registrations`, `ice_market_share_pct` |
# MAGIC
# MAGIC ## Transformations
# MAGIC | Step | Spark concept used |
# MAGIC |---|---|
# MAGIC | Aggregate by category | `groupBy` + `agg(sum)` |
# MAGIC | Reshape categories into columns | `pivot` |
# MAGIC | Market share % | Column arithmetic |
# MAGIC | Prior-year value for YoY | `Window.partitionBy().orderBy()` + `lag()` |
# MAGIC | Write Gold Delta table | `saveAsTable` with `overwrite` |
# MAGIC | Optimize for queries | `OPTIMIZE` + `ZORDER BY` |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_TABLE = "silver.car_registrations_eurostat"
GOLD_TABLE   = "gold.ev_market_share"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Create Gold schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold
# MAGIC COMMENT 'Count Electric — aggregated metrics ready for the dashboard';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Aggregate registrations by country, year, fuel category
# MAGIC
# MAGIC We `groupBy` + `agg(sum)` to collapse individual fuel type rows
# MAGIC into one row per (country, year, category).

# COMMAND ----------

df_silver = spark.table(SILVER_TABLE)

# Sum registrations per country / year / fuel_category
# Exclude "Other" and "Total" from the category-level aggregation —
# we'll use the Eurostat TOTAL code directly so the denominator is exact.
df_cat = (
    df_silver
    .groupBy("country_code", "year", "fuel_category")
    .agg(F.sum("new_registrations").alias("registrations"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Pivot: fuel categories become columns
# MAGIC
# MAGIC `pivot` turns row values into column names — one row per (country, year)
# MAGIC instead of one row per (country, year, category).
# MAGIC
# MAGIC Before pivot:
# MAGIC ```
# MAGIC RO | 2022 | Electric |  3 500
# MAGIC RO | 2022 | ICE      | 85 000
# MAGIC RO | 2022 | Total    | 95 000
# MAGIC ```
# MAGIC After pivot:
# MAGIC ```
# MAGIC RO | 2022 | 3 500 | 85 000 | ... | 95 000
# MAGIC ```

# COMMAND ----------

df_pivot = (
    df_cat
    .groupBy("country_code", "year")
    .pivot("fuel_category", ["Electric", "ICE", "Hybrid", "Other", "Total"])
    .agg(F.first("registrations"))   # one value per cell after groupBy
    .withColumnRenamed("Electric", "electric_registrations")
    .withColumnRenamed("ICE",      "ice_registrations")
    .withColumnRenamed("Hybrid",   "hybrid_registrations")
    .withColumnRenamed("Other",    "other_registrations")
    .withColumnRenamed("Total",    "total_registrations")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Calculate market share percentages
# MAGIC
# MAGIC Simple column arithmetic on the pivoted DataFrame.
# MAGIC `F.round(..., 2)` keeps two decimal places.

# COMMAND ----------

df_share = (
    df_pivot
    .withColumn(
        "ev_market_share_pct",
        F.round(
            F.when(F.col("total_registrations") > 0,
                   F.col("electric_registrations") / F.col("total_registrations") * 100),
            2
        )
    )
    .withColumn(
        "ice_market_share_pct",
        F.round(
            F.when(F.col("total_registrations") > 0,
                   F.col("ice_registrations") / F.col("total_registrations") * 100),
            2
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — Year-over-year growth using Window functions
# MAGIC
# MAGIC A **Window function** computes a value for each row by looking at a
# MAGIC *window* of surrounding rows — without collapsing the DataFrame like `groupBy` does.
# MAGIC
# MAGIC `Window.partitionBy("country_code")` — each country is its own partition
# MAGIC (rows are not compared across countries).
# MAGIC
# MAGIC `.orderBy("year")` — within a partition, rows are sorted by year.
# MAGIC
# MAGIC `lag("electric_registrations", 1)` — for each row, look back 1 row
# MAGIC (the previous year) and return its value.
# MAGIC
# MAGIC ```
# MAGIC RO 2021  electric=2000   lag=null  (no prior year)
# MAGIC RO 2022  electric=3500   lag=2000  → growth = (3500-2000)/2000*100 = +75%
# MAGIC RO 2023  electric=5100   lag=3500  → growth = (5100-3500)/3500*100 = +45.7%
# MAGIC ```

# COMMAND ----------

w_country = Window.partitionBy("country_code").orderBy("year")

df_yoy = (
    df_share
    # Prior-year electric registrations
    .withColumn("electric_prev_year", F.lag("electric_registrations", 1).over(w_country))
    # YoY growth % in EV registrations
    .withColumn(
        "ev_yoy_growth_pct",
        F.round(
            F.when(F.col("electric_prev_year") > 0,
                   (F.col("electric_registrations") - F.col("electric_prev_year"))
                   / F.col("electric_prev_year") * 100),
            2
        )
    )
    # Prior-year EV market share
    .withColumn("ev_share_prev_year", F.lag("ev_market_share_pct", 1).over(w_country))
    # Change in market share in percentage points (pp)
    .withColumn(
        "ev_share_yoy_change_pp",
        F.round(F.col("ev_market_share_pct") - F.col("ev_share_prev_year"), 2)
    )
    .drop("electric_prev_year", "ev_share_prev_year")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 — Final column order and write to Gold

# COMMAND ----------

df_gold = df_yoy.select(
    "country_code",
    "year",
    "total_registrations",
    "electric_registrations",
    "ice_registrations",
    "hybrid_registrations",
    "other_registrations",
    "ev_market_share_pct",
    "ice_market_share_pct",
    "ev_yoy_growth_pct",
    "ev_share_yoy_change_pp",
)

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

S3_EXPORT = "s3://count-electric/gold/ev_market_share/"
df_gold.coalesce(1).write.mode("overwrite").parquet(S3_EXPORT)
print(f"Exported Parquet to {S3_EXPORT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 — Optimize the Delta table
# MAGIC
# MAGIC `OPTIMIZE` compacts many small Parquet files written during ingestion
# MAGIC into fewer, larger files — faster reads.
# MAGIC
# MAGIC `ZORDER BY (country_code, year)` co-locates rows with the same
# MAGIC country_code and year in the same files. When the dashboard filters
# MAGIC `WHERE country_code = 'RO'`, Databricks can skip most files entirely
# MAGIC (this is called **data skipping**).

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE gold.ev_market_share ZORDER BY (country_code, year)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 — Validate

# COMMAND ----------

df = spark.table(GOLD_TABLE)
print(f"Total rows   : {df.count()}")
print(f"Countries    : {df.select('country_code').distinct().count()}")
print(f"Year range   : {df.agg(F.min('year'), F.max('year')).collect()[0]}")

# COMMAND ----------

print("Romania — EV market share trend:")
display(
    df.filter(F.col("country_code") == "RO")
    .select("year", "electric_registrations", "total_registrations",
            "ev_market_share_pct", "ev_yoy_growth_pct", "ev_share_yoy_change_pp")
    .orderBy("year")
)

# COMMAND ----------

print("Top 10 countries by EV market share in latest year:")
latest_year = df.agg(F.max("year")).collect()[0][0]
display(
    df.filter(F.col("year") == latest_year)
    .filter(F.col("total_registrations") > 1000)   # exclude micro-markets
    .orderBy(F.desc("ev_market_share_pct"))
    .limit(10)
    .select("country_code", "year", "electric_registrations",
            "total_registrations", "ev_market_share_pct")
)
