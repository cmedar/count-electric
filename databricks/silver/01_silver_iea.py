# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — IEA Global EV Data
# MAGIC
# MAGIC **Source table:** `bronze.ev_iea_raw`
# MAGIC **Target table:** `silver.ev_registrations_iea`
# MAGIC
# MAGIC ## Transformations applied
# MAGIC | Step | What |
# MAGIC |---|---|
# MAGIC | Deduplicate | Natural key: (region, parameter, mode, powertrain, year) — removes duplicate ingestion runs |
# MAGIC | Filter mode | Keep Cars only — drop Buses, Trucks, 2-wheelers |
# MAGIC | Add country_code | Map IEA country names → ISO 3166-1 alpha-2 codes (e.g. "Romania" → "RO") |
# MAGIC | Drop internal cols | Remove category (always "Historical"), source_file, source_name |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

BRONZE_TABLE = "bronze.ev_iea_raw"
SILVER_TABLE = "silver.ev_registrations_iea"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Create Silver schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver
# MAGIC COMMENT 'Count Electric — cleaned and conformed data, ready for aggregation';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Country name → ISO code lookup
# MAGIC
# MAGIC IEA uses full country names. We standardise to ISO 3166-1 alpha-2
# MAGIC so both IEA and Eurostat tables share the same country key.

# COMMAND ----------

IEA_COUNTRY_CODES = {
    # Europe
    "Albania": "AL", "Austria": "AT", "Belgium": "BE", "Bulgaria": "BG",
    "Croatia": "HR", "Cyprus": "CY", "Czech Republic": "CZ", "Denmark": "DK",
    "Estonia": "EE", "Finland": "FI", "France": "FR", "Germany": "DE",
    "Greece": "GR", "Hungary": "HU", "Iceland": "IS", "Ireland": "IE",
    "Italy": "IT", "Latvia": "LV", "Lithuania": "LT", "Luxembourg": "LU",
    "Malta": "MT", "Netherlands": "NL", "Norway": "NO", "Poland": "PL",
    "Portugal": "PT", "Romania": "RO", "Slovakia": "SK", "Slovenia": "SI",
    "Spain": "ES", "Sweden": "SE", "Switzerland": "CH",
    "United Kingdom": "GB",
    # Americas
    "United States of America": "US", "Canada": "CA", "Brazil": "BR",
    "Chile": "CL", "Mexico": "MX",
    # Asia-Pacific
    "Australia": "AU", "China": "CN", "India": "IN", "Japan": "JP",
    "New Zealand": "NZ", "South Korea": "KR", "Thailand": "TH",
    # Other
    "Israel": "IL", "Morocco": "MA", "South Africa": "ZA",
    # Aggregates — keep as-is with a pseudo-code
    "World": "WORLD",
    "Europe": "EUR",
    "EU27": "EU27",
    "Rest of the world": "ROW",
    "Other Europe": "EUR_OTHER",
    "Other Asia Pacific": "APAC_OTHER",
}

mapping_expr = F.create_map([F.lit(x) for pair in IEA_COUNTRY_CODES.items() for x in pair])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Read Bronze, apply transformations

# COMMAND ----------

df_bronze = spark.table(BRONZE_TABLE)

df_silver = (
    df_bronze
    # Deduplicate on natural key — handles multiple ingestion runs of same file
    .dropDuplicates(["region", "parameter", "mode", "powertrain", "year"])
    # Cars only — project focuses on passenger vehicles
    .filter(F.col("mode") == "Cars")
    # Drop columns not needed downstream
    .drop("category", "source_file", "source_name", "ingested_at")
    # Add ISO country code
    .withColumn("country_code", mapping_expr[F.col("region")])
    # Rename for clarity
    .withColumnRenamed("region", "country_name")
    .withColumnRenamed("value", "ev_count")
    # Reorder columns
    .select(
        "country_code",
        "country_name",
        "year",
        "parameter",
        "powertrain",
        "ev_count",
        "unit",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Write Silver table

# COMMAND ----------

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)
print(f"Written {df_silver.count()} rows to {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — Validate

# COMMAND ----------

df = spark.table(SILVER_TABLE)
print(f"Total rows: {df.count()}")

# COMMAND ----------

# Countries with no ISO mapping (country_code is null)
unmapped = df.filter(F.col("country_code").isNull()).select("country_name").distinct()
if unmapped.count() > 0:
    print("WARNING — unmapped countries (add to IEA_COUNTRY_CODES):")
    unmapped.show(50, truncate=False)
else:
    print("All countries mapped to ISO codes.")

# COMMAND ----------

print("Romania EV sales — deduplicated and clean:")
display(
    df.filter(
        (F.col("country_code") == "RO") &
        (F.col("parameter")    == "EV sales")
    )
    .orderBy("year", "powertrain")
)
