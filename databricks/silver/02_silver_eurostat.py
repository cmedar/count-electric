# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Eurostat Car Registrations
# MAGIC
# MAGIC **Source table:** `bronze.car_registrations_eurostat_raw`
# MAGIC **Target table:** `silver.car_registrations_eurostat`
# MAGIC
# MAGIC ## Transformations applied
# MAGIC | Step | What |
# MAGIC |---|---|
# MAGIC | Cast year | `time` string → `year` integer |
# MAGIC | Add labels | `fuel_type_label` — human readable fuel type name |
# MAGIC | Add category | `fuel_category` — groups fuel types into Electric / ICE / Hybrid / Total |
# MAGIC | Filter geo | Keep country-level rows only — drop EU/EEA aggregates |
# MAGIC | Deduplicate | Natural key: (geo, mot_nrg, time) |
# MAGIC | Drop nulls | Remove rows where value is null |
# MAGIC
# MAGIC ## fuel_category — the key column for Gold aggregations
# MAGIC | fuel_category | Includes |
# MAGIC |---|---|
# MAGIC | Electric | ELC (BEV), ELC_PET_PI, ELC_DIE_PI (PHEVs) |
# MAGIC | ICE | PET (petrol), DIE (diesel) |
# MAGIC | Hybrid | ELC_PET_HYB, ELC_DIE_HYB (non-plug-in hybrids) |
# MAGIC | Other | LPG, GAS, HYD_FCELL, BIFUEL, BIOETH, BIODIE, ALT, OTH |
# MAGIC | Total | TOTAL |

# COMMAND ----------

from pyspark.sql import functions as F

BRONZE_TABLE = "bronze.car_registrations_eurostat_raw"
SILVER_TABLE = "silver.car_registrations_eurostat"

# Eurostat geo codes that are aggregates, not individual countries
# We drop these — Gold layer will re-aggregate as needed
GEO_AGGREGATES = {
    "EU27_2020",  # EU 27 member states
    "EU28",       # EU including UK
    "EEA",        # European Economic Area
    "EA19",       # Euro area
    "EA20",       # Euro area (updated)
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Fuel type label and category mappings

# COMMAND ----------

FUEL_LABELS = {
    "ELC":         "Electric (BEV)",
    "ELC_PET_PI":  "Plug-in hybrid petrol (PHEV)",
    "ELC_DIE_PI":  "Plug-in hybrid diesel (PHEV)",
    "ELC_PET_HYB": "Hybrid petrol (non-plug-in)",
    "ELC_DIE_HYB": "Hybrid diesel (non-plug-in)",
    "PET":         "Petrol (ICE)",
    "DIE":         "Diesel (ICE)",
    "LPG":         "LPG",
    "GAS":         "Natural gas",
    "HYD_FCELL":   "Hydrogen fuel cell",
    "ALT":         "All alternative fuels",
    "BIFUEL":      "Bifuel",
    "BIOETH":      "Bioethanol",
    "BIODIE":      "Biodiesel",
    "OTH":         "Other",
    "TOTAL":       "Total",
}

FUEL_CATEGORIES = {
    "ELC":         "Electric",
    "ELC_PET_PI":  "Electric",
    "ELC_DIE_PI":  "Electric",
    "ELC_PET_HYB": "Hybrid",
    "ELC_DIE_HYB": "Hybrid",
    "PET":         "ICE",
    "DIE":         "ICE",
    "LPG":         "Other",
    "GAS":         "Other",
    "HYD_FCELL":   "Other",
    "ALT":         "Other",
    "BIFUEL":      "Other",
    "BIOETH":      "Other",
    "BIODIE":      "Other",
    "OTH":         "Other",
    "TOTAL":       "Total",
}

label_expr    = F.create_map([F.lit(x) for pair in FUEL_LABELS.items()    for x in pair])
category_expr = F.create_map([F.lit(x) for pair in FUEL_CATEGORIES.items() for x in pair])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Read Bronze, apply transformations

# COMMAND ----------

df_bronze = spark.table(BRONZE_TABLE)

df_silver = (
    df_bronze
    # Drop aggregate geo rows
    .filter(~F.col("geo").isin(GEO_AGGREGATES))
    # Drop null values
    .filter(F.col("value").isNotNull())
    # Deduplicate on natural key
    .dropDuplicates(["geo", "mot_nrg", "time"])
    # Cast year
    .withColumn("year", F.col("time").cast("integer"))
    # Add readable labels and category grouping
    .withColumn("fuel_type_label", label_expr[F.col("mot_nrg")])
    .withColumn("fuel_category",   category_expr[F.col("mot_nrg")])
    # Rename for clarity
    .withColumnRenamed("geo",   "country_code")
    .withColumnRenamed("mot_nrg", "fuel_type_code")
    .withColumnRenamed("value", "new_registrations")
    # Reorder columns
    .select(
        "country_code",
        "year",
        "fuel_type_code",
        "fuel_type_label",
        "fuel_category",
        "new_registrations",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Write Silver table

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
# MAGIC ## 4 — Validate

# COMMAND ----------

df = spark.table(SILVER_TABLE)
print(f"Total rows: {df.count()}")
print(f"Countries:  {df.select('country_code').distinct().count()}")
print(f"Year range: {df.agg(F.min('year'), F.max('year')).collect()[0]}")

# COMMAND ----------

print("Rows by fuel_category:")
df.groupBy("fuel_category").count().orderBy("fuel_category").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — Romania: EV vs ICE trend (the core story)

# COMMAND ----------

print("Romania — new registrations by fuel category and year:")
display(
    df.filter(F.col("country_code") == "RO")
    .groupBy("year", "fuel_category")
    .agg(F.sum("new_registrations").alias("total_registrations"))
    .orderBy("year", "fuel_category")
)
