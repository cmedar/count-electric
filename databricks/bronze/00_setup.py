# Databricks notebook source
# MAGIC %md
# MAGIC # Count Electric — Databricks Setup (Serverless)
# MAGIC
# MAGIC Run this notebook **once** to:
# MAGIC 1. Verify the S3 External Location is working
# MAGIC 2. Create the Bronze schema in Unity Catalog
# MAGIC
# MAGIC ## Prerequisites (done via the Databricks UI before running this notebook)
# MAGIC
# MAGIC 1. **Storage Credential** — Catalog → External Data → Storage Credentials
# MAGIC    - Name: `count-electric-s3`
# MAGIC    - Type: AWS access keys
# MAGIC
# MAGIC 2. **External Location** — Catalog → External Data → External Locations
# MAGIC    - Name: `count_electric_s3`
# MAGIC    - URL: `s3://count-electric`
# MAGIC    - Credential: `count-electric-s3`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Verify S3 External Location

# COMMAND ----------

# List the root of the S3 bucket via the external location
# If this fails, the storage credential or external location is misconfigured
files = dbutils.fs.ls("s3://count-electric/")
print(f"S3 connection OK — {len(files)} item(s) at bucket root:")
for f in files:
    print(f"  {f.path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Create the Bronze Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze
# MAGIC COMMENT 'Count Electric — raw ingested data, append-only. One table per source.';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Verify Landing Zone Contents

# COMMAND ----------

# Check what raw files have landed from ingestion scripts
import os

prefixes = [
    "s3://count-electric/landing/raw/iea/",
    "s3://count-electric/landing/raw/eurostat/",
]

for prefix in prefixes:
    try:
        files = dbutils.fs.ls(prefix)
        print(f"\n{prefix}")
        for f in files:
            size_kb = round(f.size / 1024, 1)
            print(f"  {f.name}  ({size_kb} KB)")
    except Exception as e:
        print(f"\n{prefix}  — empty or not found ({e})")
