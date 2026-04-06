# Databricks and PySpark in Count Electric

A detailed walkthrough of every Databricks platform concept and PySpark transformation used in the project — with the real code from the notebooks and an explanation of why each approach was chosen.

---

## Databricks and PySpark across the project

All data transformation happens inside Databricks notebooks, organised in three layers. Each layer is a separate Python file run as a Databricks notebook.

```
┌──────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  (databricks/bronze/)                              │
│                                                                  │
│  00_setup.py             → create schemas, verify S3 location   │
│  01_bronze_iea.py        → S3 CSV → bronze.ev_iea_raw           │
│  02_bronze_eurostat.py   → S3 JSON → bronze.car_registrations_  │
│                                       eurostat_raw              │
│  03_bronze_eurostat_stock.py → S3 JSON → bronze.car_stock_      │
│                                           eurostat_raw          │
│                                                                  │
│  PySpark used: spark.read.csv, spark.createDataFrame,           │
│                withColumn, write.saveAsTable (append)           │
└──────────────────────────┬───────────────────────────────────────┘
                           │ Delta tables
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│  SILVER LAYER  (databricks/silver/)                              │
│                                                                  │
│  01_silver_iea.py        → bronze → silver.ev_registrations_iea │
│  02_silver_eurostat.py   → bronze → silver.car_registrations_   │
│                                      eurostat                   │
│  03_silver_eurostat_stock.py → bronze → silver.car_stock_       │
│                                          eurostat               │
│                                                                  │
│  PySpark used: dropDuplicates, filter, withColumn, cast,        │
│                create_map, withColumnRenamed, select            │
└──────────────────────────┬───────────────────────────────────────┘
                           │ Delta tables
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│  GOLD LAYER  (databricks/gold/)                                  │
│                                                                  │
│  01_gold_market_share.py → silver → gold.ev_market_share        │
│  02_gold_romania.py      → gold + silver → gold.romania_        │
│                                             ev_summary          │
│  03_gold_stock_snapshot.py → silver → gold.car_stock_snapshot   │
│                                                                  │
│  PySpark used: groupBy + agg, pivot, Window (lag + rank),       │
│                F.when, join, createOrReplaceTempView,           │
│                coalesce().write.parquet, OPTIMIZE + ZORDER BY   │
└──────────────────────────────────────────────────────────────────┘
```

---

## Table of Contents

**Databricks Platform**
1. [Serverless compute](#1-serverless-compute)
2. [Unity Catalog — three-level namespace](#2-unity-catalog--three-level-namespace)
3. [External Location and Storage Credential](#3-external-location-and-storage-credential)
4. [Delta Lake — ACID writes](#4-delta-lake--acid-writes)
5. [dbutils — the Databricks utility belt](#5-dbutils--the-databricks-utility-belt)
6. [Mixed Python and SQL cells](#6-mixed-python-and-sql-cells)
7. [Git folders and notebook deployment](#7-git-folders-and-notebook-deployment)

**PySpark**
8. [Lazy evaluation — how Spark actually runs](#8-lazy-evaluation--how-spark-actually-runs)
9. [Defining schemas explicitly](#9-defining-schemas-explicitly)
10. [Reading data into a DataFrame](#10-reading-data-into-a-dataframe)
11. [Core transformations — filter, select, withColumn, drop](#11-core-transformations--filter-select-withcolumn-drop)
12. [dropDuplicates — idempotent Bronze tables](#12-dropduplicates--idempotent-bronze-tables)
13. [create_map — in-memory lookup without a join](#13-create_map--in-memory-lookup-without-a-join)
14. [groupBy + agg — collapsing rows](#14-groupby--agg--collapsing-rows)
15. [pivot — reshaping rows into columns](#15-pivot--reshaping-rows-into-columns)
16. [Window functions — lag and rank](#16-window-functions--lag-and-rank)
17. [F.when — null-safe arithmetic](#17-fwhen--null-safe-arithmetic)
18. [join — combining DataFrames](#18-join--combining-dataframes)
19. [createOrReplaceTempView — session-scoped SQL tables](#19-createorreplacetempview--session-scoped-sql-tables)
20. [Writing data — Delta tables and Parquet export](#20-writing-data--delta-tables-and-parquet-export)
21. [OPTIMIZE and ZORDER BY](#21-optimize-and-zorder-by)

---

## 1. Serverless Compute

Databricks serverless means there is no cluster to provision, configure, or shut down. When a notebook runs, compute spins up automatically and terminates when the run finishes. You pay only for execution time, not idle time.

This is the compute model used for all notebooks in this project. The alternative — a classic cluster — requires manually choosing instance types, number of workers, and spark configurations. For a learning or portfolio project, serverless removes all of that overhead.

> **Why this matters:** Understanding the difference between serverless notebooks and classic clusters is fundamental to Databricks. Serverless is great for scheduled pipelines and notebooks. Classic clusters give more control for long-running jobs or very large workloads.

---

## 2. Unity Catalog — Three-Level Namespace

Unity Catalog is Databricks' data governance layer. It organises all data assets in a three-level hierarchy:

```
catalog.schema.table

Examples from this project:
  bronze.ev_iea_raw
  silver.car_registrations_eurostat
  gold.ev_market_share
```

- **Catalog** — the top level (defaults to the workspace catalog)
- **Schema** — equivalent to a database; groups related tables
- **Table** — the actual data asset

Schemas are created once per layer:

```sql
-- From 00_setup.py
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Count Electric — raw ingested data, append-only. One table per source.';

-- From 01_silver_iea.py
CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Count Electric — cleaned and conformed data, ready for aggregation';

-- From 01_gold_market_share.py
CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Count Electric — aggregated metrics ready for the dashboard';
```

`IF NOT EXISTS` makes these safe to re-run — no error if the schema is already there.

> **Why this matters:** Before Unity Catalog, each Databricks workspace had its own isolated Hive metastore. Unity Catalog is a centralised catalog that can govern data across multiple workspaces, with fine-grained access controls (row-level, column-level). Understanding the three-level namespace is essential for any Databricks role.

---

## 3. External Location and Storage Credential

Databricks serverless cannot use IAM instance profiles to access S3. Instead, it uses Unity Catalog's governance layer:

1. **Storage Credential** — an AWS IAM role registered in Unity Catalog. It holds the trust relationship between Databricks and your AWS account. Created once in the Databricks UI under *Catalog → External Data → Storage Credentials*.

2. **External Location** — maps an S3 path (`s3://count-electric`) to a Storage Credential. Once registered, any notebook can read/write to that path without any AWS keys in the code.

```python
# From 00_setup.py — verifying the external location works
files = dbutils.fs.ls("s3://count-electric/")
print(f"S3 connection OK — {len(files)} items at bucket root")
```

If `dbutils.fs.ls` returns results, the external location is correctly configured. If it raises an exception, something is wrong with the Storage Credential or IAM trust policy.

The cross-account trust policy on the IAM role looks like:
```json
{
  "Principal": {
    "AWS": "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-..."
  },
  "Action": "sts:AssumeRole"
}
```

This gives Databricks' AWS account permission to assume your IAM role when accessing your S3 bucket.

> **Why this matters:** This is the standard way to connect Databricks to S3 in a production setup. The key concept is that credentials never appear in notebook code — governance happens at the platform level through Unity Catalog.

---

## 4. Delta Lake — ACID Writes

All tables in the project are stored as Delta format, not plain Parquet. Delta adds a transaction log on top of Parquet files that makes every write **atomic** — if a notebook fails mid-write, the table is not left in a corrupted or partial state.

```python
df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.ev_registrations_iea")
```

Key options:

- **`format("delta")`** — store as Delta, not plain Parquet or CSV
- **`mode("overwrite")`** — replace the entire table on each run (appropriate for Silver and Gold where we recompute from scratch)
- **`mode("append")`** — add new rows without touching existing data (used in Bronze, which is append-only)
- **`option("overwriteSchema", "true")`** — allows the table schema to change between runs without manually dropping and recreating the table; useful during development when column names or types evolve

> **Why this matters:** ACID guarantees are what separate a data lakehouse (Delta Lake) from a plain data lake (raw Parquet). Without them, a failed pipeline run can leave tables in inconsistent states that are hard to detect and fix. Delta's transaction log also enables time travel — you can query a table as it looked at any previous point in time.

---

## 5. dbutils — the Databricks Utility Belt

`dbutils` is a Databricks-specific object available in every notebook. It is not a Python library you install — it is injected by the Databricks runtime.

**`dbutils.fs` — filesystem operations:**
```python
# List files at an S3 path (via External Location)
files = dbutils.fs.ls("s3://count-electric/landing/raw/iea/")
# Returns a list of FileInfo objects with .path, .name, .size

for f in files:
    print(f.path, f.size)
```

**`dbutils.notebook.exit` — stop a notebook early:**
```python
if not new_files:
    print("No new files to ingest.")
    dbutils.notebook.exit("No new files.")   # clean exit, not an error
```

Used in Bronze notebooks when no new files have landed since the last run. This is cleaner than raising an exception — the notebook reports success with a message, rather than failing.

**`dbutils.secrets` — read secrets without exposing them:**
```python
access_key = dbutils.secrets.get(scope="count-electric", key="aws-access-key-id")
secret_key = dbutils.secrets.get(scope="count-electric", key="aws-secret-access-key")
```

Secrets are stored in a Databricks secret scope (backed by Databricks or Azure Key Vault). When printed, Databricks replaces the value with `[REDACTED]` — the actual value never appears in notebook output or logs.

> **Why this matters:** `dbutils` is one of the first things you encounter in Databricks notebooks. It is not PySpark — it's a Databricks-specific convenience layer that handles the platform-level operations (filesystem access, notebook control flow, secrets) that Spark itself doesn't cover.

---

## 6. Mixed Python and SQL Cells

Databricks notebooks support Python and SQL cells in the same file. A `# MAGIC %sql` comment switches a cell to SQL mode. Both cell types share the same Spark session — a table created in SQL is immediately queryable in Python, and vice versa.

```python
# Python cell — this runs Python
df_silver.write.saveAsTable("silver.car_registrations_eurostat")

# MAGIC %sql
# SQL cell — this runs Spark SQL
OPTIMIZE gold.ev_market_share ZORDER BY (country_code, year)
```

In the `.py` source files in this repo, `# MAGIC %sql` is a comment that tells the Databricks notebook runtime to interpret the next lines as SQL. When Databricks renders the file as a notebook, it creates a separate SQL cell.

```python
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze
# MAGIC COMMENT 'Count Electric — raw ingested data, append-only.';
```

> **Why this matters:** Being comfortable switching between Python and SQL within the same notebook is a key Databricks skill. Some operations (like `OPTIMIZE`, `DESCRIBE EXTENDED`, schema creation) are more natural in SQL. Data transformations are usually cleaner in PySpark. Knowing when to use each is part of working efficiently in Databricks.

---

## 7. Git Folders and Notebook Deployment

Notebooks in this project are stored in a **Databricks Git Folder** — a link between a Databricks workspace folder and a GitHub repository. Any changes pushed to GitHub can be pulled into Databricks automatically.

In the GitHub Actions deploy workflow, after pushing code, the Databricks Repos API is called to sync the workspace folder:

```bash
curl -X PATCH \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -d '{"branch": "main"}' \
  "$DATABRICKS_HOST/api/2.0/repos/$DATABRICKS_REPO_ID"
```

This means: edit a notebook locally → push to GitHub → GitHub Actions runs → Databricks pulls the latest commit automatically. No manual notebook uploads.

> **Why this matters:** Git folders solve a real pain point in Databricks — without them, notebooks exist only in the workspace and have no proper version control. Git folders bring notebooks into the same software development workflow as the rest of the codebase.

---

## 8. Lazy Evaluation — How Spark Actually Runs

This is the most important concept to understand before reading any PySpark code.

Spark does **not** execute transformations when you write them. It builds a plan — a Directed Acyclic Graph (DAG) of all the operations — and only executes when an **action** is called.

```python
# NONE of these lines run anything:
df_cat   = df_silver.groupBy("country_code", "year", "fuel_category").agg(...)
df_pivot = df_cat.groupBy("country_code", "year").pivot(...).agg(...)
df_share = df_pivot.withColumn("ev_market_share_pct", ...)
df_yoy   = df_share.withColumn("ev_yoy_growth_pct", ...)

# THIS line triggers execution of the entire plan above:
df_yoy.write.format("delta").saveAsTable("gold.ev_market_share")
```

**Transformations** (lazy — build the plan): `filter`, `withColumn`, `groupBy`, `pivot`, `join`, `select`, `drop`, `withColumnRenamed`, `dropDuplicates`

**Actions** (eager — trigger execution): `write`, `count`, `show`, `collect`, `display`

This is why errors like division by zero or type mismatches surface at the `write` step, even if the problematic `withColumn` was defined 10 lines earlier.

> **Why this matters:** Every Spark performance optimization depends on understanding laziness. Spark can reorder, merge, and push down operations in the plan before executing. Calling `.count()` mid-pipeline to debug forces a full execution pass — fine for debugging, but expensive in production. A `.show()` on a filtered DataFrame is cheap; a `.count()` on the full dataset before filtering is not.

---

## 9. Defining Schemas Explicitly

In Bronze notebooks, the schema is defined upfront rather than letting Spark infer it:

```python
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

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
```

The second argument in each `StructField` is the type, the third (`True`/`False`) is whether nulls are allowed.

Why not use `inferSchema=True`? Schema inference reads the file twice (once to guess types, once to load) and can make wrong choices — reading a column with values like `"2022"` as `LongType` when you need `IntegerType`, or inferring a numeric column as `StringType` because of one bad row. Explicit schemas are faster, predictable, and fail loudly if the source format changes.

```python
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")   # never infer — we define it above
    .schema(iea_schema)
    .csv(file_path)
)
```

> **Why this matters:** Explicit schemas are a production best practice. They make the notebook self-documenting (the schema is visible code, not inferred at runtime), and they catch upstream format changes at ingestion time rather than silently producing wrong data.

---

## 10. Reading Data into a DataFrame

**Reading a CSV from S3:**
```python
df = (
    spark.read
    .option("header", "true")
    .schema(iea_schema)
    .csv("s3://count-electric/landing/raw/iea/iea_ev_sales_20240415_083012.csv")
)
```

**Reading a Delta table from the catalog:**
```python
df_silver = spark.table("silver.car_registrations_eurostat")
```

`spark.table("catalog.schema.table")` is the idiomatic way to load a managed Delta table. It doesn't read the data immediately (lazy) — it just builds a reference.

**Creating a DataFrame from Python objects:**
```python
from pyspark.sql import Row

spark_rows = [
    Row(freq="A", mot_nrg="ELC", geo="RO", time="2022", value=3500.0,
        source_file="s3://...", source_name="eurostat_road_eqr_carpda")
    for r in parsed_rows
]

df = spark.createDataFrame(spark_rows, schema=bronze_schema)
```

Used in Bronze Eurostat notebooks where data arrives as a Python list (from JSON-stat2 parsing) and needs to be converted into a Spark DataFrame before writing to Delta.

**Checking if a table exists before reading:**
```python
if spark.catalog.tableExists("bronze.ev_iea_raw"):
    already_ingested = {
        row.source_file
        for row in spark.sql("SELECT DISTINCT source_file FROM bronze.ev_iea_raw").collect()
    }
```

`spark.catalog.tableExists` is used to make Bronze notebooks safe to run on first setup — when no table exists yet, the code skips the "what's already loaded" check and creates the table fresh.

---

## 11. Core Transformations — filter, select, withColumn, drop

These are the building blocks used throughout Silver and Gold notebooks.

**`filter` — keep rows matching a condition:**
```python
# Keep only Cars (not Buses, Trucks, etc.)
df.filter(F.col("mode") == "Cars")

# Keep only EU countries, exclude aggregate geo codes
df.filter(~F.col("geo").isin({"EU27_2020", "EU28", "EEA", "EA19", "EA20"}))

# Multiple conditions
df.filter(
    (F.col("country_code") == "RO") &
    (F.col("parameter").isin(["EV sales", "EV stock"]))
)
```

**`withColumn` — add or replace a column:**
```python
# Cast year from string to integer
df.withColumn("year", F.col("time").cast("integer"))

# Add ingestion timestamp
df.withColumn("ingested_at", F.current_timestamp())

# Add a constant value column
df.withColumn("source_name", F.lit("iea_global_ev_data"))
```

**`select` — choose and reorder columns:**
```python
df.select(
    "country_code",
    "year",
    "parameter",
    "powertrain",
    "ev_count",
    "unit",
)
```

`select` both picks columns and defines their order in the output table. Any column not listed is dropped.

**`drop` — remove specific columns:**
```python
df.drop("category", "source_file", "source_name", "ingested_at")
```

**`withColumnRenamed` — rename a column:**
```python
df.withColumnRenamed("region", "country_name")
  .withColumnRenamed("value",  "ev_count")
  .withColumnRenamed("geo",    "country_code")
```

These are chained — each call returns a new DataFrame with one column renamed.

---

## 12. dropDuplicates — Idempotent Bronze Tables

Bronze tables are append-only. If the same ingestion script runs twice (e.g. after a failure), the same file gets ingested twice, creating duplicate rows. Silver notebooks handle this with `dropDuplicates` on the natural key.

```python
# Silver IEA — natural key is the combination of these columns
df_silver = (
    df_bronze
    .dropDuplicates(["region", "parameter", "mode", "powertrain", "year"])
    ...
)

# Silver Eurostat — natural key
df_silver = (
    df_bronze
    .dropDuplicates(["geo", "mot_nrg", "time"])
    ...
)
```

The natural key is the minimal set of columns that uniquely identifies a row in the business domain. For IEA data: a country + metric type + vehicle type + powertrain + year uniquely identifies one measurement.

> **Why this matters:** Deduplication at Silver means duplicates from Bronze never reach Gold. This is the correct separation of concerns: Bronze accepts everything (resilient ingestion), Silver cleans it (data quality). Gold then computes on clean data.

---

## 13. create_map — In-Memory Lookup Without a Join

`create_map` builds a Spark map column from a Python dictionary. It's used when you need to translate codes to labels or country names to ISO codes — a lookup operation that doesn't require a separate table.

**Country name → ISO code (Silver IEA):**
```python
IEA_COUNTRY_CODES = {
    "Romania": "RO", "Germany": "DE", "France": "FR",
    # ... 50+ countries
}

# Build a Spark map from the dict
mapping_expr = F.create_map([F.lit(x) for pair in IEA_COUNTRY_CODES.items() for x in pair])

# Apply it as a column lookup
df.withColumn("country_code", mapping_expr[F.col("region")])
```

The list comprehension `[F.lit(x) for pair in IEA_COUNTRY_CODES.items() for x in pair]` flattens `{"RO": "Romania", ...}` into `[F.lit("Romania"), F.lit("RO"), F.lit("Germany"), F.lit("DE"), ...]` — alternating key/value literals, which is exactly what `create_map` expects.

**Fuel type code → readable label (Silver Eurostat):**
```python
FUEL_LABELS    = {"ELC": "Electric (BEV)", "PET": "Petrol (ICE)", ...}
FUEL_CATEGORIES = {"ELC": "Electric", "ELC_PET_PI": "Electric", "PET": "ICE", ...}

label_expr    = F.create_map([F.lit(x) for pair in FUEL_LABELS.items()    for x in pair])
category_expr = F.create_map([F.lit(x) for pair in FUEL_CATEGORIES.items() for x in pair])

df.withColumn("fuel_type_label", label_expr[F.col("mot_nrg")])
  .withColumn("fuel_category",   category_expr[F.col("mot_nrg")])
```

> **Why this matters:** `create_map` is more efficient than a join for small, static lookup tables. The map is broadcast to all workers and applied row-by-row without shuffling data across the network. It also keeps the mapping in code (version-controlled) rather than in a separate reference table.

---

## 14. groupBy + agg — Collapsing Rows

`groupBy` groups rows that share the same values in the specified columns. `agg` then applies aggregate functions to each group, collapsing many rows into one.

```python
# From 01_gold_market_share.py
# Before: one row per (country, year, fuel_type_code) — e.g. RO / 2022 / ELC, RO / 2022 / ELC_PET_PI
# After:  one row per (country, year, fuel_category) — e.g. RO / 2022 / Electric

df_cat = (
    df_silver
    .groupBy("country_code", "year", "fuel_category")
    .agg(F.sum("new_registrations").alias("registrations"))
)
```

Common aggregate functions used in this project:

| Function | Use |
|---|---|
| `F.sum("col")` | Total registrations or stock across fuel types |
| `F.first("col")` | Take the single value in a pivoted cell (one value per group after pivot) |
| `F.min("col")`, `F.max("col")` | Year range for validation |
| `F.count("*")` | Row counts for validation |
| `F.countDistinct("col")` | Count unique countries |
| `F.round(expr, 2)` | Round to 2 decimal places |

---

## 15. pivot — Reshaping Rows into Columns

`pivot` is one of the most powerful DataFrame operations. It takes a column whose distinct values become new column names — transforming a "tall" DataFrame into a "wide" one.

**The transformation:**
```
Before pivot (tall):
  RO | 2022 | Electric  |  3 500
  RO | 2022 | ICE        | 85 000
  RO | 2022 | Hybrid     |  5 000
  RO | 2022 | Total      | 95 000

After pivot (wide):
  RO | 2022 | 3 500 | 85 000 | 5 000 | ... | 95 000
              ^Electric ^ICE   ^Hybrid      ^Total
```

**The code:**
```python
# From 01_gold_market_share.py
df_pivot = (
    df_cat
    .groupBy("country_code", "year")            # these become the row key
    .pivot("fuel_category",                      # this column's values become column names
           ["Electric", "ICE", "Hybrid", "Other", "Total"])  # explicit list = faster + predictable
    .agg(F.first("registrations"))              # aggregate within each cell
    .withColumnRenamed("Electric", "electric_registrations")
    .withColumnRenamed("ICE",      "ice_registrations")
    # ...
)
```

Specifying the pivot values explicitly (`["Electric", "ICE", ...]`) is a best practice — without it, Spark scans the entire column to discover distinct values, which is an extra pass through the data.

Used twice in `02_gold_romania.py` to reshape IEA parameter rows:
```python
# Before: one row per (country, year, parameter) where parameter is "EV sales" or "EV stock"
# After:  one row per (country, year) with ev_sales and ev_stock as columns

df_iea_ro = (
    df_iea
    .filter(F.col("parameter").isin(["EV sales", "EV stock"]))
    .groupBy("country_code", "year", "parameter")
    .agg(F.sum("ev_count").alias("value"))         # sum BEV + PHEV
    .groupBy("country_code", "year")
    .pivot("parameter", ["EV sales", "EV stock"])
    .agg(F.first("value"))
    .withColumnRenamed("EV sales", "ev_sales_iea")
    .withColumnRenamed("EV stock", "ev_stock_iea")
)
```

> **Why this matters:** `pivot` comes up often in data engineering — whenever a categorical column needs to become separate numeric columns. The mental model: `groupBy` defines the rows, `pivot` defines the new columns, `agg` defines the values.

---

## 16. Window Functions — lag and rank

Window functions compute a value for each row by looking at a *window* of surrounding rows — without collapsing the DataFrame the way `groupBy` does. Each row keeps its own identity; it just gains a new column with a value derived from nearby rows.

A window is defined by:
- `partitionBy` — which rows are in scope (the "window frame")
- `orderBy` — how rows within a partition are sorted

### lag — look back N rows (Year-over-Year growth)

```python
from pyspark.sql.window import Window

# Each country is its own partition; rows ordered by year within each partition
w_country = Window.partitionBy("country_code").orderBy("year")

df_yoy = (
    df_share
    # For each row, get the electric_registrations value from 1 year ago
    .withColumn("electric_prev_year", F.lag("electric_registrations", 1).over(w_country))
    # YoY growth = (current - previous) / previous * 100
    .withColumn(
        "ev_yoy_growth_pct",
        F.round(
            F.when(F.col("electric_prev_year") > 0,
                   (F.col("electric_registrations") - F.col("electric_prev_year"))
                   / F.col("electric_prev_year") * 100),
            2
        )
    )
    .drop("electric_prev_year")   # intermediate column, not needed in output
)
```

What `lag(..., 1)` produces row by row:
```
RO | 2010 | electric=200    | lag=null    (no prior year → growth is null)
RO | 2011 | electric=250    | lag=200     → growth = +25%
RO | 2022 | electric=3500   | lag=2000    → growth = +75%
RO | 2023 | electric=5100   | lag=3500    → growth = +45.7%
```

### rank — position within a group (Romania's EU rank)

```python
# Each year is its own partition; rows ordered by EV share descending
w_year = Window.partitionBy("year").orderBy(F.desc("ev_market_share_pct"))

df_rank = (
    df_ms
    .filter(F.col("country_code").isin(EU_COUNTRIES))  # EU only
    .withColumn("ev_share_rank", F.rank().over(w_year)) # rank 1 = highest EV share
    .filter(F.col("country_code") == "RO")              # keep only Romania's row
    .select("year", "ev_share_rank",
            F.lit(len(EU_COUNTRIES)).alias("eu_country_total"))
)
```

`rank()` assigns 1 to the highest EV share country in each year. After the window is applied, filtering to Romania gives Romania's position among all EU countries for every year.

> **Why this matters:** Window functions are one of the most commonly tested Spark concepts. The key distinction: `groupBy` collapses rows (you lose individual row identity), Window functions keep every row and add a computed column. Use `groupBy` when you want aggregates; use Window when you want each row to know something about its neighbours.

---

## 17. F.when — Null-Safe Arithmetic

`F.when` is the Spark equivalent of SQL `CASE WHEN`. It evaluates a condition and returns different values for the true and false branches.

The critical use in this project: guarding against division by zero in market share calculations. Databricks runs in ANSI SQL mode by default, which raises `SQLSTATE: 22012` (division by zero error) rather than silently returning `null` or `Infinity`.

```python
# Without the guard — raises SQLSTATE: 22012 if total_registrations = 0
df.withColumn("ev_market_share_pct",
    F.col("electric_registrations") / F.col("total_registrations") * 100
)

# With the guard — returns null when denominator is 0
df.withColumn(
    "ev_market_share_pct",
    F.round(
        F.when(F.col("total_registrations") > 0,
               F.col("electric_registrations") / F.col("total_registrations") * 100),
        2
    )
)
```

When `F.when` has no `.otherwise(...)` clause, it returns `null` for rows where the condition is false. This is intentional — a null market share is more honest than a `0` or an error.

> **Why this matters:** Division by zero is a silent bug in many pipelines. It surfaces at the action (write) step due to lazy evaluation, which can make it look like the error is in the write logic. The real fix belongs in the transformation — guard any division with `F.when` whenever the denominator could be zero.

---

## 18. join — Combining DataFrames

Used in `02_gold_romania.py` to combine three data sources into one Gold table.

```python
# Left join: Romania rows are always kept, even if a year is missing from the other source
df_joined = (
    df_ro                                           # Romania EV share (Eurostat-based)
    .join(df_eu_spark, on="year", how="left")       # + EU average
    .join(df_iea_ro,   on="year", how="left")       # + IEA sales & stock
)
```

`left` join means: keep all rows from the left DataFrame (`df_ro`), match with the right where possible, fill with nulls where no match exists. This is the right choice here — if IEA data for a year is missing, we still want Romania's Eurostat-based row.

```python
# Add a derived column after join
.withColumn(
    "vs_eu_avg_pp",
    F.round(F.col("ev_market_share_pct") - F.col("eu_avg_ev_share_pct"), 2)
)
```

`vs_eu_avg_pp` (percentage points vs EU average) can only be computed after the join brings both values into the same row.

> **Why this matters:** Joins in Spark are expensive operations — they require shuffling data across workers so matching keys end up on the same machine. For this project's dataset size it doesn't matter, but understanding join types (inner, left, right, outer) and when each is appropriate is a fundamental skill.

---

## 19. createOrReplaceTempView — Session-Scoped SQL Tables

Registers a DataFrame under a name so it can be referenced in subsequent Spark SQL queries within the same session. It is not persisted to the catalog — it lives only for the duration of the notebook run.

```python
# Register the EU average DataFrame as a temp view
df_eu.createOrReplaceTempView("eu_avg")

# Now queryable by name (used in the join in the next step)
df_eu_spark = spark.table("eu_avg")
df_joined = df_ro.join(df_eu_spark, on="year", how="left")
```

`createOrReplaceTempView` is useful when you want to give an intermediate DataFrame a name you can refer to later — either in another Python cell or in a `%sql` cell — without writing it to the catalog.

`OrReplace` means re-running the notebook doesn't fail with "view already exists".

---

## 20. Writing Data — Delta Tables and Parquet Export

Each Gold notebook writes its output in two formats: once as a managed Delta table (for Databricks), and once as Parquet to S3 (for the dashboard to read directly).

**Delta table write:**
```python
(
    df_gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.ev_market_share")
)
```

`saveAsTable` registers the table in the Unity Catalog with the full three-level name. Subsequent `spark.table("gold.ev_market_share")` calls will find it.

**Parquet export to S3:**
```python
S3_EXPORT = "s3://count-electric/gold/ev_market_share/"
df_gold.coalesce(1).write.mode("overwrite").parquet(S3_EXPORT)
```

`coalesce(1)` reduces the DataFrame to a single partition before writing. Without it, Databricks writes one Parquet file per partition — potentially dozens of small files. The Streamlit app reads the entire folder with `pd.read_parquet("s3://...")`, which handles multiple files, but a single file is simpler and avoids the overhead of many small files.

**Write modes:**
| Mode | Behaviour | Used for |
|---|---|---|
| `overwrite` | Replace entire table/folder | Silver, Gold — recomputed fresh each run |
| `append` | Add new rows, keep existing | Bronze — accumulates new ingestion files |

> **Why this matters:** The dual-write pattern (Delta for governance + Parquet for serving) is a real architectural decision. Delta tables stay inside Databricks-managed storage; the Parquet export lands in your own S3 bucket where it's visible, portable, and readable by any tool that understands Parquet — no Databricks connection needed.

---

## 21. OPTIMIZE and ZORDER BY

After writing a Gold table, it is optimized for query performance.

```sql
OPTIMIZE gold.ev_market_share ZORDER BY (country_code, year)
```

**`OPTIMIZE`** — Delta Lake writes many small Parquet files during incremental writes. `OPTIMIZE` compacts them into fewer, larger files. Fewer files = fewer S3 GET requests = faster queries.

**`ZORDER BY`** — physically sorts and co-locates rows with the same column values in the same files. When a query filters `WHERE country_code = 'RO'`, Databricks reads the transaction log, identifies which files contain Romanian data, and **skips the rest** without reading them. This is called **data skipping**.

```sql
-- After ZORDER BY (country_code, year):
-- Query: WHERE country_code = 'RO' AND year = 2022
-- Databricks reads: 1-2 files
-- Without ZORDER: Databricks might read every file
```

The `%sql` magic cell after each Gold write:
```python
# MAGIC %sql
# MAGIC OPTIMIZE gold.ev_market_share ZORDER BY (country_code, year)

# MAGIC %sql
# MAGIC OPTIMIZE gold.car_stock_snapshot ZORDER BY (country_code, year)

# MAGIC %sql
# MAGIC OPTIMIZE gold.romania_ev_summary ZORDER BY (year)
```

Romania summary is zodered by `year` only (it's already a single-country table, so `country_code` adds no value).

> **Why this matters:** `OPTIMIZE` + `ZORDER` is the standard way to tune Delta table read performance in Databricks. It's especially relevant for dashboard queries that filter by a small number of countries — exactly the usage pattern in this project.
