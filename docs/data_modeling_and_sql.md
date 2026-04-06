# Data Modeling and SQL in Count Electric

A walkthrough of the data modeling decisions behind the project — why the pipeline is structured the way it is, how each layer is designed, and what SQL techniques are used. Read this to understand the *why* behind the schema design, not just the column names.

---

## Data modeling across the project

The entire pipeline follows a **medallion architecture** — three progressive layers of data quality, each with a distinct purpose. Data flows in one direction only: Bronze → Silver → Gold. No layer reads from a later one.

```
┌─────────────────────────────────────────────────────────────┐
│  RAW SOURCES                                                │
│  IEA CSV · Eurostat ROAD_EQR_CARPDA · Eurostat ROAD_EQS     │
└──────────────────────────┬──────────────────────────────────┘
                           │ raw files land in S3
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  BRONZE  — raw, append-only, no transformations             │
│                                                             │
│  bronze.ev_iea_raw                    3 454 rows            │
│  bronze.car_registrations_eurostat_raw  6 257 rows          │
│  bronze.car_stock_eurostat_raw          ~ 5 000 rows        │
│                                                             │
│  Design: preserve source exactly + add metadata columns     │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  SILVER  — cleaned, typed, deduplicated, standardised       │
│                                                             │
│  silver.ev_registrations_iea                                │
│  silver.car_registrations_eurostat                          │
│  silver.car_stock_eurostat                                  │
│                                                             │
│  Design: one clean row per real-world observation           │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  GOLD  — aggregated, business-ready metrics                 │
│                                                             │
│  gold.ev_market_share      → dashboard chart data          │
│  gold.romania_ev_summary   → Romania deep-dive             │
│  gold.car_stock_snapshot   → fleet composition             │
│                                                             │
│  Design: one row per analytical question the dashboard asks │
└─────────────────────────────────────────────────────────────┘
```

---

## Table of Contents

1. [Why a medallion architecture](#1-why-a-medallion-architecture)
2. [Bronze — design decisions](#2-bronze--design-decisions)
3. [Silver — design decisions](#3-silver--design-decisions)
4. [Gold — design decisions](#4-gold--design-decisions)
5. [The three Gold table schemas explained](#5-the-three-gold-table-schemas-explained)
6. [Key modeling tradeoff — registrations vs fleet](#6-key-modeling-tradeoff--registrations-vs-fleet)
7. [SQL techniques used](#7-sql-techniques-used)
8. [How the fuel category mapping works](#8-how-the-fuel-category-mapping-works)

---

## 1. Why a Medallion Architecture

The medallion architecture is a pattern for organizing data pipelines into layers of increasing quality and specificity. It solves a real problem: raw API data is messy, inconsistently formatted, and changes shape over time. Business metrics (charts, KPIs) need clean, stable, aggregated data.

Without layers, you have two bad choices:
- Apply transformations during ingestion → fragile, hard to recover if something goes wrong
- Serve raw API data to the dashboard → slow, complex queries, dashboard breaks when the API changes

With three layers, each layer has one job:

| Layer | Job | Who reads it |
|---|---|---|
| Bronze | Land everything, touch nothing | Silver notebooks |
| Silver | Clean and standardise | Gold notebooks |
| Gold | Answer the business questions | Dashboard |

**Recovery is easy.** If a Silver transformation has a bug, fix it and re-run the Silver notebook — Bronze is untouched, the fix flows forward. If a Gold aggregation is wrong, re-run Gold — Silver is untouched. You never need to re-ingest from the API.

**Auditability.** Bronze preserves exactly what arrived. You can always trace a Gold metric back to the raw source value that produced it.

> **Why this matters:** Medallion architecture is the most common data lake pattern you'll encounter. Almost every data engineering role that uses Databricks, Delta Lake, or a data lakehouse platform uses some form of it. Understanding why the layers exist matters more than knowing the names.

---

## 2. Bronze — Design Decisions

**Purpose:** land raw data exactly as received. No transformations, no filtering, no type changes.

**Append-only.** Bronze tables only grow. New ingestion runs append rows — existing rows are never modified or deleted. This means Bronze is a complete history of everything that ever arrived from the API.

**Metadata columns added at ingestion time:**
```python
df = (
    df
    .withColumn("source_file",  F.lit(file_path))         # which S3 file this row came from
    .withColumn("ingested_at",  F.current_timestamp())    # when it was loaded into Bronze
    .withColumn("source_name",  F.lit("iea_global_ev_data"))  # which source system
)
```

These three columns are not in the source data — they are added by the Bronze notebook. They serve two purposes:
- **Traceability** — given any row in Bronze, you know exactly which file it came from and when
- **Deduplication** — Silver notebooks use `source_file` to check which files have already been processed

**Explicit schema.** Bronze notebooks define the schema upfront (see `databricks_and_pyspark.md` section 9) rather than letting Spark infer it. The Bronze table schema mirrors the source exactly:

```python
# bronze.ev_iea_raw schema — mirrors IEA CSV columns
StructType([
    StructField("region",     StringType(),  True),   # "Romania" — not yet ISO code
    StructField("category",   StringType(),  True),   # always "Historical"
    StructField("parameter",  StringType(),  True),   # "EV sales", "EV stock"
    StructField("mode",       StringType(),  True),   # "Cars", "Buses", "Trucks"
    StructField("powertrain", StringType(),  True),   # "BEV", "PHEV", "EV"
    StructField("year",       IntegerType(), True),
    StructField("unit",       StringType(),  True),   # "Vehicles" or "Percent"
    StructField("value",      DoubleType(),  True),
])
```

Note: `region` is a full country name ("Romania") at Bronze. It only becomes an ISO code ("RO") at Silver. This is intentional — Bronze preserves the source format.

**Incremental loading.** Bronze notebooks skip files that have already been loaded:

```python
already_ingested = set()
if spark.catalog.tableExists(BRONZE_TABLE):
    already_ingested = {
        row.source_file
        for row in spark.sql(f"SELECT DISTINCT source_file FROM {BRONZE_TABLE}").collect()
    }

new_files = [f for f in landing_files if f not in already_ingested]
```

This makes Bronze notebooks safe to re-run — they skip work that's already been done and only process genuinely new files.

> **Why this matters:** The "land raw, touch nothing" principle means Bronze is always recoverable. If your Silver logic has a bug, your Bronze data is still correct and complete. The source of truth for raw data is Bronze, not the API (APIs can change, go offline, or start returning different data).

---

## 3. Silver — Design Decisions

**Purpose:** produce one clean, deduplicated, correctly typed row for every real-world observation.

**What Silver fixes:**

| Problem | How it's fixed |
|---|---|
| Duplicate rows from multiple ingestion runs | `dropDuplicates` on natural key |
| Year stored as string ("2022") | Cast to integer |
| Country names instead of ISO codes (IEA) | `create_map` lookup |
| Raw fuel type codes (ELC, PET) with no labels | Add `fuel_type_label` and `fuel_category` columns |
| Aggregate geo rows (EU27, EEA) mixed with country rows | Filter out with `.filter(~F.col("geo").isin(...))` |
| Null values in the `value` column | `.filter(F.col("value").isNotNull())` |
| Buses, trucks, 2-wheelers mixed with cars (IEA) | `.filter(F.col("mode") == "Cars")` |
| Unnecessary metadata columns (source_file, ingested_at) | `.drop(...)` |

**Natural key** — the minimal set of columns that uniquely identifies one real-world data point:

```
IEA:       (region, parameter, mode, powertrain, year)
           → one row = one measurement type for one vehicle/powertrain in one country in one year

Eurostat:  (geo, mot_nrg, time)
           → one row = one fuel type count in one country in one year
```

If two rows share the same natural key, one of them is a duplicate from re-ingestion and can be safely dropped.

**Silver tables are overwritten, not appended.** Unlike Bronze (append-only), Silver is recomputed from scratch on every run:

```python
df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.ev_registrations_iea")
```

This is safe because Silver is fully derived from Bronze. If Bronze has the complete history, running Silver again produces the same result — it's idempotent. The `overwrite` mode avoids compounding deduplication issues from partial runs.

---

## 4. Gold — Design Decisions

**Purpose:** answer the specific questions the dashboard asks, nothing more.

Gold tables are not general-purpose. Each one is designed around a specific analytical use case:

- `gold.ev_market_share` → "what % of new cars were electric, by country and year?"
- `gold.romania_ev_summary` → "how does Romania compare to the EU average, and where does it rank?"
- `gold.car_stock_snapshot` → "how many cars are on the road in total, and what share are electric?"

**Pre-aggregated.** Gold computes and stores derived metrics (percentages, YoY growth, rankings) rather than leaving that work to the dashboard. This keeps the dashboard code simple — it reads a DataFrame and plots it, with no transformation logic.

**Gold is overwritten on every run.** Like Silver, Gold is fully derived from Silver. Re-running produces the same result.

**Gold is exported to S3 as Parquet** after being written as a Delta table:

```python
# Delta table — for Databricks queries, governance, OPTIMIZE
df_gold.write.format("delta").mode("overwrite").saveAsTable("gold.ev_market_share")

# Parquet export — for the dashboard to read directly, no SQL Warehouse needed
df_gold.coalesce(1).write.mode("overwrite").parquet("s3://count-electric/gold/ev_market_share/")
```

The Parquet copy is what the dashboard reads. This decouples the dashboard from Databricks entirely — as long as the pipeline has run at least once, the dashboard works with no Databricks connection.

---

## 5. The Three Gold Table Schemas Explained

### `gold.ev_market_share`

One row per country per year. Source: Eurostat new car registrations.

| Column | Type | What it means |
|---|---|---|
| `country_code` | string | ISO 3166-1 alpha-2 (e.g. "RO") |
| `year` | integer | Calendar year |
| `total_registrations` | double | All new cars registered (Eurostat TOTAL) |
| `electric_registrations` | double | BEV + PHEV registrations combined |
| `ice_registrations` | double | Petrol + diesel registrations |
| `hybrid_registrations` | double | Non-plug-in hybrid registrations |
| `other_registrations` | double | LPG, gas, hydrogen, other |
| `ev_market_share_pct` | double | `electric / total * 100` — rounded to 2dp |
| `ice_market_share_pct` | double | `ice / total * 100` |
| `ev_yoy_growth_pct` | double | YoY % change in electric registrations |
| `ev_share_yoy_change_pp` | double | YoY change in market share (percentage points) |

The distinction between `ev_yoy_growth_pct` and `ev_share_yoy_change_pp`:
- Growth % measures how much faster electric registrations are growing: Romania went from 2 000 to 3 500 BEVs → +75% growth
- Percentage point change measures the share shift: EV share went from 2.1% to 3.6% → +1.5pp change

Both are useful. Growth % tells you momentum; pp change tells you how fast the market is actually shifting.

### `gold.romania_ev_summary`

One row per year. Romania-specific deep-dive combining Eurostat + IEA data.

| Column | Type | What it means |
|---|---|---|
| `year` | integer | Calendar year |
| `electric_registrations` | double | New electric cars registered in Romania |
| `total_registrations` | double | All new cars registered in Romania |
| `ev_market_share_pct` | double | Romania's electric car share % |
| `eu_avg_ev_share_pct` | double | Weighted EU average across all 27 member states |
| `vs_eu_avg_pp` | double | Romania minus EU average (negative = below average) |
| `ev_yoy_growth_pct` | double | Romania YoY growth in electric registrations |
| `ev_share_yoy_change_pp` | double | Romania YoY change in market share (pp) |
| `ev_sales_iea` | double | New electric car sales from IEA (BEV + PHEV) |
| `ev_stock_iea` | double | Cumulative electric cars on the road (IEA) |
| `ev_share_rank` | double | Romania's rank among 27 EU countries (1 = highest share) |
| `eu_country_total` | integer | Total EU countries in the ranking (27) |
| `eu_country_count` | long | Countries with data available that year |

`vs_eu_avg_pp` being negative means Romania is below the EU average. The chart shows this gap narrowing (or widening) over time.

### `gold.car_stock_snapshot`

One row per country per year. Source: Eurostat total fleet on the road.

| Column | Type | What it means |
|---|---|---|
| `country_code` | string | ISO 3166-1 alpha-2 |
| `year` | integer | Calendar year |
| `total_stock` | double | All cars currently registered (not just sold this year) |
| `electric_stock` | double | Electric cars on the road |
| `combustion_stock` | double | Petrol + diesel cars on the road |
| `hybrid_stock` | double | Hybrid cars on the road |
| `other_stock` | double | Other fuel types |
| `electric_share_pct` | double | `electric_stock / total_stock * 100` |
| `combustion_share_pct` | double | `combustion_stock / total_stock * 100` |

---

## 6. Key Modeling Tradeoff — Registrations vs Fleet

This is the most important conceptual distinction in the project, and it's why there are two separate Gold tables for electric cars.

**`ev_market_share` — new car registrations (annual flow)**
Counts only cars sold in a given year. A good indicator of market momentum and consumer behaviour.

**`car_stock_snapshot` — total fleet on the road (stock)**
Counts all cars currently registered, accumulated over all years. A much slower-moving number.

The gap between them is striking:

```
Romania, 2023:
  Electric share of new car sales (flow):   ~8%
  Electric share of all cars on the road:   ~1.5%
```

Why the difference? The fleet is enormous and turns over slowly. Romania has ~6 million registered cars. Even if 10% of new sales are electric, adding ~30 000 electric cars to a fleet of 6 million moves the needle by less than 0.5%. It takes decades for the fleet composition to follow the sales mix.

**Modeling decision:** keep these as separate tables rather than combining them. They answer different questions, have different data sources (Eurostat ROAD_EQR for registrations, ROAD_EQS for stock), different cadences (registrations are reported faster), and different use cases in the dashboard.

> **Why this matters:** Confusing "share of new sales" with "share of all cars on the road" is one of the most common mistakes in public discourse about electric car adoption. Understanding the difference — and why both metrics matter — is a good example of how data modeling choices reflect a real understanding of the domain.

---

## 7. SQL Techniques Used

### CREATE SCHEMA IF NOT EXISTS

```sql
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Count Electric — raw ingested data, append-only. One table per source.';
```

Idempotent schema creation — the notebook can be re-run without failing on "schema already exists". The `COMMENT` is stored in Unity Catalog and visible in the Databricks UI.

### OPTIMIZE + ZORDER BY

```sql
OPTIMIZE gold.ev_market_share ZORDER BY (country_code, year)
OPTIMIZE gold.car_stock_snapshot ZORDER BY (country_code, year)
OPTIMIZE gold.romania_ev_summary ZORDER BY (year)
```

Two separate operations:
- `OPTIMIZE` — compacts many small Delta files into fewer large ones (faster reads, fewer S3 requests)
- `ZORDER BY` — physically co-locates rows with the same column values in the same files, enabling data skipping on filter queries

Applied after every Gold write because each pipeline run creates a new set of files.

### Spark SQL for validation

Throughout the notebooks, Spark SQL is used for quick validation checks alongside PySpark:

```python
# Count distinct source files in Bronze
spark.sql("SELECT DISTINCT source_file FROM bronze.ev_iea_raw").collect()

# Check year range
df.agg(F.min("year"), F.max("year")).collect()[0]

# Grouped counts for distribution checks
df.groupBy("fuel_category").count().orderBy("fuel_category").show()
```

### Mixed Python/SQL cells — DESCRIBE EXTENDED

During development, `DESCRIBE EXTENDED` is useful for inspecting a table's metadata, storage location, and schema in the Databricks notebook:

```sql
DESCRIBE EXTENDED gold.ev_market_share;
```

This shows the physical storage path (the managed Delta location in Databricks' S3 bucket), the table format, creation timestamp, and the full column schema with types and nullable flags.

### createOrReplaceTempView — session-scoped subquery

```python
df_eu.createOrReplaceTempView("eu_avg")
df_eu_spark = spark.table("eu_avg")
```

Gives a DataFrame a referenceable name within the Spark session without persisting it to the catalog. Used to make the EU average computation joinable by name in the Romania Gold notebook.

---

## 8. How the Fuel Category Mapping Works

Eurostat reports 15+ distinct fuel type codes. The dashboard only needs four categories: Electric, Combustion (petrol + diesel), Hybrid, Other. The mapping from raw codes to categories happens at Silver, not Gold.

**Raw Eurostat codes → fuel_category:**

| Raw code | Raw label | Assigned category |
|---|---|---|
| `ELC` | Electric (BEV) | **Electric** |
| `ELC_PET_PI` | Plug-in hybrid petrol | **Electric** |
| `ELC_DIE_PI` | Plug-in hybrid diesel | **Electric** |
| `ELC_PET_HYB` | Hybrid petrol (non-plug-in) | Hybrid |
| `ELC_DIE_HYB` | Hybrid diesel (non-plug-in) | Hybrid |
| `PET` | Petrol (ICE) | **Combustion** |
| `DIE` | Diesel (ICE) | **Combustion** |
| `LPG` | LPG | Other |
| `GAS` | Natural gas | Other |
| `HYD_FCELL` | Hydrogen fuel cell | Other |
| `TOTAL` | All types combined | Total |

**The design decision:** plug-in hybrids (`ELC_PET_PI`, `ELC_DIE_PI`) are counted in the **Electric** category. Non-plug-in hybrids (`ELC_PET_HYB`, `ELC_DIE_HYB`) are counted in **Hybrid**.

This is consistent with how the EU and IEA count electric vehicle adoption — plug-in hybrids are included because they can run on electricity and are eligible for EV incentives. Non-plug-in hybrids are conventional cars with a small battery assist; they cannot be externally charged.

The mapping uses `create_map` (see `databricks_and_pyspark.md` section 13) to apply this lookup efficiently across millions of rows without a join.
