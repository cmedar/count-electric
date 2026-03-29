### Count Electric
# Counting electric cars on the streets, so you don't have to

You've probably noticed more EVs on the street lately. But are they actually taking over, or does it just feel that way? **Count Electric** is a data engineering project that answers exactly that — tracking global EV adoption trends across countries and years, so the numbers can tell the story.

Raw data is ingested from public APIs into AWS S3, processed through a **medallion architecture on Databricks**, and surfaced via an interactive **Streamlit dashboard**.

> Built to demonstrate end-to-end data engineering skills: pipeline design, Delta Lake, Spark transformations, Window functions, data governance with Unity Catalog, and CI/CD automation.

**Live:** [app.countelectric.com](https://app.countelectric.com)

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Medallion Layer Design](#medallion-layer-design)
- [Databricks Concepts Covered](#databricks-concepts-covered)
- [Tech Stack](#tech-stack)
- [Data Sources](#data-sources)
- [Project Phases & Roadmap](#project-phases--roadmap)
- [Setup & Installation](#setup--installation)
- [Repository Structure](#repository-structure)

---

## Project Overview

**Count Electric** answers the following analytical questions:

- Which countries are leading EV adoption in Europe, and how is that changing year-on-year?
- As EV registrations grow, are petrol and diesel (ICE) registrations actually declining — and how fast?
- Where does Romania sit in the European EV adoption picture, and how does it compare to the EU average?
- What is Romania's EV market share rank among EU countries, and is it improving?

> **Romania focus:** All dashboards include a Romania lens. Country-level data is available from 2010 onwards via IEA and Eurostat. Bucharest city-level data is not currently published as open data by DRPCIV (Romania's vehicle registry).

---

## Architecture

```
┌──────────────────────────────────────────────────┐
│                  DATA SOURCES                    │
│   IEA Global EV Data    │   Eurostat ROAD_EQR    │
└────────────┬─────────────────────┬───────────────┘
             │                     │
             ▼                     ▼
┌──────────────────────────────────────────────────┐
│         INGESTION (EC2 + Docker)                 │
│   ingest_iea.py          ingest_eurostat.py      │
│   Python + requests + boto3                      │
│   MD5 dedup — skips upload if data unchanged     │
└─────────────────────────┬────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────┐
│                   AWS S3                         │
│     s3://count-electric/landing/raw/             │
│     (raw CSV + JSON files, append-only)          │
└─────────────────────────┬────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────┐
│       DATABRICKS (serverless + Unity Catalog)    │
│                                                  │
│  ┌──────────┐   ┌──────────┐   ┌─────────────┐  │
│  │  BRONZE  │ → │  SILVER  │ → │    GOLD     │  │
│  │  Delta   │   │  Delta   │   │    Delta    │  │
│  └──────────┘   └──────────┘   └─────────────┘  │
│                                                  │
│  Unity Catalog  │  Delta Lake  │  Window fns     │
└─────────────────────────┬────────────────────────┘
                          │ SQL Warehouse (HTTP)
                          ▼
┌──────────────────────────────────────────────────┐
│            STREAMLIT DASHBOARD (EC2)             │
│   app + cloudflared — Docker Compose            │
│   MD3 design, Bootstrap-style tab navigation     │
└─────────────────────────┬────────────────────────┘
                          │ Cloudflare Tunnel
                          ▼
┌──────────────────────────────────────────────────┐
│         app.countelectric.com (HTTPS)            │
│   Cloudflare edge — no open ports on EC2         │
└──────────────────────────────────────────────────┘
```

**Deployment:** GitHub Actions builds Docker images and runs `docker compose up` on EC2 on every push to `main`. The compose stack has two services: `app` (Streamlit) and `cloudflared` (Cloudflare Tunnel). In the same workflow, it syncs the Databricks Git folder via the Repos API so notebooks are always up to date.

---

## Medallion Layer Design

### Bronze — Raw Ingestion
- **Purpose:** Land raw data exactly as received, no transformations applied
- **Format:** Delta tables (converted from raw CSV/JSON on S3)
- **Schema:** Source fields preserved + `ingested_at` timestamp + `source_file` name
- **Key tables:**
  - `bronze.ev_iea_raw` — IEA global EV sales & stock, 3 454 rows, 2010–2024
  - `bronze.car_registrations_eurostat_raw` — Eurostat new car registrations by fuel type, 6 257 rows

### Silver — Cleaned & Conformed
- **Purpose:** Typed, deduplicated, standardised data ready for aggregation
- **Transformations applied:**

| Notebook | Transformation | Detail |
|---|---|---|
| `01_silver_iea.py` | Deduplicate | Natural key: `(region, parameter, mode, powertrain, year)` |
| `01_silver_iea.py` | Filter mode | Cars only — drops Buses, Trucks, 2-wheelers |
| `01_silver_iea.py` | Country code mapping | IEA country names → ISO 3166-1 alpha-2 via `create_map` |
| `02_silver_eurostat.py` | Cast year | `time` string → `year` integer |
| `02_silver_eurostat.py` | Fuel category | Groups `ELC/ELC_PET_PI/ELC_DIE_PI` → `Electric`, `PET/DIE` → `ICE`, etc. |
| `02_silver_eurostat.py` | Drop aggregates | Removes EU27/EEA rows — Gold will re-aggregate as needed |

- **Key tables:**
  - `silver.ev_registrations_iea` — clean IEA data, columns: `country_code, country_name, year, parameter, powertrain, ev_count, unit`
  - `silver.car_registrations_eurostat` — clean Eurostat data, columns: `country_code, year, fuel_type_code, fuel_type_label, fuel_category, new_registrations`

### Gold — Aggregated Insights
- **Purpose:** Business-ready metrics for the dashboard
- **Key tables:**
  - `gold.ev_market_share` — EV market share % and YoY growth per country/year (from Eurostat)
  - `gold.romania_ev_summary` — Romania deep-dive: EV share vs EU average, EU rank, IEA stock data joined

**`gold.ev_market_share` schema:**

| Column | Description |
|---|---|
| `country_code` | ISO 3166-1 alpha-2 |
| `year` | Calendar year |
| `total_registrations` | Total new car registrations (Eurostat TOTAL) |
| `electric_registrations` | Sum of BEV + PHEV registrations |
| `ice_registrations` | Sum of petrol + diesel registrations |
| `ev_market_share_pct` | `electric / total * 100` |
| `ev_yoy_growth_pct` | YoY % change in electric registrations |
| `ev_share_yoy_change_pp` | YoY change in market share (percentage points) |

**`gold.romania_ev_summary` schema:**

| Column | Description |
|---|---|
| `ev_market_share_pct` | Romania EV share % |
| `eu_avg_ev_share_pct` | EU-wide weighted average EV share % |
| `vs_eu_avg_pp` | Romania minus EU average (positive = above average) |
| `ev_yoy_growth_pct` | Romania EV registrations YoY growth |
| `ev_sales_iea` | New EV sales from IEA (BEV + PHEV combined) |
| `ev_stock_iea` | Cumulative EVs on the road (IEA) |
| `ev_share_rank` | Romania's rank among 27 EU countries by EV share |

---

## Databricks Concepts Covered

This section documents every Databricks/Spark concept used in the project, with the exact notebook and real code context. Useful for interviews and for circling back when a concept needs refreshing.

---

### 1. Serverless Compute
No cluster to provision. Databricks spins up compute on demand when a notebook runs and terminates it after. You pay only for execution time, not idle time.

> Used throughout — all notebooks run on Databricks serverless, no cluster configuration required.

---

### 2. Unity Catalog — Three-Level Namespace
Databricks organises data as `catalog.schema.table`. In this project:
- **Catalog:** the default workspace catalog
- **Schemas:** `bronze`, `silver`, `gold`
- **Tables:** `bronze.ev_iea_raw`, `silver.car_registrations_eurostat`, `gold.ev_market_share`, etc.

```sql
CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Count Electric — aggregated metrics ready for the dashboard';
```

---

### 3. External Location + Storage Credential
Databricks serverless cannot use IAM instance profiles directly. Instead:
1. An AWS IAM role is created with a **cross-account trust policy** pointing to Databricks' master role (`arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-...`)
2. That role is registered as a **Storage Credential** in Unity Catalog
3. An **External Location** maps `s3://count-electric` to that credential

This means notebooks read/write S3 without any AWS keys in code — access is governed by Unity Catalog.

---

### 4. Delta Lake — ACID writes
All tables are stored as Delta (not plain Parquet). Every write is atomic — if a notebook fails mid-write, the table is not corrupted.

```python
df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")   # allow schema changes between runs
    .saveAsTable("silver.ev_registrations_iea")
```

`overwriteSchema` lets the schema evolve between notebook runs without manually dropping and recreating the table.

---

### 5. Databricks Git Folder (Repos API)
Notebooks are stored in a **Git folder** in the Databricks Workspace, linked to this GitHub repo. On every push to `main`, the GitHub Actions workflow calls the Databricks Repos API to pull the latest commit:

```bash
curl -X PATCH \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -d '{"branch": "main"}' \
  "$DATABRICKS_HOST/api/2.0/repos/$DATABRICKS_REPO_ID"
```

No manual notebook uploads — code changes deploy automatically.

---

### 6. Lazy Evaluation — Transformations vs Actions
Spark does not execute transformations (`withColumn`, `groupBy`, `filter`, `join`) when they are defined. It builds a **DAG** and executes the entire chain only when an **action** is called (`write`, `count`, `show`, `collect`).

```python
df_cat   = df_silver.groupBy(...).agg(...)   # nothing runs
df_pivot = df_cat.groupBy(...).pivot(...)    # nothing runs
df_share = df_pivot.withColumn(...)          # nothing runs

df_share.write.saveAsTable(...)              # ← ACTION: full DAG executes here
```

This is why errors (e.g. division by zero) surface at the write step, not where the transformation was written.

---

### 7. `groupBy` + `agg`
Collapses many rows into one per group, applying aggregate functions.

```python
# Sum all fuel type registrations per country and year
df_cat = (
    df_silver
    .groupBy("country_code", "year", "fuel_category")
    .agg(F.sum("new_registrations").alias("registrations"))
)
```

> Used in `01_gold_market_share.py` to aggregate Eurostat fuel type rows into category-level totals before pivoting.

---

### 8. `pivot` — Reshape Rows into Columns
Turns distinct values in one column into separate columns. Converts a tall/narrow DataFrame into a wide one.

```python
# Before: one row per (country, year, fuel_category)
# After:  one row per (country, year) with Electric/ICE/Hybrid/Total as columns
df_pivot = (
    df_cat
    .groupBy("country_code", "year")
    .pivot("fuel_category", ["Electric", "ICE", "Hybrid", "Other", "Total"])
    .agg(F.first("registrations"))
)
```

> Used in `01_gold_market_share.py` and `02_gold_romania.py` (also pivots IEA `parameter` values into `ev_sales` / `ev_stock` columns).

---

### 9. Window Functions — `lag` and `rank`
Window functions compute a value for each row by looking at a *window* of surrounding rows, without collapsing the DataFrame like `groupBy` does.

**`lag` — look back N rows (previous year value):**
```python
w_country = Window.partitionBy("country_code").orderBy("year")

df_yoy = df_share.withColumn(
    "electric_prev_year",
    F.lag("electric_registrations", 1).over(w_country)
)
# RO 2021: electric=2000, lag=null  (no prior year)
# RO 2022: electric=3500, lag=2000  → YoY growth = +75%
# RO 2023: electric=5100, lag=3500  → YoY growth = +45.7%
```

**`rank` — rank each row within a partition:**
```python
w_year = Window.partitionBy("year").orderBy(F.desc("ev_market_share_pct"))

df_rank = df_ms.withColumn("ev_share_rank", F.rank().over(w_year))
# Ranks all EU countries by EV share within each year
# Romania's rank tells us: are we 15th? 20th? Is it improving?
```

> Used in `01_gold_market_share.py` (lag for YoY) and `02_gold_romania.py` (rank for Romania EU position).

---

### 10. `create_map` — In-Memory Lookup Table
Builds a Spark map column from a Python dictionary, used for efficient row-level lookups without a join.

```python
IEA_COUNTRY_CODES = {"Romania": "RO", "Germany": "DE", ...}
mapping_expr = F.create_map([F.lit(x) for pair in IEA_COUNTRY_CODES.items() for x in pair])

df = df.withColumn("country_code", mapping_expr[F.col("region")])
```

> Used in `01_silver_iea.py` to map IEA country names to ISO codes. Faster than a join for small lookup tables.

---

### 11. `createOrReplaceTempView` — Session-Scoped SQL Tables
Registers a DataFrame under a name so it can be referenced by subsequent Spark SQL or DataFrame joins. Lives only for the duration of the Spark session — nothing is written to disk or the catalog.

```python
df_eu.createOrReplaceTempView("eu_avg")
# Now joinable by name in the same session:
df_joined = df_ro.join(spark.table("eu_avg"), on="year", how="left")
```

> Used in `02_gold_romania.py` to make the EU average DataFrame joinable by name.

---

### 12. `F.when` — Null-Safe Conditional Expressions
The Spark equivalent of SQL `CASE WHEN`. Used here to guard against division by zero — returns `NULL` instead of raising an error when the denominator is 0.

```python
.withColumn(
    "ev_market_share_pct",
    F.round(
        F.when(F.col("total_registrations") > 0,
               F.col("electric_registrations") / F.col("total_registrations") * 100),
        2
    )
)
```

> Used in both Gold notebooks. Without the guard, Databricks raises `SQLSTATE: 22012` (division by zero) at write time due to ANSI SQL mode being enabled by default.

---

### 13. `OPTIMIZE` + `ZORDER BY` — Delta Performance
Delta Lake writes many small Parquet files during incremental ingestion. `OPTIMIZE` compacts them into fewer, larger files for faster reads.

`ZORDER BY` physically co-locates rows with the same column values in the same files. When a query filters `WHERE country_code = 'RO'`, Databricks reads only the files that contain Romanian data and skips the rest (**data skipping**).

```sql
OPTIMIZE gold.ev_market_share ZORDER BY (country_code, year)
```

> Applied to both Gold tables after writing.

---

### 14. Spark SQL `MAGIC %sql` cells
Databricks notebooks support mixed Python and SQL cells. `# MAGIC %sql` switches a cell to SQL mode:

```sql
-- Create schema if not already present
CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Count Electric — cleaned and conformed data';
```

Python and SQL cells share the same Spark session — a table created in a `%sql` cell is immediately readable with `spark.table(...)` in a Python cell.

---

### 15. Databricks SQL Warehouse — External Query Endpoint

A **SQL Warehouse** is a dedicated query server that external applications connect to over HTTP. It is separate from the serverless compute used by notebooks.

Think of the Gold Delta tables as a database sitting on disk (S3). The data is there, but nothing is listening for incoming connections from the outside world. A SQL Warehouse is what listens — it accepts SQL queries from external clients, runs them against Delta tables, and returns results.

**Why not use a regular cluster?**

| | Notebook / Job Cluster | SQL Warehouse |
|---|---|---|
| Purpose | ETL, ML, data engineering | Analytics queries from external apps |
| Lifecycle | Start → run notebook → shut down | Always-on (auto-suspends when idle) |
| External access | Not directly connectable | Exposes HTTP endpoint for JDBC/ODBC |
| Latency | Startup time on each run | Sub-second for cached/small queries |

**How it connects to Streamlit:**

```python
from databricks import sql

conn = sql.connect(
    server_hostname="your-workspace.cloud.databricks.com",  # DATABRICKS_HOST
    http_path="/sql/1.0/warehouses/abc123def456",           # DATABRICKS_HTTP_PATH
    access_token="dapiXXXXXXXX",                            # DATABRICKS_TOKEN
)
```

The `http_path` is the address of your specific SQL Warehouse. Find it in Databricks → **SQL Warehouses** → your warehouse → **Connection Details → HTTP Path**.

Results are cached in Streamlit for 1 hour via `@st.cache_data(ttl=3600)` — so the warehouse only wakes up on the first visit (or after the cache expires), then auto-suspends again.

> Used in `streamlit/app.py` — `load_romania_summary()` and `load_top10_ev_share()` both dial into the SQL Warehouse to query `gold.romania_ev_summary` and `gold.ev_market_share`.

---

### 16. Control Plane vs Data Plane
- **Control Plane** — Databricks' infrastructure (UI, job scheduler, cluster manager). Runs in Databricks' AWS account.
- **Data Plane** — where compute actually runs and where data lives. Runs in **your** AWS account. Your S3 data never leaves your account.

The cross-account IAM trust policy set up in Phase 1 is the bridge: it gives the Databricks control plane permission to spin up compute resources in the customer data plane.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python 3.11, `requests`, `boto3` |
| Storage | AWS S3 |
| Compute | AWS EC2 t2.micro + Docker Compose |
| Processing | Databricks serverless, Apache Spark |
| Table format | Delta Lake |
| Governance | Unity Catalog + External Location (IAM cross-account role) |
| Dashboard | Streamlit (MD3 theme, Bootstrap-style tab navigation) |
| Networking | Cloudflare Tunnel (`cloudflared`) — HTTPS, no open ports on EC2 |
| CI/CD | GitHub Actions — EC2 deploy + Databricks Git sync on push to `main` |

---

## Data Sources

| Source | Data | Format | Status |
|---|---|---|---|
| [IEA Global EV Data Explorer](https://www.iea.org/data-and-statistics/data-tools/global-ev-data-explorer) | EV sales & stock by country/year/powertrain (BEV/PHEV). Global incl. Romania. 2010–2024. | CSV API | ✅ Live |
| [Eurostat ROAD_EQR_CARPDA](https://ec.europa.eu/eurostat/databrowser/view/ROAD_EQR_CARPDA/) | New car registrations by fuel type — petrol, diesel, BEV, PHEV, hybrid for all EU countries. Core dataset for EV vs ICE comparison. | JSON-stat2 API | ✅ Live |
| [EAFO](https://alternative-fuels-observatory.ec.europa.eu/) | Romania EV fleet detail | API | Planned |

> **Note on Bucharest city-level data:** DRPCIV (Romania's national vehicle registry) does not publish open data. Country-level Romania data is available from IEA and Eurostat.

---

## Project Phases & Roadmap

### Phase 1 — Foundation ✅
- [x] AWS S3 bucket and folder structure
- [x] AWS EC2 (t2.micro) + Docker container running Streamlit on port 8501
- [x] GitHub Actions pipeline — builds Docker image, deploys to EC2 via SSH
- [x] Databricks free tier — serverless compute, Unity Catalog enabled
- [x] Storage Credential + External Location (`s3://count-electric` via IAM cross-account role)
- [x] Databricks Git folder synced via GitHub Actions Repos API on every push

### Phase 2 — Ingestion & Bronze/Silver ✅
- [x] IEA ingestion script — fetches CSV, lands to `s3://count-electric/landing/raw/iea/`
- [x] Eurostat ingestion script — fetches JSON-stat2, lands to `s3://count-electric/landing/raw/eurostat/`
- [x] Bronze notebooks (`00_setup.py`, `01_bronze_iea.py`, `02_bronze_eurostat.py`) — verified in Databricks, Romania data confirmed
- [x] Silver notebooks (`01_silver_iea.py`, `02_silver_eurostat.py`) — deduplication, type casting, fuel category mapping, ISO country codes
- [x] Streamlit ingestion UI — run IEA/Eurostat scripts from browser, S3 file listing

### Phase 3 — Gold Layer ✅
- [x] `01_gold_market_share.py` — EV market share % + YoY growth per country/year using pivot + Window lag
- [x] `02_gold_romania.py` — Romania vs EU average, EU rank via Window rank, IEA stock join

### Phase 4 — Dashboard ✅
- [x] Databricks SQL Warehouse connection via `databricks-sql-connector`
- [x] Romania EV share vs EU average (dual line chart)
- [x] Year-over-year EV growth % (bar chart, colour by sign)
- [x] Romania EU rank over time (inverted y-axis — lower rank = better)
- [x] Top 10 EU countries by EV share in latest year (horizontal bar + Romania in blue)
- [x] Cloudflare Tunnel — `app.countelectric.com`, HTTPS, no open ports
- [x] Docker Compose stack (`app` + `cloudflared`)
- [x] Mobile-responsive nav bar (CSS media queries)
- [x] Idempotent ingestion — MD5 dedup, skip S3 upload if data unchanged
- [x] All charts static (no hover/toolbar), legends positioned below title

### Phase 5 — Polish
- [ ] README screenshots
- [ ] Portfolio write-up

---

## Setup & Installation

### Prerequisites
- AWS account (free tier sufficient)
- Databricks account (free tier at databricks.com — serverless + Unity Catalog included)
- Python 3.11+
- Docker
- GitHub repository with Actions enabled

### 1. AWS S3

Go to **S3 → Create bucket**, name it `count-electric`, leave defaults.

### 2. AWS EC2

1. **EC2 → Launch instance** — Ubuntu 24.04 LTS, t2.micro, create key pair
2. **IAM → Roles → Create role** — EC2 → attach `AmazonS3FullAccess`, name it `count-electric-ec2-role`
3. Attach role to instance: **EC2 → Actions → Security → Modify IAM role**
4. SSH in and run:
```bash
sudo apt update && sudo apt install -y docker.io git
git clone https://github.com/YOUR_USERNAME/count-electric.git
```

### 3. GitHub Actions Secrets

| Secret | Value |
|---|---|
| `EC2_HOST` | EC2 public IP |
| `EC2_USER` | `ubuntu` |
| `EC2_SSH_KEY` | Contents of `.pem` file |
| `EC2_SG_ID` | Security group ID (e.g. `sg-0abc123`) |
| `AWS_ACCESS_KEY_ID` | IAM user key for deploy runner |
| `AWS_SECRET_ACCESS_KEY` | Matching secret |
| `AWS_REGION` | e.g. `eu-west-1` |
| `DATABRICKS_HOST` | Workspace URL (e.g. `https://dbc-xxxxx.cloud.databricks.com`) |
| `DATABRICKS_TOKEN` | Databricks personal access token |
| `DATABRICKS_REPO_ID` | Git folder repo ID (from `/api/2.0/workspace/list`) |
| `DATABRICKS_HTTP_PATH` | SQL Warehouse HTTP path (e.g. `/sql/1.0/warehouses/abc123`) |
| `CLOUDFLARE_TUNNEL_TOKEN` | Cloudflare Tunnel token (from Zero Trust → Tunnels → your tunnel → Configure) |

> **Note:** `S3_BUCKET` is NOT a required secret — the app defaults to `count-electric` via `os.getenv("S3_BUCKET", "count-electric")`. Do not add it as a GitHub Secret: if the secret is missing, GitHub Actions expands it to `""` (empty string), which overrides the hardcoded default and causes boto3 to fail with an invalid bucket name error.

### 4. Databricks

1. Create account at **databricks.com** (free tier)
2. **Catalog → External Data → Storage Credentials → Create** (AWS IAM Role)
   - Create IAM role with cross-account trust policy for `arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-...`
   - Attach S3 read/write policy scoped to the `count-electric` bucket
3. **Catalog → External Data → External Locations → Create** — URL: `s3://count-electric`
4. **Workspace → Add → Git folder** — connect GitHub repo
5. Add `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_REPO_ID` to GitHub Secrets

### 5. Cloudflare Tunnel

Cloudflare Tunnel exposes the app at `app.countelectric.com` over HTTPS without opening any ports on EC2. The `cloudflared` container runs alongside the Streamlit app in Docker Compose — no manual setup needed on each deployment.

**One-time setup (done once, not repeated per deploy):**

1. **Create a free Cloudflare account** at cloudflare.com and register your domain via Cloudflare Registrar (countelectric.com was ~$10/year — no markup, domain is instantly on Cloudflare DNS)

2. **Create a named tunnel** — Cloudflare dashboard → **Zero Trust** → **Networks** → **Tunnels** → **Create a tunnel** → **Cloudflared** → give it a name → note the tunnel token

3. **Add a DNS record** — Cloudflare dashboard → **countelectric.com** → **DNS** → **Add record**:
   - Type: `CNAME`
   - Name: `app`
   - Target: `<your-tunnel-id>.cfargotunnel.com`
   - Proxy: orange cloud (proxied) ✅

4. **Add the tunnel token** as a GitHub Secret: `CLOUDFLARE_TUNNEL_TOKEN`

That's it. On the next push to `main`, GitHub Actions deploys the compose stack which starts both `app` and `cloudflared`. The `cloudflared/config.yml` in the repo maps `app.countelectric.com` → `http://app:8501` over the Docker internal network.

> **Note on `--config` flag order:** In cloudflared CLI, the `--config` flag must come *before* the `tunnel` subcommand: `cloudflared --config /etc/cloudflared/config.yml tunnel run --token ...` — not after `run`. This is already correct in `docker-compose.yml`.

> **Note on DNS verification:** Cloudflare proxied CNAME records do not expose the actual CNAME target in DNS lookups — they return Cloudflare IPs instead. This is expected and correct. `curl -I https://app.countelectric.com` returning HTTP/2 200 confirms it's working.

### 6. Run the Pipeline

In Databricks, run notebooks in order:
```
databricks/bronze/00_setup.py
databricks/bronze/01_bronze_iea.py
databricks/bronze/02_bronze_eurostat.py
databricks/silver/01_silver_iea.py
databricks/silver/02_silver_eurostat.py
databricks/gold/01_gold_market_share.py
databricks/gold/02_gold_romania.py
```

---

## Repository Structure

```
count-electric/
│
├── README.md
├── requirements.txt
├── Dockerfile
├── docker-compose.yml          # app + cloudflared services
├── .gitignore
│
├── cloudflared/
│   └── config.yml              # ingress: app.countelectric.com → http://app:8501
│
├── ingestion/
│   ├── ingest_iea.py           # IEA Global EV Data → S3
│   └── ingest_eurostat.py      # Eurostat ROAD_EQR_CARPDA → S3
│
├── databricks/
│   ├── bronze/
│   │   ├── 00_setup.py         # Create schemas, verify External Location
│   │   ├── 01_bronze_iea.py    # S3 CSV → bronze.ev_iea_raw
│   │   └── 02_bronze_eurostat.py  # S3 JSON → bronze.car_registrations_eurostat_raw
│   ├── silver/
│   │   ├── 01_silver_iea.py    # Dedupe, filter Cars, ISO country codes
│   │   └── 02_silver_eurostat.py  # Cast year, fuel categories, drop aggregates
│   ├── gold/
│   │   ├── 01_gold_market_share.py  # pivot + Window lag → gold.ev_market_share
│   │   └── 02_gold_romania.py       # join + Window rank → gold.romania_ev_summary
│   ├── aws_iam/
│   │   ├── trust_policy.json   # IAM cross-account trust for Databricks
│   │   └── s3_access_policy.json
│   └── utils/
│       └── spark_utils.py
│
├── streamlit/
│   └── app.py                  # MD3 Streamlit app, Bootstrap-style tab nav
│
└── .github/
    └── workflows/
        └── deploy.yml          # EC2 deploy + Databricks Git sync on push
```

---

*Phase 4 complete — Dashboard live at [app.countelectric.com](https://app.countelectric.com). Phase 5: README screenshots + portfolio write-up.*
