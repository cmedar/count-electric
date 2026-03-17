### Count Electric   
# Counting electric cars on the streets, so you don't have to

You've probably noticed more EVs on the street lately. But are they actually taking over, or does it just feel that way? **Count Electric** is a data engineering project that answers exactly that — tracking global EV adoption trends across countries, manufacturers, and years, so the numbers can tell the story.

Raw data is ingested from public APIs into AWS S3, processed through a **medallion architecture on Databricks**, and surfaced via an interactive **Streamlit dashboard**.

> Built to demonstrate end-to-end data engineering skills: pipeline design, Delta Lake, Spark optimisation, orchestration, and data governance with Unity Catalog.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Medallion Layer Design](#medallion-layer-design)
- [Databricks Concepts Covered](#databricks-concepts-covered)
- [Tech Stack](#tech-stack)
- [Data Sources](#data-sources)
- [Key Insights & Dashboard](#key-insights--dashboard)
- [Project Phases & Roadmap](#project-phases--roadmap)
- [Setup & Installation](#setup--installation)
- [Repository Structure](#repository-structure)

---

## Project Overview

**Count Electric** answers the following analytical questions:

- Which countries are leading global EV adoption, and how is that changing year-on-year?
- As EV registrations grow, are petrol and diesel (ICE) registrations actually declining — and how fast?
- Where does Romania sit in the European EV adoption picture, and how does it compare to leaders like Norway or Germany?
- Which EV manufacturers and models are growing fastest?
- How has the global EV market share evolved over the last 5 years?

> **Romania focus:** All dashboards include a Romania lens. Country-level data is available from 2010 onwards via IEA and Eurostat. Bucharest city-level data is not currently published as open data by DRPCIV (Romania's vehicle registry).

The project is intentionally scoped to EV adoption trends to allow depth over breadth — both in the data engineering architecture and the quality of insights produced.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      DATA SOURCES                       │
│  IEA Global EV Data  │  EU Open Data  │  US DOE / AFDC  │
└────────────┬────────────────┬──────────────────┬────────┘
             │                │                  │
             ▼                ▼                  ▼
┌─────────────────────────────────────────────────────────┐
│                 INGESTION LAYER                         │
│         Python ingest scripts (API / CSV fetch)         │
│    (Airflow orchestration deferred to Phase 2)          │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│                    AWS S3                               │
│         s3://count-electric/landing/raw/                  │
│         (raw files: JSON, CSV — append only)            │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│           DATABRICKS COMMUNITY EDITION                  │
│                                                         │
│  ┌─────────┐    ┌─────────┐    ┌─────────────────────┐ │
│  │ BRONZE  │ →  │ SILVER  │ →  │        GOLD         │ │
│  │  Delta  │    │  Delta  │    │       Delta          │ │
│  └─────────┘    └─────────┘    └─────────────────────┘ │
│                                                         │
│  Unity Catalog  │  Spark Jobs  │  Delta Lake            │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│                 STREAMLIT DASHBOARD                     │
│     Reads from Gold layer via Databricks SQL connector  │
│     Deployed locally (or Streamlit Community Cloud)     │
└─────────────────────────────────────────────────────────┘
```

**Orchestration:** Apache Airflow will schedule the full pipeline — ingest → Bronze → Silver → Gold — on a weekly cadence. Airflow is deferred to Phase 2 (excluded from the current Docker image to stay within t2.micro memory limits).

**Deployment:** GitHub Actions builds a Docker image and deploys it to EC2 on every push to `main`. The container runs Streamlit on port 8501. In the same workflow, it syncs the Databricks Git folder via the Repos API so notebooks are always up to date. AWS credentials are never stored in the repo — EC2 uses an IAM role for S3 access, and secrets are managed via GitHub Secrets.

---

## Medallion Layer Design

### Bronze — Raw Ingestion
- **Purpose:** Land raw data exactly as received, no transformations
- **Format:** Delta tables (converted from raw CSV/JSON)
- **Schema:** Append-only, includes ingestion timestamp and source identifier
- **Location:** `s3://count-electric/bronze/`
- **Key tables:**
  - `bronze.ev_registrations_raw` — registration counts by country/year/fuel type
  - `bronze.ev_models_raw` — make/model/segment metadata
  - `bronze.country_metadata_raw` — country codes, regions, population

### Silver — Cleaned & Conformed
- **Purpose:** Typed, deduplicated, standardised data ready for analysis
- **Transformations:** Null handling, type casting, country code standardisation (ISO 3166), deduplication on natural keys, schema validation
- **Format:** Delta tables with schema enforcement
- **Location:** `s3://count-electric/silver/`
- **Key tables:**
  - `silver.ev_registrations` — clean registration records with country, year, fuel type, count
  - `silver.ev_models` — standardised model catalogue
  - `silver.countries` — reference/dimension table

### Gold — Aggregated Insights
- **Purpose:** Business-ready aggregations for the dashboard
- **Transformations:** Window functions, year-on-year growth rates, market share calculations, regional rollups
- **Format:** Delta tables, partitioned by region and year
- **Location:** `s3://count-electric/gold/`
- **Key tables:**
  - `gold.ev_market_share_by_country` — EV % of total new registrations per country per year
  - `gold.ev_yoy_growth` — year-on-year growth rates by country and manufacturer
  - `gold.top_ev_models_global` — top growing models by registration volume
  - `gold.regional_summary` — aggregated Europe vs US vs RoW view

---

## Databricks Concepts Covered

| Concept | Where Applied |
|---|---|
| **Delta Lake** | All three medallion layers stored as Delta tables; time travel used for auditability |
| **Schema enforcement & evolution** | Silver layer enforces schema; Bronze allows evolution for new source fields |
| **Spark transformations** | Window functions (LAG, RANK) for YoY growth; broadcast joins for dimension tables; partitioning strategy on Gold layer |
| **Spark optimisation** | Partition pruning, Z-ordering on high-cardinality columns (country, year), caching of Silver layer for Gold aggregations |
| **Unity Catalog** | Three-level namespace: `count_electric.bronze`, `count_electric.silver`, `count_electric.gold`; column-level tagging; lineage tracking |
| **Data governance** | Source tagging at Bronze, PII-free by design, data quality checks logged to a `quality_log` table |
| **Workflows / Jobs** | Databricks notebook jobs chained: ingest → bronze → silver → gold (Community Edition: triggered via Airflow or manually) |
| **Delta time travel** | Used to compare current Gold snapshot vs previous week's run |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python 3.11, `requests`, `boto3` |
| Storage | AWS S3 (free tier) |
| Compute | AWS EC2 (t2.micro, free tier) |
| Processing | Databricks Community Edition, Apache Spark |
| Table format | Delta Lake |
| Governance | Unity Catalog |
| Containerisation | Docker (app runs as a container on EC2) |
| Orchestration | Apache Airflow *(deferred to Phase 2 — too heavy for t2.micro)* |
| Dashboard | Streamlit |
| CI/CD | GitHub Actions (deploy to EC2 via SSH) |
| Secrets | GitHub Secrets (CI/CD) + AWS IAM role (EC2) |
| Version control | Git / GitHub |

---

## Data Sources

| Source | Data | Format | Cadence | Status |
|---|---|---|---|---|
| [IEA Global EV Data Explorer](https://www.iea.org/data-and-statistics/data-tools/global-ev-data-explorer) | EV sales & stock by country, year, powertrain (BEV/PHEV/FCEV). Global including Romania. 2010–2024. | CSV API | Annual | ✅ Ingestion script complete |
| [Eurostat ROAD_EQR_CARPDA](https://ec.europa.eu/eurostat/databrowser/view/ROAD_EQR_CARPDA/) | **New car registrations by fuel type** — petrol, diesel, BEV, PHEV, hybrid, LPG for all EU countries including Romania. Core dataset for EV vs ICE comparison. | JSON-stat2 API | Annual | ✅ Ingestion script complete |
| [EAFO (EU Alt. Fuels Observatory)](https://alternative-fuels-observatory.ec.europa.eu/) | Romania EV fleet totals, market share, BEV vs PHEV breakdown | API / Web | Annual | Planned Phase 2 |
| [CarQuery API](http://www.carqueryapi.com) | Vehicle make/model/year metadata | API (JSON) | Static | Deferred to Phase 3 |

> All sources are free and publicly available. No API keys required.
>
> **Note on Bucharest city-level data:** DRPCIV (Romania's national vehicle registry) does not currently publish open data. Country-level Romania data is available from IEA and Eurostat. Bucharest-specific breakdowns would require a direct public data request to DRPCIV or Bucharest City Hall under Legea 544/2001.

---

## Key Insights & Dashboard

The Streamlit app is live on EC2 at port 8501 (served via Docker). Currently it shows an **S3 Landing Zone browser** — a table of all raw files in the S3 bucket with key, size, and last-modified timestamp. This validates ingestion before the Gold layer is ready.

Planned dashboard views:

1. **Global EV Adoption Map** — choropleth map of EV market share by country, with year slider
2. **Top 10 Countries by YoY Growth** — bar chart, filterable by region
3. **EV Market Share Trend** — line chart of EV % of total registrations over time (global + per country)
4. **Manufacturer Leaderboard** — top EV brands by registration volume globally
5. **Europe vs US vs RoW** — stacked area chart of regional EV registration volumes

---

## Project Phases & Roadmap

### Phase 1 — Foundation ✅
- [x] Project design and README
- [x] Repository structure setup
- [x] AWS S3 bucket creation and folder structure
- [x] AWS EC2 instance setup (t2.micro, free tier)
- [x] Dockerised app — Dockerfile + container running Streamlit on port 8501
- [x] GitHub Actions deployment pipeline — builds Docker image and deploys to EC2 via SSH
- [x] Databricks free tier workspace setup (serverless compute)
- [x] Unity Catalog — storage credential + external location connected to S3
- [x] Databricks Git folder synced via GitHub Actions on every push

### Phase 2 — Ingestion ⬅️ (current)
- [x] IEA Global EV Data ingestion script (`ingestion/ingest_iea.py`) — fetches CSV and lands to `s3://count-electric/landing/raw/iea/`
- [x] Eurostat ROAD_EQR_CARPDA ingestion script (`ingestion/ingest_eurostat.py`) — EV vs ICE new registrations for all EU countries including Romania
- [x] Databricks Bronze notebooks (`databricks/bronze/`) — S3 mount setup, IEA Bronze table, Eurostat Bronze table
- [x] Bronze notebooks run in Databricks — Romania data verified in both tables
- [ ] EAFO ingestion script (Romania fleet + market share detail)
- [ ] Airflow DAG: ingest job scheduled (deferred — Airflow excluded from Docker for now)

### Phase 3 — Transformation
- [ ] Silver layer: cleaning, typing, deduplication notebooks
- [ ] Gold layer: aggregation notebooks (YoY growth, market share, regional rollups)
- [ ] Data quality checks logged to `quality_log` table
- [ ] Z-ordering and partitioning applied to Gold tables

### Phase 4 — Dashboard
- [ ] Streamlit app scaffolded
- [ ] Connected to Gold layer via Databricks SQL connector
- [ ] All 5 dashboard views implemented
- [ ] Deployed to Streamlit Community Cloud

### Phase 5 — Polish & Documentation
- [ ] README updated with final architecture and screenshots
- [ ] Notebook documentation and inline comments
- [ ] Architecture diagram (visual)
- [ ] LinkedIn post / portfolio write-up

---

## Setup & Installation

### Prerequisites
- AWS account (free tier)
- Databricks Community Edition account
- Python 3.12+
- Git
- GitHub repository with Actions enabled

---

### 1. AWS S3

1. Go to **S3 → Create bucket**
   - Name: `count-electric` (must be globally unique)
   - Region: your preferred region (e.g. `eu-west-1`)
   - Leave all other defaults

---

### 2. AWS EC2

1. Go to **EC2 → Launch instance**
   - Name: `count-electric`
   - AMI: Ubuntu Server 24.04 LTS (free tier)
   - Instance type: `t2.micro`
   - Key pair: create new → download `.pem` file
   - Security group: allow inbound SSH (port 22) from your IP only

2. Create an **IAM role** for S3 access (no keys stored on EC2):
   - **IAM → Roles → Create role** → EC2 → attach `AmazonS3FullAccess`
   - Name: `count-electric-ec2-role`
   - Attach to instance: **EC2 → Actions → Security → Modify IAM role**

3. SSH in and set up the project:
```bash
chmod 400 ~/Downloads/count-electric-key.pem
ssh -i ~/Downloads/count-electric-key.pem ubuntu@YOUR_EC2_IP

# On EC2:
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3.12 python3-pip python3-venv git
git clone https://github.com/YOUR_USERNAME/count-electric.git
cd count-electric
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install "apache-airflow==2.10.5" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"
export AIRFLOW_HOME=~/airflow
airflow db init
```

---

### 3. GitHub Actions — Deploy Pipeline

Pushes to `main` automatically deploy to EC2 via SSH. The workflow dynamically whitelists the runner IP, deploys, then removes it.

**GitHub Secrets required** (repo → Settings → Secrets → Actions):

| Secret | Description |
|---|---|
| `EC2_HOST` | Public IP of your EC2 instance |
| `EC2_USER` | `ubuntu` |
| `EC2_SSH_KEY` | Full contents of your `.pem` file |
| `EC2_SG_ID` | Security group ID (e.g. `sg-0abc123...`) |
| `AWS_ACCESS_KEY_ID` | IAM user key — deploy-only permissions |
| `AWS_SECRET_ACCESS_KEY` | Matching secret |
| `AWS_REGION` | e.g. `eu-west-1` |
| `DATABRICKS_HOST` | Your Databricks workspace URL (e.g. `https://dbc-xxxxx.cloud.databricks.com`) |
| `DATABRICKS_TOKEN` | Databricks personal access token |
| `DATABRICKS_REPO_ID` | Databricks Git folder repo ID (from `/api/2.0/workspace/list`) |
| `AFDC_API_KEY` | US DOE AFDC API key *(Phase 2)* |

**IAM policy for the deploy user** (scoped to security group only):
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "ec2:AuthorizeSecurityGroupIngress",
      "ec2:RevokeSecurityGroupIngress"
    ],
    "Resource": "arn:aws:ec2:REGION:ACCOUNT_ID:security-group/SG_ID"
  }]
}
```

> AWS credentials for S3 access are NOT stored as secrets — the EC2 instance uses an IAM role instead.

---

### 4. Databricks

1. Create a free account at **databricks.com** (free tier — serverless compute + Unity Catalog included)
2. **Storage Credential** — Catalog → External Data → Storage Credentials → Create (AWS IAM Role)
   - Create an IAM role with a cross-account trust policy (Databricks account `414351767826`)
   - Attach S3 read/write policy scoped to the `count-electric` bucket
3. **External Location** — Catalog → External Data → External Locations → Create
   - URL: `s3://count-electric`, credential: the role from step 2
4. **Git Folder** — Workspace → your user folder → Add → Git folder → connect GitHub repo
5. **Secrets** — install Databricks CLI, run `databricks configure --token`, then:
   ```bash
   databricks secrets create-scope --scope count-electric
   databricks secrets put --scope count-electric --key aws-access-key-id
   databricks secrets put --scope count-electric --key aws-secret-access-key
   databricks secrets put --scope count-electric --key aws-region
   ```
6. Add `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_REPO_ID` to GitHub Secrets — subsequent pushes auto-sync notebooks

---

## Repository Structure

```
count-electric/
│
├── README.md
├── requirements.txt
├── .env.example
│
├── ingestion/                  # Python scripts to fetch from APIs and land to S3
│   ├── ingest_iea.py
│   ├── ingest_eu_open_data.py
│   ├── ingest_afdc.py
│   └── ingest_carquery.py
│
├── databricks/
│   ├── bronze/                 # Notebooks: raw → Bronze Delta tables
│   ├── silver/                 # Notebooks: Bronze → Silver (clean & conform)
│   ├── gold/                   # Notebooks: Silver → Gold (aggregations)
│   └── utils/                  # Shared helpers (logging, quality checks)
│
├── airflow/
│   └── dags/
│       └── count_electric_pipeline.py
│
├── streamlit/
│   ├── app.py
│   └── pages/
│       ├── global_map.py
│       ├── yoy_growth.py
│       ├── market_share_trend.py
│       ├── manufacturer_leaderboard.py
│       └── regional_summary.py
│
└── docs/
    └── architecture.png        # To be added in Phase 5
```

---

*README last updated: Phase 2 in progress — Databricks fully connected (serverless, Unity Catalog, Git sync), Bronze tables live, Romania data verified*
