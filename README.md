# Count Electric — counting electric cars so you don't have to

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
- Which EV manufacturers and models are growing fastest?
- What does the regional breakdown of EV registrations look like across Europe and the US?
- How has the global EV market share evolved over the last 5 years?

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
│              Orchestrated via Apache Airflow            │
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

**Orchestration:** Apache Airflow (running on EC2) schedules the full pipeline — ingest → Bronze → Silver → Gold — on a weekly cadence (EV registration data is typically published monthly or quarterly).

**Deployment:** GitHub Actions deploys code to EC2 via SSH on every push to `main`. AWS credentials are never stored in the repo — EC2 uses an IAM role for S3 access, and secrets are managed via GitHub Secrets.

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
| Orchestration | Apache Airflow (on EC2) |
| Dashboard | Streamlit |
| CI/CD | GitHub Actions (deploy to EC2 via SSH) |
| Secrets | GitHub Secrets (CI/CD) + AWS IAM role (EC2) |
| Version control | Git / GitHub |

---

## Data Sources

| Source | Data | Format | Cadence |
|---|---|---|---|
| [IEA Global EV Data Explorer](https://www.iea.org/data-and-statistics/data-tools/global-ev-data-explorer) | EV registrations by country, year, powertrain | CSV download | Annual |
| [EU Open Data Portal](https://data.europa.eu) | New car registrations by fuel type, EU countries | CSV / API | Monthly |
| [US DOE AFDC](https://afdc.energy.gov/api) | US EV registration data by state and model | API (JSON) | Annual |
| [CarQuery API](http://www.carqueryapi.com) | Vehicle make/model/year metadata | API (JSON) | Static |

> All sources are free and publicly available. No API keys required for IEA and EU Open Data; AFDC requires a free key registration.

---

## Key Insights & Dashboard

The Streamlit dashboard will expose the following views:

1. **Global EV Adoption Map** — choropleth map of EV market share by country, with year slider
2. **Top 10 Countries by YoY Growth** — bar chart, filterable by region
3. **EV Market Share Trend** — line chart of EV % of total registrations over time (global + per country)
4. **Manufacturer Leaderboard** — top EV brands by registration volume globally
5. **Europe vs US vs RoW** — stacked area chart of regional EV registration volumes

---

## Project Phases & Roadmap

### Phase 1 — Foundation ✅ (current)
- [x] Project design and README
- [x] Repository structure setup
- [x] AWS S3 bucket creation and folder structure
- [ ] AWS EC2 instance setup (Airflow + deployment target)
- [ ] GitHub Actions deployment pipeline (SSH deploy to EC2)
- [ ] Databricks Community Edition workspace setup
- [ ] Unity Catalog configuration

### Phase 2 — Ingestion
- [ ] Python ingest scripts for each data source
- [ ] S3 landing zone populated with raw files
- [ ] Bronze Delta tables created and loaded
- [ ] Airflow DAG: ingest job scheduled

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

> ⚠️ Setup instructions will be completed in Phase 1. Placeholder below.

### Prerequisites
- AWS account (free tier) — S3 bucket + EC2 t2.micro + IAM role
- Databricks Community Edition account
- Python 3.11+
- Git
- GitHub repository with Actions enabled

### Secrets required in GitHub
| Secret | Description |
|---|---|
| `EC2_HOST` | Public IP or DNS of your EC2 instance |
| `EC2_USER` | SSH user (e.g. `ec2-user` or `ubuntu`) |
| `EC2_SSH_KEY` | Private key for SSH access (contents of `.pem` file) |
| `DATABRICKS_HOST` | Your Databricks workspace URL |
| `DATABRICKS_TOKEN` | Databricks personal access token |
| `AFDC_API_KEY` | US DOE AFDC API key |

> AWS credentials are NOT stored as GitHub Secrets — the EC2 instance uses an IAM role with S3 access instead.

### Quick Start
```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/count-electric.git
cd count-electric

# Install dependencies
pip install -r requirements.txt
```

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

*README last updated: Phase 1 — Project Design*
