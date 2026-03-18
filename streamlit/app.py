"""
Count Electric — Streamlit App

Tabs:
  About      — project overview, architecture, roadmap
  Ingestion  — S3 landing zone browser + ingestion trigger buttons
  Data       — Romania EV vs ICE charts from raw S3 data (pre-Gold)
"""

import json
import os
import sys
from itertools import product

import boto3
import pandas as pd
import plotly.express as px
import streamlit as st

# Allow importing ingestion modules from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

S3_BUCKET = os.getenv("S3_BUCKET", "count-electric")

st.set_page_config(page_title="Count Electric", page_icon="⚡", layout="wide")
st.title("⚡ Count Electric")
st.caption("Tracking global EV adoption trends — by country, manufacturer, and year.")

tab_about, tab_ingest, tab_data = st.tabs(["About", "Ingestion", "Data Preview"])


# ── helpers ──────────────────────────────────────────────────────────────────

def get_s3_client():
    return boto3.client("s3")


@st.cache_data(ttl=300)
def list_s3_files(prefix=""):
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    return response.get("Contents", [])


@st.cache_data(ttl=300)
def read_latest_iea_csv() -> pd.DataFrame:
    """Read the most recently uploaded IEA CSV from S3."""
    files = [f for f in list_s3_files("landing/raw/iea/") if f["Key"].endswith(".csv")]
    if not files:
        return pd.DataFrame()
    latest = max(files, key=lambda f: f["LastModified"])
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=S3_BUCKET, Key=latest["Key"])
    return pd.read_csv(obj["Body"])


@st.cache_data(ttl=300)
def read_latest_eurostat_json() -> pd.DataFrame:
    """Read and parse the most recently uploaded Eurostat JSON from S3."""
    files = [f for f in list_s3_files("landing/raw/eurostat/") if f["Key"].endswith(".json")]
    if not files:
        return pd.DataFrame()
    latest = max(files, key=lambda f: f["LastModified"])
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=S3_BUCKET, Key=latest["Key"])
    raw = json.loads(obj["Body"].read())
    return _parse_jsonstat2(raw)


def _parse_jsonstat2(raw: dict) -> pd.DataFrame:
    dims   = raw["id"]
    sizes  = raw["size"]
    values = raw["value"]
    label_maps = {}
    for dim in dims:
        cats = raw["dimension"][dim]["category"]
        label_maps[dim] = {str(v): k for k, v in cats["index"].items()}
    rows = []
    for i, combo in enumerate(product(*[range(s) for s in sizes])):
        val = values.get(str(i))
        if val is None:
            continue
        row = {dim: label_maps[dim][str(idx)] for dim, idx in zip(dims, combo)}
        row["value"] = float(val)
        rows.append(row)
    return pd.DataFrame(rows)


FUEL_LABELS = {
    "ELC": "Electric (BEV)", "ELC_PET_PI": "Plug-in hybrid (PHEV)",
    "ELC_DIE_PI": "Plug-in hybrid diesel (PHEV)", "ELC_PET_HYB": "Hybrid (non-plug-in)",
    "ELC_DIE_HYB": "Hybrid diesel (non-plug-in)", "PET": "Petrol (ICE)",
    "DIE": "Diesel (ICE)", "LPG": "LPG", "GAS": "Natural gas",
    "HYD_FCELL": "Hydrogen", "ALT": "Alternative (all)", "TOTAL": "Total",
}

FUEL_CATEGORIES = {
    "ELC": "Electric", "ELC_PET_PI": "Electric", "ELC_DIE_PI": "Electric",
    "ELC_PET_HYB": "Hybrid", "ELC_DIE_HYB": "Hybrid",
    "PET": "ICE", "DIE": "ICE",
    "LPG": "Other", "GAS": "Other", "HYD_FCELL": "Other",
    "ALT": "Other", "TOTAL": "Total",
}

CATEGORY_COLORS = {
    "Electric": "#00CC96",
    "ICE":      "#EF553B",
    "Hybrid":   "#FFA15A",
    "Other":    "#B6B6B6",
    "Total":    "#636EFA",
}


# ── TAB 1: ABOUT ─────────────────────────────────────────────────────────────

with tab_about:
    st.header("What is Count Electric?")
    st.markdown("""
You've probably noticed more EVs on the street lately. But are they actually taking over,
or does it just feel that way?

**Count Electric** tracks global EV adoption trends — and compares them directly against
petrol and diesel registrations, so you can see one grow while the other shrinks.

Built as a data engineering portfolio project to demonstrate end-to-end skills:
pipeline design, Delta Lake, Spark, and Databricks — with a special focus on **Romania**.
""")

    st.divider()
    st.subheader("Architecture")
    st.markdown("""
```
Data Sources (IEA, Eurostat)
        ↓
Ingestion scripts (Python, running on EC2 via Docker)
        ↓
AWS S3  —  s3://count-electric/landing/raw/
        ↓
Databricks (serverless) — Bronze → Silver → Gold  (Delta Lake)
        ↓
Streamlit Dashboard  ←  this app
```
""")

    st.divider()
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Tech Stack")
        st.markdown("""
| Layer | Technology |
|---|---|
| Ingestion | Python, boto3, requests |
| Storage | AWS S3 |
| Compute | AWS EC2 (t2.micro) + Docker |
| Processing | Databricks serverless, Apache Spark |
| Table format | Delta Lake |
| Governance | Unity Catalog |
| Dashboard | Streamlit |
| CI/CD | GitHub Actions |
""")

    with col2:
        st.subheader("Data Sources")
        st.markdown("""
| Source | What |
|---|---|
| **IEA Global EV Data** | EV sales & stock by country, 2010–2024 |
| **Eurostat ROAD_EQR_CARPDA** | New car registrations by fuel type — the EV vs ICE dataset |
| **EAFO** | Romania EV fleet detail *(planned)* |
""")

    st.divider()
    st.subheader("Project Roadmap")

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.success("**Phase 1** Foundation\n\nS3 · EC2 · Docker · GitHub Actions · Databricks")
    with col2:
        st.success("**Phase 2** Ingestion\n\nIEA · Eurostat · Bronze tables · Silver tables")
    with col3:
        st.warning("**Phase 3** Transformation\n\nGold layer · YoY growth · Market share")
    with col4:
        st.info("**Phase 4** Dashboard\n\nFull charts · Country comparisons · Romania lens")
    with col5:
        st.info("**Phase 5** Polish\n\nDocs · Architecture diagram · Portfolio write-up")


# ── TAB 2: INGESTION ─────────────────────────────────────────────────────────

with tab_ingest:
    st.header("Ingestion Control")
    st.caption("Run ingestion scripts to fetch latest data from sources and land to S3.")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("IEA Global EV Data")
        st.markdown("Fetches EV sales & stock CSV from the IEA API and uploads to `landing/raw/iea/`.")
        if st.button("Run IEA Ingestion", type="primary"):
            with st.spinner("Fetching from IEA..."):
                try:
                    from ingestion.ingest_iea import main as run_iea
                    run_iea()
                    st.cache_data.clear()
                    st.success("IEA ingestion complete.")
                except Exception as e:
                    st.error(f"Failed: {e}")

    with col2:
        st.subheader("Eurostat ROAD_EQR_CARPDA")
        st.markdown("Fetches new car registrations by fuel type from Eurostat API and uploads to `landing/raw/eurostat/`.")
        if st.button("Run Eurostat Ingestion", type="primary"):
            with st.spinner("Fetching from Eurostat..."):
                try:
                    from ingestion.ingest_eurostat import main as run_eurostat
                    run_eurostat()
                    st.cache_data.clear()
                    st.success("Eurostat ingestion complete.")
                except Exception as e:
                    st.error(f"Failed: {e}")

    st.divider()
    st.subheader("S3 Landing Zone")

    try:
        all_files = list_s3_files("landing/raw/")
        if not all_files:
            st.info("No files yet. Run an ingestion script above.")
        else:
            rows = [
                {
                    "Source": f["Key"].split("/")[2].upper(),
                    "File": f["Key"].split("/")[-1],
                    "Size (KB)": round(f["Size"] / 1024, 1),
                    "Ingested at": f["LastModified"].strftime("%Y-%m-%d %H:%M UTC"),
                }
                for f in sorted(all_files, key=lambda x: x["LastModified"], reverse=True)
            ]
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            st.caption(f"{len(all_files)} file(s) in s3://{S3_BUCKET}/landing/raw/")
    except Exception as e:
        st.error(f"Could not connect to S3: {e}")


# ── TAB 3: DATA PREVIEW ──────────────────────────────────────────────────────

with tab_data:
    st.header("Data Preview")
    st.caption("Reading directly from S3 landing zone — pre-Gold layer, for pipeline validation.")

    # ── IEA: Romania EV sales ──
    st.subheader("Romania — EV Sales by Powertrain (IEA)")

    try:
        df_iea = read_latest_iea_csv()
        if df_iea.empty:
            st.info("No IEA data yet. Run ingestion first.")
        else:
            df_ro = df_iea[
                (df_iea["region"] == "Romania") &
                (df_iea["parameter"] == "EV sales") &
                (df_iea["mode"] == "Cars") &
                (df_iea["powertrain"].isin(["BEV", "PHEV"]))
            ].copy()

            fig = px.line(
                df_ro, x="year", y="value", color="powertrain",
                markers=True,
                labels={"value": "Vehicles sold", "year": "Year", "powertrain": "Type"},
                color_discrete_map={"BEV": "#00CC96", "PHEV": "#FFA15A"},
            )
            fig.update_layout(hovermode="x unified", legend_title="Powertrain")
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Source: IEA Global EV Data Explorer")
    except Exception as e:
        st.error(f"Could not load IEA data: {e}")

    st.divider()

    # ── Eurostat: Romania EV vs ICE ──
    st.subheader("Romania — New Car Registrations by Fuel Category (Eurostat)")

    try:
        df_eur = read_latest_eurostat_json()
        if df_eur.empty:
            st.info("No Eurostat data yet. Run ingestion first.")
        else:
            focus_codes = ["ELC", "PET", "DIE", "ELC_PET_PI", "ELC_DIE_PI"]
            df_ro_eur = df_eur[
                (df_eur["geo"] == "RO") &
                (df_eur["mot_nrg"].isin(focus_codes))
            ].copy()

            df_ro_eur["fuel_label"]    = df_ro_eur["mot_nrg"].map(FUEL_LABELS)
            df_ro_eur["fuel_category"] = df_ro_eur["mot_nrg"].map(FUEL_CATEGORIES)
            df_ro_eur["year"]          = df_ro_eur["time"].astype(int)

            # Aggregate by category and year
            df_cat = (
                df_ro_eur.groupby(["year", "fuel_category"])["value"]
                .sum().reset_index()
                .rename(columns={"value": "new_registrations"})
            )
            df_cat = df_cat[df_cat["fuel_category"] != "Total"]

            fig2 = px.bar(
                df_cat, x="year", y="new_registrations", color="fuel_category",
                barmode="stack",
                labels={"new_registrations": "New registrations", "year": "Year", "fuel_category": "Category"},
                color_discrete_map=CATEGORY_COLORS,
            )
            fig2.update_layout(hovermode="x unified", legend_title="Fuel category")
            st.plotly_chart(fig2, use_container_width=True)

            # Also show the raw trend lines for ELC, PET, DIE
            df_lines = df_ro_eur[df_ro_eur["mot_nrg"].isin(["ELC", "PET", "DIE"])].copy()
            fig3 = px.line(
                df_lines, x="year", y="value", color="fuel_label",
                markers=True,
                labels={"value": "New registrations", "year": "Year", "fuel_label": "Fuel type"},
                color_discrete_map={
                    "Electric (BEV)": "#00CC96",
                    "Petrol (ICE)":   "#EF553B",
                    "Diesel (ICE)":   "#B34A00",
                },
                title="Electric vs Petrol vs Diesel — trend lines",
            )
            fig3.update_layout(hovermode="x unified")
            st.plotly_chart(fig3, use_container_width=True)
            st.caption("Source: Eurostat ROAD_EQR_CARPDA")

    except Exception as e:
        st.error(f"Could not load Eurostat data: {e}")
