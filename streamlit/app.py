"""
Count Electric — Streamlit App
Material Design 3 light theme, Top App Bar with inline navigation.
"""

import json
import os
import sys
from itertools import product

import boto3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

S3_BUCKET = os.getenv("S3_BUCKET", "count-electric")

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Count Electric",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Material Design 3 — light teal theme ─────────────────────────────────────

st.markdown("""
<style>
/* ── Global ── */
html, body, [class*="css"] {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
}
.stApp {
    background-color: #F4FBFA;
}

/* ── Hide native Streamlit header + sidebar ── */
header[data-testid="stHeader"],
[data-testid="stSidebar"],
[data-testid="stSidebarCollapsedControl"] {
    display: none !important;
}

/* ── App header row ── */
.app-header {
    background: #FFFFFF;
    border-bottom: 1px solid #E0F2F1;
    padding: 0 8px;
    margin-bottom: 24px;
}

/* ── Horizontal nav radio — pill style ── */
div[data-testid="stHorizontalBlock"] .stRadio {
    display: flex;
    align-items: center;
    height: 64px;
}
div[data-testid="stHorizontalBlock"] .stRadio > label {
    display: none;
}
div[data-testid="stHorizontalBlock"] .stRadio [data-baseweb="radio-group"] {
    flex-direction: row;
    gap: 4px;
}
div[data-testid="stHorizontalBlock"] .stRadio [data-baseweb="radio"] {
    margin: 0;
}
div[data-testid="stHorizontalBlock"] .stRadio [data-baseweb="radio"] > div:first-child {
    display: none;
}
div[data-testid="stHorizontalBlock"] .stRadio label {
    padding: 8px 16px;
    border-radius: 20px;
    font-size: 14px;
    font-weight: 500;
    color: #546E7A;
    cursor: pointer;
    white-space: nowrap;
    transition: background 0.15s, color 0.15s;
}
div[data-testid="stHorizontalBlock"] .stRadio label:hover {
    background: #F0FAF9;
    color: #00695C;
}
div[data-testid="stHorizontalBlock"] .stRadio [aria-checked="true"] label {
    background: #E0F2F1 !important;
    color: #00695C !important;
}

/* ── Reduce top padding ── */
.main .block-container {
    padding-top: 0 !important;
}

/* ── Hide fullscreen button on all elements ── */
[data-testid="StyledFullScreenButton"],
[data-testid="stElementToolbar"] {
    display: none !important;
}

/* Headings */
h1 { color: #00695C; font-weight: 500; letter-spacing: -0.5px; }
h2 { color: #00796B; font-weight: 500; }
h3 { color: #00897B; font-weight: 500; }

/* ── Buttons — MD3 filled ── */
.stButton > button {
    background-color: #00897B;
    color: #FFFFFF;
    border: none;
    border-radius: 20px;
    padding: 10px 28px;
    font-family: inherit;
    font-size: 14px;
    font-weight: 500;
    letter-spacing: 0.4px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.15);
    transition: all 0.2s ease;
}
.stButton > button:hover {
    background-color: #00796B;
    box-shadow: 0 3px 8px rgba(0,0,0,0.2);
}
.stButton > button:active {
    background-color: #00695C;
    box-shadow: none;
}

/* ── Cards ── */
.md-card {
    background: #FFFFFF;
    border-radius: 16px;
    padding: 24px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.08), 0 2px 8px rgba(0,0,0,0.04);
    margin-bottom: 20px;
}

/* ── Phase badges ── */
.phase-done    { background:#E8F5E9; color:#1B5E20; padding:16px; border-radius:12px; border-left:4px solid #43A047; }
.phase-current { background:#E3F2FD; color:#0D47A1; padding:16px; border-radius:12px; border-left:4px solid #1E88E5; }
.phase-planned { background:#FAFAFA; color:#616161; padding:16px; border-radius:12px; border-left:4px solid #BDBDBD; }

/* ── Dataframe ── */
[data-testid="stDataFrame"] { border-radius: 12px; overflow: hidden; }

/* ── Alerts ── */
[data-testid="stSuccess"] { border-radius: 12px; }
[data-testid="stInfo"]    { border-radius: 12px; }
[data-testid="stError"]   { border-radius: 12px; }

/* ── Divider ── */
hr { border-color: #E0F2F1; }

/* ── Caption ── */
.stCaption { color: #78909C; font-size: 12px; }
</style>
""", unsafe_allow_html=True)

# ── Inline SVG car icon ───────────────────────────────────────────────────────

CAR_ICON = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="{size}" height="{size}" fill="{color}">
  <path d="M18.92 6.01C18.72 5.42 18.16 5 17.5 5h-11c-.66 0-1.21.42-1.42 1.01L3 12v8c0 .55.45 1 1 1h1c.55 0 1-.45 1-1v-1h12v1c0 .55.45 1 1 1h1c.55 0 1-.45 1-1v-8l-2.08-5.99zM6.85 7h10.29l1.08 3H5.77L6.85 7zM19 17H5v-5h14v5z"/>
  <circle cx="7.5" cy="14.5" r="1.5"/><circle cx="16.5" cy="14.5" r="1.5"/>
</svg>"""

# ── Session state ─────────────────────────────────────────────────────────────

NAV_ITEMS = ["About", "Ingestion", "Data Preview"]

if "page" not in st.session_state:
    st.session_state.page = "About"

# ── Header row: logo + nav ────────────────────────────────────────────────────

col_brand, col_nav = st.columns([3, 5])

with col_brand:
    st.markdown(
        f"""<div style="display:flex;align-items:center;gap:10px;height:64px">
        {CAR_ICON.format(size=26, color="#00897B")}
        <span style="font-size:1.15rem;font-weight:500;color:#00695C;letter-spacing:-0.3px">Count Electric</span>
        </div>""",
        unsafe_allow_html=True,
    )

with col_nav:
    page = st.radio(
        "",
        NAV_ITEMS,
        index=NAV_ITEMS.index(st.session_state.page),
        horizontal=True,
        label_visibility="collapsed",
    )
    st.session_state.page = page

st.markdown('<div style="border-bottom:1px solid #E0F2F1;margin-bottom:24px"></div>', unsafe_allow_html=True)

# ── Helpers ───────────────────────────────────────────────────────────────────

def s3():
    return boto3.client("s3")

@st.cache_data(ttl=300)
def list_s3_files(prefix=""):
    r = s3().list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    return r.get("Contents", [])

@st.cache_data(ttl=300)
def read_latest_iea_csv() -> pd.DataFrame:
    files = [f for f in list_s3_files("landing/raw/iea/") if f["Key"].endswith(".csv")]
    if not files:
        return pd.DataFrame()
    key = max(files, key=lambda f: f["LastModified"])["Key"]
    obj = s3().get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_csv(obj["Body"])

@st.cache_data(ttl=300)
def read_latest_eurostat_json() -> pd.DataFrame:
    files = [f for f in list_s3_files("landing/raw/eurostat/") if f["Key"].endswith(".json")]
    if not files:
        return pd.DataFrame()
    key = max(files, key=lambda f: f["LastModified"])["Key"]
    obj = s3().get_object(Bucket=S3_BUCKET, Key=key)
    return _parse_jsonstat2(json.loads(obj["Body"].read()))

def _parse_jsonstat2(raw: dict) -> pd.DataFrame:
    dims, sizes, values = raw["id"], raw["size"], raw["value"]
    label_maps = {
        dim: {str(v): k for k, v in raw["dimension"][dim]["category"]["index"].items()}
        for dim in dims
    }
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
    "ELC_DIE_PI": "Plug-in hybrid diesel", "ELC_PET_HYB": "Hybrid (non-plug-in)",
    "ELC_DIE_HYB": "Hybrid diesel", "PET": "Petrol (ICE)", "DIE": "Diesel (ICE)",
    "LPG": "LPG", "GAS": "Natural gas", "HYD_FCELL": "Hydrogen", "TOTAL": "Total",
}
FUEL_CATEGORIES = {
    "ELC": "Electric", "ELC_PET_PI": "Electric", "ELC_DIE_PI": "Electric",
    "ELC_PET_HYB": "Hybrid", "ELC_DIE_HYB": "Hybrid",
    "PET": "ICE", "DIE": "ICE",
    "LPG": "Other", "GAS": "Other", "HYD_FCELL": "Other", "TOTAL": "Total",
}
CATEGORY_COLORS = {
    "Electric": "#00BFA5", "ICE": "#EF5350",
    "Hybrid": "#FFA726", "Other": "#B0BEC5", "Total": "#5C6BC0",
}

# ── PAGE: ABOUT ───────────────────────────────────────────────────────────────

if page == "About":
    st.markdown("---")

    # Mission
    st.markdown("""
<div class="md-card" style="padding:16px 24px;margin-bottom:16px">
<h3 style="margin:0 0 6px 0;font-size:1rem">What is this?</h3>
<p style="margin:0;line-height:1.5">
You've probably noticed more EVs on the street lately — but are they actually taking over?
<strong>Count Electric</strong> answers that with data, tracking EV adoption country by country, year by year,
with a special focus on <strong>Romania</strong>.</p>
</div>
""", unsafe_allow_html=True)

    # Architecture + Tech Stack side by side
    col_arch, col_tech = st.columns([1, 1])

    with col_arch:
        st.subheader("Architecture")
        st.graphviz_chart("""
digraph pipeline {
    rankdir=TB
    bgcolor=transparent
    graph [fontname="Helvetica", splines=ortho, nodesep=0.2, ranksep=0.3]
    node  [fontname="Helvetica", fontsize=13, style="rounded,filled", shape=box,
           margin="0.12,0.08", width=1.6, fillcolor="#E0F2F1", color="#00897B", fontcolor="#004D40"]
    edge  [fontname="Helvetica", fontsize=11, color="#80CBC4", arrowsize=0.55]

    sources    [label="Data Sources\nIEA · Eurostat"]
    ingest     [label="Ingestion\nEC2 · Docker"]
    s3         [label="AWS S3\nlanding/raw/"]
    databricks [label="Databricks\nBronze→Silver→Gold"]
    streamlit  [label="Streamlit\n:8501"]

    sources    -> ingest     [label="API/CSV"]
    ingest     -> s3         [label="raw files"]
    s3         -> databricks [label="Delta Lake"]
    databricks -> streamlit  [label="Gold  Ph.4", style=dashed]
}
""", use_container_width=True)

    with col_tech:
        st.subheader("Tech Stack")
        st.markdown("""
| Layer | Technology |
|---|---|
| Ingestion | Python, boto3, requests |
| Storage | AWS S3 |
| Compute | AWS EC2 t2.micro + Docker |
| Processing | Databricks serverless, Spark |
| Table format | Delta Lake |
| Governance | Unity Catalog |
| Dashboard | Streamlit |
| CI/CD | GitHub Actions |
""")

    st.markdown("---")
    st.subheader("Project Roadmap")

    cols = st.columns(5)
    phases = [
        ("done",    "Phase 1",  "Foundation",      "S3 · EC2 · Docker\nGitHub Actions\nDatabricks setup"),
        ("done",    "Phase 2",  "Ingestion",        "IEA · Eurostat\nBronze tables\nSilver tables"),
        ("current", "Phase 3",  "Transformation",   "Gold layer\nYoY growth\nMarket share"),
        ("planned", "Phase 4",  "Dashboard",        "Full charts\nCountry compare\nRomania lens"),
        ("planned", "Phase 5",  "Polish",           "Docs\nArchitecture diagram\nPortfolio write-up"),
    ]
    for col, (status, phase, title, detail) in zip(cols, phases):
        with col:
            icon = "✅" if status == "done" else ("🔵" if status == "current" else "○")
            st.markdown(f"""
<div class="phase-{'done' if status=='done' else ('current' if status=='current' else 'planned')}">
<strong>{icon} {phase}</strong><br>
<span style="font-size:15px;font-weight:500">{title}</span><br>
<span style="font-size:12px;opacity:0.8">{detail.replace(chr(10), ' · ')}</span>
</div>""", unsafe_allow_html=True)

# ── PAGE: INGESTION ───────────────────────────────────────────────────────────

elif page == "Ingestion":
    st.title("Ingestion Control")
    st.markdown("Fetch latest data from sources and land raw files to S3. Runs directly on the server.")
    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("""<div class="md-card">
<h3 style="margin-top:0">🌍 IEA Global EV Data</h3>
<p style="color:#546E7A;font-size:14px">Fetches annual EV sales & stock CSV from the IEA API.<br>
Lands to <code>s3://count-electric/landing/raw/iea/</code></p>
</div>""", unsafe_allow_html=True)
        if st.button("Run IEA Ingestion", use_container_width=True):
            with st.spinner("Fetching from IEA API…"):
                try:
                    from ingestion.ingest_iea import main as run_iea
                    run_iea()
                    st.cache_data.clear()
                    st.success("IEA ingestion complete.")
                except Exception as e:
                    st.error(f"Failed: {e}")

    with col2:
        st.markdown("""<div class="md-card">
<h3 style="margin-top:0">🇪🇺 Eurostat ROAD_EQR_CARPDA</h3>
<p style="color:#546E7A;font-size:14px">Fetches new car registrations by fuel type for all EU countries.<br>
Lands to <code>s3://count-electric/landing/raw/eurostat/</code></p>
</div>""", unsafe_allow_html=True)
        if st.button("Run Eurostat Ingestion", use_container_width=True):
            with st.spinner("Fetching from Eurostat API…"):
                try:
                    from ingestion.ingest_eurostat import main as run_eurostat
                    run_eurostat()
                    st.cache_data.clear()
                    st.success("Eurostat ingestion complete.")
                except Exception as e:
                    st.error(f"Failed: {e}")

    st.markdown("---")
    st.subheader("S3 Landing Zone")

    try:
        all_files = list_s3_files("landing/raw/")
        if not all_files:
            st.info("No files yet. Run an ingestion script above.")
        else:
            rows = [
                {
                    "Source":       f["Key"].split("/")[2].upper(),
                    "File":         f["Key"].split("/")[-1],
                    "Size (KB)":    round(f["Size"] / 1024, 1),
                    "Ingested at":  f["LastModified"].strftime("%Y-%m-%d %H:%M UTC"),
                }
                for f in sorted(all_files, key=lambda x: x["LastModified"], reverse=True)
            ]
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            st.caption(f"{len(all_files)} file(s) · s3://{S3_BUCKET}/landing/raw/")
    except Exception as e:
        st.error(f"Could not connect to S3: {e}")

# ── PAGE: DATA PREVIEW ────────────────────────────────────────────────────────

elif page == "Data Preview":
    st.title("Data Preview")
    st.markdown("Reading directly from the S3 landing zone — raw data, pre-Gold layer. For pipeline validation and early insights.")
    st.markdown("---")

    # ── IEA chart ──
    st.subheader("Romania — EV Sales by Powertrain")
    st.caption("Source: IEA Global EV Data Explorer · Cars only · BEV and PHEV")

    try:
        df_iea = read_latest_iea_csv()
        if df_iea.empty:
            st.info("No IEA data yet. Go to Ingestion and run the IEA script.")
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
                color_discrete_map={"BEV": "#00BFA5", "PHEV": "#FFA726"},
            )
            fig.update_layout(
                plot_bgcolor="white", paper_bgcolor="white",
                font_family="system-ui, -apple-system, sans-serif",
                hovermode="x unified",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                margin=dict(l=0, r=0, t=32, b=0),
            )
            fig.update_xaxes(showgrid=False)
            fig.update_yaxes(gridcolor="#F0F0F0")
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Could not load IEA data: {e}")

    st.markdown("---")

    # ── Eurostat charts ──
    st.subheader("Romania — New Car Registrations by Fuel Type")
    st.caption("Source: Eurostat ROAD_EQR_CARPDA · Annual new registrations")

    try:
        df_eur = read_latest_eurostat_json()
        if df_eur.empty:
            st.info("No Eurostat data yet. Go to Ingestion and run the Eurostat script.")
        else:
            df_ro_eur = df_eur[
                (df_eur["geo"] == "RO") &
                (df_eur["mot_nrg"].isin(["ELC", "PET", "DIE", "ELC_PET_PI", "ELC_DIE_PI"]))
            ].copy()
            df_ro_eur["fuel_label"]    = df_ro_eur["mot_nrg"].map(FUEL_LABELS)
            df_ro_eur["fuel_category"] = df_ro_eur["mot_nrg"].map(FUEL_CATEGORIES)
            df_ro_eur["year"]          = df_ro_eur["time"].astype(int)

            col1, col2 = st.columns(2)

            with col1:
                # Stacked bar: EV vs ICE by category
                df_cat = (
                    df_ro_eur.groupby(["year", "fuel_category"])["value"]
                    .sum().reset_index()
                    .rename(columns={"value": "registrations"})
                )
                df_cat = df_cat[df_cat["fuel_category"] != "Total"]
                fig2 = px.bar(
                    df_cat, x="year", y="registrations", color="fuel_category",
                    barmode="stack",
                    labels={"registrations": "New registrations", "year": "Year", "fuel_category": ""},
                    color_discrete_map=CATEGORY_COLORS,
                    title="By fuel category",
                )
                fig2.update_layout(
                    plot_bgcolor="white", paper_bgcolor="white", font_family="system-ui, -apple-system, sans-serif",
                    hovermode="x unified",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    margin=dict(l=0, r=0, t=48, b=0),
                )
                fig2.update_xaxes(showgrid=False)
                fig2.update_yaxes(gridcolor="#F0F0F0")
                st.plotly_chart(fig2, use_container_width=True)

            with col2:
                # Line: ELC vs PET vs DIE trend
                df_lines = df_ro_eur[df_ro_eur["mot_nrg"].isin(["ELC", "PET", "DIE"])].copy()
                fig3 = px.line(
                    df_lines, x="year", y="value", color="fuel_label",
                    markers=True,
                    labels={"value": "New registrations", "year": "Year", "fuel_label": ""},
                    color_discrete_map={
                        "Electric (BEV)": "#00BFA5",
                        "Petrol (ICE)":   "#EF5350",
                        "Diesel (ICE)":   "#B71C1C",
                    },
                    title="Electric vs Petrol vs Diesel",
                )
                fig3.update_layout(
                    plot_bgcolor="white", paper_bgcolor="white", font_family="system-ui, -apple-system, sans-serif",
                    hovermode="x unified",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    margin=dict(l=0, r=0, t=48, b=0),
                )
                fig3.update_xaxes(showgrid=False)
                fig3.update_yaxes(gridcolor="#F0F0F0")
                st.plotly_chart(fig3, use_container_width=True)

    except Exception as e:
        st.error(f"Could not load Eurostat data: {e}")
