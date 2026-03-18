"""
Count Electric — Streamlit App
Material Design 3 light theme, sidebar navigation.
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
    initial_sidebar_state="expanded",
)

# ── Material Design 3 — light teal theme ─────────────────────────────────────

st.markdown("""
<style>
/* ── Google Fonts — Roboto type scale ── */
@import url('https://fonts.googleapis.com/css2?family=Roboto:ital,wght@0,300;0,400;0,500;0,700;1,400&family=Roboto+Mono:wght@400;500&display=swap');
@import url('https://fonts.googleapis.com/icon?family=Material+Icons+Round');

/* ── MD3 color tokens ── */
:root {
    --md-primary:           #00897B;
    --md-primary-dark:      #00695C;
    --md-primary-container: #B2DFDB;
    --md-on-primary:        #FFFFFF;
    --md-surface:           #FFFFFF;
    --md-surface-variant:   #E0F2F1;
    --md-background:        #F4FBFA;
    --md-on-surface:        #1C1B1F;
    --md-on-surface-variant:#49454F;
    --md-outline:           #CAE6E3;
    --md-shadow:            rgba(0, 105, 92, 0.08);
}

/* ── Global — force Roboto everywhere ── */
html, body, [class*="css"], * {
    font-family: 'Roboto', sans-serif !important;
    -webkit-font-smoothing: antialiased;
}
.stApp {
    background-color: var(--md-background);
    color: var(--md-on-surface);
}

/* ── MD3 type scale ── */
h1 {
    font-family: 'Roboto', sans-serif !important;
    font-size: 1.75rem !important;   /* Display Small — was ~2rem, -2pt */
    font-weight: 400 !important;
    letter-spacing: -0.25px !important;
    color: var(--md-primary-dark) !important;
    line-height: 1.3 !important;
}
h2 {
    font-family: 'Roboto', sans-serif !important;
    font-size: 1.25rem !important;   /* Headline Small */
    font-weight: 500 !important;
    letter-spacing: 0px !important;
    color: var(--md-primary) !important;
    line-height: 1.4 !important;
}
h3 {
    font-family: 'Roboto', sans-serif !important;
    font-size: 1.05rem !important;   /* Title Large */
    font-weight: 500 !important;
    letter-spacing: 0.1px !important;
    color: var(--md-primary) !important;
}
p, li, span, label, div {
    font-size: 14px;
    line-height: 1.6;
    letter-spacing: 0.15px;
    color: var(--md-on-surface);
}

/* ── Sidebar — MD3 Navigation Drawer ── */
[data-testid="stSidebar"] {
    background-color: var(--md-surface);
    border-right: 1px solid var(--md-outline);
}
[data-testid="stSidebar"] label {
    font-size: 14px !important;
    font-weight: 500 !important;
    letter-spacing: 0.1px !important;
    color: var(--md-primary-dark) !important;
    padding: 8px 12px !important;
    border-radius: 28px !important;
    transition: background 0.15s ease;
}
[data-testid="stSidebar"] label:hover {
    background-color: var(--md-surface-variant) !important;
}
[data-testid="stSidebar"] .stRadio div[role="radiogroup"] {
    gap: 2px;
}

/* ── Buttons — MD3 filled ── */
.stButton > button {
    background-color: var(--md-primary) !important;
    color: var(--md-on-primary) !important;
    border: none !important;
    border-radius: 20px !important;
    padding: 10px 28px !important;
    font-family: 'Roboto', sans-serif !important;
    font-size: 14px !important;
    font-weight: 500 !important;
    letter-spacing: 0.1px !important;
    box-shadow: 0 1px 2px var(--md-shadow), 0 2px 6px var(--md-shadow) !important;
    transition: box-shadow 0.2s ease, background-color 0.2s ease !important;
}
.stButton > button:hover {
    background-color: var(--md-primary-dark) !important;
    box-shadow: 0 2px 8px var(--md-shadow), 0 4px 16px var(--md-shadow) !important;
}

/* ── Cards — MD3 Elevated ── */
.md-card {
    background: var(--md-surface);
    border-radius: 16px;
    padding: 24px 28px;
    box-shadow: 0 1px 2px var(--md-shadow), 0 2px 8px var(--md-shadow);
    margin-bottom: 20px;
    border: 1px solid var(--md-outline);
}

/* ── Phase cards ── */
.phase-done    { background:#E8F5E9; color:#1B5E20; padding:14px 16px; border-radius:12px; border-left:4px solid #43A047; font-size:13px; line-height:1.6; }
.phase-current { background:#E8F0FE; color:#1A237E; padding:14px 16px; border-radius:12px; border-left:4px solid #3949AB; font-size:13px; line-height:1.6; }
.phase-planned { background:#FAFAFA; color:#616161; padding:14px 16px; border-radius:12px; border-left:4px solid #BDBDBD; font-size:13px; line-height:1.6; }

/* ── Dataframe ── */
[data-testid="stDataFrame"] {
    border-radius: 12px;
    overflow: hidden;
    border: 1px solid var(--md-outline);
}

/* ── Alerts ── */
[data-testid="stSuccess"],
[data-testid="stInfo"],
[data-testid="stError"],
[data-testid="stWarning"] { border-radius: 12px !important; }

/* ── Divider ── */
hr { border-color: var(--md-outline) !important; margin: 24px 0 !important; }

/* ── Caption / helper text ── */
small, .stCaption, [data-testid="stCaptionContainer"] {
    font-size: 12px !important;
    color: var(--md-on-surface-variant) !important;
    letter-spacing: 0.4px !important;
}

/* ── Code blocks ── */
code { font-family: 'Roboto Mono', monospace !important; font-size: 13px !important; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar navigation ────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("""
<div style="display:flex;align-items:center;gap:10px;margin-bottom:4px;">
  <span class="material-icons-round" style="font-size:32px;color:#00897B;">electric_car</span>
  <span style="font-size:20px;font-weight:500;color:#00695C;">Count Electric</span>
</div>
<p style='color:#78909C;font-size:13px;margin-top:0;'>EV adoption tracker</p>
""", unsafe_allow_html=True)
    st.markdown("---")
    page = st.radio(
        "Navigation",
        options=["About", "Ingestion", "Data Preview"],
        label_visibility="collapsed",
        format_func=lambda x: {
            "About":        "🏠  About",
            "Ingestion":    "⚡  Ingestion",
            "Data Preview": "📊  Data Preview",
        }[x],
    )
    st.markdown("---")
    st.markdown("<p style='color:#B0BEC5;font-size:11px;'>Phase 2 of 5 · Bronze & Silver live</p>", unsafe_allow_html=True)

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
    st.markdown("""
<div style="display:flex;align-items:center;gap:12px;margin-bottom:4px;">
  <span class="material-icons-round" style="font-size:44px;color:#00897B;">electric_car</span>
  <span style="font-size:2.2rem;font-weight:500;color:#00695C;letter-spacing:-0.5px;">Count Electric</span>
</div>
<p style="color:#78909C;font-size:16px;margin-top:0;margin-bottom:24px;">
  Counting the shift from combustion to electric — country by country, year by year.
</p>
""", unsafe_allow_html=True)
    st.markdown("---")

    # Mission
    st.markdown("""
<div class="md-card">
<h3 style="margin-top:0">What is this?</h3>
<p>You've probably noticed more EVs on the street lately. But are they actually taking over, or does it just feel that way?</p>
<p><strong>Count Electric</strong> answers that with data — tracking EV adoption trends globally and comparing them directly against petrol and diesel registrations, so you can see one grow while the other shrinks.</p>
<p>Built as a data engineering portfolio project with a special focus on <strong>Romania</strong>.</p>
</div>
""", unsafe_allow_html=True)

    # Architecture diagram
    st.subheader("Architecture")
    col_diag, _ = st.columns([1, 1])
    with col_diag:
        st.graphviz_chart("""
digraph pipeline {
    rankdir=TB
    bgcolor=transparent
    graph [fontname="Helvetica", splines=ortho, nodesep=0.3, ranksep=0.4]
    node  [fontname="Helvetica", fontsize=10, style="rounded,filled", shape=box, margin="0.2,0.12", width=2]
    edge  [fontname="Helvetica", fontsize=9, color="#78909C", arrowsize=0.7]

    sources    [label="Data Sources\nIEA · Eurostat",             fillcolor="#E3F2FD", color="#1565C0", fontcolor="#0D47A1"]
    ingest     [label="Ingestion · EC2 · Docker",                 fillcolor="#E8F5E9", color="#2E7D32", fontcolor="#1B5E20"]
    s3         [label="AWS S3  landing/raw/",                     fillcolor="#FFF8E1", color="#F57F17", fontcolor="#E65100"]
    databricks [label="Databricks  Bronze→Silver→Gold",           fillcolor="#F3E5F5", color="#6A1B9A", fontcolor="#4A148C"]
    streamlit  [label="Streamlit Dashboard  :8501",               fillcolor="#E0F2F1", color="#00695C", fontcolor="#004D40"]

    sources    -> ingest     [label=" API/CSV "]
    ingest     -> s3         [label=" raw files "]
    s3         -> databricks [label=" Delta Lake "]
    databricks -> streamlit  [label=" Gold (Phase 4) ", style=dashed]
}
""", use_container_width=True)

    st.markdown("---")

    # Tech stack + Data sources side by side
    col1, col2 = st.columns(2)
    with col1:
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

    with col2:
        st.subheader("Data Sources")
        st.markdown("""
| Source | Coverage |
|---|---|
| **IEA Global EV Data** | EV sales & stock, 2010–2024, global incl. Romania |
| **Eurostat ROAD_EQR_CARPDA** | New registrations by fuel type — EV vs ICE |
| **EAFO** | Romania EV fleet detail *(planned)* |
""")
        st.info("**Romania focus:** Country-level data 2013–2024. Bucharest city-level data is not published as open data by DRPCIV.")

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
                font_family="Roboto",
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
                    plot_bgcolor="white", paper_bgcolor="white", font_family="Roboto",
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
                    plot_bgcolor="white", paper_bgcolor="white", font_family="Roboto",
                    hovermode="x unified",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    margin=dict(l=0, r=0, t=48, b=0),
                )
                fig3.update_xaxes(showgrid=False)
                fig3.update_yaxes(gridcolor="#F0F0F0")
                st.plotly_chart(fig3, use_container_width=True)

    except Exception as e:
        st.error(f"Could not load Eurostat data: {e}")
