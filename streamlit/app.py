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
import streamlit.components.v1 as components

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

/* ── Top App Bar ── */
.md3-top-bar {
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    height: 64px;
    background: #FFFFFF;
    box-shadow: 0 1px 3px rgba(0,0,0,0.12), 0 2px 8px rgba(0,0,0,0.06);
    display: flex;
    align-items: center;
    padding: 0 24px;
    gap: 32px;
    z-index: 1000;
}

/* ── Nav links inside top bar ── */
.top-nav {
    display: flex;
    align-items: center;
    gap: 4px;
    margin-left: 16px;
}
.top-nav a {
    text-decoration: none;
    font-size: 14px;
    font-weight: 500;
    color: #546E7A;
    padding: 8px 16px;
    border-radius: 20px;
    transition: background 0.15s, color 0.15s;
    white-space: nowrap;
}
.top-nav a:hover {
    background: #F0FAF9;
    color: #00695C;
}
.top-nav a.active {
    background: #E0F2F1;
    color: #00695C;
}

/* ── Push content below top bar ── */
.main .block-container {
    padding-top: 88px !important;
}

/* ── Hide fullscreen button on all elements ── */
[data-testid="StyledFullScreenButton"],
[data-testid="stElementToolbar"] {
    display: none !important;
}

/* ── Hide Streamlit's default tab bar (driven by HTML nav instead) ── */
[data-baseweb="tab-list"],
[data-baseweb="tab-border"],
[data-baseweb="tab-highlight"] {
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

# ── Top App Bar (visual only — JS drives tab switching below) ────────────────

st.markdown(
    f"""<div class="md3-top-bar">
    <a href="#" data-tab="0" style="text-decoration:none;display:flex;align-items:center;gap:10px;flex-shrink:0">
        {CAR_ICON.format(size=26, color="#00897B")}
        <span style="font-size:1.15rem;font-weight:500;color:#00695C;letter-spacing:-0.3px">Count Electric</span>
    </a>
    <nav class="top-nav">
        <a href="#" data-tab="0" class="active">About</a>
        <a href="#" data-tab="1">Ingestion</a>
        <a href="#" data-tab="2">Data Preview</a>
        <a href="#" data-tab="3">Dashboard</a>
    </nav>
</div>""",
    unsafe_allow_html=True,
)

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
    "ELC_DIE_HYB": "Hybrid diesel", "PET": "Petrol (Combustion)", "DIE": "Diesel (Combustion)",
    "LPG": "LPG", "GAS": "Natural gas", "HYD_FCELL": "Hydrogen", "TOTAL": "Total",
}
FUEL_CATEGORIES = {
    "ELC": "Electric", "ELC_PET_PI": "Electric", "ELC_DIE_PI": "Electric",
    "ELC_PET_HYB": "Hybrid", "ELC_DIE_HYB": "Hybrid",
    "PET": "Combustion", "DIE": "Combustion",
    "LPG": "Other", "GAS": "Other", "HYD_FCELL": "Other", "TOTAL": "Total",
}
CATEGORY_COLORS = {
    "Electric": "#00BFA5", "Combustion": "#EF5350",
    "Hybrid": "#FFA726", "Other": "#B0BEC5", "Total": "#5C6BC0",
}

# ── Pages — all rendered at once, tab bar hidden, JS drives switching ─────────

_tab_about, _tab_ingest, _tab_data, _tab_dash = st.tabs(["About", "Ingestion", "Data Preview", "Dashboard"])

# ── PAGE: ABOUT ───────────────────────────────────────────────────────────────

with _tab_about:
    # Mission
    st.subheader("What is this?")
    st.markdown("""
<div class="md-card" style="padding:16px 24px;margin-bottom:16px">
<p style="margin:0;line-height:2">
You've probably noticed more and more <strong>electric cars</strong> on the street lately — but are they actually taking over?
<strong>Count Electric</strong> answers that with data, tracking electric cars adoption country by country, year by year,
with a special focus on <strong>Romania</strong>.</p>
</div>
""", unsafe_allow_html=True)

    # Architecture — full width
    st.subheader("Architecture")
    st.graphviz_chart("""
digraph pipeline {
    rankdir=LR
    bgcolor=transparent
    graph [fontname="Helvetica", splines=ortho, nodesep=0.2, ranksep=0.5, size="10,1.6"]
    node  [fontname="Helvetica", fontsize=13, style="rounded,filled", shape=box,
           margin="0.15,0.1", width=1.6, fillcolor="#E0F2F1", color="#00897B", fontcolor="#004D40"]
    edge  [fontname="Helvetica", fontsize=10, color="#80CBC4", arrowsize=0.55]

    sources    [label="Data Sources\nIEA · Eurostat"]
    ingest     [label="Ingestion\nEC2 · Docker"]
    s3         [label="AWS S3\nlanding/raw/"]
    databricks [label="Databricks\nBronze→Silver→Gold"]
    streamlit  [label="Streamlit\n:8501"]

    sources    -> ingest     [label="API/CSV"]
    ingest     -> s3         [label="raw files"]
    s3         -> databricks [label="Delta Lake"]
    databricks -> streamlit  [label="Gold Ph.4", style=dashed]
}
""", use_container_width=True)

    # Tech Stack + Databricks concepts side by side
    col_tech, col_db = st.columns(2)

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

    with col_db:
        st.subheader("Databricks Concepts")
        st.markdown("""
| Concept | Used for |
|---|---|
| Serverless compute | Running notebooks without managing clusters |
| Unity Catalog | Data governance and access control |
| External Location | S3 access via IAM cross-account role |
| Delta Lake | ACID-compliant table format |
| Medallion architecture | Bronze → Silver → Gold pipeline |
| Git Folder sync | Auto-deploy notebooks from GitHub |
| Secrets | Storing AWS credentials securely |
| SQL Warehouse | Querying Delta tables |
""")

    st.subheader("Project Roadmap")

    cols = st.columns(5)
    phases = [
        ("done",    "Phase 1",  "Foundation",      "S3 · EC2 · Docker\nGitHub Actions\nDatabricks setup"),
        ("done",    "Phase 2",  "Ingestion",        "IEA · Eurostat\nBronze tables\nSilver tables"),
        ("done",    "Phase 3",  "Transformation",   "Gold layer\nYoY growth\nMarket share"),
        ("current", "Phase 4",  "Dashboard",        "Gold charts\nRomania vs EU\nCountry rank"),
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

with _tab_ingest:
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

with _tab_data:
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
                        "Petrol (Combustion)":   "#EF5350",
                        "Diesel (Combustion)":   "#B71C1C",
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

# ── PAGE: DASHBOARD ───────────────────────────────────────────────────────────

DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH", "")

def _db_connection():
    from databricks import sql
    return sql.connect(
        server_hostname=os.getenv("DATABRICKS_HOST", "").replace("https://", ""),
        http_path=DATABRICKS_HTTP_PATH,
        access_token=os.getenv("DATABRICKS_TOKEN", ""),
    )

@st.cache_data(ttl=3600)
def load_romania_summary() -> pd.DataFrame:
    with _db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT year, electric_registrations, total_registrations,
                       ev_market_share_pct, eu_avg_ev_share_pct, vs_eu_avg_pp,
                       ev_yoy_growth_pct, ev_sales_iea, ev_stock_iea,
                       ev_share_rank, eu_country_total
                FROM gold.romania_ev_summary
                ORDER BY year
            """)
            return pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])

@st.cache_data(ttl=3600)
def load_top10_ev_share() -> pd.DataFrame:
    with _db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT country_code, year, electric_registrations,
                       total_registrations, ev_market_share_pct
                FROM gold.ev_market_share
                WHERE year = (SELECT MAX(year) FROM gold.ev_market_share)
                  AND total_registrations > 1000
                ORDER BY ev_market_share_pct DESC
                LIMIT 10
            """)
            return pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])

with _tab_dash:
    st.title("EV Dashboard")
    st.markdown("Analytics from Databricks Gold Delta tables — cleaned, aggregated, production-ready data.")
    st.markdown("---")

    if not DATABRICKS_HTTP_PATH:
        st.info(
            "**Databricks SQL Warehouse not configured.** "
            "Set the `DATABRICKS_HTTP_PATH` environment variable to enable this dashboard.  \n"
            "Find it in Databricks → **SQL Warehouses** → your warehouse → **Connection Details → HTTP Path**."
        )
    else:
        try:
            df_ro  = load_romania_summary()
            df_top = load_top10_ev_share()
        except Exception as e:
            st.error(f"Could not connect to Databricks: {e}")
            st.stop()

        _layout = dict(
            plot_bgcolor="white", paper_bgcolor="white",
            font_family="system-ui, -apple-system, sans-serif",
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(l=0, r=0, t=48, b=0),
        )

        col_a, col_b = st.columns(2)

        with col_a:
            st.subheader("Romania vs EU Average — EV Market Share")
            st.caption("Source: gold.romania_ev_summary")
            fig1 = go.Figure()
            fig1.add_trace(go.Scatter(
                x=df_ro["year"], y=df_ro["ev_market_share_pct"],
                mode="lines+markers", name="Romania",
                line=dict(color="#00BFA5", width=2), marker=dict(size=6),
                hovertemplate="Romania %{x}: %{y:.2f}%<extra></extra>",
            ))
            fig1.add_trace(go.Scatter(
                x=df_ro["year"], y=df_ro["eu_avg_ev_share_pct"],
                mode="lines+markers", name="EU Average",
                line=dict(color="#5C6BC0", width=2, dash="dash"), marker=dict(size=6),
                hovertemplate="EU Avg %{x}: %{y:.2f}%<extra></extra>",
            ))
            # Annotate the gap in the latest year
            latest = df_ro.dropna(subset=["vs_eu_avg_pp"]).iloc[-1]
            gap_text = f"{latest['vs_eu_avg_pp']:+.1f} pp vs EU" if not pd.isna(latest["vs_eu_avg_pp"]) else ""
            if gap_text:
                fig1.add_annotation(
                    x=latest["year"], y=latest["ev_market_share_pct"],
                    text=gap_text, showarrow=True, arrowhead=2, arrowcolor="#888",
                    ax=40, ay=-30, font=dict(size=11, color="#555"),
                )
            fig1.update_layout(**_layout, yaxis_title="EV Market Share (%)", xaxis_title="Year")
            fig1.update_xaxes(showgrid=False)
            fig1.update_yaxes(gridcolor="#F0F0F0")
            st.plotly_chart(fig1, use_container_width=True)

        with col_b:
            st.subheader("Romania — EV Registrations YoY Growth")
            st.caption("Source: gold.romania_ev_summary")
            df_yoy = df_ro.dropna(subset=["ev_yoy_growth_pct"]).copy()
            colors = ["#00BFA5" if v >= 0 else "#EF5350" for v in df_yoy["ev_yoy_growth_pct"]]
            fig2 = go.Figure(go.Bar(
                x=df_yoy["year"], y=df_yoy["ev_yoy_growth_pct"],
                marker_color=colors,
                hovertemplate="%{x}: %{y:.1f}%<extra></extra>",
            ))
            fig2.add_hline(y=0, line_color="#BDBDBD", line_width=1)
            # Colour legend as annotations
            fig2.add_annotation(
                x=0.01, y=1.08, xref="paper", yref="paper",
                text="▮ Growth", showarrow=False,
                font=dict(size=11, color="#00BFA5"), xanchor="left",
            )
            fig2.add_annotation(
                x=0.15, y=1.08, xref="paper", yref="paper",
                text="▮ Decline", showarrow=False,
                font=dict(size=11, color="#EF5350"), xanchor="left",
            )
            fig2.update_layout(**_layout, yaxis_title="YoY Growth (%)", xaxis_title="Year", showlegend=False)
            fig2.update_xaxes(showgrid=False)
            fig2.update_yaxes(gridcolor="#F0F0F0")
            st.plotly_chart(fig2, use_container_width=True)

        col_c, col_d = st.columns(2)

        with col_c:
            st.subheader("Romania — EU Rank by EV Market Share")
            st.caption("Source: gold.romania_ev_summary · Rank 1 = highest EV share in EU")
            df_rank = df_ro.dropna(subset=["ev_share_rank"]).copy()
            fig3 = go.Figure(go.Scatter(
                x=df_rank["year"], y=df_rank["ev_share_rank"],
                mode="lines+markers", name="EU Rank",
                line=dict(color="#FFA726", width=2), marker=dict(size=8),
                hovertemplate="Year %{x}: Rank #%{y} of %{customdata}<extra></extra>",
                customdata=df_rank["eu_country_total"],
            ))
            # Label each point with its rank
            for _, row in df_rank.iterrows():
                fig3.add_annotation(
                    x=row["year"], y=row["ev_share_rank"],
                    text=f"#{int(row['ev_share_rank'])}",
                    showarrow=False, yshift=14,
                    font=dict(size=10, color="#FFA726"),
                )
            fig3.update_layout(
                **_layout,
                yaxis=dict(title="EU Rank (lower = better)", autorange="reversed", tickformat="d", gridcolor="#F0F0F0"),
                xaxis_title="Year",
            )
            fig3.update_xaxes(showgrid=False)
            st.plotly_chart(fig3, use_container_width=True)

        with col_d:
            st.subheader("Top 10 EU Countries — EV Share of New Cars")
            latest_year = int(df_top["year"].iloc[0]) if not df_top.empty else "N/A"
            st.caption(f"Source: gold.ev_market_share · {latest_year} · new car registrations · min 1 000 cars")

            # If Romania isn't already in top 10, append it from the Romania summary
            df_chart = df_top.copy()
            if "RO" not in df_chart["country_code"].values and not df_ro.empty:
                ro_latest = df_ro.dropna(subset=["ev_market_share_pct"]).iloc[-1]
                df_chart = pd.concat([df_chart, pd.DataFrame([{
                    "country_code": "RO",
                    "year": int(ro_latest["year"]),
                    "ev_market_share_pct": ro_latest["ev_market_share_pct"],
                }])], ignore_index=True)

            df_top_s = df_chart.sort_values("ev_market_share_pct", ascending=True)

            def bar_color(code):
                if code == "RO":
                    return "#1565C0"   # blue — Romania
                return "#00BFA5"       # teal — top 10

            fig4 = go.Figure(go.Bar(
                x=df_top_s["ev_market_share_pct"],
                y=df_top_s["country_code"],
                orientation="h",
                marker_color=[bar_color(c) for c in df_top_s["country_code"]],
                hovertemplate="%{y}: %{x:.1f}% of new cars<extra></extra>",
                text=[f"{v:.1f}%" for v in df_top_s["ev_market_share_pct"]],
                textposition="outside",
                textfont=dict(size=11),
            ))
            fig4.add_annotation(
                x=0.99, y=0.01, xref="paper", yref="paper",
                text="▮ Romania", showarrow=False,
                font=dict(size=11, color="#1565C0"), xanchor="right",
            )
            fig4.update_layout(**_layout, xaxis_title="EV Share of New Cars (%)", showlegend=False)
            fig4.update_layout(hovermode="y unified")
            fig4.update_xaxes(gridcolor="#F0F0F0", range=[0, df_top_s["ev_market_share_pct"].max() * 1.18])
            fig4.update_yaxes(showgrid=False)
            st.plotly_chart(fig4, use_container_width=True)

# ── JS: connect HTML nav clicks to hidden Streamlit tab buttons ───────────────
# Streamlit tab switching is purely client-side (no rerun). We find the hidden
# [data-baseweb="tab"] buttons in the parent window and click them when a nav
# link is clicked. Active pill state is also updated client-side.

components.html("""
<script>
(function () {
    var doc = window.parent.document;

    function switchTab(index) {
        var tabs = doc.querySelectorAll('[data-baseweb="tab"]');
        if (tabs[index]) tabs[index].click();

        // Update active pill in the nav
        doc.querySelectorAll('.top-nav a').forEach(function (a, i) {
            a.classList.toggle('active', i === index);
        });
    }

    function attachListeners() {
        // Nav links
        var navLinks = doc.querySelectorAll('.top-nav a');
        if (!navLinks.length) return false;

        navLinks.forEach(function (a) {
            if (a._ce) return;
            a._ce = true;
            a.addEventListener('click', function (e) {
                e.preventDefault();
                switchTab(parseInt(a.getAttribute('data-tab'), 10));
            });
        });

        // Logo → always About (tab 0)
        var logo = doc.querySelector('.md3-top-bar > a');
        if (logo && !logo._ce) {
            logo._ce = true;
            logo.addEventListener('click', function (e) {
                e.preventDefault();
                switchTab(0);
            });
        }

        return true;
    }

    // Retry until elements exist (Streamlit renders async)
    var attempts = 0;
    var timer = setInterval(function () {
        if (attachListeners() || ++attempts > 100) clearInterval(timer);
    }, 50);
}());
</script>
""", height=0)
