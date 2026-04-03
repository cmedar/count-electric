"""
Count Electric — Streamlit App (dev)
Apple casual but distinctive — DM Serif Display headlines, DM Sans body, IBM Plex Mono data.
"""

import json
import os
import sys
import time
from itertools import product

import boto3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
import streamlit.components.v1 as components

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

S3_BUCKET = os.getenv("S3_BUCKET", "count-electric")

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Count Electric",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Theme ─────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;1,9..40,300&family=DM+Serif+Display:ital@0;1&family=IBM+Plex+Mono:wght@400;500&display=swap');

:root {
    --teal:          #00897B;
    --teal-dark:     #00695C;
    --teal-mid:      #00796B;
    --teal-light:    #00BFA5;
    --teal-surface:  #DCF0EE;
    --teal-faint:    #F0FAF9;
    --bg:            #F8FAF9;
    --surface:       #FFFFFF;
    --text:          #111827;
    --text-2:        #6B7280;
    --text-3:        #9CA3AF;
    --border:        rgba(0,0,0,0.07);
    --border-strong: rgba(0,0,0,0.12);
    --radius-sm:     10px;
    --radius-md:     16px;
    --radius-lg:     24px;
    --shadow-sm:     0 1px 2px rgba(0,0,0,0.04), 0 2px 6px rgba(0,0,0,0.04);
    --shadow-md:     0 2px 8px rgba(0,0,0,0.06), 0 8px 24px rgba(0,0,0,0.05);
}

/* ── Global ── */
html, body, [class*="css"] {
    font-family: 'DM Sans', -apple-system, BlinkMacSystemFont, sans-serif;
    font-size: 15px;
    color: var(--text);
    -webkit-font-smoothing: antialiased;
}
.stApp { background-color: var(--bg); }

/* ── Hide Streamlit chrome ── */
header[data-testid="stHeader"],
[data-testid="stSidebar"],
[data-testid="stSidebarCollapsedControl"],
[data-testid="StyledFullScreenButton"],
[data-testid="stElementToolbar"],
[data-baseweb="tab-list"],
[data-baseweb="tab-border"],
[data-baseweb="tab-highlight"] { display: none !important; }

/* ── Top bar ── */
.ce-top-bar {
    position: fixed;
    top: 0; left: 0; right: 0;
    height: 60px;
    background: rgba(255,255,255,0.88);
    backdrop-filter: blur(16px);
    -webkit-backdrop-filter: blur(16px);
    border-bottom: 1px solid var(--border);
    display: flex;
    align-items: center;
    padding: 0 40px;
    z-index: 1000;
}
.ce-logo {
    text-decoration: none !important;
    display: flex;
    align-items: center;
    gap: 10px;
    flex-shrink: 0;
    margin-right: auto;
}
.ce-logo * { text-decoration: none !important; }
.ce-logo-mark {
    width: 32px; height: 32px;
    background: var(--teal);
    border-radius: 9px;
    display: flex; align-items: center; justify-content: center;
    box-shadow: 0 1px 4px rgba(0,137,123,0.3);
}
.ce-logo-text {
    font-family: 'DM Sans', sans-serif;
    font-size: 1.15rem;
    font-weight: 500;
    color: var(--teal-dark);
    letter-spacing: -0.3px;
    text-decoration: none;
}
.ce-nav {
    display: flex;
    align-items: center;
    gap: 2px;
}
.ce-nav a {
    text-decoration: none;
    font-family: 'DM Sans', sans-serif;
    font-size: 14px;
    font-weight: 500;
    color: var(--text-2);
    padding: 6px 14px;
    border-radius: 8px;
    transition: background 0.12s ease, color 0.12s ease;
    white-space: nowrap;
    letter-spacing: -0.1px;
}
.ce-nav a:hover  { background: var(--teal-faint); color: var(--teal-dark); }
.ce-nav a.active { background: var(--teal-surface); color: var(--teal-dark); font-weight: 600; }

/* ── Content ── */
.main .block-container {
    padding-top: 68px !important;
    padding-left: 40px !important;
    padding-right: 40px !important;
    max-width: 1280px;
}

/* ── Typography ── */
h1, h2, h3 {
    font-family: 'DM Sans', sans-serif;
    font-weight: 600;
    letter-spacing: -0.5px;
}
h1 { color: var(--teal-dark); font-size: 1.6rem; }
h2 { color: var(--text);      font-size: 1.25rem; }
h3 { color: var(--text);      font-size: 1.05rem; }

/* ── Hero ── */
.ce-hero { padding: 24px 0 20px; }
.ce-eyebrow {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 11.5px;
    font-weight: 500;
    color: var(--teal);
    letter-spacing: 0.1em;
    text-transform: uppercase;
    margin: 0 0 14px;
}
.ce-hero-title {
    font-family: 'DM Serif Display', Georgia, serif;
    font-style: normal;
    font-size: clamp(2.2rem, 4.5vw, 3.2rem);
    line-height: 1.15;
    color: var(--text);
    letter-spacing: -0.02em;
    margin: 0 0 16px;
    max-width: 700px;
}
.ce-hero-sub {
    font-size: 1.05rem;
    font-weight: 400;
    color: var(--text-2);
    line-height: 1.65;
    max-width: 580px;
    margin: 0 0 28px;
}

/* ── Stats row ── */
.ce-stats {
    display: flex;
    border-top: 1px solid var(--border);
    border-bottom: 1px solid var(--border);
    padding: 24px 0;
    margin-bottom: 48px;
}
.ce-stat { flex: 1; padding-right: 32px; }
.ce-stat + .ce-stat { padding-left: 32px; border-left: 1px solid var(--border); }
.ce-stat-number {
    font-family: 'DM Serif Display', serif;
    font-size: 2.6rem;
    color: var(--teal-dark);
    line-height: 1;
    margin-bottom: 5px;
    letter-spacing: -0.02em;
}
.ce-stat-label {
    font-size: 12.5px;
    color: var(--text-3);
    font-weight: 400;
}

/* ── Section label ── */
.ce-section-label {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 11px;
    font-weight: 500;
    color: var(--text-3);
    letter-spacing: 0.1em;
    text-transform: uppercase;
    margin: 0 0 18px;
    display: block;
}

/* ── Generic card ── */
.ce-card {
    background: var(--surface);
    border-radius: var(--radius-md);
    border: 1px solid var(--border);
    padding: 28px;
    box-shadow: var(--shadow-sm);
}

/* ── Tech badge row ── */
.ce-badges { display: flex; flex-wrap: wrap; gap: 8px; margin: 14px 0 4px; }
.ce-badge {
    font-family: 'DM Sans', sans-serif;
    font-size: 13px;
    font-weight: 500;
    color: var(--teal-dark);
    background: var(--teal-faint);
    border: 1px solid var(--teal-surface);
    border-radius: 8px;
    padding: 5px 13px;
    white-space: nowrap;
}
.ce-badge-mono {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 12px;
    font-weight: 400;
    color: var(--text-2);
    background: #F5F5F5;
    border: 1px solid var(--border);
    border-radius: 7px;
    padding: 4px 10px;
    white-space: nowrap;
}

/* ── Roadmap cards ── */
.ce-phase {
    background: var(--surface);
    border-radius: var(--radius-md);
    border: 1px solid var(--border);
    border-top-width: 3px;
    padding: 20px;
    box-shadow: var(--shadow-sm);
    height: 100%;
}
.ce-phase-done    { border-top-color: #22C55E; }
.ce-phase-current { border-top-color: var(--teal-light); }
.ce-phase-planned { border-top-color: #E5E7EB; }
.ce-phase-label {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 10.5px;
    color: var(--text-3);
    letter-spacing: 0.08em;
    text-transform: uppercase;
    margin-bottom: 5px;
}
.ce-phase-title {
    font-family: 'DM Sans', sans-serif;
    font-size: 14.5px;
    font-weight: 600;
    color: var(--text);
    margin-bottom: 8px;
}
.ce-phase-items { font-size: 12.5px; color: var(--text-2); line-height: 1.65; }

/* ── Source cards (ingestion) ── */
.ce-source-card {
    background: var(--surface);
    border-radius: var(--radius-md);
    border: 1px solid var(--border);
    padding: 24px;
    box-shadow: var(--shadow-sm);
    transition: box-shadow 0.15s ease;
    height: 280px;
}
.ce-source-card:hover { box-shadow: var(--shadow-md); }
.ce-source-icon {
    width: 40px; height: 40px;
    border-radius: 10px;
    background: var(--teal-faint);
    border: 1px solid var(--teal-surface);
    display: flex; align-items: center; justify-content: center;
    font-size: 19px;
    margin-bottom: 14px;
}
.ce-source-badge {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 10.5px;
    color: var(--teal);
    background: var(--teal-faint);
    border: 1px solid var(--teal-surface);
    border-radius: 6px;
    padding: 2px 8px;
    margin-bottom: 12px;
    display: inline-block;
    letter-spacing: 0.04em;
}
.ce-source-title {
    font-family: 'DM Sans', sans-serif;
    font-size: 15px;
    font-weight: 600;
    color: var(--text);
    margin-bottom: 7px;
    line-height: 1.3;
}
.ce-source-desc {
    font-size: 13.5px;
    color: var(--text-2);
    line-height: 1.6;
    margin-bottom: 14px;
}
.ce-s3-path {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 11px;
    color: var(--text-3);
    background: #F5F5F5;
    border-radius: 6px;
    padding: 5px 10px;
    display: block;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    margin-bottom: 18px;
}

/* ── Pipeline section ── */
.ce-pipeline-header {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 16px;
}
.ce-pipeline-title {
    font-family: 'DM Sans', sans-serif;
    font-size: 15px;
    font-weight: 600;
    color: var(--text);
}
.ce-pipeline-subtitle {
    font-size: 13px;
    color: var(--text-2);
    margin-bottom: 20px;
    line-height: 1.5;
}

/* ── S3 file table ── */
[data-testid="stDataFrame"] {
    border-radius: var(--radius-sm) !important;
    overflow: hidden;
    border: 1px solid var(--border) !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 12px !important;
}

/* ── Buttons ── */
.stButton > button {
    background: var(--teal);
    color: #FFFFFF;
    border: none;
    border-radius: var(--radius-sm);
    padding: 10px 20px;
    font-family: 'DM Sans', sans-serif;
    font-size: 14px;
    font-weight: 500;
    letter-spacing: -0.1px;
    box-shadow: 0 1px 2px rgba(0,0,0,0.1), 0 2px 8px rgba(0,137,123,0.18);
    transition: all 0.15s ease;
}
.stButton > button:hover {
    background: var(--teal-mid);
    box-shadow: 0 2px 10px rgba(0,137,123,0.28);
    transform: translateY(-1px);
}
.stButton > button:active {
    background: var(--teal-dark);
    box-shadow: none;
    transform: translateY(0);
}

/* ── Alerts ── */
[data-testid="stSuccess"],
[data-testid="stInfo"],
[data-testid="stError"],
[data-testid="stWarning"] {
    border-radius: var(--radius-sm);
    font-family: 'DM Sans', sans-serif;
}

/* ── Caption ── */
.stCaption { color: var(--text-3); font-family: 'DM Sans', sans-serif; font-size: 12.5px; }

/* ── Divider ── */
hr { border-color: var(--border); }

/* ── Mobile ── */
@media (max-width: 768px) {
    .ce-top-bar { height: auto; min-height: 52px; flex-wrap: wrap; padding: 8px 16px; gap: 0; }
    .ce-logo-text { display: none; }
    .ce-nav { width: 100%; overflow-x: auto; -webkit-overflow-scrolling: touch;
               margin-left: 0; padding: 6px 0; scrollbar-width: none; }
    .ce-nav::-webkit-scrollbar { display: none; }
    .ce-nav a { font-size: 13px; padding: 5px 12px; flex-shrink: 0; }
    .main .block-container {
        padding-top: 116px !important;
        padding-left: 16px !important;
        padding-right: 16px !important;
    }
    .ce-hero { padding: 28px 0 20px; }
    .ce-hero-title { font-size: 2rem; }
    .ce-stats { flex-wrap: wrap; }
    .ce-stat { flex: 0 0 48%; padding: 14px 0; border: none !important;
               border-bottom: 1px solid var(--border) !important; }
}
</style>
""", unsafe_allow_html=True)

# ── Top bar ───────────────────────────────────────────────────────────────────

st.markdown("""
<div class="ce-top-bar">
    <a href="#" data-tab="0" class="ce-logo">
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="26" height="26" fill="#00897B">
            <path d="M18.92 6.01C18.72 5.42 18.16 5 17.5 5h-11c-.66 0-1.21.42-1.42 1.01L3 12v8c0 .55.45 1 1 1h1c.55 0 1-.45 1-1v-1h12v1c0 .55.45 1 1 1h1c.55 0 1-.45 1-1v-8l-2.08-5.99zM6.85 7h10.29l1.08 3H5.77L6.85 7zM19 17H5v-5h14v5z"/>
            <circle cx="7.5" cy="14.5" r="1.5"/><circle cx="16.5" cy="14.5" r="1.5"/>
        </svg>
        <span class="ce-logo-text">Count Electric</span>
    </a>
    <nav class="ce-nav">
        <a href="#" data-tab="0" class="active">About</a>
        <a href="#" data-tab="1">Ingestion</a>
        <a href="#" data-tab="2">Dashboard</a>
    </nav>
</div>
""", unsafe_allow_html=True)

# ── Helpers ───────────────────────────────────────────────────────────────────

def s3():
    return boto3.client("s3")

def _ytitle(fig, text, color="#546E7A"):
    """Show y-axis tick labels on both left and right; title annotation on the left."""
    fig.update_yaxes(title_text="")
    fig.add_trace(go.Scatter(x=[], y=[], yaxis="y2", showlegend=False, hoverinfo="skip"))
    fig.update_layout(
        yaxis2=dict(
            overlaying="y", side="right", matches="y",
            showticklabels=True, showgrid=False,
            title_text="", showline=False, zeroline=False,
        )
    )
    fig.add_annotation(
        text=text, xref="paper", yref="paper",
        x=-0.12, y=0.5, textangle=-90, showarrow=False,
        font=dict(size=18, color=color), xanchor="center", yanchor="middle",
    )

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
    "ELC": "Electric", "ELC_PET_PI": "Plug-in hybrid (PHEV)",
    "ELC_DIE_PI": "Plug-in hybrid diesel", "ELC_PET_HYB": "Hybrid (non-plug-in)",
    "ELC_DIE_HYB": "Hybrid diesel", "PET": "Petrol", "DIE": "Diesel",
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

# ── Tabs (3 — no Data Preview) ────────────────────────────────────────────────

_tab_about, _tab_ingest, _tab_dash = st.tabs(["About", "Ingestion", "Dashboard"])

# ── PAGE: ABOUT ───────────────────────────────────────────────────────────────

with _tab_about:

    # Hero
    st.markdown("""
<div class="ce-hero">
    <p class="ce-eyebrow">Data Engineering &nbsp;·&nbsp; Portfolio Project</p>
    <h1 class="ce-hero-title">Counting electric cars<br>on the streets, so you<br>don't have to.</h1>
    <p class="ce-hero-sub">Data pipeline tracking electric cars numbers across Europe — raw API data ingested to AWS S3, processed through a Databricks medallion architecture, and served as a live dashboard.</p>
    <div class="ce-stats">
        <div class="ce-stat">
            <div class="ce-stat-number">3</div>
            <div class="ce-stat-label">Data sources</div>
        </div>
        <div class="ce-stat">
            <div class="ce-stat-number">9</div>
            <div class="ce-stat-label">Databricks notebooks</div>
        </div>
        <div class="ce-stat">
            <div class="ce-stat-number">27</div>
            <div class="ce-stat-label">EU countries tracked</div>
        </div>
        <div class="ce-stat">
            <div class="ce-stat-number">15y</div>
            <div class="ce-stat-label">Historical data (2010–2024)</div>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

    # Architecture diagram
    st.markdown('<span class="ce-section-label">Architecture</span>', unsafe_allow_html=True)
    st.graphviz_chart("""
digraph pipeline {
    rankdir=LR
    bgcolor=transparent
    graph [fontname="Helvetica", splines=ortho, nodesep=0.25, ranksep=0.6]
    node  [fontname="Helvetica", fontsize=13, style="rounded,filled", shape=box,
           margin="0.2,0.12", width=1.7, fillcolor="#E0F2F1", color="#00897B", fontcolor="#004D40"]
    edge  [fontname="Helvetica", fontsize=10, color="#80CBC4", arrowsize=0.55]

    sources    [label="Data Sources\nIEA · Eurostat"]
    ingest     [label="Ingestion\nEC2 · Docker"]
    s3         [label="Landing Zone\nAWS S3"]
    databricks [label="Databricks\nBronze → Silver → Gold"]
    s3gold     [label="Gold Parquet\nAWS S3"]
    streamlit  [label="Dashboard\nStreamlit"]

    sources    -> ingest     [label="API"]
    ingest     -> s3         [label="raw files"]
    s3         -> databricks [label="Delta Lake"]
    databricks -> s3gold     [label="export"]
    s3gold     -> streamlit  [label="pd.read_parquet"]
}
""", use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)

    col_tech, col_db = st.columns(2)

    with col_tech:
        st.markdown('<span class="ce-section-label">Tech Stack</span>', unsafe_allow_html=True)
        st.markdown("""
<div class="ce-badges">
<span class="ce-badge-mono">Python 3.11</span>
<span class="ce-badge-mono">AWS S3</span>
<span class="ce-badge-mono">AWS EC2</span>
<span class="ce-badge-mono">Docker Compose</span>
<span class="ce-badge-mono">Databricks Serverless</span>
<span class="ce-badge-mono">Apache Spark</span>
<span class="ce-badge-mono">Delta Lake</span>
<span class="ce-badge-mono">Unity Catalog</span>
<span class="ce-badge-mono">Streamlit</span>
<span class="ce-badge-mono">Cloudflare Tunnel</span>
<span class="ce-badge-mono">GitHub Actions</span>
</div>
""", unsafe_allow_html=True)

    with col_db:
        st.markdown('<span class="ce-section-label">Databricks Concepts</span>', unsafe_allow_html=True)
        st.markdown("""
<div class="ce-badges">
<span class="ce-badge-mono">serverless compute</span>
<span class="ce-badge-mono">unity catalog</span>
<span class="ce-badge-mono">external location</span>
<span class="ce-badge-mono">delta lake</span>
<span class="ce-badge-mono">medallion architecture</span>
<span class="ce-badge-mono">groupBy + agg</span>
<span class="ce-badge-mono">pivot</span>
<span class="ce-badge-mono">window functions</span>
<span class="ce-badge-mono">lag + rank</span>
<span class="ce-badge-mono">create_map</span>
<span class="ce-badge-mono">OPTIMIZE + ZORDER</span>
<span class="ce-badge-mono">jobs api</span>
</div>
""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<span class="ce-section-label">Project Roadmap</span>', unsafe_allow_html=True)

    cols = st.columns(5)
    phases = [
        ("done",    "Phase 1", "Foundation",      "S3 · EC2 · Docker · GitHub Actions · Databricks setup"),
        ("done",    "Phase 2", "Ingestion",        "IEA · Eurostat · Bronze tables · Silver tables"),
        ("done",    "Phase 3", "Transformation",   "Gold layer · YoY growth · Market share · Window fns"),
        ("done",    "Phase 4", "Dashboard",        "Gold charts · Romania vs EU · Fleet snapshot · Jobs API"),
        ("planned", "Phase 5", "Polish",           "Screenshots · Portfolio write-up"),
    ]
    for col, (status, phase, title, detail) in zip(cols, phases):
        with col:
            css = {"done": "ce-phase-done", "current": "ce-phase-current", "planned": "ce-phase-planned"}[status]
            icon = "✓" if status == "done" else ("→" if status == "current" else "○")
            st.markdown(f"""
<div class="ce-phase {css}">
<div class="ce-phase-label">{icon} {phase}</div>
<div class="ce-phase-title">{title}</div>
<div class="ce-phase-items">{detail.replace(" · ", "<br>")}</div>
</div>""", unsafe_allow_html=True)

# ── DATABRICKS PIPELINE HELPERS ───────────────────────────────────────────────

DATABRICKS_REPO_PATH = os.getenv("DATABRICKS_REPO_PATH", "")

_PIPELINE_STEPS = [
    ("Bronze — IEA",                    "databricks/bronze/01_bronze_iea"),
    ("Bronze — Eurostat Registrations", "databricks/bronze/02_bronze_eurostat"),
    ("Bronze — Eurostat Stock",         "databricks/bronze/03_bronze_eurostat_stock"),
    ("Silver — IEA",                    "databricks/silver/01_silver_iea"),
    ("Silver — Eurostat Registrations", "databricks/silver/02_silver_eurostat"),
    ("Silver — Eurostat Stock",         "databricks/silver/03_silver_eurostat_stock"),
    ("Gold — EV Market Share",          "databricks/gold/01_gold_market_share"),
    ("Gold — Romania Summary",          "databricks/gold/02_gold_romania"),
    ("Gold — Stock Snapshot",           "databricks/gold/03_gold_stock_snapshot"),
]


def _db_submit_notebook(notebook_path: str) -> int:
    host  = os.getenv("DATABRICKS_HOST", "").rstrip("/")
    token = os.getenv("DATABRICKS_TOKEN", "")
    resp  = requests.post(
        f"{host}/api/2.1/jobs/runs/submit",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "run_name": f"count-electric: {notebook_path.split('/')[-1]}",
            "tasks": [{
                "task_key": "run",
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "source": "WORKSPACE",
                },
            }],
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["run_id"]


def _db_run_status(run_id: int) -> tuple[str, str]:
    host  = os.getenv("DATABRICKS_HOST", "").rstrip("/")
    token = os.getenv("DATABRICKS_TOKEN", "")
    resp  = requests.get(
        f"{host}/api/2.1/jobs/runs/get",
        headers={"Authorization": f"Bearer {token}"},
        params={"run_id": run_id},
        timeout=30,
    )
    resp.raise_for_status()
    state = resp.json()["state"]
    return state["life_cycle_state"], state.get("result_state", "")


# ── PAGE: INGESTION ───────────────────────────────────────────────────────────

with _tab_ingest:

    st.markdown("""
<div style="padding: 16px 0 20px">
    <p class="ce-eyebrow">Data Ingestion</p>
    <h1 style="font-family:'DM Serif Display',serif;font-style:italic;font-size:2rem;
               color:#111827;letter-spacing:-0.02em;margin:0 0 10px;font-weight:400">
        Fetch, land, transform.
    </h1>
    <p style="font-size:1rem;color:#6B7280;line-height:1.6;max-width:520px;margin:0">
        Pull the latest data from source APIs to S3, then run the full
        Bronze&nbsp;→&nbsp;Silver&nbsp;→&nbsp;Gold pipeline on Databricks.
    </p>
</div>
""", unsafe_allow_html=True)

    # ── Source cards ──────────────────────────────────────────────────────────
    st.markdown('<span class="ce-section-label">Data Sources</span>', unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("""
<div class="ce-source-card">
    <div class="ce-source-icon">🌍</div>
    <span class="ce-source-badge">IEA · CSV API</span>
    <div class="ce-source-title">IEA Global EV Data</div>
    <div class="ce-source-desc">Annual EV sales and cumulative stock figures by country
    and powertrain (BEV / PHEV). The global benchmark dataset — 50+ countries, 2010 to present.</div>
    <code class="ce-s3-path">s3://count-electric/landing/raw/iea/</code>
</div>
""", unsafe_allow_html=True)
        if st.button("Run IEA Ingestion", use_container_width=True, key="btn_iea"):
            with st.spinner("Fetching from IEA API…"):
                try:
                    from ingestion.ingest_iea import main as run_iea
                    run_iea()
                    st.cache_data.clear()
                    st.success("IEA ingestion complete.")
                except Exception as e:
                    st.error(f"Failed: {e}")

    with col2:
        st.markdown("""
<div class="ce-source-card">
    <div class="ce-source-icon">🇪🇺</div>
    <span class="ce-source-badge">Eurostat · ROAD_EQR_CARPDA</span>
    <div class="ce-source-title">New Car Registrations</div>
    <div class="ce-source-desc">New passenger car registrations by fuel type across all EU member states.
    The definitive source for Electric vs Petrol vs Diesel market share trends year by year.</div>
    <code class="ce-s3-path">s3://count-electric/landing/raw/eurostat/</code>
</div>
""", unsafe_allow_html=True)
        if st.button("Run Eurostat Ingestion", use_container_width=True, key="btn_eurostat"):
            with st.spinner("Fetching from Eurostat API…"):
                try:
                    from ingestion.ingest_eurostat import main as run_eurostat
                    run_eurostat()
                    st.cache_data.clear()
                    st.success("Eurostat ingestion complete.")
                except Exception as e:
                    st.error(f"Failed: {e}")

    with col3:
        st.markdown("""
<div class="ce-source-card">
    <div class="ce-source-icon">📊</div>
    <span class="ce-source-badge">Eurostat · ROAD_EQS_CARPDA</span>
    <div class="ce-source-title">Total Fleet on the Road</div>
    <div class="ce-source-desc">All cars currently registered, not just new sales.
    Shows how slowly the full fleet transitions — electric cars may be 10% of new sales but 1–2% of all cars on the road.</div>
    <code class="ce-s3-path">s3://count-electric/landing/raw/eurostat_stock/</code>
</div>
""", unsafe_allow_html=True)
        if st.button("Run Eurostat Stock Ingestion", use_container_width=True, key="btn_stock"):
            with st.spinner("Fetching from Eurostat API…"):
                try:
                    from ingestion.ingest_eurostat_stock import main as run_eurostat_stock
                    run_eurostat_stock()
                    st.cache_data.clear()
                    st.success("Eurostat stock ingestion complete.")
                except Exception as e:
                    st.error(f"Failed: {e}")

    # ── S3 landing zone (compact) ─────────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<span class="ce-section-label">S3 Landing Zone</span>', unsafe_allow_html=True)

    try:
        all_files = list_s3_files("landing/raw/")
        if not all_files:
            st.info("No files yet. Run an ingestion script above.")
        else:
            rows = [
                {
                    "Source":      f["Key"].split("/")[2].upper(),
                    "File":        f["Key"].split("/")[-1],
                    "Size (KB)":   round(f["Size"] / 1024, 1),
                    "Ingested":    f["LastModified"].strftime("%Y-%m-%d %H:%M UTC"),
                }
                for f in sorted(all_files, key=lambda x: x["LastModified"], reverse=True)[:20]
            ]
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True, height=240)
            st.caption(f"{len(all_files)} file(s) total · showing 20 most recent · s3://{S3_BUCKET}/landing/raw/")
    except Exception as e:
        st.error(f"Could not connect to S3: {e}")

    # ── Databricks pipeline ───────────────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<span class="ce-section-label">Databricks Pipeline</span>', unsafe_allow_html=True)
    st.markdown("""
<p class="ce-pipeline-subtitle">
Runs all 9 notebooks sequentially — Bronze&nbsp;→&nbsp;Silver&nbsp;→&nbsp;Gold — via the Databricks Jobs API.
Each notebook is submitted as a one-off run and polled until it completes.
The dashboard cache is cleared automatically on success.
</p>
""", unsafe_allow_html=True)

    if not DATABRICKS_REPO_PATH:
        st.info(
            "**`DATABRICKS_REPO_PATH` not configured.** "
            "Add it to your environment (e.g. `/Workspace/Users/you@email.com/count-electric`). "
            "Find it in Databricks → Workspace → right-click your folder → Copy path."
        )
    else:
        if st.button("▶ Run Full Pipeline", use_container_width=False, type="primary"):
            step_placeholders = [st.empty() for _ in _PIPELINE_STEPS]
            failed = False
            for i, (label, rel_path) in enumerate(_PIPELINE_STEPS):
                if failed:
                    step_placeholders[i].markdown(f"⬜ {label}")
                    continue
                notebook_path = f"{DATABRICKS_REPO_PATH.rstrip('/')}/{rel_path}"
                step_placeholders[i].markdown(f"⏳ **{label}** — submitting…")
                try:
                    run_id = _db_submit_notebook(notebook_path)
                except Exception as e:
                    step_placeholders[i].markdown(f"❌ **{label}** — submit failed: {e}")
                    failed = True
                    continue
                while True:
                    lc, result = _db_run_status(run_id)
                    step_placeholders[i].markdown(f"⏳ **{label}** — {lc.lower().replace('_', ' ')}")
                    if lc in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                        break
                    time.sleep(6)
                if result == "SUCCESS":
                    step_placeholders[i].markdown(f"✅ **{label}**")
                else:
                    step_placeholders[i].markdown(f"❌ **{label}** — {result or lc}")
                    failed = True
            if not failed:
                st.success("Pipeline complete. Clearing cache…")
                st.cache_data.clear()
            else:
                st.error("Pipeline stopped — fix the failing notebook and re-run.")

# ── JS: connect nav to hidden Streamlit tabs ──────────────────────────────────

components.html("""
<script>
(function () {
    var doc = window.parent.document;

    function switchTab(index) {
        var tabs = doc.querySelectorAll('[data-baseweb="tab"]');
        if (tabs[index]) tabs[index].click();
        doc.querySelectorAll('.ce-nav a').forEach(function (a, i) {
            a.classList.toggle('active', i === index);
        });
    }

    function attachListeners() {
        var navLinks = doc.querySelectorAll('.ce-nav a');
        if (!navLinks.length) return false;
        navLinks.forEach(function (a) {
            if (a._ce) return;
            a._ce = true;
            a.addEventListener('click', function (e) {
                e.preventDefault();
                switchTab(parseInt(a.getAttribute('data-tab'), 10));
            });
        });
        var logo = doc.querySelector('.ce-logo');
        if (logo && !logo._ce) {
            logo._ce = true;
            logo.addEventListener('click', function (e) { e.preventDefault(); switchTab(0); });
        }
        return true;
    }

    var attempts = 0;
    var timer = setInterval(function () {
        if (attachListeners() || ++attempts > 100) clearInterval(timer);
    }, 50);
}());
</script>
""", height=0)

# ── PAGE: DASHBOARD ───────────────────────────────────────────────────────────

def _s3_parquet(key: str) -> pd.DataFrame:
    return pd.read_parquet(f"s3://{S3_BUCKET}/gold/{key}/")

@st.cache_data(ttl=None)
def load_ev_market_share() -> pd.DataFrame:
    return _s3_parquet("ev_market_share")

@st.cache_data(ttl=None)
def load_romania_summary() -> pd.DataFrame:
    return _s3_parquet("romania_ev_summary").sort_values("year")

@st.cache_data(ttl=None)
def load_top10_ev_share() -> pd.DataFrame:
    df = load_ev_market_share()
    latest = df["year"].max()
    return (
        df[(df["year"] == latest) & (df["total_registrations"] > 1000)]
        .nlargest(10, "ev_market_share_pct")
    )

@st.cache_data(ttl=None)
def load_romania_registrations() -> pd.DataFrame:
    return (
        load_ev_market_share()
        .pipe(lambda d: d[d["country_code"] == "RO"])
        [["year", "electric_registrations", "ice_registrations", "total_registrations"]]
        .sort_values("year")
    )

@st.cache_data(ttl=None)
def load_eu_latest_ev_combustion() -> pd.DataFrame:
    df = load_ev_market_share()
    latest = df["year"].max()
    return (
        df[(df["year"] == latest) & (df["total_registrations"] > 1000)]
        [["country_code", "electric_registrations", "ice_registrations",
          "total_registrations", "ev_market_share_pct"]]
        .sort_values("country_code")
    )

@st.cache_data(ttl=None)
def load_stock_snapshot() -> pd.DataFrame:
    return (
        _s3_parquet("car_stock_snapshot")
        .pipe(lambda d: d[d["country_code"] == "RO"])
        .sort_values("year")
    )

with _tab_dash:

    st.markdown("""
<div style="padding: 16px 0 20px">
    <p class="ce-eyebrow">Live Analytics</p>
    <h1 style="font-family:'DM Serif Display',serif;font-style:italic;font-size:2rem;
               color:#111827;letter-spacing:-0.02em;margin:0 0 10px;font-weight:400">
        EV Dashboard
    </h1>
    <p style="font-size:1rem;color:#6B7280;line-height:1.6;max-width:520px;margin:0">
        Gold-layer metrics served directly from S3 — no query latency, no SQL Warehouse cold start.
        Cache refreshes only when the pipeline runs.
    </p>
</div>
""", unsafe_allow_html=True)

    try:
        df_ro      = load_romania_summary()
        df_top     = load_top10_ev_share()
        df_ro_reg  = load_romania_registrations()
        df_eu_comb = load_eu_latest_ev_combustion()
    except Exception:
        st.info(
            "**Dashboard data not yet available.** "
            "Go to the **Ingestion** tab and run the Databricks pipeline to generate the Gold tables."
        )
        st.stop()

    try:
        df_stock = load_stock_snapshot()
    except Exception:
        df_stock = pd.DataFrame()

    _layout = dict(
        plot_bgcolor="white", paper_bgcolor="white",
        font_family="'DM Sans', system-ui, sans-serif",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.0, xanchor="left", x=0),
        margin=dict(l=80, r=50, t=48, b=0),
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
        latest = df_ro.dropna(subset=["vs_eu_avg_pp"]).iloc[-1]
        gap_text = f"{latest['vs_eu_avg_pp']:+.1f} pp vs EU" if not pd.isna(latest["vs_eu_avg_pp"]) else ""
        if gap_text:
            fig1.add_annotation(
                x=latest["year"], y=latest["ev_market_share_pct"],
                text=gap_text, showarrow=True, arrowhead=2, arrowcolor="#888",
                ax=40, ay=-30, font=dict(size=11, color="#555"),
            )
        fig1.update_layout(**_layout, xaxis_title="Year")
        fig1.update_xaxes(showgrid=False)
        fig1.update_yaxes(gridcolor="#F0F0F0")
        _ytitle(fig1, "EV Market Share (%)")
        st.plotly_chart(fig1, use_container_width=True, config={"staticPlot": True})

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
        fig2.add_annotation(x=0.01, y=1.08, xref="paper", yref="paper",
            text="▮ Growth", showarrow=False, font=dict(size=11, color="#00BFA5"), xanchor="left")
        fig2.add_annotation(x=0.15, y=1.08, xref="paper", yref="paper",
            text="▮ Decline", showarrow=False, font=dict(size=11, color="#EF5350"), xanchor="left")
        fig2.update_layout(**_layout, xaxis_title="Year", showlegend=False)
        fig2.update_xaxes(showgrid=False)
        fig2.update_yaxes(gridcolor="#F0F0F0")
        _ytitle(fig2, "YoY Growth (%)")
        st.plotly_chart(fig2, use_container_width=True, config={"staticPlot": True})

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
        for _, row in df_rank.iterrows():
            fig3.add_annotation(
                x=row["year"], y=row["ev_share_rank"],
                text=f"#{int(row['ev_share_rank'])}", showarrow=False, yshift=14,
                font=dict(size=10, color="#FFA726"),
            )
        fig3.update_layout(
            **_layout,
            yaxis=dict(autorange="reversed", tickformat="d", gridcolor="#F0F0F0"),
            xaxis_title="Year",
        )
        fig3.update_xaxes(showgrid=False)
        _ytitle(fig3, "EU Rank (lower = better)")
        st.plotly_chart(fig3, use_container_width=True, config={"staticPlot": True})

    with col_d:
        st.subheader("Top 10 EU Countries — EV Share of New Cars")
        latest_year = int(df_top["year"].iloc[0]) if not df_top.empty else "N/A"
        st.caption(f"Source: gold.ev_market_share · {latest_year} · new car registrations · min 1 000 cars")
        df_chart = df_top.copy()
        if "RO" not in df_chart["country_code"].values and not df_ro.empty:
            ro_latest = df_ro.dropna(subset=["ev_market_share_pct"]).iloc[-1]
            df_chart = pd.concat([df_chart, pd.DataFrame([{
                "country_code": "RO", "year": int(ro_latest["year"]),
                "ev_market_share_pct": ro_latest["ev_market_share_pct"],
            }])], ignore_index=True)
        df_top_s = df_chart.sort_values("ev_market_share_pct", ascending=True)
        fig4 = go.Figure(go.Bar(
            x=df_top_s["ev_market_share_pct"], y=df_top_s["country_code"], orientation="h",
            marker_color=["#1565C0" if c == "RO" else "#00BFA5" for c in df_top_s["country_code"]],
            hovertemplate="%{y}: %{x:.1f}% of new cars<extra></extra>",
            text=[f"{v:.1f}%" for v in df_top_s["ev_market_share_pct"]],
            textposition="outside", textfont=dict(size=11),
        ))
        fig4.add_annotation(x=0.99, y=0.01, xref="paper", yref="paper",
            text="▮ Romania", showarrow=False, font=dict(size=11, color="#1565C0"), xanchor="right")
        fig4.update_layout(**_layout, xaxis_title="EV Share of New Cars (%)", showlegend=False)
        fig4.update_layout(hovermode="y unified")
        fig4.update_xaxes(gridcolor="#F0F0F0", range=[0, df_top_s["ev_market_share_pct"].max() * 1.18])
        fig4.update_yaxes(showgrid=False)
        st.plotly_chart(fig4, use_container_width=True, config={"staticPlot": True})

    st.markdown("---")
    st.subheader("Electric vs Combustion")

    col_e, col_f = st.columns(2)

    with col_e:
        st.subheader("Romania — Electric vs Combustion Registrations")
        st.caption("Source: gold.ev_market_share · absolute new car registrations per year")
        fig5 = go.Figure()
        fig5.add_trace(go.Bar(x=df_ro_reg["year"], y=df_ro_reg["electric_registrations"],
            name="Electric", marker_color="#00BFA5",
            hovertemplate="%{x}: %{y:,} Electric<extra></extra>"))
        fig5.add_trace(go.Bar(x=df_ro_reg["year"], y=df_ro_reg["ice_registrations"],
            name="Combustion", marker_color="#EF5350",
            hovertemplate="%{x}: %{y:,} Combustion<extra></extra>"))
        fig5.update_layout(**_layout, barmode="group", xaxis_title="Year")
        fig5.update_xaxes(showgrid=False)
        fig5.update_yaxes(gridcolor="#F0F0F0")
        _ytitle(fig5, "New Registrations")
        st.plotly_chart(fig5, use_container_width=True, config={"staticPlot": True})

    with col_f:
        st.subheader("Romania — Fuel Mix Shift")
        st.caption("Source: gold.ev_market_share · share of new car registrations")
        df_mix = df_ro_reg.copy()
        df_mix["other"] = (
            df_mix["total_registrations"] - df_mix["electric_registrations"] - df_mix["ice_registrations"]
        ).clip(lower=0)
        fig6 = go.Figure()
        fig6.add_trace(go.Scatter(x=df_mix["year"], y=df_mix["electric_registrations"],
            name="Electric", stackgroup="one", groupnorm="percent",
            fillcolor="#00BFA5", line=dict(color="#00BFA5", width=0),
            hovertemplate="%{x} Electric: %{y:.1f}%<extra></extra>"))
        fig6.add_trace(go.Scatter(x=df_mix["year"], y=df_mix["other"],
            name="Other", stackgroup="one", groupnorm="percent",
            fillcolor="#B0BEC5", line=dict(color="#B0BEC5", width=0),
            hovertemplate="%{x} Other: %{y:.1f}%<extra></extra>"))
        fig6.add_trace(go.Scatter(x=df_mix["year"], y=df_mix["ice_registrations"],
            name="Combustion", stackgroup="one", groupnorm="percent",
            fillcolor="#EF5350", line=dict(color="#EF5350", width=0),
            hovertemplate="%{x} Combustion: %{y:.1f}%<extra></extra>"))
        fig6.update_layout(**_layout, xaxis_title="Year")
        fig6.update_xaxes(showgrid=False)
        fig6.update_yaxes(gridcolor="#F0F0F0")
        _ytitle(fig6, "Share of New Cars (%)")
        st.plotly_chart(fig6, use_container_width=True, config={"staticPlot": True})

    col_g, col_h = st.columns(2)

    with col_g:
        st.subheader("Romania — Electric Rise vs Combustion Decline")
        st.caption("Source: gold.ev_market_share · indexed to first available year = 100")
        df_idx = df_ro_reg.dropna(subset=["electric_registrations", "ice_registrations"]).copy()
        base_elec = df_idx["electric_registrations"].iloc[0]
        base_ice  = df_idx["ice_registrations"].iloc[0]
        df_idx["elec_idx"] = df_idx["electric_registrations"] / base_elec * 100
        df_idx["ice_idx"]  = df_idx["ice_registrations"]  / base_ice  * 100
        fig7 = go.Figure()
        fig7.add_trace(go.Scatter(x=df_idx["year"], y=df_idx["elec_idx"],
            mode="lines+markers", name="Electric",
            line=dict(color="#00BFA5", width=2), marker=dict(size=6),
            hovertemplate="%{x} Electric: %{y:.0f} (base 100)<extra></extra>"))
        fig7.add_trace(go.Scatter(x=df_idx["year"], y=df_idx["ice_idx"],
            mode="lines+markers", name="Combustion",
            line=dict(color="#EF5350", width=2), marker=dict(size=6),
            hovertemplate="%{x} Combustion: %{y:.0f} (base 100)<extra></extra>"))
        fig7.add_hline(y=100, line_color="#BDBDBD", line_width=1, line_dash="dot")
        fig7.update_layout(**_layout, xaxis_title="Year")
        fig7.update_xaxes(showgrid=False)
        fig7.update_yaxes(gridcolor="#F0F0F0")
        _ytitle(fig7, "Index (first year = 100)")
        st.plotly_chart(fig7, use_container_width=True, config={"staticPlot": True})

    with col_h:
        st.subheader("EU Countries — Electric Share of Electric + Combustion")
        latest_year_eu = int(load_top10_ev_share()["year"].iloc[0]) if not df_eu_comb.empty else "N/A"
        st.caption(f"Source: gold.ev_market_share · {latest_year_eu} · Electric / (Electric + Combustion)")
        df_ratio = df_eu_comb.copy()
        df_ratio["elec_comb_total"] = df_ratio["electric_registrations"] + df_ratio["ice_registrations"]
        df_ratio = df_ratio[df_ratio["elec_comb_total"] > 0].copy()
        df_ratio["ratio_pct"] = df_ratio["electric_registrations"] / df_ratio["elec_comb_total"] * 100
        df_ratio = df_ratio.sort_values("ratio_pct", ascending=True)
        fig8 = go.Figure(go.Bar(
            x=df_ratio["ratio_pct"], y=df_ratio["country_code"], orientation="h",
            marker_color=["#1565C0" if c == "RO" else "#00BFA5" for c in df_ratio["country_code"]],
            hovertemplate="%{y}: %{x:.1f}%<extra></extra>",
            text=[f"{v:.1f}%" for v in df_ratio["ratio_pct"]],
            textposition="outside", textfont=dict(size=11),
        ))
        fig8.add_annotation(x=0.99, y=0.01, xref="paper", yref="paper",
            text="▮ Romania", showarrow=False, font=dict(size=11, color="#1565C0"), xanchor="right")
        fig8.update_layout(**_layout, xaxis_title="Electric / (Electric + Combustion) %", showlegend=False)
        fig8.update_layout(hovermode="y unified")
        fig8.update_xaxes(gridcolor="#F0F0F0", range=[0, df_ratio["ratio_pct"].max() * 1.18])
        fig8.update_yaxes(showgrid=False)
        st.plotly_chart(fig8, use_container_width=True, config={"staticPlot": True})

    if df_stock.empty:
        st.info(
            "**Fleet snapshot data not yet available.** "
            "Run *Eurostat Stock Ingestion* then execute the Databricks pipeline: "
            "`03_bronze_eurostat_stock` → `03_silver_eurostat_stock` → `03_gold_stock_snapshot`."
        )
    else:
        st.markdown("---")
        st.subheader("Total Fleet on the Road (Stock)")
        st.caption("Source: gold.car_stock_snapshot · Eurostat ROAD_EQS_CARPDA · all cars currently registered")

        col_i, col_j = st.columns(2)

        with col_i:
            st.subheader("Romania — Fleet Composition Over Time")
            st.caption("All cars on the road, stacked by fuel category")
            df_stk = df_stock.dropna(subset=["total_stock"]).copy()
            df_stk = df_stk[df_stk["year"].between(2018, 2024)]
            df_stk["other_combined"] = (
                df_stk["hybrid_stock"].fillna(0) + df_stk["other_stock"].fillna(0)
            )
            fig9 = go.Figure()
            fig9.add_trace(go.Bar(x=df_stk["year"], y=df_stk["combustion_stock"],
                name="Combustion", marker_color="#EF5350",
                hovertemplate="%{x} Combustion: %{y:,}<extra></extra>"))
            fig9.add_trace(go.Bar(x=df_stk["year"], y=df_stk["other_combined"],
                name="Other / Hybrid", marker_color="#B0BEC5",
                hovertemplate="%{x} Other/Hybrid: %{y:,}<extra></extra>"))
            fig9.add_trace(go.Bar(x=df_stk["year"], y=df_stk["electric_stock"],
                name="Electric", marker_color="#00BFA5",
                hovertemplate="%{x} Electric: %{y:,}<extra></extra>"))
            fig9.update_layout(**_layout, barmode="stack", xaxis_title="Year")
            fig9.update_xaxes(showgrid=False)
            fig9.update_yaxes(gridcolor="#F0F0F0", range=[5_000_000, 10_000_000])
            _ytitle(fig9, "Cars on the Road")
            st.plotly_chart(fig9, use_container_width=True, config={"staticPlot": True})

        with col_j:
            st.subheader("Romania — Electric Share of Total Fleet")
            st.caption("% of all cars on the road that are Electric — fleet transitions slowly")
            df_share_stk = df_stock.dropna(subset=["electric_share_pct"]).copy()
            df_share_stk = df_share_stk[df_share_stk["year"].between(2018, 2024)]
            latest_stock = df_share_stk.iloc[-1]
            fig10 = go.Figure()
            fig10.add_trace(go.Scatter(
                x=df_share_stk["year"], y=df_share_stk["electric_share_pct"],
                mode="lines+markers", name="Electric fleet share",
                line=dict(color="#00BFA5", width=2), marker=dict(size=6),
                hovertemplate="%{x}: %{y:.2f}% of all cars<extra></extra>",
            ))
            fig10.add_annotation(
                x=latest_stock["year"], y=latest_stock["electric_share_pct"],
                text=f"{latest_stock['electric_share_pct']:.2f}% of total fleet",
                showarrow=True, arrowhead=2, arrowcolor="#888",
                ax=40, ay=-30, font=dict(size=11, color="#555"),
            )
            fig10.update_layout(**_layout, xaxis_title="Year")
            fig10.update_xaxes(showgrid=False)
            fig10.update_yaxes(gridcolor="#F0F0F0")
            _ytitle(fig10, "Electric Fleet Share (%)")
            st.plotly_chart(fig10, use_container_width=True, config={"staticPlot": True})
