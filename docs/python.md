# Python in Count Electric

A complete walkthrough of every place Python is used in the project — what library, why, and the exact pattern used. This is the base for interview questions on Python, APIs, S3, caching, and data pipeline design.

---

## Table of Contents

1. [Project-wide Python patterns](#1-project-wide-python-patterns)
2. [Ingestion — fetching from APIs](#2-ingestion--fetching-from-apis)
3. [Idempotency — MD5 deduplication](#3-idempotency--md5-deduplication)
4. [S3 interaction with boto3](#4-s3-interaction-with-boto3)
5. [JSON-stat2 parsing](#5-json-stat2-parsing)
6. [Reading Gold Parquet from S3](#6-reading-gold-parquet-from-s3)
7. [Streamlit caching](#7-streamlit-caching)
8. [Databricks Jobs API — orchestration from Python](#8-databricks-jobs-api--orchestration-from-python)
9. [Environment variables and configuration](#9-environment-variables-and-configuration)
10. [Standard library usage](#10-standard-library-usage)

---

## 1. Project-wide Python patterns

Python version: **3.11**. Used in three distinct contexts:

| Context | Files | Role |
|---|---|---|
| Ingestion scripts | `ingestion/ingest_*.py` | Fetch from APIs, land to S3 |
| Streamlit app | `streamlit/app_dev.py` | Dashboard, S3 reads, Jobs API |
| Databricks notebooks | `databricks/**/*.py` | PySpark transformations (covered in `databricks_and_pyspark.md`) |

All scripts follow the same structural pattern:
- Module-level constants (`IEA_URL`, `S3_BUCKET`, `S3_PREFIX`)
- Pure functions with a single responsibility
- A `main()` function that orchestrates them
- `if __name__ == "__main__": main()` guard — allows the script to be both imported and run directly

```python
def fetch_iea_data() -> bytes:    # one job: fetch
def md5(data: bytes) -> str:      # one job: hash
def latest_s3_object(...):        # one job: find latest
def upload_to_s3(...):            # one job: upload

def main() -> None:               # orchestrates the above
    ...

if __name__ == "__main__":
    main()
```

> **Interview angle:** "I kept functions small and single-purpose so they're independently testable. `main()` reads like a script of what happens; each function is the implementation detail."

---

## 2. Ingestion — fetching from APIs

### Library: `requests`

Used to fetch from two different API styles — a CSV endpoint (IEA) and a JSON-stat2 REST API (Eurostat).

**IEA — CSV download:**
```python
IEA_URL = "https://api.iea.org/evs?parameter=EV+sales&category=Historical&mode=Cars&csv=true"

def fetch_iea_data() -> bytes:
    response = requests.get(IEA_URL, timeout=30)
    response.raise_for_status()   # raises HTTPError for 4xx/5xx
    return response.content       # raw bytes — preserved exactly as received
```

Key decisions:
- `timeout=30` — always set a timeout on external requests; without it, a hanging API call blocks forever
- `raise_for_status()` — turns any HTTP error response into an exception immediately, instead of silently processing an error body
- Return `bytes`, not `str` — preserves the exact content for MD5 hashing

**Eurostat — JSON REST API:**
```python
EUROSTAT_URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/ROAD_EQR_CARPDA"
    "?format=JSON&freq=A&lang=EN"
)

def fetch_eurostat_data() -> dict:
    response = requests.get(EUROSTAT_URL, timeout=60)  # longer timeout — larger payload
    response.raise_for_status()
    return response.json()    # parse JSON body into a Python dict
```

Key decisions:
- `timeout=60` for Eurostat (larger dataset, slower API than IEA)
- `response.json()` — requests handles JSON decoding; returns a Python `dict` directly
- Return type is `dict` not `bytes` — the upstream JSON-stat2 structure is the thing we care about

> **Interview angle:** "Both scripts follow identical structure despite different source formats — the fetch function returns the raw payload, and the rest of the pipeline doesn't care where it came from."

---

## 3. Idempotency — MD5 deduplication

**The problem:** ingestion scripts run on a schedule (or manually). If the source data hasn't changed, re-uploading creates duplicate files in S3 and wastes money on downstream reprocessing.

**The solution:** before uploading, compute an MD5 hash of the new data and compare it to the hash of the most recent file already in S3. If they match, skip the upload.

```python
import hashlib

def md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()
```

**For IEA (CSV bytes):** straightforward — hash the raw bytes directly.

```python
data = fetch_iea_data()          # bytes
new_hash = md5(data)             # hash of new download

existing = s3.get_object(...)["Body"].read()   # bytes from S3
if md5(existing) == new_hash:
    logger.info("Data unchanged — skipping upload.")
    return
```

**For Eurostat (JSON dict):** can't hash the raw response bytes directly — the server might return keys in a different order on each request, which would produce a different hash even if the data is identical.

Solution: **canonical serialisation** — sort the keys before hashing.

```python
def canonical_bytes(data: dict) -> bytes:
    """Stable JSON serialisation — sort_keys eliminates key-order differences."""
    return json.dumps(data, sort_keys=True, ensure_ascii=False).encode("utf-8")

# Usage:
new_hash = md5(canonical_bytes(data))

existing_bytes = s3.get_object(...)["Body"].read()
existing_hash  = md5(canonical_bytes(json.loads(existing_bytes)))

if existing_hash == new_hash:
    logger.info("Data unchanged — skipping upload.")
    return
```

`json.dumps(sort_keys=True)` guarantees the same byte sequence regardless of Python dict insertion order or server-side key ordering.

> **Interview angle:** "Idempotency is a first-class concern in data pipelines. Running the same job twice should produce the same result as running it once. The MD5 check makes ingestion safe to re-run at any time."

---

## 4. S3 interaction with boto3

### Library: `boto3`

Used for all S3 operations: listing objects, downloading, uploading.

**Client initialisation:**
```python
import boto3

s3 = boto3.client("s3")
```

No credentials in code — the EC2 instance has an IAM instance role attached (`count-electric-ec2-role`), so boto3 picks up credentials automatically from the instance metadata service. This is the correct pattern for production — never hardcode AWS keys.

**List objects under a prefix:**
```python
def latest_s3_object(s3, prefix: str) -> dict | None:
    result = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    objects = result.get("Contents", [])
    if not objects:
        return None
    return max(objects, key=lambda o: o["LastModified"])
```

- `list_objects_v2` returns up to 1000 objects (sufficient for this project's scale)
- `.get("Contents", [])` — if the prefix has no objects, `Contents` key is absent entirely (not an empty list), so `.get()` with default `[]` prevents a `KeyError`
- `max(..., key=lambda o: o["LastModified"])` — finds the newest file using Python's built-in `max` with a key function

**Download an object:**
```python
existing = s3.get_object(Bucket=S3_BUCKET, Key=latest["Key"])["Body"].read()
```

- `get_object` returns a dict; `["Body"]` is a streaming `StreamingBody` object
- `.read()` materialises it into `bytes` — fine for files of this size (a few MB)

**Upload an object:**
```python
# CSV upload (IEA)
s3.put_object(Bucket=S3_BUCKET, Key=key, Body=data, ContentType="text/csv")

# JSON upload (Eurostat)
body = json.dumps(data, ensure_ascii=False).encode("utf-8")
s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json")
```

- `ContentType` is set correctly for each format — makes files browsable in the S3 console and correctly handled by downstream consumers
- `ensure_ascii=False` preserves non-ASCII characters (country names, accented letters) instead of escaping them as `\uXXXX`

**In the Streamlit app — listing files for the S3 table:**
```python
def s3():
    return boto3.client("s3")

@st.cache_data(ttl=300)
def list_s3_files(prefix=""):
    r = s3().list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    return r.get("Contents", [])
```

Then transforming S3 metadata into a display table:
```python
rows = [
    {
        "Source":    f["Key"].split("/")[2].upper(),     # extract folder name
        "File":      f["Key"].split("/")[-1],            # extract filename
        "Size (KB)": round(f["Size"] / 1024, 1),
        "Ingested":  f["LastModified"].strftime("%Y-%m-%d %H:%M UTC"),
    }
    for f in sorted(all_files, key=lambda x: x["LastModified"], reverse=True)[:20]
]
```

> **Interview angle:** "boto3 is the standard AWS SDK for Python. Key patterns: never put credentials in code, use `.get()` on potentially absent dict keys, and set `ContentType` on uploads."

---

## 5. JSON-stat2 parsing

Eurostat's API returns data in **JSON-stat2** format — a compact, multidimensional format designed for statistical datasets. It's not a flat table; it's a cube described by dimension labels and a flat array of values.

**The structure:**
```json
{
  "id":    ["geo", "time", "mot_nrg"],        // dimension names, in order
  "size":  [30, 15, 11],                       // number of values per dimension
  "value": {"0": 1234, "1": null, "42": 567},  // flat index → value (sparse)
  "dimension": {
    "geo": {
      "category": {
        "index": {"AT": 0, "BE": 1, "BG": 2, ...}
      }
    }
  }
}
```

The `value` dict is **sparse** — missing indices mean no data for that combination.

**The parser:**
```python
from itertools import product

def _parse_jsonstat2(raw: dict) -> pd.DataFrame:
    dims, sizes, values = raw["id"], raw["size"], raw["value"]

    # Build a reverse-lookup map: index position → label
    # raw["dimension"][dim]["category"]["index"] is {"AT": 0, "BE": 1, ...}
    # We invert it to {0: "AT", 1: "BE", ...}
    label_maps = {
        dim: {str(v): k for k, v in raw["dimension"][dim]["category"]["index"].items()}
        for dim in dims
    }

    rows = []
    for i, combo in enumerate(product(*[range(s) for s in sizes])):
        val = values.get(str(i))   # sparse — missing index = no data
        if val is None:
            continue
        row = {dim: label_maps[dim][str(idx)] for dim, idx in zip(dims, combo)}
        row["value"] = float(val)
        rows.append(row)

    return pd.DataFrame(rows)
```

**How `itertools.product` works here:**

`product(*[range(s) for s in sizes])` generates every combination of dimension indices:
```python
# sizes = [30, 15, 11]  → 30 × 15 × 11 = 4950 combinations
# combo 0  → (0, 0, 0)  → AT, 2010, ELC
# combo 1  → (0, 0, 1)  → AT, 2010, ELC_PET_PI
# combo 42 → (0, 2, 9)  → AT, 2012, PET
```

`enumerate(product(...))` gives the flat index `i` alongside the combo, which is exactly the key used in `"value"`.

The dict comprehension `{str(v): k for k, v in ...items()}` inverts `{"AT": 0}` into `{"0": "AT"}` — so given an index position we can look up the label.

> **Interview angle:** "JSON-stat2 is common in European public statistics APIs. The key insight is that `enumerate(product(*[range(s) for s in sizes]))` reconstructs the same flat index the format uses, so you can map each value back to its dimension labels without a join."

---

## 6. Reading Gold Parquet from S3

### Libraries: `s3fs`, `pyarrow`, `pandas`

The dashboard reads Gold tables directly from S3 as Parquet files — no database connection, no SQL Warehouse.

```python
S3_BUCKET = os.getenv("S3_BUCKET", "count-electric")

def _s3_parquet(key: str) -> pd.DataFrame:
    return pd.read_parquet(f"s3://{S3_BUCKET}/gold/{key}/")
```

`pd.read_parquet` with an `s3://` path requires two packages installed:
- `s3fs` — provides a filesystem abstraction over S3 that pandas uses transparently
- `pyarrow` — the Parquet engine that actually reads the file format

The trailing `/` in the path means "read all Parquet files in this folder" — Databricks writes Gold tables as a folder containing one `.parquet` file (plus metadata). Pandas/pyarrow handle the folder scan automatically.

**The full load pipeline for one table:**
```python
@st.cache_data(ttl=None)
def load_romania_summary() -> pd.DataFrame:
    return _s3_parquet("romania_ev_summary").sort_values("year")
```

**Derived tables — filter and transform in Python, not on disk:**
```python
@st.cache_data(ttl=None)
def load_top10_ev_share() -> pd.DataFrame:
    df = load_ev_market_share()                          # cached — no second S3 read
    latest = df["year"].max()
    return (
        df[(df["year"] == latest) & (df["total_registrations"] > 1000)]
        .nlargest(10, "ev_market_share_pct")
    )

@st.cache_data(ttl=None)
def load_romania_registrations() -> pd.DataFrame:
    return (
        load_ev_market_share()                           # reuse cached result
        .pipe(lambda d: d[d["country_code"] == "RO"])   # .pipe for chaining
        [["year", "electric_registrations", "ice_registrations", "total_registrations"]]
        .sort_values("year")
    )
```

`.pipe(lambda d: d[...])` allows filtering to be chained in a single expression rather than requiring a named intermediate variable. It's idiomatic pandas for method chaining.

> **Interview angle:** "By caching the base table with `ttl=None` and deriving filtered views from it in memory, we avoid repeated S3 reads. The base table is read once per app restart; filtered views are free."

---

## 7. Streamlit caching

Streamlit re-runs the entire script from top to bottom on every user interaction. Without caching, every chart render would trigger a new S3 read.

Two TTL strategies are used:

**`ttl=None` — cache forever (Gold tables):**
```python
@st.cache_data(ttl=None)
def load_ev_market_share() -> pd.DataFrame:
    return _s3_parquet("ev_market_share")
```

The Gold data only changes when the pipeline runs. There's no point expiring the cache on a timer — `ttl=None` keeps the data in memory until `st.cache_data.clear()` is called explicitly, which happens immediately after a successful pipeline run:

```python
if not failed:
    st.success("Pipeline complete. Clearing cache…")
    st.cache_data.clear()   # forces a fresh S3 read on next interaction
```

**`ttl=300` — 5-minute cache (S3 file listing):**
```python
@st.cache_data(ttl=300)
def list_s3_files(prefix=""):
    r = s3().list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    return r.get("Contents", [])
```

The landing zone file list is used for display only — slightly stale data is acceptable, and 5 minutes balances freshness vs unnecessary S3 LIST calls.

> **Interview angle:** "Cache strategy should match data freshness requirements. Gold tables are only updated by the pipeline, so `ttl=None` + explicit invalidation is more correct than a time-based TTL that would either expire too early (wasted reads) or too late (stale data)."

---

## 8. Databricks Jobs API — orchestration from Python

The Streamlit app can trigger the full Bronze → Silver → Gold pipeline without the user opening Databricks. Python drives the entire orchestration via HTTP requests to the Databricks REST API.

### Submit a notebook run

```python
def _db_submit_notebook(notebook_path: str) -> int:
    host  = os.getenv("DATABRICKS_HOST", "").rstrip("/")
    token = os.getenv("DATABRICKS_TOKEN", "")

    resp = requests.post(
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
    return resp.json()["run_id"]   # int — used to poll status
```

`runs/submit` creates a **one-off run** — no pre-existing job definition needed. The notebook path is the full workspace path (e.g. `/Workspace/Users/you@email.com/count-electric/databricks/bronze/01_bronze_iea`).

### Poll for completion

```python
def _db_run_status(run_id: int) -> tuple[str, str]:
    resp = requests.get(
        f"{host}/api/2.1/jobs/runs/get",
        headers={"Authorization": f"Bearer {token}"},
        params={"run_id": run_id},
        timeout=30,
    )
    resp.raise_for_status()
    state = resp.json()["state"]
    return state["life_cycle_state"], state.get("result_state", "")
```

`life_cycle_state` values: `PENDING` → `RUNNING` → `TERMINATED` (or `SKIPPED` / `INTERNAL_ERROR`)
`result_state` (only set when terminated): `SUCCESS` or `FAILED`

### The polling loop — sequential execution

```python
_PIPELINE_STEPS = [
    ("Bronze — IEA",                    "databricks/bronze/01_bronze_iea"),
    ("Bronze — Eurostat Registrations", "databricks/bronze/02_bronze_eurostat"),
    # ... 7 more steps
]

step_placeholders = [st.empty() for _ in _PIPELINE_STEPS]  # one UI slot per step
failed = False

for i, (label, rel_path) in enumerate(_PIPELINE_STEPS):
    if failed:
        step_placeholders[i].markdown(f"⬜ {label}")   # show skipped steps
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
        time.sleep(6)   # poll every 6 seconds

    if result == "SUCCESS":
        step_placeholders[i].markdown(f"✅ **{label}**")
    else:
        step_placeholders[i].markdown(f"❌ **{label}** — {result or lc}")
        failed = True

if not failed:
    st.cache_data.clear()   # invalidate Gold table cache
```

Key design decisions:
- **`st.empty()` placeholders** — created upfront so all steps are visible immediately; each placeholder is updated in-place as the step progresses
- **`failed` flag** — if any step fails, remaining steps show ⬜ (not started) rather than being skipped silently
- **Sequential, not parallel** — each notebook depends on the previous layer's output; parallelism would break Bronze → Silver → Gold ordering
- **`time.sleep(6)`** — Databricks serverless notebooks typically take 30–120 seconds; polling every 6 seconds balances responsiveness vs API rate limits

> **Interview angle:** "This is lightweight orchestration — no Airflow, no job definitions, no extra infrastructure. Just Python, HTTP, and a polling loop. The tradeoff is no retry logic, no scheduling, and no DAG visualisation — but for a project this size those aren't needed."

---

## 9. Environment variables and configuration

All secrets and configuration are passed via environment variables — never hardcoded.

```python
import os
from dotenv import load_dotenv

load_dotenv()   # reads .env file locally; no-op in Docker (env vars already set)

S3_BUCKET            = os.getenv("S3_BUCKET", "count-electric")    # has default
DATABRICKS_HOST      = os.getenv("DATABRICKS_HOST", "")            # required
DATABRICKS_TOKEN     = os.getenv("DATABRICKS_TOKEN", "")           # required
DATABRICKS_REPO_PATH = os.getenv("DATABRICKS_REPO_PATH", "")       # required
```

`os.getenv(key, default)` returns `None` if no default is given and the variable is absent. Setting `""` as default for required vars (rather than `None`) means string operations like `.rstrip("/")` won't raise `AttributeError` — the app degrades gracefully and shows an info message rather than crashing.

`S3_BUCKET` has a real default (`"count-electric"`) so the app works without that secret. This is intentional — if the GitHub secret is misconfigured as empty string, `os.getenv` would return `""` which overrides the hardcoded default and breaks boto3.

> **Interview angle:** "Twelve-factor app principle: configuration comes from the environment. `python-dotenv` bridges local development (`.env` file) and production (Docker/GitHub Actions env vars) without changing any code."

---

## 10. Standard library usage

| Module | Where used | Why |
|---|---|---|
| `hashlib` | `ingest_*.py` | MD5 hashing for deduplication |
| `json` | `ingest_eurostat*.py`, `app_dev.py` | Parse API responses, serialise for upload, canonical bytes |
| `os` | everywhere | `os.getenv()` for configuration |
| `logging` | `ingest_*.py` | Structured log output with timestamps and levels |
| `datetime` | `ingest_*.py` | Timestamp for S3 filenames (`%Y%m%d_%H%M%S`) |
| `time` | `app_dev.py` | `time.sleep(6)` in the Jobs API polling loop |
| `itertools.product` | `app_dev.py` | Generate all dimension index combinations for JSON-stat2 parsing |
| `sys`, `os.path` | `app_dev.py` | `sys.path.insert` so the app can import `ingestion.*` modules |

**Logging pattern (ingestion scripts):**
```python
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

logger.info("Fetched %d bytes", len(response.content))   # %-style formatting, not f-string
```

`%`-style formatting in `logger.info` is intentional — if the log level is set to WARNING, the string interpolation is never performed (lazy evaluation). f-strings always interpolate, even if the message is never logged.

**Timestamp filenames:**
```python
timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
filename  = f"iea_ev_sales_{timestamp}.csv"    # e.g. iea_ev_sales_20240415_083012.csv
```

UTC timestamps on filenames ensure consistent ordering in S3 regardless of the server timezone, and `%Y%m%d` sorts lexicographically as chronologically.
