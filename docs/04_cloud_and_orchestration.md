# Cloud and Orchestration in Count Electric

A walkthrough of the infrastructure layer — how the project runs on AWS, how Docker packages and runs the application, how GitHub Actions automates deployments, and how the pipeline is orchestrated without a dedicated workflow tool.

---

## Infrastructure across the project

```
┌─────────────────────────────────────────────────────────────────┐
│  DEVELOPER LAPTOP                                               │
│  git push → GitHub (main branch)                               │
└──────────────────────────┬──────────────────────────────────────┘
                           │ push triggers workflow
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  GITHUB ACTIONS  (.github/workflows/deploy.yml)                 │
│                                                                 │
│  1. Get runner IP                                               │
│  2. Whitelist runner IP on EC2 security group (port 22)        │
│  3. SSH into EC2 → git pull → docker compose up --build        │
│  4. Sync Databricks Git folder via Repos API                   │
│  5. Remove runner IP from security group                       │
└──────────┬────────────────────────────┬────────────────────────┘
           │ SSH deploy                 │ Repos API call
           ▼                            ▼
┌──────────────────────┐   ┌───────────────────────────────────┐
│  AWS EC2 (t2.micro)  │   │  DATABRICKS WORKSPACE             │
│                      │   │  Git folder synced to main branch │
│  Docker Compose      │   │  Notebooks ready to run           │
│  ┌────────────────┐  │   └───────────────────────────────────┘
│  │ app (8501)     │  │
│  │ Streamlit prod │  │
│  ├────────────────┤  │
│  │ app-dev (8502) │  │
│  │ Streamlit dev  │  │
│  ├────────────────┤  │
│  │ cloudflared    │──┼──→ Cloudflare edge network
│  └────────────────┘  │        │
│                      │        ▼
│  AWS S3              │   app.countelectric.com  (→ :8501)
│  s3://count-electric │   dev.countelectric.com  (→ :8502)
└──────────────────────┘
```

---

## Table of Contents

**AWS**
- [1. S3 — bucket structure and design](#1-s3--bucket-structure-and-design)
- [2. EC2 — the compute layer](#2-ec2--the-compute-layer)
- [3. IAM — identity and access management](#3-iam--identity-and-access-management)

**Docker**
- [4. Dockerfile — packaging the app](#4-dockerfile--packaging-the-app)
- [5. Docker Compose — multi-service orchestration](#5-docker-compose--multi-service-orchestration)
- [6. Why no ports are exposed](#6-why-no-ports-are-exposed)

**CI/CD**
- [7. GitHub Actions — the deploy workflow](#7-github-actions--the-deploy-workflow)
- [8. The security group dance](#8-the-security-group-dance)
- [9. Databricks Git folder sync](#9-databricks-git-folder-sync)

**Networking**
- [10. Cloudflare Tunnel — HTTPS with no open ports](#10-cloudflare-tunnel--https-with-no-open-ports)
- [11. Dual hostname routing](#11-dual-hostname-routing)

**Orchestration**
- [12. Why no Airflow](#12-why-no-airflow)
- [13. The Jobs API as a lightweight orchestrator](#13-the-jobs-api-as-a-lightweight-orchestrator)
- [14. Idempotency across the pipeline](#14-idempotency-across-the-pipeline)

---

## 1. S3 — Bucket Structure and Design

The single S3 bucket `count-electric` holds data at all stages of the pipeline.

```
s3://count-electric/
│
├── landing/
│   └── raw/
│       ├── iea/                        ← raw IEA CSV files
│       │   ├── iea_ev_sales_20240415_083012.csv
│       │   └── iea_ev_sales_20241201_140523.csv
│       ├── eurostat/                   ← raw Eurostat JSON files (registrations)
│       │   └── eurostat_road_eqr_carpda_20240415_090012.json
│       └── eurostat_stock/             ← raw Eurostat JSON files (fleet stock)
│           └── eurostat_road_eqs_carpda_20240415_091500.json
│
└── gold/                               ← Parquet exports from Gold notebooks
    ├── ev_market_share/
    │   └── part-00000-xxxx.parquet
    ├── romania_ev_summary/
    │   └── part-00000-xxxx.parquet
    └── car_stock_snapshot/
        └── part-00000-xxxx.parquet
```

**Design decisions:**

**`landing/raw/` is append-only.** Ingestion scripts never delete or overwrite existing files. Each run creates a new timestamped file. This means the full ingestion history is preserved — you can see exactly what the API returned on any date.

**Timestamped filenames.** `iea_ev_sales_20240415_083012.csv` uses UTC timestamp in `YYYYMMDD_HHMMSS` format. Sorting filenames alphabetically gives chronological order — no metadata query needed to find the most recent file.

**`gold/` holds one folder per table.** The Streamlit app reads `s3://count-electric/gold/ev_market_share/` — the trailing slash tells pandas/pyarrow to read all Parquet files in the folder. Databricks writes one `.parquet` file per call to `coalesce(1).write.parquet(...)`.

**Why Gold data in your own S3 bucket?** Databricks managed Delta tables live in Databricks-controlled storage — not visible in your S3 console, not directly readable without a Databricks connection. Exporting to your own S3 bucket makes the data:
- Visible and inspectable in the AWS console
- Readable by any tool that understands Parquet (pandas, DuckDB, Athena)
- Available to the dashboard with no Databricks dependency

> **Why this matters:** S3 bucket structure is a design choice. Append-only landing zones protect raw data; structured prefixes make it easy to list, monitor, and audit what has been ingested. The separation of `landing/` (raw) from `gold/` (processed) mirrors the medallion layers in the data catalog.

---

## 2. EC2 — The Compute Layer

The entire application stack runs on a single **t2.micro** EC2 instance — the smallest AWS compute option, within the free tier.

**What runs on EC2:**
- Docker Engine
- Three Docker containers (app, app-dev, cloudflared)
- The ingestion scripts (triggered from the Streamlit app or run manually)

**IAM instance role.** The EC2 instance has an IAM role (`count-electric-ec2-role`) attached. This role has `AmazonS3FullAccess` for the `count-electric` bucket. Because the role is attached to the instance, any process running on EC2 can access S3 without credentials — boto3 picks them up automatically from the EC2 instance metadata service at `169.254.169.254`.

```python
# No credentials needed — IAM role handles auth
s3 = boto3.client("s3")
s3.put_object(Bucket="count-electric", Key="...", Body=data)
```

**t2.micro limitations.** At 1 vCPU and 1GB RAM, t2.micro is tight for running three Docker containers. Streamlit is lightweight, cloudflared is a single binary, and the ingestion scripts run briefly and exit. In practice, the machine stays well within memory limits because containers are not all active simultaneously — the ingestion scripts run for seconds, not continuously.

> **Why this matters:** Right-sizing compute is a real engineering skill. A t2.micro is appropriate here because the load profile is spiky (brief ingestion runs, light web serving) rather than sustained. Understanding this tradeoff helps you justify infrastructure choices.

---

## 3. IAM — Identity and Access Management

Two separate IAM roles are used in this project for two completely different purposes.

**EC2 instance role (`count-electric-ec2-role`)**
- Attached to the EC2 instance
- Allows the instance to read/write S3 (`AmazonS3FullAccess` scoped to the bucket)
- Used by: boto3 in ingestion scripts and the Streamlit app (s3fs reads)
- Trust policy: allows the EC2 service to assume this role

**Databricks cross-account role**
- A separate IAM role with a cross-account trust policy
- The trust allows Databricks' AWS account (`arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-...`) to assume this role
- Allows Databricks compute to read/write `s3://count-electric`
- Registered as a Storage Credential in Unity Catalog
- Used by: all Databricks notebooks when accessing S3

**GitHub Actions IAM user**
- A third identity — an IAM user (not a role) with permissions to:
  - Modify EC2 security groups (`ec2:AuthorizeSecurityGroupIngress`, `ec2:RevokeSecurityGroupIngress`)
- Access key stored as GitHub secret (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- Used only by the deploy workflow to whitelist/de-whitelist the runner IP

Three identities, three purposes, three minimal sets of permissions. No single credential has more access than it needs.

> **Why this matters:** IAM is one of the most important AWS topics. The key principle is least-privilege — each identity gets only the permissions it actually needs, nothing more. Understanding the difference between roles (assumable by services) and users (long-lived credentials for humans/automation) is fundamental.

---

## 4. Dockerfile — Packaging the App

```dockerfile
FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project code
COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "streamlit/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

**Key decisions:**

**`python:3.12-slim`** — the slim variant includes only what's needed to run Python. It's significantly smaller than the full `python:3.12` image (no build tools, no docs). Smaller image = faster builds and pulls.

**`--no-install-recommends`** on apt-get — avoids installing optional packages that weren't requested. `git` is needed because some Python packages are installed directly from git during `pip install`.

**`rm -rf /var/lib/apt/lists/*`** — deletes the apt package index after installing. It's not needed at runtime, so removing it shrinks the image layer.

**`COPY requirements.txt .` before `COPY . .`** — Docker builds images in layers. If requirements.txt hasn't changed, Docker reuses the cached `pip install` layer and skips reinstalling all dependencies. Only the code layer gets rebuilt. Reversing the order would bust the cache on every code change.

**`--server.address=0.0.0.0`** — Streamlit must listen on all network interfaces, not just localhost, for Docker port forwarding to work. Without this, the container accepts connections only from within itself.

**`EXPOSE 8501`** — documents that the container listens on port 8501. This is a declaration, not a firewall rule — it doesn't actually open any port. The real port binding happens in `docker compose` or `docker run`.

> **Why this matters:** Dockerfile layer order has a direct impact on build performance. The pattern of copying dependency files before copying source code is one of the most common Dockerfile optimisations — it keeps the expensive package installation layer cached as long as requirements haven't changed.

---

## 5. Docker Compose — Multi-Service Orchestration

`docker-compose.yml` defines all three services and their relationships:

```yaml
services:
  app:
    build: .                          # build image from Dockerfile in current dir
    restart: unless-stopped           # restart automatically if it crashes
    environment:
      - DATABRICKS_HOST=${DATABRICKS_HOST}       # passed from shell env
      - DATABRICKS_TOKEN=${DATABRICKS_TOKEN}
      - DATABRICKS_HTTP_PATH=${DATABRICKS_HTTP_PATH}
      - DATABRICKS_REPO_PATH=${DATABRICKS_REPO_PATH}
    # No ports exposed — cloudflared reaches app via internal Docker network

  app-dev:
    build: .                          # same image as app
    restart: unless-stopped
    command: streamlit run streamlit/app_dev.py --server.port 8502 --server.address 0.0.0.0
    environment:                      # same env vars
      - ...

  cloudflared:
    image: cloudflare/cloudflared:latest   # no build — use official image directly
    restart: unless-stopped
    command: --config /etc/cloudflared/config.yml tunnel --no-autoupdate run --token ${CLOUDFLARE_TUNNEL_TOKEN}
    volumes:
      - ./cloudflared/config.yml:/etc/cloudflared/config.yml:ro   # mount config read-only
    depends_on:
      - app
      - app-dev
```

**Key design decisions:**

**`app` and `app-dev` use the same image** (`build: .`). The only difference is the `command` override in `app-dev` — it runs `app_dev.py` on port 8502 instead of the default `app.py` on 8501. One Dockerfile, two running variants.

**`restart: unless-stopped`** — all three services restart automatically if they crash or if the EC2 instance is rebooted. The only time they don't restart is when you explicitly `docker compose stop`.

**`depends_on`** on cloudflared — Docker starts `app` and `app-dev` before starting `cloudflared`. Without this, cloudflared might start before Streamlit is ready and fail to proxy requests.

**`${VARIABLE}` syntax** — Docker Compose reads variables from the shell environment at the time `docker compose up` is run. On EC2, these are passed via the GitHub Actions deploy step using `--preserve-env`. They are never written to any file on the machine.

**Volumes — config file mount.** `./cloudflared/config.yml:/etc/cloudflared/config.yml:ro` mounts the routing config from the repo into the container at the path cloudflared expects. `:ro` (read-only) prevents the container from modifying the file.

> **Why this matters:** Docker Compose is the standard way to run multi-container applications on a single host. Understanding `build` vs `image`, environment variable injection, `depends_on`, and volumes are the core skills for working with any Compose-based project.

---

## 6. Why No Ports Are Exposed

The `app` and `app-dev` services in `docker-compose.yml` intentionally have no `ports:` mapping:

```yaml
app:
  build: .
  # No ports: — intentionally not exposed to the internet.
  # cloudflared reaches the app via the internal Docker network (http://app:8501).
  # To re-enable direct IP access for debugging: uncomment the line below.
  # ports: ["8501:8501"]
```

When multiple Docker containers run under the same Compose project, they share an internal Docker network. Each service is reachable by its service name — `cloudflared` can reach `http://app:8501` without any port being exposed to the host machine or the internet.

This means:
- Port 8501 and 8502 are never open on EC2's public IP
- There is no way to directly reach Streamlit from the internet
- The only entry point is through Cloudflare Tunnel

This is a deliberate security decision. Exposing Streamlit directly would mean the app is reachable on a public IP without any authentication or DDoS protection.

---

## 7. GitHub Actions — The Deploy Workflow

The full deploy workflow on every push to `main`:

```yaml
on:
  push:
    branches:
      - main
```

**Step 1 — Get the runner IP:**
```yaml
- name: Get GitHub Actions runner IP
  id: runner-ip
  run: echo "ip=$(curl -s https://checkip.amazonaws.com)" >> $GITHUB_OUTPUT
```

GitHub Actions runners have dynamic IPs — a different IP on every workflow run. `checkip.amazonaws.com` returns the public IP of the machine making the request. This IP is saved as a step output for use in later steps.

**Step 2 — Whitelist the runner IP on EC2:**
```yaml
- name: Whitelist runner IP on EC2 security group
  run: |
    aws ec2 authorize-security-group-ingress \
      --group-id ${{ secrets.EC2_SG_ID }} \
      --protocol tcp \
      --port 22 \
      --cidr ${{ steps.runner-ip.outputs.ip }}/32
```

Temporarily opens SSH (port 22) to only the GitHub Actions runner's IP. The EC2 security group has no other inbound rules — port 22 is closed to the world except during this deploy window.

**Step 3 — SSH deploy:**
```yaml
- name: Deploy to EC2 via SSH
  uses: appleboy/ssh-action@v1.0.3
  with:
    script: |
      git fetch origin main
      git reset --hard origin/main
      sudo --preserve-env=... docker compose up -d --build
```

On EC2:
- `git reset --hard origin/main` — ensures the working directory exactly matches the latest commit (no partial changes)
- `docker compose up -d --build` — builds a new image from the updated code and starts all services. `-d` runs them in the background. Containers not affected by the code change (like `cloudflared`) are not recreated.

**Step 4 — Sync Databricks Git Folder:**
```yaml
- name: Sync Databricks Git Folder
  run: |
    curl --fail -s -X PATCH \
      -H "Authorization: Bearer ${{ secrets.DATABRICKS_TOKEN }}" \
      -d '{"branch": "main"}' \
      "$DATABRICKS_HOST/api/2.0/repos/$DATABRICKS_REPO_ID"
```

Triggers a pull in the Databricks Git folder — so notebooks in the Databricks workspace are always in sync with the GitHub repo. This runs from the GitHub Actions runner directly to the Databricks API; no EC2 involvement.

**Step 5 — Remove runner IP (runs even if earlier steps fail):**
```yaml
- name: Remove runner IP from security group
  if: always()
```

`if: always()` ensures this cleanup step runs even if the deploy fails. Without it, a failed deploy would leave port 22 open to the runner's IP until someone manually removed it.

> **Why this matters:** The `if: always()` pattern is a critical CI/CD concept — cleanup steps must be guaranteed to run regardless of whether the job succeeded. A security group rule left open is a real security risk. This pattern also applies to releasing locks, cleaning up temporary resources, or sending failure notifications.

---

## 8. The Security Group Dance

The EC2 security group has no persistent inbound rules for SSH. Port 22 is opened only for the duration of the deploy, and only to the specific IP of the GitHub Actions runner running that workflow.

```
Inbound rules at rest:          Inbound rules during deploy:
  (none — completely closed)      22/tcp from 18.185.42.123/32 (runner IP)

After deploy completes:
  (none — closed again)
```

This is called **just-in-time access** — the minimum access, for the minimum time, for the minimum audience. Even if someone was watching the security group, the window is open for roughly 30 seconds.

The alternative — keeping port 22 open to `0.0.0.0/0` — is common but makes the instance a target for SSH brute force attacks. EC2 instances with open port 22 receive automated login attempts within minutes of being reachable.

---

## 9. Databricks Git Folder Sync

The Databricks workspace has a **Git folder** (also called a Repo) linked to the GitHub repository. When the GitHub Actions workflow runs, it sends a PATCH request to the Databricks Repos API to pull the latest commit:

```bash
curl -X PATCH \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"branch": "main"}' \
  "https://your-workspace.cloud.databricks.com/api/2.0/repos/123456"
```

The `DATABRICKS_REPO_ID` is the numeric ID of the Git folder, found by calling `GET /api/2.0/repos` and looking up the folder path.

This means: edit a notebook locally → push to GitHub → the deploy workflow fires → EC2 gets the new app code AND Databricks gets the new notebook code in the same workflow run.

---

## 10. Cloudflare Tunnel — HTTPS With No Open Ports

Cloudflare Tunnel solves the problem of making a local service reachable on the internet without opening firewall ports.

**How it works:**

1. The `cloudflared` container runs inside EC2
2. It establishes an outbound connection to Cloudflare's edge network (not inbound — no ports needed)
3. Cloudflare routes incoming HTTPS requests for `app.countelectric.com` through this tunnel to `http://app:8501` on the internal Docker network

```
User browser
    │ HTTPS request to app.countelectric.com
    ▼
Cloudflare edge (handles TLS termination)
    │ encrypted tunnel (outbound connection from EC2)
    ▼
cloudflared container (inside EC2)
    │ internal Docker network
    ▼
app container (port 8501)
```

The EC2 instance never receives a direct inbound connection from the internet. Its security group can remain completely closed.

**Benefits:**
- No open inbound ports on EC2
- Free TLS certificate managed by Cloudflare
- DDoS protection from Cloudflare's network
- No IP address management — the domain always resolves to Cloudflare's IPs, not EC2's

**One-time setup:**
1. Create a free Cloudflare account and register the domain
2. Create a named tunnel in Cloudflare Zero Trust → Networks → Tunnels
3. Get the tunnel token (added as GitHub Secret `CLOUDFLARE_TUNNEL_TOKEN`)
4. Add DNS records (CNAME) pointing hostnames to the tunnel ID

Once done, the token is the only thing that needs to be on the server — the routing rules live in `cloudflared/config.yml` in the repo.

---

## 11. Dual Hostname Routing

The `cloudflared/config.yml` routes two hostnames to two different services on the same machine:

```yaml
ingress:
  - hostname: app.countelectric.com
    service: http://app:8501        # production Streamlit app
  - hostname: dev.countelectric.com
    service: http://app-dev:8502    # development Streamlit app
  - service: http_status:404        # catch-all — any other hostname gets a 404
```

Cloudflare matches the `Host` header of incoming requests and routes accordingly. The `app` and `app-dev` service names are DNS names on the internal Docker network — Compose registers them automatically.

The catch-all rule (`http_status:404`) is required by cloudflared — without a final catch-all, the config is invalid. It ensures that requests for any other hostname (e.g. direct access to the tunnel ID) get a clean 404 rather than an error.

> **Why this matters:** Running multiple services behind a single tunnel on one machine is a common pattern for small-scale deployments. It's the same conceptual model as virtual hosting in nginx — one IP, multiple domains, route by hostname.

---

## 12. Why No Airflow

Airflow is the standard orchestration tool for data pipelines. It's not used in this project for a practical reason: it's too heavy for a t2.micro instance.

```
# requirements.txt
# Airflow must be installed separately with constraints:
# pip install "apache-airflow==2.10.5" --constraint "..."
# (commented out — excluded from Docker image)
```

Airflow's web server, scheduler, and metadata database together consume several hundred MB of RAM at minimum. On a 1GB t2.micro already running three Docker containers, adding Airflow would exhaust available memory.

The alternative implemented here: the Databricks Jobs API, called directly from the Streamlit UI. This provides:
- Manual trigger (click a button in the browser)
- Sequential execution of all 9 notebooks
- Live status updates per step
- Automatic dashboard cache invalidation on success

What it lacks compared to Airflow:
- Scheduling (cron-based runs)
- Automatic retry on failure
- DAG visualisation
- Dependency graph management
- Alerting

For the current project scope (a portfolio project with manual or semi-regular pipeline runs), the Jobs API approach is the right tradeoff. A production pipeline serving a business would warrant Airflow or Databricks Workflows.

> **Why this matters:** Technology choices should match requirements. Knowing what a tool adds — and whether you actually need it — is more valuable than always reaching for the most powerful option. The ability to articulate this tradeoff clearly is a sign of engineering maturity.

---

## 13. The Jobs API as a Lightweight Orchestrator

The full pipeline execution from Python (see `python.md` section 8 for the detailed code):

```python
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
```

**Why sequential execution?** Each layer depends on the previous one:
- Silver reads from Bronze → Bronze must complete first
- Gold reads from Silver → Silver must complete first
- Gold notebooks also have dependencies between them (`02_gold_romania` reads from `gold.ev_market_share` produced by `01_gold_market_share`)

Parallelism within a layer would be possible in theory (e.g. all three Bronze notebooks could run simultaneously) but adds complexity for minimal gain at this scale. Sequential execution is simpler, easier to debug, and the total pipeline runtime is acceptable.

**Failure handling:** if any step fails, all subsequent steps show ⬜ (not started) and the pipeline stops. The user sees exactly which notebook failed and can investigate in the Databricks UI before re-running.

---

## 14. Idempotency Across the Pipeline

Every stage of the pipeline is designed to be safely re-runnable:

| Stage | Idempotency mechanism |
|---|---|
| Ingestion | MD5 hash check — skips upload if data unchanged |
| Bronze | `source_file` deduplication — skips files already in the table |
| Silver | Full overwrite — recomputes from Bronze entirely |
| Gold | Full overwrite — recomputes from Silver entirely |
| S3 Parquet | `mode("overwrite")` — replaces the export folder |

**What "idempotent" means in practice:** you can run the full pipeline twice in a row and the second run produces exactly the same result as the first — no duplicate data, no error, no side effects.

This matters because pipelines fail. A Databricks notebook can timeout, a network call can fail, a job can be interrupted. When a failure happens, you need to be able to re-run from the beginning (or from the failing step) with confidence that you won't corrupt the data.

The MD5 check at ingestion is the most subtle part. Without it:
- Run 1: fetch API → upload `file_v1.json` → Bronze loads it
- Run 2 (same data): fetch API → upload `file_v2.json` (same content, new name) → Bronze loads it again → Silver deduplicates → OK but wasteful
- Run 2 (new data): fetch API → upload `file_v2.json` (new content) → Bronze loads it → Silver deduplicates → correct new data flows through

With the MD5 check:
- Run 2 (same data): fetch API → MD5 matches → skip upload → Bronze unchanged → no reprocessing needed
