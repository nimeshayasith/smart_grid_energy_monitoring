# ⚡ GridPulse — Smart Grid Energy Monitoring & Billing

> EC8203 Applied Big Data Engineering · Use Case 3 · Kappa Architecture  
> Group Project — Nimesha, Kaveesha, Lasindu

---

## What This System Does

GridPulse is a real-time data pipeline that simulates a utility company's smart grid:

- **30 smart meters** emit power consumption + solar generation readings every 2–5 seconds
- **Apache Kafka** ingests the stream across 3 grid zones
- **Apache Spark Structured Streaming** processes the data in 1-minute windows, joins it with daily tariff rates, and calculates household bills
- **Alerts fire automatically** when a zone's renewable contribution drops below 20%
- A **live dashboard** shows zone status, renewable trends, alerts, and billing — updating every 5 seconds
- **Apache Airflow** runs the tariff generator every 5 minutes (= 1 simulated day)

**Simulated clock:** 5 real minutes = 1 simulated day

---

## Architecture

```
Smart Meters (meter_producer.py)
        │  JSON every 2–5 s · keyed by grid_zone
        ▼
  ┌─────────────────────────────────┐
  │   Kafka · meter-readings topic  │  ← 3 partitions (one per zone)
  └─────────────────────────────────┘
        │
        ▼
  ┌─────────────────────────────────────────────────┐
  │         Spark Structured Streaming              │
  │  • 1-min tumbling window  → grid_load_by_zone   │
  │  • 5-min tumbling window  → household_billing   │
  │  • Alert rule (renew < 20%) → alerts table      │
  │  • Stream-static join with tariff data          │
  └─────────────────────────────────────────────────┘
        │
        ▼
  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
  │  PostgreSQL  │     │   FastAPI    │     │  Dashboard   │
  │  3 tables +  │────▶│  REST API   │────▶│  localhost   │
  │  tariff ref  │     │  port 8000   │     │  port 3000   │
  └──────────────┘     └──────────────┘     └──────────────┘

Airflow (every 5 min = 1 simulated day)
  └─▶ tariff_generator.py → tariff-updates (Kafka, compacted) + tariff table (PostgreSQL)

Prometheus + Grafana → pipeline observability (ports 9090, 3001)
```

**Architecture choice: Kappa** — tariff data flows through Kafka (log-compacted topic), so no separate batch layer is needed. One unified streaming path handles everything.

---

## Prerequisites — Install These First

| Tool | Purpose | Download |
|------|---------|----------|
| **Docker Desktop** | Runs all services as containers | https://www.docker.com/products/docker-desktop/ |
| **Git** | Clone the repository | https://git-scm.com/downloads |
| **Python 3.10+** | Run tests locally (optional) | https://www.python.org/downloads/ |

> **Windows users:** After installing Docker Desktop, restart your computer. Make sure the Docker whale icon in the system tray is **green** before running any commands.

> **macOS/Linux users:** Follow the Docker Engine installation guide at https://docs.docker.com/engine/install/

### Verify your tools are ready

Open a terminal and run:
```bash
docker --version        # should show Docker version 20+
docker compose version  # should show Docker Compose version 2+
git --version
```

---

## Quick Start (5 steps)

### Step 1 — Clone the repository
```bash
git clone https://github.com/nimeshayasith/smart_grid_energy_monitoring.git
cd smart_grid_energy_monitoring
```

### Step 2 — Create the environment config
```bash
# Windows PowerShell
copy .env.example .env

# macOS / Linux
cp .env.example .env
```
The default values in `.env` work out of the box — no edits needed for local setup.

### Step 3 — Build and start everything
```bash
docker compose up -d --build
```

> **First run takes 5–10 minutes** — Docker downloads images and builds the Spark container (which installs Java + PySpark). Subsequent starts take under 1 minute.

### Step 4 — Create the Airflow admin user
Run this once after first startup:
```bash
docker compose exec airflow-webserver airflow users create \
  --username admin --password admin \
  --firstname GridPulse --lastname Admin \
  --role Admin --email admin@gridpulse.local
```

### Step 5 — Check everything is running
```bash
docker compose ps -a
```

You should see all containers as **healthy** or **exited (0)**:

| Container | Expected Status |
|-----------|----------------|
| zookeeper | Up (healthy) |
| kafka | Up (healthy) |
| kafka-setup | Exited (0) ✓ |
| kafka-exporter | Up (healthy) |
| postgres | Up (healthy) |
| spark-job | Up (healthy) |
| meter-producer | Up (healthy) |
| airflow-init | Exited (0) ✓ |
| airflow-webserver | Up (healthy) |
| airflow-scheduler | Up (healthy) |
| api | Up (healthy) |
| dashboard | Up (healthy) |
| prometheus | Up (healthy) |
| grafana | Up (healthy) |

---

## Service URLs

Once everything is running, open these in your browser:

| Service | URL | Login |
|---------|-----|-------|
| 📊 **Live Dashboard** | http://localhost:3000 | — |
| 🔌 **REST API** | http://localhost:8000 | — |
| 📖 **API Docs (Swagger)** | http://localhost:8000/docs | — |
| 🌊 **Airflow UI** | http://localhost:8080 | admin / admin |
| 📈 **Grafana** | http://localhost:3001 | admin / admin |
| 🔭 **Prometheus** | http://localhost:9090 | — |
| 🐘 **PostgreSQL** | localhost:5432 | gridpulse / gridpulse_secret |

---

## Dashboard Guide

Open **http://localhost:3000** — it auto-refreshes every 5 seconds.

| Section | What it shows |
|---------|-------------|
| **KPI Cards** | Average renewable %, total consumption kWh, active alert count, today's total revenue |
| **Live Zone Status** | Per-zone renewable %, solar vs grid kWh, reading count with colour-coded health |
| **Renewable % Trend** | Chart of last 20 one-minute windows for all 3 zones |
| **Active Alerts** | Renewable-shortfall warnings (fires when zone drops below 20%) |
| **Daily Billing Report** | All 30 households — consumption, solar contribution, tariff, subsidy, bill amount |
| **Billing Lookup** | Search any household (H001–H030) for their latest bill |
| **Pipeline Stats** | Architecture constants — window sizes, topic config, row counts |

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Liveness check |
| GET | `/grid-load` | Latest 1-min zone aggregation (renewable mix) |
| GET | `/grid-load/history/{zone}` | Last N windows for a zone (e.g. `/grid-load/history/ZONE_A?limit=20`) |
| GET | `/billing` | **Daily consolidated report** — all 30 households |
| GET | `/billing?date=2024-01-15` | Billing report filtered by date |
| GET | `/billing/{household_id}` | Latest bill for one household (e.g. `/billing/H001`) |
| GET | `/billing/{household_id}/all` | Full billing history for one household |
| GET | `/alerts` | Open renewable-shortfall alerts |
| GET | `/alerts?include_resolved=true` | All alerts including resolved |
| GET | `/metrics` | Prometheus metrics |

### Quick API test
```bash
# Windows PowerShell
curl.exe http://localhost:8000/grid-load
curl.exe http://localhost:8000/billing/H001
curl.exe http://localhost:8000/alerts

# macOS / Linux
curl http://localhost:8000/grid-load
curl http://localhost:8000/billing
```

---

## Verify the Pipeline is Working

### Check Kafka topics were created
```bash
docker compose exec kafka \
  kafka-topics --bootstrap-server localhost:9092 --describe
```
Expected: 3 topics — `meter-readings` (3 partitions), `tariff-updates` (compacted), `alerts`

### Check PostgreSQL has live data
```bash
docker compose exec postgres \
  psql -U gridpulse -d gridpulse -c \
  "SELECT grid_zone, ROUND(renewable_pct::numeric,1) AS renew_pct,
   reading_count, window_start
   FROM grid_load_by_zone ORDER BY window_start DESC LIMIT 6;"
```

### Check meter producer is sending data
```bash
docker compose logs meter-producer --tail 10
```
You should see: `"Total readings sent: 300"`, `"Total readings sent: 600"`, etc.

### Check Spark is processing
```bash
docker compose logs spark-job --tail 20
```
You should see: `"epoch N: wrote zone aggregates"` and `"epoch N: wrote billing rows"`

### Trigger an alert deliberately
```bash
# Stop the meter producer — Spark will detect missing data
docker compose stop meter-producer

# Wait ~3 minutes, then check alerts
curl.exe http://localhost:8000/alerts   # Windows
curl http://localhost:8000/alerts       # macOS/Linux

# Restart the producer
docker compose start meter-producer
```

---

## Project Structure

```
smart_grid_energy_monitoring/
├── docker-compose.yml          # Full 13-service stack definition
├── .env.example                # Config template (copy to .env)
├── .env                        # Your local config (NOT committed to git)
├── .gitignore
├── requirements.txt            # Python dependencies
├── README.md                   # This file
├── CLAUDE.md                   # Claude Code operating guide
│
├── generators/                 # Data source simulators
│   ├── meter_producer.py       # Streaming: 30 smart meters → Kafka
│   ├── tariff_generator.py     # Batch: tariff rates → Kafka + PostgreSQL
│   ├── Dockerfile
│   └── requirements-gen.txt
│
├── spark/                      # Core processing layer
│   ├── streaming_job.py        # Spark Structured Streaming job
│   ├── schemas.py              # PySpark schema definitions
│   ├── Dockerfile
│   └── requirements-spark.txt
│
├── airflow/                    # Orchestration layer
│   ├── Dockerfile
│   └── dags/
│       └── daily_tariff_dag.py # DAG: runs tariff_generator every 5 min
│
├── db/                         # Database layer
│   ├── init.sql                # PostgreSQL schema + seed tariff data
│   └── init-airflow.sql        # Airflow database setup
│
├── api/                        # Serving layer
│   ├── main.py                 # FastAPI application (6 endpoints)
│   ├── Dockerfile
│   └── requirements-api.txt
│
├── dashboard/                  # Frontend
│   └── index.html              # Live Chart.js dashboard (no build step)
│
├── observability/              # Monitoring
│   ├── prometheus.yml          # Scrape config
│   └── grafana/
│       ├── dashboards/
│       │   └── gridpulse.json  # Pre-built Grafana dashboard
│       └── provisioning/       # Auto-provisioning config
│
├── scripts/
│   ├── create-topics.sh        # Kafka topic creation script
│   └── healthcheck.py          # Pipeline health check script
│
└── tests/
    ├── test_generators.py      # 15 unit tests for data generators
    └── test_api.py             # 7 unit tests for API routes
```

---

## Configuration

All settings are in `.env`. Key variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `SIMULATED_DAY_SECONDS` | `300` | Real seconds = 1 simulated day (5 min) |
| `NUM_METERS` | `30` | Total smart meters (10 per zone) |
| `GRID_ZONES` | `ZONE_A,ZONE_B,ZONE_C` | Grid zones |
| `RENEWABLE_ALERT_THRESHOLD` | `20.0` | Alert fires when renewable % drops below this |
| `WINDOW_DURATION_SHORT` | `1 minute` | Spark zone aggregation window |
| `WINDOW_DURATION_LONG` | `5 minutes` | Spark billing window (= 1 simulated day) |
| `METER_EMIT_INTERVAL_MIN` | `2` | Min seconds between meter reading batches |
| `METER_EMIT_INTERVAL_MAX` | `5` | Max seconds between meter reading batches |
| `POSTGRES_PASSWORD` | `gridpulse_secret` | Change this for any non-local deployment |

---

## Running Tests

Tests run locally without Docker:

```bash
# Install test dependencies
pip install pytest fastapi httpx sqlalchemy psycopg2-binary kafka-python \
            pydantic python-dotenv prometheus-client uvicorn

# Run all 22 tests
python -m pytest tests/ -v
```

Expected output: `22 passed`

---

## Stopping and Restarting

```bash
# Stop all containers (keeps your data)
docker compose down

# Stop AND delete all data (clean slate)
docker compose down -v

# Restart everything (no rebuild — fast)
docker compose up -d

# Restart a single service
docker compose restart spark-job
docker compose restart meter-producer
```

---

## Troubleshooting

### Docker Desktop not found
Make sure Docker Desktop is **open and running** — look for the whale icon in the system tray (Windows) or menu bar (Mac). It must show as green/running before any `docker compose` command.

### "docker: command not found" in PowerShell
Add Docker to your PATH:
```powershell
$env:PATH = "C:\Users\$env:USERNAME\AppData\Local\Programs\DockerDesktop\resources\bin;$env:PATH"
```
Then open a **new PowerShell window** — Docker should work permanently after restart.

### Spark job keeps restarting
Kafka may not be ready yet. Wait 60 seconds — Spark will retry automatically. Check with:
```bash
docker compose logs spark-job --tail 20
```

### Dashboard shows "Connecting to API…"
The API is still starting. Wait 30 seconds and refresh. Verify:
```bash
docker compose ps api
```

### Airflow login fails (admin / admin doesn't work)
Run the user creation command manually:
```bash
docker compose exec airflow-webserver airflow users create \
  --username admin --password admin \
  --firstname GridPulse --lastname Admin \
  --role Admin --email admin@gridpulse.local
```

### Grafana shows "failed to load application files"
Hard refresh: **Ctrl + Shift + R** in the browser, or open in an incognito window.

### Port already in use
Another service on your machine is using the port. Stop it, or change the port mapping in `docker-compose.yml`. Common conflicts:
- Port 5432 → another PostgreSQL instance
- Port 8080 → another web server (Tomcat, etc.)

### First build is very slow
Normal — Docker is downloading ~3 GB of images and building the Spark container (Java + PySpark). This only happens once. Subsequent `docker compose up -d` starts in under 1 minute.

### Behind a university/corporate proxy
Configure Docker Desktop's proxy in: Settings → Resources → Proxies. Also set in your terminal:
```bash
export HTTP_PROXY=http://your-proxy:port
export HTTPS_PROXY=http://your-proxy:port
```

---

## Technology Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| Ingestion | Apache Kafka 3.6 | Distributed, partitioned, replay-capable message bus |
| Stream Processing | Apache Spark 3.5 Structured Streaming | Exactly-once semantics, native windowing, SQL API |
| Orchestration | Apache Airflow 2.8 | Industry-standard DAG scheduler for the daily tariff trigger |
| Storage | PostgreSQL 16 | ACID-compliant; relational model suits billing queries |
| Serving | FastAPI + uvicorn | High-performance async Python API with auto docs |
| Dashboard | HTML + Chart.js | Zero build-step, browser-native, works anywhere |
| Observability | Prometheus + Grafana + kafka-exporter | Industry-standard metrics stack |
| Containerisation | Docker Compose | Reproducible single-command deployment |

---

## Team Contributions

| Member | Role | Phases |
|--------|------|--------|
| **Nimesha** | Infra/DevOps — Docker Compose, Kafka setup, Airflow, Spark deployment, Prometheus/Grafana | 3, 5, 7, 10 |
| **Kaveesha** | Data & Processing — Python generators, Spark transformation/join/aggregation logic, PostgreSQL schema | 4, 6, 8 |
| **Lasindu** | Serving & Delivery — FastAPI, live dashboard, README, report, demo video | 9, 11, 12 |

All three members contributed to Phase 1 (business requirements) and Phase 2 (architecture decision).
