# GridPulse – Smart Grid Energy Monitoring & Billing

EC8203 Applied Big Data Engineering | Use Case 3 | Kappa Architecture

## Architecture Overview

```
Smart Meters (simulated)
        │  JSON every 2-5 s
        ▼
  meter-readings (Kafka, 3 partitions, keyed by grid_zone)
        │
        ▼
  Spark Structured Streaming
   ├─ 1-min tumbling window → grid_load_by_zone (PostgreSQL)
   ├─ 5-min tumbling window → household_billing (PostgreSQL)  [1 sim-day]
   └─ alert rule (renewable_pct < 20%) → alerts topic + alerts table
        │
        ▼
  FastAPI  ──►  Dashboard (port 3000)
  (port 8000)

Airflow (every 5 min = 1 simulated day)
  └─ tariff_generator.py → tariff-updates (Kafka, compacted) + tariff (PostgreSQL)

Prometheus + Grafana (ports 9090 / 3001)
```

**Simulated clock:** 1 real minute ≈ 288 simulated minutes.  
5 real minutes = 1 simulated day. Configured via `SIMULATED_DAY_SECONDS=300`.

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Docker Desktop | 4.x+ | https://www.docker.com/products/docker-desktop/ |
| Docker Compose | v2 (bundled) | included with Docker Desktop |
| Python | 3.10+ | for running tests locally |
| Git | any | for version control |

> **Windows users:** Make sure Docker Desktop is running before any `docker compose` command.

## Quick Start

```bash
# 1. Clone and enter the project
git clone <repo-url>
cd gridpulse

# 2. Copy config (defaults work out of the box)
cp .env.example .env

# 3. Build images and start everything
docker compose up -d --build

# 4. Watch startup logs
docker compose logs -f
```

First run takes **5-10 minutes** — Docker builds the Spark image and downloads connector JARs.  
Subsequent starts take ~1 minute.

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Dashboard | http://localhost:3000 | — |
| API | http://localhost:8000 | — |
| API docs | http://localhost:8000/docs | — |
| Airflow UI | http://localhost:8080 | admin / admin |
| Grafana | http://localhost:3001 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| PostgreSQL | localhost:5432 | gridpulse / gridpulse_secret |
| Kafka | localhost:29092 | — |

## Verifying the Pipeline

### 1. Kafka topics
```bash
docker compose exec kafka \
  kafka-topics --bootstrap-server localhost:9092 --describe
```
Expect three topics: `meter-readings` (3 partitions), `tariff-updates` (compacted), `alerts`.

### 2. Live meter readings
```bash
# Requires kafkacat/kcat installed on host, or:
docker compose exec kafka \
  kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic meter-readings --max-messages 5 --from-beginning
```

### 3. PostgreSQL tables
```bash
docker compose exec postgres \
  psql -U gridpulse -d gridpulse -c \
  "SELECT grid_zone, renewable_pct, window_start FROM grid_load_by_zone ORDER BY window_start DESC LIMIT 5;"
```

### 4. API
```bash
curl http://localhost:8000/grid-load        # zone renewable mix
curl http://localhost:8000/billing/H001     # household bill
curl http://localhost:8000/alerts           # active alerts
```

### 5. Trigger an alert manually
Stop the meter producer temporarily:
```bash
docker compose stop meter-producer
# Wait 3 minutes, then check:
curl http://localhost:8000/alerts
# Restart:
docker compose start meter-producer
```

## Stopping the Stack

```bash
docker compose down          # stops containers, keeps volumes
docker compose down -v       # also deletes all data (clean slate)
```

## Running Tests Locally

```bash
pip install kafka-python psycopg2-binary fastapi httpx pytest
python -m pytest tests/ -v
```

## Configuration Reference

All settings live in `.env`. Key values:

| Variable | Default | Description |
|----------|---------|-------------|
| `SIMULATED_DAY_SECONDS` | `300` | Real seconds per simulated day |
| `NUM_METERS` | `30` | Total simulated meters (10 per zone) |
| `RENEWABLE_ALERT_THRESHOLD` | `20.0` | % below which an alert fires |
| `WINDOW_DURATION_SHORT` | `1 minute` | Spark zone aggregation window |
| `WINDOW_DURATION_LONG` | `5 minutes` | Spark billing window (= 1 sim-day) |
| `METER_EMIT_INTERVAL_MIN/MAX` | `2 / 5` | Seconds between meter batches |

## Team & Contributions

| Member | Role | Phases |
|--------|------|--------|
| Nimesha | Infra/DevOps: Docker Compose, Kafka, Airflow, Spark setup, observability | 3, 5, 7, 10 |
| Kaveesha | Data & processing: generators, Spark logic, PostgreSQL schema | 4, 6, 8 |
| Lasindu | Serving & delivery: API, dashboard, README, report | 9, 11, 12 |

## Troubleshooting

**Spark job exits immediately**  
Check logs: `docker compose logs spark-job`. Most common cause: Kafka not ready yet. The job will restart automatically.

**`airflow-init` fails with "already exists"**  
Normal on second run — the user creation is idempotent (`|| true`). Ignore.

**`docker compose build` fails on Spark Dockerfile (curl timeout)**  
You may be behind a corporate proxy. Set `HTTP_PROXY` / `HTTPS_PROXY` in your environment before building, or configure Docker Desktop's proxy settings.

**PostgreSQL data persists after `docker compose down`**  
Use `docker compose down -v` to also remove volumes and start fresh.

**Dashboard shows "Connecting to API…"**  
API may still be starting. Wait ~30 s then refresh. Check: `docker compose ps api`.
