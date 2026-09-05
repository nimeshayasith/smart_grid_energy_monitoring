# GridPulse – Claude Code Operating Guide

See the full project specification and phase-by-phase plan in the parent
CLAUDE.md one level up (c:\Users\Admin\Desktop\Big Data\CLAUDE.md).

## Quick start for Claude Code in this repo

```powershell
# Start the full stack
docker compose up -d

# Check all services
docker compose ps -a

# Watch Spark processing
docker compose logs -f spark-job

# Run tests (no Docker needed)
python -m pytest tests/ -v
```

## Key files

| File | Purpose |
|------|---------|
| docker-compose.yml | Full 13-service stack |
| .env | All config constants (single source of truth) |
| generators/meter_producer.py | Smart-meter Kafka producer |
| generators/tariff_generator.py | Daily tariff Kafka producer |
| spark/streaming_job.py | Core Kappa processing (Spark Structured Streaming) |
| spark/schemas.py | PySpark schemas |
| db/init.sql | PostgreSQL schema + seed tariff data |
| airflow/dags/daily_tariff_dag.py | Airflow DAG (every 5 min = 1 simulated day) |
| api/main.py | FastAPI serving layer |
| dashboard/index.html | Live web dashboard |
| observability/ | Prometheus + Grafana config |
| scripts/healthcheck.py | Pipeline health check script |
| tests/ | Unit tests (22 tests, no Docker needed) |

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Dashboard | http://localhost:3000 | — |
| API | http://localhost:8000/docs | — |
| Airflow | http://localhost:8080 | admin / admin |
| Grafana | http://localhost:3001 | admin / admin |
| Prometheus | http://localhost:9090 | — |
