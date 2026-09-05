"""
GridPulse – FastAPI serving layer (Phase 9)

Endpoints:
  GET /health                      – liveness probe
  GET /grid-load                   – latest zone aggregations (renewable mix)
  GET /grid-load/history/{zone}    – last N records for a zone
  GET /billing/{household_id}      – latest billing record for a household
  GET /billing/{household_id}/all  – all billing records (most recent first)
  GET /alerts                      – open (unresolved) alerts
  GET /metrics                     – Prometheus text metrics
"""

import os
import logging
from typing import Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

# ── Config ────────────────────────────────────────────────────────────────────
PG_HOST     = os.environ.get("POSTGRES_HOST",     "postgres")
PG_PORT     = os.environ.get("POSTGRES_PORT",     "5432")
PG_DB       = os.environ.get("POSTGRES_DB",       "gridpulse")
PG_USER     = os.environ.get("POSTGRES_USER",     "gridpulse")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "gridpulse_secret")

DB_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","service":"api","msg":"%(message)s"}',
)
logger = logging.getLogger(__name__)

engine = create_engine(DB_URL, pool_pre_ping=True, pool_size=5, max_overflow=10)

# ── Prometheus metrics ────────────────────────────────────────────────────────
REQUEST_COUNT = Counter("gridpulse_api_requests_total", "Total API requests", ["endpoint"])
REQUEST_LATENCY = Histogram("gridpulse_api_latency_seconds", "API request latency", ["endpoint"])

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="GridPulse API",
    description="Smart Grid Energy Monitoring & Billing REST API",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ── Pydantic response models ──────────────────────────────────────────────────

class ZoneLoad(BaseModel):
    grid_zone:             str
    window_start:          datetime
    window_end:            datetime
    total_consumption_kwh: float
    total_solar_kwh:       float
    renewable_pct:         float
    reading_count:         int

class BillingRecord(BaseModel):
    household_id:          str
    billing_date:          str
    window_start:          datetime
    window_end:            datetime
    total_consumption_kwh: float
    total_solar_kwh:       float
    tariff_rate:           float
    billing_tier:          str
    subsidy_flag:          bool
    bill_amount:           float

class Alert(BaseModel):
    id:            int
    grid_zone:     str
    window_start:  datetime
    window_end:    datetime
    renewable_pct: float
    threshold:     float
    severity:      str
    created_at:    datetime
    resolved_at:   Optional[datetime]


# ── Helpers ───────────────────────────────────────────────────────────────────

def query(sql: str, params: dict = None) -> list[dict]:
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        cols = list(result.keys())
        return [dict(zip(cols, row)) for row in result.fetchall()]


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    try:
        query("SELECT 1")
        return {"status": "ok", "db": "connected"}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/grid-load", response_model=list[ZoneLoad])
def get_grid_load():
    """Most recent completed 1-min window for every zone."""
    REQUEST_COUNT.labels(endpoint="grid-load").inc()
    with REQUEST_LATENCY.labels(endpoint="grid-load").time():
        rows = query("""
            SELECT DISTINCT ON (grid_zone)
                grid_zone, window_start, window_end,
                total_consumption_kwh, total_solar_kwh,
                renewable_pct, reading_count
            FROM grid_load_by_zone
            ORDER BY grid_zone, window_start DESC
        """)
    return rows


@app.get("/grid-load/history/{zone}", response_model=list[ZoneLoad])
def get_zone_history(zone: str, limit: int = Query(default=20, le=200)):
    """Last N 1-min windows for a specific zone."""
    REQUEST_COUNT.labels(endpoint="grid-load-history").inc()
    rows = query("""
        SELECT grid_zone, window_start, window_end,
               total_consumption_kwh, total_solar_kwh,
               renewable_pct, reading_count
        FROM grid_load_by_zone
        WHERE grid_zone = :zone
        ORDER BY window_start DESC
        LIMIT :limit
    """, {"zone": zone.upper(), "limit": limit})
    if not rows:
        raise HTTPException(status_code=404, detail=f"No data for zone '{zone}'")
    return rows


@app.get("/billing", response_model=list[BillingRecord])
def get_daily_report(date: str = Query(default=None, description="YYYY-MM-DD, defaults to today")):
    """Daily consolidated billing + solar-contribution report for ALL households."""
    REQUEST_COUNT.labels(endpoint="billing-report").inc()
    if date:
        sql = """
            SELECT DISTINCT ON (household_id)
                household_id, billing_date::text, window_start, window_end,
                total_consumption_kwh, total_solar_kwh,
                tariff_rate, billing_tier, subsidy_flag, bill_amount
            FROM household_billing
            WHERE billing_date = CAST(:filter_date AS DATE)
            ORDER BY household_id, window_start DESC
        """
        params = {"filter_date": date}
    else:
        sql = """
            SELECT DISTINCT ON (household_id)
                household_id, billing_date::text, window_start, window_end,
                total_consumption_kwh, total_solar_kwh,
                tariff_rate, billing_tier, subsidy_flag, bill_amount
            FROM household_billing
            ORDER BY household_id, window_start DESC
        """
        params = {}
    return query(sql, params)


@app.get("/billing/{household_id}", response_model=BillingRecord)
def get_latest_bill(household_id: str):
    """Most recent billing record for a household."""
    REQUEST_COUNT.labels(endpoint="billing").inc()
    rows = query("""
        SELECT household_id, billing_date::text, window_start, window_end,
               total_consumption_kwh, total_solar_kwh,
               tariff_rate, billing_tier, subsidy_flag, bill_amount
        FROM household_billing
        WHERE household_id = :hid
        ORDER BY window_start DESC
        LIMIT 1
    """, {"hid": household_id.upper()})
    if not rows:
        raise HTTPException(status_code=404, detail=f"No billing data for '{household_id}'")
    return rows[0]


@app.get("/billing/{household_id}/all", response_model=list[BillingRecord])
def get_all_bills(household_id: str, limit: int = Query(default=30, le=200)):
    """All billing records for a household, newest first."""
    REQUEST_COUNT.labels(endpoint="billing-all").inc()
    rows = query("""
        SELECT household_id, billing_date::text, window_start, window_end,
               total_consumption_kwh, total_solar_kwh,
               tariff_rate, billing_tier, subsidy_flag, bill_amount
        FROM household_billing
        WHERE household_id = :hid
        ORDER BY window_start DESC
        LIMIT :limit
    """, {"hid": household_id.upper(), "limit": limit})
    if not rows:
        raise HTTPException(status_code=404, detail=f"No billing data for '{household_id}'")
    return rows


@app.get("/alerts", response_model=list[Alert])
def get_alerts(include_resolved: bool = Query(default=False)):
    """Open (unresolved) renewable-shortfall alerts, newest first."""
    REQUEST_COUNT.labels(endpoint="alerts").inc()
    sql = """
        SELECT id, grid_zone, window_start, window_end,
               renewable_pct, threshold, severity, created_at, resolved_at
        FROM alerts
    """
    if not include_resolved:
        sql += " WHERE resolved_at IS NULL"
    sql += " ORDER BY created_at DESC LIMIT 100"
    return query(sql)


@app.get("/metrics")
def metrics():
    """Prometheus text-format metrics."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
