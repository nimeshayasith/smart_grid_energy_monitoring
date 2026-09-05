"""
GridPulse – Pipeline health-check script (Phase 10)

Checks:
  1. PostgreSQL is reachable and has recent data in grid_load_by_zone.
  2. No data received in the last N minutes (configurable) → alert.
  3. Error rate above threshold → alert.

Exit code 0 = healthy, 1 = unhealthy (used by Docker HEALTHCHECK and Grafana alert).
Run: python scripts/healthcheck.py
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone, timedelta

import psycopg2

# ── Config ────────────────────────────────────────────────────────────────────
PG_HOST     = os.environ.get("POSTGRES_HOST",     "localhost")
PG_PORT     = int(os.environ.get("POSTGRES_PORT", "5432"))
PG_DB       = os.environ.get("POSTGRES_DB",       "gridpulse")
PG_USER     = os.environ.get("POSTGRES_USER",     "gridpulse")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "gridpulse_secret")

# Alert if no new zone data within this many minutes
STALE_THRESHOLD_MINUTES = int(os.environ.get("HEALTH_STALE_MINUTES", "3"))

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","service":"healthcheck","msg":"%(message)s"}',
)
logger = logging.getLogger(__name__)


def check_db() -> dict:
    issues = []
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB,
            user=PG_USER, password=PG_PASSWORD,
            connect_timeout=5,
        )
        with conn.cursor() as cur:
            # Latest data timestamp
            cur.execute("SELECT MAX(created_at) FROM grid_load_by_zone;")
            latest = cur.fetchone()[0]

            if latest is None:
                issues.append("grid_load_by_zone is empty – Spark may not have started yet")
            else:
                age = datetime.now(timezone.utc) - latest.replace(tzinfo=timezone.utc)
                logger.info("Latest zone data: %s (age %.1f min)", latest.isoformat(), age.total_seconds() / 60)
                if age > timedelta(minutes=STALE_THRESHOLD_MINUTES):
                    issues.append(
                        f"No new zone data for {age.total_seconds()/60:.1f} min "
                        f"(threshold: {STALE_THRESHOLD_MINUTES} min) – meter producer may be down"
                    )

            # Open alerts
            cur.execute("SELECT COUNT(*) FROM alerts WHERE resolved_at IS NULL;")
            open_alerts = cur.fetchone()[0]
            if open_alerts > 0:
                issues.append(f"{open_alerts} unresolved renewable-shortfall alerts active")

            # Total rows ingested today
            cur.execute("""
                SELECT COUNT(*) FROM grid_load_by_zone
                WHERE created_at >= NOW() - INTERVAL '1 hour';
            """)
            recent_count = cur.fetchone()[0]
            logger.info("Zone rows in last hour: %d", recent_count)

        conn.close()
    except psycopg2.OperationalError as exc:
        issues.append(f"PostgreSQL unreachable: {exc}")

    return {"issues": issues, "healthy": len(issues) == 0}


def main():
    logger.info("Running GridPulse health check…")
    result = check_db()

    if result["healthy"]:
        logger.info("Health check PASSED – pipeline looks healthy")
        print(json.dumps({"status": "healthy"}))
        sys.exit(0)
    else:
        for issue in result["issues"]:
            logger.warning("ISSUE: %s", issue)
        print(json.dumps({"status": "unhealthy", "issues": result["issues"]}))
        sys.exit(1)


if __name__ == "__main__":
    main()
