"""
GridPulse – Daily tariff/billing reference data producer (Phase 4)

Emits one record per household to the 'tariff-updates' Kafka topic
and upserts into the PostgreSQL tariff table.

Called in two ways:
  1. Manually: python tariff_generator.py
  2. Via Airflow BashOperator (every SIMULATED_DAY_SECONDS = 5 min)

Topic is log-compacted and keyed by household_id so consumers always
see the latest rate for each household.
"""

import json
import logging
import os
import random
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP   = os.environ.get("KAFKA_BOOTSTRAP_SERVERS",   "localhost:9092")
TARIFF_TOPIC      = os.environ.get("KAFKA_TOPIC_TARIFF_UPDATES", "tariff-updates")
NUM_METERS        = int(os.environ.get("NUM_METERS",             "30"))
GRID_ZONES        = os.environ.get("GRID_ZONES", "ZONE_A,ZONE_B,ZONE_C").split(",")

PG_HOST     = os.environ.get("POSTGRES_HOST",     "localhost")
PG_PORT     = int(os.environ.get("POSTGRES_PORT", "5432"))
PG_DB       = os.environ.get("POSTGRES_DB",       "gridpulse")
PG_USER     = os.environ.get("POSTGRES_USER",     "gridpulse")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "gridpulse_secret")

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","service":"tariff-generator","msg":"%(message)s"}',
)
logger = logging.getLogger(__name__)

BILLING_TIERS = ["RESIDENTIAL", "COMMERCIAL"]
TARIFF_RANGES = {"RESIDENTIAL": (0.10, 0.20), "COMMERCIAL": (0.18, 0.30)}


def generate_tariff_records(seed: int = None) -> list[dict]:
    """Generate one tariff record per household with realistic rates."""
    rng = random.Random(seed)
    records = []
    per_zone = NUM_METERS // len(GRID_ZONES)

    for z_idx, zone in enumerate(GRID_ZONES):
        for m_idx in range(per_zone):
            num = z_idx * per_zone + m_idx + 1
            tier = rng.choice(BILLING_TIERS)
            lo, hi = TARIFF_RANGES[tier]
            records.append({
                "household_id": f"H{num:03d}",
                "tariff_rate":  round(rng.uniform(lo, hi), 4),
                "billing_tier": tier,
                "subsidy_flag": rng.random() < 0.20,   # 20 % of households get subsidy
                "updated_at":   datetime.now(timezone.utc).isoformat(),
            })
    return records


def send_to_kafka(records: list[dict]) -> None:
    retries = 10
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
            )
            for rec in records:
                producer.send(
                    TARIFF_TOPIC,
                    key=rec["household_id"],
                    value=rec,
                )
            producer.flush()
            producer.close()
            logger.info("Sent %d tariff records to topic '%s'", len(records), TARIFF_TOPIC)
            return
        except NoBrokersAvailable:
            logger.warning("Kafka not ready (attempt %d/%d)…", attempt, retries)
            time.sleep(5)
    logger.error("Could not reach Kafka; skipping Kafka publish")


def upsert_to_postgres(records: list[dict]) -> None:
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB,
            user=PG_USER, password=PG_PASSWORD,
        )
        with conn, conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO tariff (household_id, tariff_rate, billing_tier, subsidy_flag, updated_at)
                VALUES %s
                ON CONFLICT (household_id) DO UPDATE SET
                    tariff_rate  = EXCLUDED.tariff_rate,
                    billing_tier = EXCLUDED.billing_tier,
                    subsidy_flag = EXCLUDED.subsidy_flag,
                    updated_at   = EXCLUDED.updated_at
                """,
                [(r["household_id"], r["tariff_rate"], r["billing_tier"],
                  r["subsidy_flag"], r["updated_at"]) for r in records],
            )
        conn.close()
        logger.info("Upserted %d tariff records to PostgreSQL", len(records))
    except Exception as exc:
        logger.error("PostgreSQL upsert failed: %s", exc)


def main():
    # Use current minute as seed so rates change each simulated day but stay
    # stable within one invocation.
    seed = int(time.time() // 60)
    records = generate_tariff_records(seed=seed)
    send_to_kafka(records)
    upsert_to_postgres(records)
    logger.info("Tariff generation complete for simulated day cycle %d", seed)


if __name__ == "__main__":
    main()
