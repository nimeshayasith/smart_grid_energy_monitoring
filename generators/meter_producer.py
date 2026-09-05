"""
GridPulse – Smart-meter Kafka producer (Phase 4)

Simulates NUM_METERS smart meters spread across GRID_ZONES.
Each iteration emits one reading per meter, then sleeps
METER_EMIT_INTERVAL_MIN–METER_EMIT_INTERVAL_MAX seconds.

Simulated clock: SIMULATED_DAY_SECONDS real seconds = 1 simulated day.
Solar and consumption patterns follow a realistic diurnal cycle derived
from the simulated time-of-day — no additional constants needed.
"""

import json
import logging
import math
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Config (all from environment) ────────────────────────────────────────────
KAFKA_BOOTSTRAP        = os.environ.get("KAFKA_BOOTSTRAP_SERVERS",    "localhost:9092")
METER_TOPIC            = os.environ.get("KAFKA_TOPIC_METER_READINGS",  "meter-readings")
SIMULATED_DAY_SECONDS  = int(os.environ.get("SIMULATED_DAY_SECONDS",  "300"))
EMIT_MIN               = float(os.environ.get("METER_EMIT_INTERVAL_MIN", "2"))
EMIT_MAX               = float(os.environ.get("METER_EMIT_INTERVAL_MAX", "5"))
NUM_METERS             = int(os.environ.get("NUM_METERS",              "30"))
GRID_ZONES             = os.environ.get("GRID_ZONES", "ZONE_A,ZONE_B,ZONE_C").split(",")

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","service":"meter-producer","msg":"%(message)s"}',
)
logger = logging.getLogger(__name__)


def build_meter_fleet() -> list[dict]:
    """Create a deterministic fleet of meters distributed evenly across zones."""
    meters = []
    per_zone = NUM_METERS // len(GRID_ZONES)
    for z_idx, zone in enumerate(GRID_ZONES):
        for m_idx in range(per_zone):
            num = z_idx * per_zone + m_idx + 1
            meters.append({
                "meter_id":     f"M{num:03d}",
                "household_id": f"H{num:03d}",
                "grid_zone":    zone,
                # Fixed capacity per meter (seeded for reproducibility)
                "max_power_kwh":  round(random.uniform(2.0, 5.0), 3),
                "max_solar_kwh":  round(random.uniform(1.0, 3.0), 3),
            })
    return meters


def simulated_hour() -> float:
    """Return the current simulated hour (0–24) based on wall-clock position within a day cycle."""
    elapsed_in_cycle = time.time() % SIMULATED_DAY_SECONDS
    return (elapsed_in_cycle / SIMULATED_DAY_SECONDS) * 24.0


def generate_reading(meter: dict) -> dict:
    hour = simulated_hour()
    day_fraction = hour / 24.0

    # Solar: bell curve peaking at solar noon (hour 12)
    solar_factor = max(0.0, math.sin(math.pi * day_fraction))
    solar_gen = meter["max_solar_kwh"] * solar_factor * random.uniform(0.75, 1.0)

    # Consumption: double-peaked at 07:00 and 19:00
    morning = math.exp(-((hour - 7.0) ** 2) / 8.0)
    evening = math.exp(-((hour - 19.0) ** 2) / 8.0)
    power_factor = 0.35 + 0.65 * (morning + evening)
    power_con = meter["max_power_kwh"] * power_factor * random.uniform(0.8, 1.1)

    # Enforce physical bounds
    power_con = round(max(0.1, min(power_con, meter["max_power_kwh"] * 1.2)), 4)
    solar_gen = round(max(0.0, min(solar_gen, meter["max_solar_kwh"])), 4)

    return {
        "meter_id":              meter["meter_id"],
        "household_id":          meter["household_id"],
        "power_consumption_kwh": power_con,
        "solar_generation_kwh":  solar_gen,
        "grid_zone":             meter["grid_zone"],
        "timestamp":             datetime.now(timezone.utc).isoformat(),
    }


def wait_for_kafka(bootstrap: str, retries: int = 15, delay: int = 5) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                retries=5,
                acks="all",
                linger_ms=50,
            )
            logger.info("Connected to Kafka at %s", bootstrap)
            return producer
        except NoBrokersAvailable:
            logger.warning("Kafka not ready (attempt %d/%d); retrying in %ds…", attempt, retries, delay)
            time.sleep(delay)
    raise RuntimeError(f"Cannot reach Kafka at {bootstrap} after {retries} attempts")


def main():
    random.seed(42)
    meters = build_meter_fleet()
    logger.info("Fleet: %d meters across zones %s", len(meters), GRID_ZONES)
    logger.info("Simulated day = %ds real time; emitting every %.0f–%.0fs", SIMULATED_DAY_SECONDS, EMIT_MIN, EMIT_MAX)

    producer = wait_for_kafka(KAFKA_BOOTSTRAP)
    sent = 0

    while True:
        for meter in meters:
            reading = generate_reading(meter)
            producer.send(
                METER_TOPIC,
                key=reading["grid_zone"],   # Kafka partitioned by zone
                value=reading,
            )
            sent += 1

        producer.flush()
        if sent % (len(meters) * 10) == 0:
            logger.info("Total readings sent: %d", sent)

        time.sleep(random.uniform(EMIT_MIN, EMIT_MAX))


if __name__ == "__main__":
    main()
