"""
GridPulse – Spark Structured Streaming job (Kappa architecture core)

Reads meter-readings from Kafka, joins with tariff data from PostgreSQL,
computes tumbling-window aggregations per zone (1 min) and per household
(5 min = 1 simulated day), and writes results to PostgreSQL.  Shortfall
alerts are written to both PostgreSQL and the Kafka 'alerts' topic.
"""

import os
import logging
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, lit, when, coalesce,
    sum as _sum, count, to_timestamp, broadcast, window,
)
from pyspark.sql.types import BooleanType, DoubleType, StringType

from schemas import meter_schema

# ── Config (all from environment / .env) ─────────────────────────────────────
KAFKA_BOOTSTRAP   = os.environ.get("KAFKA_BOOTSTRAP_SERVERS",      "kafka:9092")
METER_TOPIC       = os.environ.get("KAFKA_TOPIC_METER_READINGS",   "meter-readings")
ALERTS_TOPIC      = os.environ.get("KAFKA_TOPIC_ALERTS",           "alerts")
PG_HOST           = os.environ.get("POSTGRES_HOST",                "postgres")
PG_PORT           = os.environ.get("POSTGRES_PORT",                "5432")
PG_DB             = os.environ.get("POSTGRES_DB",                  "gridpulse")
PG_USER           = os.environ.get("POSTGRES_USER",                "gridpulse")
PG_PASSWORD       = os.environ.get("POSTGRES_PASSWORD",            "gridpulse_secret")
ALERT_THRESHOLD   = float(os.environ.get("RENEWABLE_ALERT_THRESHOLD", "20.0"))
CHECKPOINT_DIR    = os.environ.get("CHECKPOINT_DIR",               "/tmp/gridpulse-checkpoints")
WINDOW_SHORT      = os.environ.get("WINDOW_DURATION_SHORT",        "1 minute")
WINDOW_LONG       = os.environ.get("WINDOW_DURATION_LONG",         "5 minutes")

PG_JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_PROPS    = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","service":"spark-job","msg":"%(message)s"}',
)
logger = logging.getLogger(__name__)


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("GridPulse-Streaming")
        .master("local[2]")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        # Kafka and PostgreSQL JARs — downloaded from Maven on first run, cached in ivy volume
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.postgresql:postgresql:42.7.1")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_tariff(spark: SparkSession):
    """Load the full tariff table from PostgreSQL as a broadcast-ready DataFrame."""
    try:
        return spark.read.jdbc(url=PG_JDBC_URL, table="tariff", properties=PG_PROPS)
    except Exception as exc:
        logger.warning("Could not read tariff table: %s – using empty fallback", exc)
        from pyspark.sql.types import StructType, StructField
        schema = StructType([
            StructField("household_id", StringType(), True),
            StructField("tariff_rate",  DoubleType(), True),
            StructField("billing_tier", StringType(), True),
            StructField("subsidy_flag", BooleanType(), True),
        ])
        return spark.createDataFrame([], schema)


# ── Sink functions (called by foreachBatch) ───────────────────────────────────

def write_zone_agg(df, epoch_id):
    """Persist 1-min zone aggregations and fire alerts when below threshold."""
    if df.rdd.isEmpty():
        return

    flat = (
        df
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
        .drop("window")
    )

    # Write to grid_load_by_zone
    (
        flat.select(
            "grid_zone", "window_start", "window_end",
            "total_consumption_kwh", "total_solar_kwh",
            "renewable_pct", "reading_count",
        )
        .write.jdbc(url=PG_JDBC_URL, table="grid_load_by_zone",
                    mode="append", properties=PG_PROPS)
    )
    logger.info("epoch %s: wrote zone aggregates", epoch_id)

    # Alert check
    alerts = flat.filter(col("renewable_pct") < lit(ALERT_THRESHOLD))
    if alerts.rdd.isEmpty():
        return

    alert_rows = (
        alerts
        .withColumn("threshold", lit(ALERT_THRESHOLD))
        .withColumn("severity",  lit("WARNING"))
        .select("grid_zone", "window_start", "window_end",
                "renewable_pct", "threshold", "severity")
    )

    # Write alerts to PostgreSQL
    alert_rows.write.jdbc(url=PG_JDBC_URL, table="alerts",
                          mode="append", properties=PG_PROPS)

    # Write alerts to Kafka alerts topic
    (
        alert_rows
        .withColumn("value", to_json(struct(
            col("grid_zone"),
            col("window_start").cast("string"),
            col("window_end").cast("string"),
            col("renewable_pct"),
            col("threshold"),
            col("severity"),
        )))
        .select("value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", ALERTS_TOPIC)
        .save()
    )
    logger.info("epoch %s: wrote %d alerts", epoch_id, alert_rows.count())


def write_billing(df, epoch_id):
    """Join 5-min household aggregations with tariff data and write billing rows."""
    if df.rdd.isEmpty():
        return

    spark = df.sparkSession
    tariff_df = read_tariff(spark)

    flat = (
        df
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
        .withColumn("billing_date", col("window.start").cast("date"))
        .drop("window")
    )

    billing = (
        flat
        .join(broadcast(tariff_df), "household_id", "left")
        .withColumn("tariff_rate",  coalesce(col("tariff_rate"),  lit(0.15)))
        .withColumn("billing_tier", coalesce(col("billing_tier"), lit("RESIDENTIAL")))
        .withColumn("subsidy_flag", coalesce(col("subsidy_flag"), lit(False)))
        .withColumn(
            "bill_amount",
            col("total_consumption_kwh") * col("tariff_rate") *
            when(col("subsidy_flag"), lit(0.85)).otherwise(lit(1.0)),
        )
        .select(
            "household_id", "billing_date", "window_start", "window_end",
            "total_consumption_kwh", "total_solar_kwh",
            "tariff_rate", "billing_tier", "subsidy_flag", "bill_amount",
        )
    )

    billing.write.jdbc(url=PG_JDBC_URL, table="household_billing",
                       mode="append", properties=PG_PROPS)
    logger.info("epoch %s: wrote billing rows", epoch_id)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session started; connecting to Kafka at %s", KAFKA_BOOTSTRAP)

    # Read raw bytes from Kafka
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", METER_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON and validate
    parsed = (
        raw
        .select(from_json(col("value").cast("string"), meter_schema).alias("d"))
        .select("d.*")
        .filter(
            col("power_consumption_kwh").isNotNull() &
            (col("power_consumption_kwh") >= 0) &
            col("solar_generation_kwh").isNotNull() &
            (col("solar_generation_kwh") >= 0) &
            col("meter_id").isNotNull() &
            col("household_id").isNotNull() &
            col("grid_zone").isNotNull()
        )
        .withColumn("event_time", to_timestamp(col("timestamp")))
    )

    # ── Query 1: 1-minute zone aggregation ───────────────────────────────────
    zone_agg = (
        parsed
        .withWatermark("event_time", "2 minutes")
        .groupBy(window("event_time", WINDOW_SHORT), col("grid_zone"))
        .agg(
            _sum("power_consumption_kwh").alias("total_consumption_kwh"),
            _sum("solar_generation_kwh").alias("total_solar_kwh"),
            count("meter_id").alias("reading_count"),
        )
        .withColumn(
            "renewable_pct",
            when(col("total_consumption_kwh") > 0,
                 col("total_solar_kwh") / col("total_consumption_kwh") * 100.0)
            .otherwise(lit(0.0)),
        )
    )

    zone_query = (
        zone_agg.writeStream
        .foreachBatch(write_zone_agg)
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/zone_agg")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # ── Query 2: 5-minute household aggregation for billing ──────────────────
    household_agg = (
        parsed
        .withWatermark("event_time", "6 minutes")
        .groupBy(window("event_time", WINDOW_LONG), col("household_id"))
        .agg(
            _sum("power_consumption_kwh").alias("total_consumption_kwh"),
            _sum("solar_generation_kwh").alias("total_solar_kwh"),
        )
    )

    billing_query = (
        household_agg.writeStream
        .foreachBatch(write_billing)
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/billing")
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info("Both streaming queries started; awaiting termination…")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
