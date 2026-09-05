"""
GridPulse – Airflow DAG: daily tariff data ingestion (Phase 7)

Scheduled every SIMULATED_DAY_SECONDS (5 real minutes = 1 simulated day).
Runs tariff_generator.py to push updated tariff/billing reference data to
both the Kafka 'tariff-updates' topic and the PostgreSQL tariff table.
"""

import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

log = logging.getLogger(__name__)

# Path where generators/ is mounted inside the Airflow container
GENERATORS_PATH = os.environ.get("GENERATORS_PATH", "/opt/airflow/generators")

default_args = {
    "owner":            "gridpulse",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(seconds=30),
    "email_on_failure": False,
}

with DAG(
    dag_id="daily_tariff_ingestion",
    description="Emit daily tariff reference data to Kafka and PostgreSQL",
    schedule_interval=timedelta(minutes=5),   # 1 simulated day
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,            # auto-start, no manual unpause needed
    default_args=default_args,
    tags=["gridpulse", "tariff", "phase7"],
) as dag:

    run_tariff_generator = BashOperator(
        task_id="run_tariff_generator",
        bash_command=f"python {GENERATORS_PATH}/tariff_generator.py",
        env={
            # Pass through the environment variables the generator needs
            "KAFKA_BOOTSTRAP_SERVERS":   os.environ.get("KAFKA_BOOTSTRAP_SERVERS",   "kafka:9092"),
            "KAFKA_TOPIC_TARIFF_UPDATES": os.environ.get("KAFKA_TOPIC_TARIFF_UPDATES", "tariff-updates"),
            "NUM_METERS":                 os.environ.get("NUM_METERS",                "30"),
            "GRID_ZONES":                 os.environ.get("GRID_ZONES",                "ZONE_A,ZONE_B,ZONE_C"),
            "POSTGRES_HOST":              os.environ.get("POSTGRES_HOST",             "postgres"),
            "POSTGRES_PORT":              os.environ.get("POSTGRES_PORT",             "5432"),
            "POSTGRES_DB":                os.environ.get("POSTGRES_DB",              "gridpulse"),
            "POSTGRES_USER":              os.environ.get("POSTGRES_USER",            "gridpulse"),
            "POSTGRES_PASSWORD":          os.environ.get("POSTGRES_PASSWORD",        "gridpulse_secret"),
        },
    )

    def verify_tariff_landed(**context):
        """Check that at least one tariff record exists in PostgreSQL."""
        import psycopg2
        conn = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST", "postgres"),
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
            dbname=os.environ.get("POSTGRES_DB", "gridpulse"),
            user=os.environ.get("POSTGRES_USER", "gridpulse"),
            password=os.environ.get("POSTGRES_PASSWORD", "gridpulse_secret"),
        )
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM tariff;")
            count = cur.fetchone()[0]
        conn.close()
        log.info("Tariff table row count after generation: %d", count)
        if count == 0:
            raise ValueError("Tariff table is empty – generator may have failed")

    verify_tariff = PythonOperator(
        task_id="verify_tariff_landed",
        python_callable=verify_tariff_landed,
    )

    run_tariff_generator >> verify_tariff
