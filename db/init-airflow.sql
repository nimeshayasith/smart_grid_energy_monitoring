-- Create the Airflow database and user on the same PostgreSQL instance.
-- Runs automatically on first container start (after 01-gridpulse.sql).

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airflow') THEN
    CREATE ROLE airflow WITH LOGIN PASSWORD 'airflow_secret';
  END IF;
END$$;

CREATE DATABASE airflow OWNER airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
