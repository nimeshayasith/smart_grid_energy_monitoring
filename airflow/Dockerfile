FROM apache/airflow:2.8.0-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Extra Python packages needed by the tariff generator DAG
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    psycopg2-binary==2.9.9 \
    python-dotenv==1.0.1
