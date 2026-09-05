FROM python:3.11-slim

WORKDIR /app

# Java is required to run Spark locally
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk-headless procps && \
    rm -rf /var/lib/apt/lists/*

COPY requirements-spark.txt /tmp/requirements-spark.txt
RUN pip install --no-cache-dir -r /tmp/requirements-spark.txt pyspark==3.5.0

COPY schemas.py .
COPY streaming_job.py .

ENV PYSPARK_PYTHON=python3

CMD ["python", "streaming_job.py"]
