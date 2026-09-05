FROM python:3.11-slim

WORKDIR /app

COPY requirements-gen.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY meter_producer.py    .
COPY tariff_generator.py  .

# Default: run the meter producer; override with CMD in docker-compose for tariff
CMD ["python", "meter_producer.py"]
