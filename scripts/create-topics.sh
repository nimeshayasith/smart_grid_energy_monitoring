#!/bin/bash
set -e

echo "Waiting for Kafka to be ready..."
sleep 5

echo "Creating Kafka topics..."

kafka-topics --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic meter-readings \
  --partitions 3 \
  --replication-factor 1

kafka-topics --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic tariff-updates \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.01 \
  --config segment.ms=10000

kafka-topics --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic alerts \
  --partitions 1 \
  --replication-factor 1

echo "=== Topics created ==="
kafka-topics --bootstrap-server kafka:9092 --describe
