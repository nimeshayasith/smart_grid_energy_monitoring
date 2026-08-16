# SmartGrid Analytics: Real-Time Energy Monitoring & Intelligent Billing Platform

## Selected Assignment Use Case

This project explicitly selects **Use Case 3 — Smart Grid Energy Monitoring & Billing** as the primary implementation track.

## Why Smart Grid is the Best Fit

Smart Grid provides the strongest overall alignment with the marking rubric by offering:

- strong architecture discussion potential (Lambda vs Kappa, replay, latency, consistency, cost trade-offs)
- native support for both required source types (streaming + daily batch)
- rich processing opportunities (joins, windowing, aggregations, threshold alerts)
- practical feasibility within a 2-week implementation window

## End-to-End Architecture (Kappa-Oriented)

```text
Smart Meter Simulator (streaming telemetry)
        │
        ▼
      Kafka  ◄──────── Daily Tariff Pipeline (Airflow → Kafka)
        │
        ▼
Spark Structured Streaming / Spark Jobs
        │
        ├── Grid load metrics (zone/time windows)
        ├── Renewable contribution metrics
        ├── Alert generation (low renewable contribution)
        └── Household billing computations (tariff join)
        │
        ▼
   PostgreSQL (serving layer)
        │
        ▼
 API / Dashboard / Daily consolidated reports
```

## Data Sources

### Streaming Source (Smart Meter Events)

- `meter_id`
- `household_id`
- `power_consumption_kwh`
- `solar_generation_kwh`
- `grid_zone`
- `timestamp`

### Daily Batch Source (Tariff + Billing Inputs)

- `household_id`
- `tariff_rate`
- `billing_tier`
- `subsidy_flag`

## Core Processing Outputs

1. **Real-time grid load by zone**
2. **Renewable contribution percentage**
3. **Time-window analytics** (e.g., 1-minute / 5-minute)
4. **Household-level bill calculation** (consumption × tariff, subsidy-aware)
5. **Operational alerts** when renewable contribution drops below threshold

## Dashboard & Reporting Goals

- current grid load
- renewable contribution percentage
- active meters and alert count
- zone-wise load comparison
- daily consolidated household billing table
- daily solar-contribution summary

## Architecture Decision Position

The preferred architecture direction is **Kappa-oriented**, using Kafka as the unifying ingestion backbone for both streaming telemetry and daily tariff ingestion, enabling:

- consistent processing model
- simpler replay/backfill from Kafka topics
- reduced duplication compared with split Lambda pipelines

Lambda remains a documented alternative but is rejected due to additional operational complexity for this project scope.

## 2-Week Delivery Plan

1. Requirements and business questions finalization
2. Architecture decision (Kappa vs Lambda) and trade-off write-up
3. Detailed solution architecture and component design
4. Smart meter and tariff data generators
5. Kafka ingestion setup
6. Spark streaming and batch processing jobs
7. Airflow daily tariff orchestration
8. PostgreSQL schema and serving layer integration
9. API/dashboard implementation
10. Observability (logs, metrics, basic health checks)
11. Validation and test execution
12. Final report, demo flow, and limitations discussion
