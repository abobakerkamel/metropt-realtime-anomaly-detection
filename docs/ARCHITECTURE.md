# Architecture

## Data path

```text
MetroPT-3 CSV
   ↓
Python Replay Producer
   ↓
Kafka topic: metropt-sensors
   ↓
Spark Structured Streaming
   ↓
PostgreSQL: spark_kpis
   ↓
Apache Superset
```

## ML path

```text
MetroPT-3 historical data
   ↓
Feature selection
   ↓
StandardScaler
   ↓
Spark MLlib K-Means (K=5)
   ↓
Distance-to-centroid anomaly score
   ↓
99th percentile threshold
   ↓
PostgreSQL: compressor_anomaly_predictions
   ↓
Superset
```

## Supporting services

- **Airflow:** service health checks for PostgreSQL, Kafka and Spark.
- **Docker Compose:** reproducible local container environment.

The final project intentionally excludes Kubernetes, Prometheus and the previous auto-tuning feedback-loop concept.
