# Project Scope

## Final Components

- MetroPT-3 dataset
- Python replay producer
- Apache Kafka 3.9.1
- Apache Spark 3.5.9 standalone cluster
- Spark Structured Streaming
- Spark MLlib K-Means
- PostgreSQL 16
- Apache Superset 6.1.0
- Apache Airflow 3.3.1
- Docker Compose

## Important implementation note

The final demonstration has two analytical paths:

1. **Live path:** MetroPT replay → Kafka → Spark Structured Streaming → PostgreSQL KPIs.
2. **ML path:** historical MetroPT-3 → Spark MLlib K-Means → PostgreSQL anomaly predictions → Superset.

Do not describe the current implementation as real-time K-Means inference on each Kafka event. That is a future extension.
