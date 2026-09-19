# Demo Flow

1. Show the architecture image.
2. Start the core Docker Compose stack.
3. Verify the `metropt-sensors` Kafka topic from the terminal.
4. Open Spark Master UI and show two workers.
5. Start the Spark Structured Streaming job.
6. Start the MetroPT replay producer in Kafka mode.
7. Show Kafka offsets increasing.
8. Show Spark processing / driver UI.
9. Query PostgreSQL.
10. Show K-Means model metadata and prediction counts.
11. Open Superset and show `Compressor Anomalies Over Time`.
12. Open Airflow and run/show the health-check DAG.

## One-sentence explanation

> The system replays real railway compressor sensor data into Kafka, processes it with Spark, detects abnormal operating patterns with Spark MLlib K-Means, stores the results in PostgreSQL, and visualizes them in Superset.
