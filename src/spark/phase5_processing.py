from __future__ import annotations

import argparse
import os
from datetime import datetime

import psycopg2
from pyspark.sql import SparkSession, functions as F, types as T


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--experiment-id", required=True)
    p.add_argument("--window", default="60 seconds")
    p.add_argument("--trigger", default="5 seconds")
    p.add_argument("--shuffle-partitions", type=int, default=8)
    p.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"])
    return p.parse_args()


SENSOR_SCHEMA = T.StructType([
    T.StructField("timestamp", T.StringType()),
    T.StructField("TP2", T.DoubleType()),
    T.StructField("TP3", T.DoubleType()),
    T.StructField("H1", T.DoubleType()),
    T.StructField("DV_pressure", T.DoubleType()),
    T.StructField("Reservoirs", T.DoubleType()),
    T.StructField("Oil_temperature", T.DoubleType()),
    T.StructField("Motor_current", T.DoubleType()),
    T.StructField("COMP", T.DoubleType()),
    T.StructField("DV_eletric", T.DoubleType()),
    T.StructField("Towers", T.DoubleType()),
    T.StructField("MPG", T.DoubleType()),
    T.StructField("LPS", T.DoubleType()),
    T.StructField("Pressure_switch", T.DoubleType()),
    T.StructField("Oil_level", T.DoubleType()),
    T.StructField("Caudal_impulses", T.DoubleType()),
])


def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS spark_kpis (
            experiment_id TEXT NOT NULL,
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            event_count BIGINT,
            avg_tp2 DOUBLE PRECISION,
            avg_tp3 DOUBLE PRECISION,
            avg_motor_current DOUBLE PRECISION,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
    conn.commit()


def write_batch(experiment_id):
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    db = os.getenv("POSTGRES_DB", "metropt")
    user = os.getenv("POSTGRES_USER", "metropt")
    password = os.getenv("POSTGRES_PASSWORD", "metropt")

    def _write(df, batch_id):
        rows = list(df.toLocalIterator())
        if not rows:
            print(f"[stream] batch={batch_id} rows=0")
            return
        conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
        try:
            ensure_table(conn)
            with conn.cursor() as cur:
                cur.executemany("""
                    INSERT INTO spark_kpis
                    (experiment_id, window_start, window_end, event_count,
                     avg_tp2, avg_tp3, avg_motor_current, created_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, [
                    (
                        experiment_id,
                        r["window_start"], r["window_end"], int(r["event_count"]),
                        r["avg_tp2"], r["avg_tp3"], r["avg_motor_current"], datetime.utcnow()
                    ) for r in rows
                ])
            conn.commit()
            print(f"[stream] batch={batch_id} rows={len(rows)} stored")
        finally:
            conn.close()
    return _write


def main():
    args = parse_args()
    spark = (
        SparkSession.builder.appName(f"metropt-{args.experiment_id}")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "broker:19092")
        .option("subscribe", "metropt-sensors")
        .option("startingOffsets", args.starting_offsets)
        .load()
    )

    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), SENSOR_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_time", F.coalesce(
            F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp("timestamp", "dd/MM/yyyy HH:mm:ss"),
            F.to_timestamp("timestamp")
        ))
        .filter(F.col("event_time").isNotNull())
    )

    kpis = (
        parsed.withWatermark("event_time", "2 minutes")
        .groupBy(F.window("event_time", args.window))
        .agg(
            F.count("*").alias("event_count"),
            F.avg("TP2").alias("avg_tp2"),
            F.avg("TP3").alias("avg_tp3"),
            F.avg("Motor_current").alias("avg_motor_current"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "event_count", "avg_tp2", "avg_tp3", "avg_motor_current"
        )
    )

    query = (
        kpis.writeStream
        .outputMode("update")
        .trigger(processingTime=args.trigger)
        .option("checkpointLocation", f"/opt/spark/checkpoints/{args.experiment_id}")
        .foreachBatch(write_batch(args.experiment_id))
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
