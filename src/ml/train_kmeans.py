from __future__ import annotations

import json
import math
import os
from datetime import datetime
from pathlib import Path

import psycopg2
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.linalg import VectorUDT
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import SparkSession, functions as F, types as T

DATASET = os.getenv("METROPT_CSV", "/opt/project/data/raw/MetroPT3(AirCompressor).csv")
MODEL_DIR = Path(os.getenv("MODEL_DIR", "/opt/project/models/compressor_kmeans"))
METADATA_PATH = Path(os.getenv("MODEL_METADATA", "/opt/project/models/compressor_kmeans_metadata.json"))
MODEL_NAME = os.getenv("MODEL_NAME", "metropt_kmeans_v1")
K = int(os.getenv("KMEANS_K", "5"))
TRAIN_FRACTION = float(os.getenv("TRAIN_FRACTION", "0.20"))
THRESHOLD_QUANTILE = float(os.getenv("ANOMALY_QUANTILE", "0.99"))
PREDICTION_LIMIT = int(os.getenv("PREDICTION_LIMIT", "50000"))
SEED = 42

FEATURES = ["TP2", "TP3", "DV_pressure", "Reservoirs", "Oil_temperature", "Motor_current"]


def distance_udf(centers):
    @F.udf(T.DoubleType())
    def _distance(v, pred):
        if v is None or pred is None:
            return None
        c = centers[int(pred)]
        return float(math.sqrt(sum((float(v[i]) - float(c[i])) ** 2 for i in range(len(c)))))
    return _distance


def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS compressor_anomaly_predictions (
            event_time TIMESTAMP,
            tp2 DOUBLE PRECISION,
            tp3 DOUBLE PRECISION,
            dv_pressure DOUBLE PRECISION,
            reservoirs DOUBLE PRECISION,
            oil_temperature DOUBLE PRECISION,
            motor_current DOUBLE PRECISION,
            cluster INTEGER,
            anomaly_score DOUBLE PRECISION,
            is_anomaly BOOLEAN,
            model_name TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
    conn.commit()


def write_predictions(df):
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "metropt"),
        user=os.getenv("POSTGRES_USER", "metropt"),
        password=os.getenv("POSTGRES_PASSWORD", "metropt"),
    )
    try:
        ensure_table(conn)
        with conn.cursor() as cur:
            cur.execute("DELETE FROM compressor_anomaly_predictions WHERE model_name=%s", (MODEL_NAME,))
            batch = []
            total = 0
            for r in df.toLocalIterator():
                batch.append((
                    r.event_time, r.TP2, r.TP3, r.DV_pressure, r.Reservoirs,
                    r.Oil_temperature, r.Motor_current, int(r.prediction),
                    float(r.anomaly_score), bool(r.is_anomaly), MODEL_NAME, datetime.utcnow()
                ))
                if len(batch) >= 1000:
                    cur.executemany("""
                        INSERT INTO compressor_anomaly_predictions
                        (event_time,tp2,tp3,dv_pressure,reservoirs,oil_temperature,motor_current,
                         cluster,anomaly_score,is_anomaly,model_name,created_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, batch)
                    total += len(batch); batch.clear()
            if batch:
                cur.executemany("""
                    INSERT INTO compressor_anomaly_predictions
                    (event_time,tp2,tp3,dv_pressure,reservoirs,oil_temperature,motor_current,
                     cluster,anomaly_score,is_anomaly,model_name,created_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, batch)
                total += len(batch)
        conn.commit()
        return total
    finally:
        conn.close()


def main():
    spark = SparkSession.builder.appName("metropt-kmeans-training").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.option("header", True).option("inferSchema", True).csv(DATASET)
    df = df.withColumn("event_time", F.coalesce(
        F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp("timestamp", "dd/MM/yyyy HH:mm:ss"),
        F.to_timestamp("timestamp")
    ))
    for c in FEATURES:
        df = df.withColumn(c, F.col(c).cast("double"))
    clean = df.dropna(subset=FEATURES)

    train = clean.sample(withReplacement=False, fraction=TRAIN_FRACTION, seed=SEED).cache()
    training_rows = train.count()

    assembler = VectorAssembler(inputCols=FEATURES, outputCol="features")
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withMean=True, withStd=True)
    kmeans = KMeans(featuresCol="scaled_features", predictionCol="prediction", k=K, seed=SEED)
    pipeline = Pipeline(stages=[assembler, scaler, kmeans])
    model: PipelineModel = pipeline.fit(train)

    km_model: KMeansModel = model.stages[-1]
    centers = [list(map(float, c)) for c in km_model.clusterCenters()]
    dist = distance_udf(centers)

    train_scored = model.transform(train).withColumn("anomaly_score", dist("scaled_features", "prediction"))
    threshold = float(train_scored.approxQuantile("anomaly_score", [THRESHOLD_QUANTILE], 0.001)[0])

    if MODEL_DIR.exists():
        import shutil; shutil.rmtree(MODEL_DIR)
    MODEL_DIR.parent.mkdir(parents=True, exist_ok=True)
    model.write().overwrite().save(str(MODEL_DIR))

    scored = (
        model.transform(clean)
        .withColumn("anomaly_score", dist("scaled_features", "prediction"))
        .withColumn("is_anomaly", F.col("anomaly_score") > F.lit(threshold))
        .select("event_time", *FEATURES, "prediction", "anomaly_score", "is_anomaly")
        .limit(PREDICTION_LIMIT)
        .cache()
    )

    stats = scored.agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("is_anomaly"), 1).otherwise(0)).alias("anomalies"),
        F.avg("anomaly_score").alias("avg_score"),
        F.max("anomaly_score").alias("max_score"),
    ).first()

    stored = write_predictions(scored)
    metadata = {
        "model_name": MODEL_NAME,
        "algorithm": "Spark MLlib K-Means",
        "features": FEATURES,
        "training_rows": training_rows,
        "k": K,
        "threshold_quantile": THRESHOLD_QUANTILE,
        "threshold": threshold,
        "stored_predictions": stored,
        "normal_predictions": int(stats.total - stats.anomalies),
        "anomalies": int(stats.anomalies),
        "avg_anomaly_score": float(stats.avg_score),
        "max_anomaly_score": float(stats.max_score),
        "created_at": datetime.utcnow().isoformat() + "Z",
    }
    METADATA_PATH.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print(json.dumps(metadata, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
