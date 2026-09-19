CREATE TABLE IF NOT EXISTS spark_kpis (
    experiment_id TEXT NOT NULL,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    event_count BIGINT,
    avg_tp2 DOUBLE PRECISION,
    avg_tp3 DOUBLE PRECISION,
    avg_motor_current DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

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
);
