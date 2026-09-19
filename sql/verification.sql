SELECT
    COUNT(*) AS total_predictions,
    COUNT(*) FILTER (WHERE is_anomaly = TRUE) AS anomalies,
    COUNT(*) FILTER (WHERE is_anomaly = FALSE) AS normal,
    ROUND(AVG(anomaly_score)::numeric,4) AS avg_anomaly_score,
    ROUND(MAX(anomaly_score)::numeric,4) AS max_anomaly_score
FROM compressor_anomaly_predictions;
