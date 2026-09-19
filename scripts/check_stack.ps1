$repo = Split-Path -Parent $PSScriptRoot
Set-Location $repo

Write-Host "=== Docker ===" -ForegroundColor Cyan
docker compose -f .\docker\docker-compose.yml ps

Write-Host "\n=== Kafka topic ===" -ForegroundColor Cyan
docker compose -f .\docker\docker-compose.yml exec broker `
  /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server broker:19092 `
  --describe --topic metropt-sensors

Write-Host "\n=== ML predictions ===" -ForegroundColor Cyan
docker compose -f .\docker\docker-compose.yml exec postgres `
  psql -U metropt -d metropt -P pager=off -c "SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE is_anomaly) AS anomalies FROM compressor_anomaly_predictions;"
