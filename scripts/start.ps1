$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
Set-Location $repo

docker compose -f .\docker\docker-compose.yml up -d

docker compose `
  -f .\docker\docker-compose.yml `
  -f .\docker\docker-compose.airflow.yml `
  up -d airflow-db-init airflow

Write-Host "\nCore services:" -ForegroundColor Cyan
docker compose -f .\docker\docker-compose.yml ps
Write-Host "\nSpark:    http://127.0.0.1:8080"
Write-Host "Superset: http://127.0.0.1:8088"
Write-Host "Airflow:  http://127.0.0.1:8085"
