$repo = Split-Path -Parent $PSScriptRoot
Set-Location $repo

docker compose `
  -f .\docker\docker-compose.yml `
  -f .\docker\docker-compose.airflow.yml `
  down

Write-Host "Stopped. Volumes were preserved." -ForegroundColor Green
