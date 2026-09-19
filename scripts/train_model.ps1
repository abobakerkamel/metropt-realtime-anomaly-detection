$repo = Split-Path -Parent $PSScriptRoot
Set-Location $repo

docker compose -f .\docker\docker-compose.yml exec spark `
  /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/project/src/ml/train_kmeans.py
