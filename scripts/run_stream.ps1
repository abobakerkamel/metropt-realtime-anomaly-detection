$repo = Split-Path -Parent $PSScriptRoot
Set-Location $repo
$demoId = "demo_" + (Get-Date -Format "yyyyMMdd_HHmmss")

docker compose -f .\docker\docker-compose.yml exec spark `
  /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.executor.memory=1024m `
  --conf spark.executor.cores=2 `
  --conf spark.cores.max=4 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 `
  /opt/project/src/spark/phase5_processing.py `
  --experiment-id $demoId `
  --window "60 seconds" `
  --trigger "5 seconds" `
  --shuffle-partitions 8 `
  --starting-offsets latest
