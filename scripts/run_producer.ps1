$repo = Split-Path -Parent $PSScriptRoot
Set-Location $repo

.\.venv\Scripts\python.exe -m src.producer.replay_producer `
  --mode kafka `
  --bootstrap-servers 127.0.0.1:9092 `
  --rate 10 `
  --limit 300
