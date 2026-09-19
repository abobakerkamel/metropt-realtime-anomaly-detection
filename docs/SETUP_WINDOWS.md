# Windows Setup

Tested workflow target: Windows + Docker Desktop + PowerShell.

## Requirements

- Docker Desktop
- Python 3.10+
- PowerShell
- 8 GB RAM minimum; 12–16 GB recommended for the full demo

## Start

```powershell
cd <repo-root>
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

docker compose -f .\docker\docker-compose.yml up -d
```

If Kafka takes longer after a machine restart, wait until the broker becomes healthy and run the same `up -d` command again. The broker health check includes a long start period to avoid false failures during KRaft recovery.
