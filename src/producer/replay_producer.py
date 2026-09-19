from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path
from typing import Dict, Any

from kafka import KafkaProducer

DEFAULT_FILE = Path("data/raw/MetroPT3(AirCompressor).csv")

NUMERIC_COLUMNS = {
    "TP2", "TP3", "H1", "DV_pressure", "Reservoirs", "Oil_temperature",
    "Motor_current", "COMP", "DV_eletric", "Towers", "MPG", "LPS",
    "Pressure_switch", "Oil_level", "Caudal_impulses"
}


def normalize_row(row: Dict[str, str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, value in row.items():
        key = key.strip()
        if value is None:
            out[key] = None
            continue
        value = value.strip()
        if key in NUMERIC_COLUMNS:
            try:
                out[key] = float(value)
            except ValueError:
                out[key] = None
        elif key.lower() == "index":
            try:
                out[key] = int(value)
            except ValueError:
                out[key] = value
        else:
            out[key] = value
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Replay MetroPT-3 rows as a live event stream")
    p.add_argument("--file", default=str(DEFAULT_FILE))
    p.add_argument("--mode", choices=["kafka", "stdout"], default="stdout")
    p.add_argument("--bootstrap-servers", default="127.0.0.1:9092")
    p.add_argument("--topic", default="metropt-sensors")
    p.add_argument("--rate", type=float, default=10.0, help="events/second; 0 = unlimited")
    p.add_argument("--limit", type=int, default=0, help="0 = no limit")
    p.add_argument("--start-row", type=int, default=0)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    path = Path(args.file)
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path.resolve()}")

    producer = None
    if args.mode == "kafka":
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
            linger_ms=20,
            retries=5,
        )

    delay = 0 if args.rate <= 0 else 1.0 / args.rate
    sent = 0
    started = time.perf_counter()

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            if idx < args.start_row:
                continue
            event = normalize_row(row)
            if args.mode == "kafka":
                producer.send(args.topic, event)
            else:
                print(json.dumps(event, ensure_ascii=False))
            sent += 1
            if args.limit and sent >= args.limit:
                break
            if delay:
                time.sleep(delay)

    if producer:
        producer.flush()
        producer.close()

    elapsed = max(time.perf_counter() - started, 1e-9)
    print(f"[producer] sent={sent} elapsed={elapsed:.2f}s avg_rate={sent/elapsed:.2f} events/s")


if __name__ == "__main__":
    main()
