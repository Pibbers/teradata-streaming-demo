#!/usr/bin/env python3
"""
Generates a sample Avro container file (data/sample/sample.avro).
Used in Demo 1 to test BLOB loading via TPT.

Requirements:
    pip install fastavro
"""

import json
import os
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path

import fastavro

SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "sample.avsc"
OUTPUT_PATH = Path(__file__).parent.parent.parent / "data" / "sample" / "sample.avro"

DEVICES = [f"device-{i:03d}" for i in range(1, 11)]
LOCATIONS = ["Plant-A", "Plant-B", "Warehouse-1"]
EVENT_TYPES = ["TEMPERATURE", "PRESSURE", "HUMIDITY", "VIBRATION"]
UNITS = {"TEMPERATURE": "°C", "PRESSURE": "bar", "HUMIDITY": "%", "VIBRATION": "mm/s"}
RANGES = {"TEMPERATURE": (15.0, 85.0), "PRESSURE": (0.5, 10.0), "HUMIDITY": (10.0, 95.0), "VIBRATION": (0.0, 25.0)}


def generate_records(n: int = 50) -> list:
    records = []
    for _ in range(n):
        et = random.choice(EVENT_TYPES)
        lo, hi = RANGES[et]
        records.append({
            "event_id": str(uuid.uuid4()),
            "device_id": random.choice(DEVICES),
            "event_type": et,
            "value": round(random.uniform(lo, hi), 3),
            "unit": UNITS[et],
            "location": random.choice(LOCATIONS),
            "ts": int(datetime.now(timezone.utc).timestamp() * 1000),
            "quality": random.randint(70, 100) if random.random() > 0.1 else None,
        })
    return records


def main():
    with open(SCHEMA_PATH) as f:
        schema = fastavro.parse_schema(json.load(f))

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    records = generate_records(50)

    with open(OUTPUT_PATH, "wb") as f:
        fastavro.writer(f, schema, records)

    print(f"Written {len(records)} records to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
