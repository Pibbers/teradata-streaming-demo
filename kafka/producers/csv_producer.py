#!/usr/bin/env python3
"""
Publishes synthetic SensorEvent messages as CSV strings to a Kafka topic.

Usage:
    python csv_producer.py [--topic demo-csv] [--count 100] [--delay 0.5]
    python csv_producer.py --continuous

Requirements:
    pip install confluent-kafka
"""

import argparse
import csv
import io
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

DEVICES = [f"device-{i:03d}" for i in range(1, 21)]
LOCATIONS = ["Plant-A", "Plant-B", "Warehouse-1", "Warehouse-2", "Dock-3"]
EVENT_TYPES = ["TEMPERATURE", "PRESSURE", "HUMIDITY", "VIBRATION"]
UNITS = {"TEMPERATURE": "C", "PRESSURE": "bar", "HUMIDITY": "pct", "VIBRATION": "mm_s"}
RANGES = {"TEMPERATURE": (15.0, 85.0), "PRESSURE": (0.5, 10.0), "HUMIDITY": (10.0, 95.0), "VIBRATION": (0.0, 25.0)}

CSV_HEADER = "event_id,device_id,event_type,value,unit,location,ts,quality"


def make_row() -> str:
    event_type = random.choice(EVENT_TYPES)
    lo, hi = RANGES[event_type]
    quality = str(random.randint(70, 100)) if random.random() > 0.1 else ""
    fields = [
        str(uuid.uuid4()),
        random.choice(DEVICES),
        event_type,
        str(round(random.uniform(lo, hi), 3)),
        UNITS[event_type],
        random.choice(LOCATIONS),
        str(int(datetime.now(timezone.utc).timestamp() * 1000)),
        quality,
    ]
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(fields)
    return buf.getvalue().strip()


def delivery_report(err, msg):
    if err:
        print(f"[ERROR] Delivery failed: {err}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="kafka:9092")
    parser.add_argument("--topic", default="demo-csv")
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--delay", type=float, default=0.5)
    parser.add_argument("--continuous", action="store_true")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap})

    count = 0
    try:
        while args.continuous or count < args.count:
            row = make_row()
            producer.produce(args.topic, value=row.encode("utf-8"), callback=delivery_report)
            producer.poll(0)
            count += 1
            if count % 10 == 0:
                print(f"[{datetime.now().isoformat()}] Sent {count} CSV rows to {args.topic}")
            if args.delay > 0:
                time.sleep(args.delay)
    except KeyboardInterrupt:
        print(f"\nStopped after {count} messages.")
    finally:
        producer.flush()
        print(f"Done. Total messages sent: {count}")


if __name__ == "__main__":
    main()
