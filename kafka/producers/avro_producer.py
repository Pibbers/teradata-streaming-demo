#!/usr/bin/env python3
"""
Publishes synthetic SensorEvent Avro messages to a Kafka topic.
Uses embedded Avro schema (no Schema Registry).

Usage:
    python avro_producer.py [--topic demo-avro] [--count 100] [--delay 0.5]
    python avro_producer.py --continuous        # runs until Ctrl-C

Requirements:
    pip install confluent-kafka fastavro
"""

import argparse
import io
import json
import random
import time
import uuid
from datetime import datetime, timezone

import fastavro
from confluent_kafka import Producer

SCHEMA_PATH = "/tpt/data/../../../kafka/schemas/sample.avsc"

DEVICES = [f"device-{i:03d}" for i in range(1, 21)]
LOCATIONS = ["Plant-A", "Plant-B", "Warehouse-1", "Warehouse-2", "Dock-3"]
EVENT_TYPES = ["TEMPERATURE", "PRESSURE", "HUMIDITY", "VIBRATION"]
UNITS = {"TEMPERATURE": "°C", "PRESSURE": "bar", "HUMIDITY": "%", "VIBRATION": "mm/s"}
RANGES = {"TEMPERATURE": (15.0, 85.0), "PRESSURE": (0.5, 10.0), "HUMIDITY": (10.0, 95.0), "VIBRATION": (0.0, 25.0)}


def load_schema(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def make_record(schema: dict) -> dict:
    event_type = random.choice(EVENT_TYPES)
    lo, hi = RANGES[event_type]
    return {
        "event_id": str(uuid.uuid4()),
        "device_id": random.choice(DEVICES),
        "event_type": event_type,
        "value": round(random.uniform(lo, hi), 3),
        "unit": UNITS[event_type],
        "location": random.choice(LOCATIONS),
        "ts": int(datetime.now(timezone.utc).timestamp() * 1000),
        "quality": random.randint(70, 100) if random.random() > 0.1 else None,
    }


def encode_avro(record: dict, parsed_schema) -> bytes:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed_schema, record)
    return buf.getvalue()


def delivery_report(err, msg):
    if err:
        print(f"[ERROR] Delivery failed: {err}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="kafka:9092")
    parser.add_argument("--topic", default="demo-avro")
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--delay", type=float, default=0.5, help="Seconds between messages")
    parser.add_argument("--continuous", action="store_true", help="Run until Ctrl-C")
    args = parser.parse_args()

    raw_schema = load_schema(SCHEMA_PATH)
    parsed_schema = fastavro.parse_schema(raw_schema)

    producer = Producer({"bootstrap.servers": args.bootstrap})

    count = 0
    try:
        while args.continuous or count < args.count:
            record = make_record(raw_schema)
            payload = encode_avro(record, parsed_schema)
            producer.produce(args.topic, key=record["device_id"], value=payload, callback=delivery_report)
            producer.poll(0)
            count += 1
            if count % 10 == 0:
                print(f"[{datetime.now().isoformat()}] Sent {count} messages to {args.topic}")
            if args.delay > 0:
                time.sleep(args.delay)
    except KeyboardInterrupt:
        print(f"\nStopped after {count} messages.")
    finally:
        producer.flush()
        print(f"Done. Total messages sent: {count}")


if __name__ == "__main__":
    main()
