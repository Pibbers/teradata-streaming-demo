#!/usr/bin/env python3
"""
Publishes synthetic weather observations to a Kafka topic as CSV batch messages.

Each Kafka message is a complete CSV file (header + 30 rows: 5 stations × 6 hourly
offsets).  Kafka Connect S3 Sink (flush.size=1) writes each message as a separate
file in MinIO, giving NOS a set of well-formed CSV files to query.

Usage:
    python weather_kafka.py [--broker localhost:29092] [--topic weather-csv]
                            [--batches 3] [--interval 5]

Requirements:
    pip install confluent-kafka
"""

import argparse
import csv
import io
import random
import time
from datetime import datetime, timezone, timedelta

from confluent_kafka import Producer

STATIONS = [
    {"id": "EGLL", "name": "London Heathrow",   "lat": 51.477, "lon":  -0.461},
    {"id": "EHAM", "name": "Amsterdam Schiphol", "lat": 52.308, "lon":   4.764},
    {"id": "KJFK", "name": "New York JFK",       "lat": 40.641, "lon": -73.778},
    {"id": "KLAX", "name": "Los Angeles LAX",    "lat": 33.943, "lon": -118.408},
    {"id": "KATL", "name": "Atlanta Hartsfield", "lat": 33.641, "lon": -84.427},
]

CONDITIONS = ["VFR", "VFR", "VFR", "MVFR", "IFR", "LIFR"]

CSV_HEADER = [
    "station_id", "observation_ts", "temperature_c", "wind_speed_kts",
    "wind_direction", "visibility_m", "precipitation_mm", "pressure_hpa", "conditions",
]


def make_observation(station: dict, obs_time: datetime) -> dict:
    return {
        "station_id":       station["id"],
        "observation_ts":   obs_time.strftime("%Y-%m-%d %H:%M:%S"),
        "temperature_c":    round(random.gauss(15, 12), 1),
        "wind_speed_kts":   random.randint(0, 35),
        "wind_direction":   random.randint(0, 359),
        "visibility_m":     random.choice([500, 1000, 2000, 5000, 9999, 9999, 9999]),
        "precipitation_mm": round(max(0, random.gauss(0.5, 2)), 2),
        "pressure_hpa":     round(random.gauss(1013, 15), 1),
        "conditions":       random.choice(CONDITIONS),
    }


def generate_csv_batch() -> str:
    """Return a complete CSV string (header + 30 rows) for all stations."""
    now = datetime.now(timezone.utc)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_HEADER)
    writer.writeheader()
    for station in STATIONS:
        for offset_min in range(0, 60, 10):
            obs_time = now - timedelta(minutes=offset_min)
            writer.writerow(make_observation(station, obs_time))
    return buf.getvalue()


def main():
    parser = argparse.ArgumentParser(description="Weather Kafka producer (CSV batch messages)")
    parser.add_argument("--broker",   default="localhost:29092", help="Kafka bootstrap server")
    parser.add_argument("--topic",    default="weather-csv",     help="Target Kafka topic")
    parser.add_argument("--batches",  type=int, default=3,       help="Number of CSV batches to publish")
    parser.add_argument("--interval", type=int, default=5,       help="Seconds between batches")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": args.broker, "acks": "all"})

    for i in range(args.batches):
        csv_data = generate_csv_batch()
        producer.produce(args.topic, value=csv_data.encode("utf-8"))
        producer.flush()
        ts = datetime.now().isoformat(timespec="seconds")
        row_count = len(csv_data.splitlines()) - 1  # subtract header
        print(f"[{ts}]  Batch {i + 1}/{args.batches}: {row_count} rows → {args.topic}")
        if i < args.batches - 1:
            time.sleep(args.interval)

    print(f"Done. {args.batches} CSV batch(es) published to {args.topic}.")


if __name__ == "__main__":
    main()
