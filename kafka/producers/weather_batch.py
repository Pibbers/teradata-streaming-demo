#!/usr/bin/env python3
"""
Generates synthetic weather observations as CSV and uploads them to MinIO.
Simulates METAR-style observations for 5 ICAO airport stations.

Writes one CSV file per run to:
    s3://demo-csv/raw/weather/YYYYMMDD_HHMMSS.csv

Usage:
    python weather_batch.py [--endpoint http://minio:9000] [--runs 1] [--interval 300]

Requirements:
    pip install boto3
"""

import argparse
import csv
import io
import random
from datetime import datetime, timezone, timedelta

import boto3
from botocore.client import Config

STATIONS = [
    {"id": "EGLL", "name": "London Heathrow", "lat": 51.477, "lon": -0.461},
    {"id": "EHAM", "name": "Amsterdam Schiphol", "lat": 52.308, "lon": 4.764},
    {"id": "KJFK", "name": "New York JFK", "lat": 40.641, "lon": -73.778},
    {"id": "KLAX", "name": "Los Angeles LAX", "lat": 33.943, "lon": -118.408},
    {"id": "KATL", "name": "Atlanta Hartsfield", "lat": 33.641, "lon": -84.427},
]

CONDITIONS = ["VFR", "VFR", "VFR", "MVFR", "IFR", "LIFR"]

CSV_HEADER = [
    "station_id", "observation_ts", "temperature_c", "wind_speed_kts",
    "wind_direction", "visibility_m", "precipitation_mm", "pressure_hpa", "conditions",
]


def make_observation(station: dict, obs_time: datetime) -> dict:
    return {
        "station_id": station["id"],
        "observation_ts": obs_time.strftime("%Y-%m-%d %H:%M:%S"),
        "temperature_c": round(random.gauss(15, 12), 1),
        "wind_speed_kts": random.randint(0, 35),
        "wind_direction": random.randint(0, 359),
        "visibility_m": random.choice([500, 1000, 2000, 5000, 9999, 9999, 9999]),
        "precipitation_mm": round(max(0, random.gauss(0.5, 2)), 2),
        "pressure_hpa": round(random.gauss(1013, 15), 1),
        "conditions": random.choice(CONDITIONS),
    }


def generate_csv_batch() -> str:
    now = datetime.now(timezone.utc)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_HEADER)
    writer.writeheader()
    for station in STATIONS:
        for offset_min in range(0, 60, 10):
            obs_time = now - timedelta(minutes=offset_min)
            writer.writerow(make_observation(station, obs_time))
    return buf.getvalue()


def upload_to_minio(csv_content: str, endpoint: str, access_key: str, secret_key: str, bucket: str):
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    key = f"raw/weather/{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    s3.put_object(Bucket=bucket, Key=key, Body=csv_content.encode("utf-8"), ContentType="text/csv")
    print(f"[{datetime.now().isoformat()}] Uploaded {key} to {bucket} ({len(csv_content)} bytes)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default="http://minio:9000")
    parser.add_argument("--access-key", default="minioadmin")
    parser.add_argument("--secret-key", default="minioadmin")
    parser.add_argument("--bucket", default="demo-csv")
    parser.add_argument("--runs", type=int, default=1, help="Number of batch uploads (0 = unlimited)")
    parser.add_argument("--interval", type=int, default=300, help="Seconds between runs")
    args = parser.parse_args()

    run = 0
    try:
        while args.runs == 0 or run < args.runs:
            csv_data = generate_csv_batch()
            upload_to_minio(csv_data, args.endpoint, args.access_key, args.secret_key, args.bucket)
            run += 1
            if args.runs == 0 or run < args.runs:
                import time
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\nStopped after {run} batches.")

    print(f"Done. {run} weather batch(es) uploaded.")


if __name__ == "__main__":
    main()
