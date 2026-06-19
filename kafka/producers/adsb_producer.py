#!/usr/bin/env python3
"""
Simulates ADS-B aircraft position broadcasts and publishes them to Kafka
as schemaless Avro messages (embedded schema, no Schema Registry).

Simulates 10 aircraft flying synthetic Europe → North America routes.
Each aircraft updates its position at --interval seconds (default 5s).

Usage:
    python adsb_producer.py [--bootstrap kafka:9092] [--topic adsb-positions]
    python adsb_producer.py --interval 2 --continuous
    python adsb_producer.py --count 200

Requirements:
    pip install confluent-kafka fastavro
"""

import argparse
import io
import json
import math
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import fastavro
from confluent_kafka import Producer

SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "adsb_position.avsc"

# Fleet of 10 synthetic aircraft with fixed ICAO addresses
# Each entry: icao24, callsign, origin_lat, origin_lon, dest_lat, dest_lon, cruise_alt
FLEET = [
    {"icao24": "4ca87a", "callsign": "EIN101 ", "squawk": "2341",
     "origin": (51.47, -0.46), "dest": (40.63, -73.78), "cruise_alt": 37000},
    {"icao24": "400a5b", "callsign": "BAW213 ", "squawk": "3142",
     "origin": (51.47, -0.46), "dest": (33.94, -118.41), "cruise_alt": 39000},
    {"icao24": "3c4b0f", "callsign": "DLH463 ", "squawk": "1234",
     "origin": (50.03, 8.57),  "dest": (40.63, -73.78), "cruise_alt": 36000},
    {"icao24": "3950f2", "callsign": "AFR674 ", "squawk": "5672",
     "origin": (49.00, 2.55),  "dest": (25.79, -80.29), "cruise_alt": 38000},
    {"icao24": "34618a", "callsign": "IBE601 ", "squawk": "4532",
     "origin": (40.47, -3.57), "dest": (40.63, -73.78), "cruise_alt": 37000},
    {"icao24": "4841d8", "callsign": "KLM649 ", "squawk": "6712",
     "origin": (52.31, 4.77),  "dest": (43.67, -79.63), "cruise_alt": 36000},
    {"icao24": "4b1803", "callsign": "SWR100 ", "squawk": "3421",
     "origin": (47.46, 8.55),  "dest": (40.63, -73.78), "cruise_alt": 40000},
    {"icao24": "440a45", "callsign": "VIR025 ", "squawk": "7142",
     "origin": (51.47, -0.46), "dest": (33.94, -118.41), "cruise_alt": 35000},
    {"icao24": "4073d6", "callsign": "TOM3XT ", "squawk": "2231",
     "origin": (53.35, -2.27), "dest": (21.32, -157.92), "cruise_alt": 38000},
    {"icao24": "3f7062", "callsign": "RYR4421", "squawk": "5523",
     "origin": (53.42, -6.27), "dest": (52.52, 13.40), "cruise_alt": 33000, "short": True},
]


def lerp_great_circle(lat1, lon1, lat2, lon2, frac):
    """Linear interpolation on a sphere (simplified for short demo distances)."""
    lat = lat1 + (lat2 - lat1) * frac
    lon = lon1 + (lon2 - lon1) * frac
    return lat, lon


def heading_between(lat1, lon1, lat2, lon2):
    """Approximate initial bearing (degrees) from point 1 to point 2."""
    dlon = math.radians(lon2 - lon1)
    lat1_r, lat2_r = math.radians(lat1), math.radians(lat2)
    x = math.sin(dlon) * math.cos(lat2_r)
    y = math.cos(lat1_r) * math.sin(lat2_r) - math.sin(lat1_r) * math.cos(lat2_r) * math.cos(dlon)
    bearing = math.degrees(math.atan2(x, y))
    return (bearing + 360) % 360


class Aircraft:
    def __init__(self, spec: dict, total_steps: int):
        self.icao24 = spec["icao24"]
        self.callsign = spec["callsign"]
        self.squawk = spec["squawk"]
        self.origin = spec["origin"]
        self.dest = spec["dest"]
        self.cruise_alt = spec["cruise_alt"]
        self.short = spec.get("short", False)
        self.total_steps = total_steps
        self.step = random.randint(0, total_steps)  # stagger start position

    def position(self) -> dict:
        frac = (self.step % self.total_steps) / self.total_steps
        lat, lon = lerp_great_circle(*self.origin, *self.dest, frac)

        # Altitude profile: climb first 10%, cruise, descend last 10%
        if frac < 0.10:
            alt = int(self.cruise_alt * (frac / 0.10))
        elif frac > 0.90:
            alt = int(self.cruise_alt * ((1 - frac) / 0.10))
        else:
            alt = self.cruise_alt + random.randint(-200, 200)

        on_ground = frac < 0.02 or frac > 0.98
        hdg = heading_between(*self.origin, *self.dest)
        vrate = 1500 if frac < 0.10 else (-1500 if frac > 0.90 else random.randint(-50, 50))

        self.step += 1
        return {
            "icao24": self.icao24,
            "callsign": self.callsign.strip(),
            "latitude": round(lat + random.uniform(-0.001, 0.001), 6),
            "longitude": round(lon + random.uniform(-0.001, 0.001), 6),
            "altitude": max(0, alt),
            "velocity": round(490.0 + random.uniform(-20, 20) if not on_ground else random.uniform(0, 30), 1),
            "heading": round(hdg + random.uniform(-2, 2), 1),
            "vertical_rate": vrate if not on_ground else 0,
            "on_ground": on_ground,
            "squawk": self.squawk,
            "ts": int(datetime.now(timezone.utc).timestamp() * 1000),
        }


def encode_avro(record: dict, parsed_schema) -> bytes:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed_schema, record)
    return buf.getvalue()


def encode_json(record: dict) -> bytes:
    """Serialise as UTF-8 JSON (debug / non-TPT consumers)."""
    ts_ms = record["ts"]
    ts_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    millis = ts_ms % 1000
    json_record = {
        **record,
        "on_ground": 1 if record["on_ground"] else 0,
        "pos_date": ts_dt.strftime("%Y-%m-%d"),
        "ts": f"{ts_dt.strftime('%Y-%m-%d %H:%M:%S')}.{millis:03d}",
    }
    return json.dumps(json_record).encode("utf-8")


def encode_delimited(record: dict) -> bytes:
    """Serialise as pipe-delimited UTF-8 text for TPT DataConnector Format='DELIMITED'.

    Column order must match ADSB_SCHEMA in kafka_stream.tbuild exactly:
      icao24 | callsign | latitude | longitude | altitude | velocity |
      heading | vertical_rate | on_ground | squawk | pos_date | ts
    """
    ts_ms = record["ts"]
    ts_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    millis = ts_ms % 1000
    fields = [
        record["icao24"],
        record["callsign"],
        record["latitude"],
        record["longitude"],
        record["altitude"],
        record["velocity"],
        record["heading"],
        record["vertical_rate"],
        1 if record["on_ground"] else 0,
        record["squawk"],
        ts_dt.strftime("%Y-%m-%d"),
        f"{ts_dt.strftime('%Y-%m-%d %H:%M:%S')}.{millis:03d}",
    ]
    return "|".join(str(f) for f in fields).encode("utf-8")


def delivery_report(err, msg):
    if err:
        print(f"[ERROR] {err}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="kafka:9092")
    parser.add_argument("--topic", default="adsb-positions")
    parser.add_argument("--interval", type=float, default=5.0, help="Seconds between updates per aircraft")
    parser.add_argument("--count", type=int, default=0, help="Total messages to send (0 = unlimited)")
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--format", choices=["avro", "json", "delimited"], default="avro",
                        help="avro: schemaless Avro (default); json: UTF-8 JSON; "
                             "delimited: pipe-delimited text for TPT DataConnector")
    args = parser.parse_args()

    parsed_schema = None
    if args.format == "avro":
        with open(SCHEMA_PATH) as f:
            raw_schema = json.load(f)
        parsed_schema = fastavro.parse_schema(raw_schema)

    producer = Producer({"bootstrap.servers": args.bootstrap})

    # One full route = ~720 steps at 5s interval ≈ 1 hour flight (compressed)
    aircraft = [Aircraft(spec, total_steps=720) for spec in FLEET]

    sent = 0
    try:
        while args.continuous or args.count == 0 or sent < args.count:
            for ac in aircraft:
                record = ac.position()
                if args.format == "avro":
                    payload = encode_avro(record, parsed_schema)
                elif args.format == "json":
                    payload = encode_json(record)
                else:
                    payload = encode_delimited(record)
                producer.produce(args.topic, key=record["icao24"], value=payload, callback=delivery_report)
                sent += 1
            producer.poll(0)
            if sent % 50 == 0:
                print(f"[{datetime.now().isoformat()}] {sent} ADS-B messages sent to {args.topic}")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\nStopped after {sent} messages.")
    finally:
        producer.flush()
        print(f"Total ADS-B messages sent: {sent}")


if __name__ == "__main__":
    main()
