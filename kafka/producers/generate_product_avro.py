#!/usr/bin/env python3
"""
Generates two Avro container files for Demo 1:
  data/sample/product_v1.avro  — 30 records using the initial schema
  data/sample/product_v2.avro  — 30 records using the evolved schema (adds 3 fields)

Used to demonstrate schema evolution: both files are loaded into the same
Teradata DATASET column; v1 rows return NULL for the new fields.

Requirements:
    pip install fastavro
"""

import json
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path

import fastavro

SCHEMA_DIR = Path(__file__).parent.parent / "schemas"
OUTPUT_DIR = Path(__file__).parent.parent.parent / "data" / "sample"

CATEGORIES = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Food & Drink"]
SUBCATEGORIES = {
    "Electronics": ["Phones", "Laptops", "Tablets", "Accessories", "Audio"],
    "Clothing":    ["Mens", "Womens", "Kids", "Footwear", "Outerwear"],
    "Home & Garden": ["Kitchen", "Bedroom", "Garden Tools", "Lighting", "Storage"],
    "Sports":      ["Cycling", "Running", "Swimming", "Team Sports", "Fitness"],
    "Books":       ["Fiction", "Non-Fiction", "Science", "History", "Children"],
    "Food & Drink": ["Snacks", "Beverages", "Organic", "International", "Dairy"],
}
DESCRIPTIONS = [
    "Premium quality product with excellent durability.",
    "Best-seller in its category, highly rated by customers.",
    "Eco-friendly materials, sustainably sourced.",
    "Compact and lightweight design for everyday use.",
    "Professional-grade specification at consumer price.",
]


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def make_v1_record() -> dict:
    cat = random.choice(CATEGORIES)
    return {
        "product_id": str(uuid.uuid4()),
        "name": f"{cat} Item {random.randint(100, 999)}",
        "price": round(random.uniform(4.99, 499.99), 2),
        "category": cat,
        "ts": now_ms(),
    }


def make_v2_record() -> dict:
    cat = random.choice(CATEGORIES)
    record = {
        "product_id": str(uuid.uuid4()),
        "name": f"{cat} Pro {random.randint(1000, 9999)}",
        "price": round(random.uniform(9.99, 999.99), 2),
        "category": cat,
        "ts": now_ms(),
        "description": random.choice(DESCRIPTIONS),
        "subcategory": random.choice(SUBCATEGORIES[cat]),
        "discount_pct": round(random.uniform(0, 30), 1) if random.random() > 0.3 else None,
    }
    return record


def write_avro(path: Path, schema_path: Path, records: list):
    with open(schema_path) as f:
        schema = fastavro.parse_schema(json.load(f))
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        fastavro.writer(f, schema, records)
    print(f"Written {len(records)} records → {path}")


def main():
    write_avro(
        OUTPUT_DIR / "product_v1.avro",
        SCHEMA_DIR / "product_v1.avsc",
        [make_v1_record() for _ in range(30)],
    )
    write_avro(
        OUTPUT_DIR / "product_v2.avro",
        SCHEMA_DIR / "product_v2.avsc",
        [make_v2_record() for _ in range(30)],
    )
    print("Done. Both Avro files ready for Demo 1.")


if __name__ == "__main__":
    main()
