"""
Simulated live order feed.

Writes a batch of synthetic "new web order" JSON records to a Unity
Catalog Volume landing path each time it runs. Scheduled as its own
Databricks Job task (e.g. every run of the hourly workflow) running
ahead of the bronze ingestion task, this gives the pipeline a genuine
incremental source on top of the static TPC-DS snapshot:
bronze_stream_orders.py (Autoloader) picks up whatever new files have
landed since the last run.

Pure Python + Faker -- no Spark/Databricks import needed, so this can
also be run and unit-tested locally.
"""

import json
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path

from faker import Faker

fake = Faker()

CATEGORIES = ["Electronics", "Home", "Sports", "Books", "Toys", "Apparel", "Grocery"]
CHANNELS = ["web"]  # this simulator only models the "web" sales channel


def generate_order(order_id: str | None = None) -> dict:
    """Build a single synthetic order record shaped like a simplified
    web_sales row, so it can be unioned with the TPC-DS web_sales bronze
    table downstream."""
    return {
        "order_id": order_id or str(uuid.uuid4()),
        "order_ts": datetime.now(timezone.utc).isoformat(),
        "customer_name": fake.name(),
        "customer_email": fake.email(),
        "customer_city": fake.city(),
        "customer_state": fake.state_abbr(),
        "item_category": random.choice(CATEGORIES),
        "item_name": fake.word().title() + " " + random.choice(CATEGORIES)[:-1],
        "quantity": random.randint(1, 5),
        "unit_price": round(random.uniform(5, 300), 2),
        "channel": random.choice(CHANNELS),
    }


def generate_batch(n: int) -> list[dict]:
    return [generate_order() for _ in range(n)]


def write_batch_to_volume(landing_path: str, n: int = 50) -> str:
    """Write one batch of n synthetic orders as a single JSON-lines file
    into the landing_path (a Unity Catalog Volume path when run on
    Databricks, or any local directory for local testing).

    landing_path example on Databricks:
      /Volumes/retail_lakehouse/bronze/order_stream_landing/
    """
    Path(landing_path).mkdir(parents=True, exist_ok=True)

    batch = generate_batch(n)
    filename = f"orders_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}.json"
    filepath = Path(landing_path) / filename

    with open(filepath, "w") as f:
        for record in batch:
            f.write(json.dumps(record) + "\n")

    print(f"Wrote {len(batch)} synthetic orders to {filepath}")
    return str(filepath)


if __name__ == "__main__":
    import sys

    path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/order_stream_landing"
    batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 50
    write_batch_to_volume(path, batch_size)
