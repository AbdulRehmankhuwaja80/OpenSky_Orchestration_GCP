""" @bruin
name: bruin_openaq_bronze
image: python:3.11
description: |
  Bruin's OpenAQ ingestion step - pulls a small number of recent PM2.5/PM10/O3/NO2 readings
  from the OpenAQ API and writes them, untouched, to the GCS bronze layer. This is a plain
  script asset (no `connection`/`materialization` fields) since Bruin has no native "write raw
  files to a bucket" primitive for a Python asset - it calls google-cloud-storage directly,
  the same way the main pipeline's Kafka consumer writes to bronze/opensky/.
@bruin """

import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv
from google.cloud import storage

# This file lives at bruin/assets/ingestion/openaq_bronze.py, so the repo root is 3 parents up.
REPO_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(REPO_ROOT / ".env")
# GOOGLE_APPLICATION_CREDENTIALS in .env is a container-internal path (/credentials/...) used by
# the dockerized tools - Bruin runs on the host, so the real path is computed here instead.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(REPO_ROOT / "credentials" / "gcp-service-account.json")

OPENAQ_API_KEY = os.environ["OPENAQ_API_KEY"]
COUNTRIES = os.environ["OPENAQ_COUNTRIES"].split(",")
BUCKET = os.environ["GCP_BUCKET_NAME"]

BASE_URL = "https://api.openaq.org/v3"
TARGET_PARAMETERS = {"pm25", "pm10", "no2", "o3"}
LOCATIONS_PER_COUNTRY = 2  # kept deliberately small - this is a demo/learning pipeline
ROWS_PER_SENSOR = 15
BACKFILL_DAYS = 7

SESSION = requests.Session()
SESSION.headers.update({"X-API-Key": OPENAQ_API_KEY})


def _get(path: str, params: dict) -> dict:
    response = SESSION.get(f"{BASE_URL}{path}", params=params, timeout=30)
    response.raise_for_status()
    time.sleep(1.1)  # OpenAQ's free tier allows ~60 requests/min
    return response.json()


def _active_locations(country: str) -> list:
    """Picks a couple of currently-reporting locations for a country. OpenAQ's default
    /locations order isn't recency-based, so candidates are filtered to ones with a
    target-parameter sensor and sorted by datetimeLast before picking the top ones.
    """
    data = _get("/locations", {"iso": country, "limit": 100, "page": 1})
    candidates = [
        loc
        for loc in data.get("results", [])
        if {sn.get("parameter", {}).get("name") for sn in loc.get("sensors", [])} & TARGET_PARAMETERS
    ]
    candidates.sort(key=lambda loc: (loc.get("datetimeLast") or {}).get("utc") or "", reverse=True)
    return candidates[:LOCATIONS_PER_COUNTRY]


def fetch_measurements() -> list:
    records = []
    for country in COUNTRIES:
        for location in _active_locations(country):
            # A location's own datetimeLast reflects whichever of its sensors is still active,
            # not necessarily all of them - recency has to be checked per sensor.
            sensors = _get(f"/locations/{location['id']}/sensors", {"limit": 100}).get("results", [])
            for sensor in sensors:
                parameter = sensor.get("parameter", {}).get("name")
                if parameter not in TARGET_PARAMETERS:
                    continue
                last_reading = (sensor.get("datetimeLast") or {}).get("utc")
                if not last_reading:
                    continue

                window_end = datetime.fromisoformat(last_reading.replace("Z", "+00:00"))
                datetime_from = (window_end - timedelta(days=BACKFILL_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
                data = _get(
                    f"/sensors/{sensor['id']}/measurements",
                    {"datetime_from": datetime_from, "limit": 1000},
                )
                results = data.get("results", [])
                # Take the most recent rows within the window, not the oldest - a wide window
                # combined with a small row cap otherwise silently returns only the earliest
                # slice of the window instead of anything close to "now".
                for measurement in results[-ROWS_PER_SENSOR:]:
                    records.append(
                        {
                            "sensor_id": sensor["id"],
                            "location_id": location["id"],
                            "location_name": location.get("name"),
                            "parameter": parameter,
                            "country": country,
                            "value": measurement.get("value"),
                            "datetime_from_utc": measurement.get("period", {})
                            .get("datetimeFrom", {})
                            .get("utc"),
                        }
                    )
    return records


def write_to_bronze(records: list) -> None:
    if not records:
        print("No records fetched - nothing written to bronze.")
        return
    body = "\n".join(json.dumps(r) for r in records)
    blob_name = f"bronze/bruin_openaq/{int(time.time() * 1000)}.jsonl"
    storage.Client().bucket(BUCKET).blob(blob_name).upload_from_string(
        body, content_type="application/json"
    )
    print(f"Wrote {len(records)} records to gs://{BUCKET}/{blob_name}")


write_to_bronze(fetch_measurements())
