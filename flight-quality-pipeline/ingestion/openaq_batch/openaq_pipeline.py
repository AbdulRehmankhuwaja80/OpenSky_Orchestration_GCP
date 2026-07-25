"""Batch-loads OpenAQ historical air quality data into the GCS data lake (bronze layer) via dlt.

OpenAQ v3 has no flat /measurements endpoint (that was v2, since sunset) - measurements are
scoped to a sensor, and sensors are scoped to a location, so we walk:
  locations (by country) -> each location's sensors -> each sensor's measurements
"""
import os
import time
from datetime import datetime, timedelta, timezone

import dlt
import requests
from dotenv import load_dotenv

load_dotenv()

OPENAQ_API_KEY = os.environ["OPENAQ_API_KEY"]
COUNTRIES = os.environ["OPENAQ_COUNTRIES"].split(",")
BACKFILL_DAYS = int(os.environ["OPENAQ_BACKFILL_DAYS"])
# Capped per-sensor (not globally) and per-location, so the walk always reaches multiple
# pollutants/locations/countries instead of a flat row cap exhausting itself on the very first
# sensor - see the dashboard's per-pollutant KPIs, which need PM2.5/PM10/NO2/O3 all represented.
MAX_ROWS_PER_SENSOR = int(os.environ.get("OPENAQ_MAX_ROWS_PER_SENSOR", "50"))
MAX_LOCATIONS_PER_COUNTRY = int(os.environ.get("OPENAQ_MAX_LOCATIONS_PER_COUNTRY", "3"))
# Only the pollutant sensors the dashboard actually uses - OpenAQ stations often also carry
# meteorological sensors (temperature, relativehumidity, ...) that aren't needed here.
TARGET_PARAMETERS = {"pm25", "pm10", "no2", "o3", "so2", "co"}

BASE_URL = "https://api.openaq.org/v3"
PAGE_SIZE = 1000
REQUEST_DELAY_SECONDS = 1.1  # OpenAQ's free tier allows ~60 requests/min; stay comfortably under that

# How many /locations pages (LOCATION_PAGE_SIZE each) to scan per country when picking which
# stations to ingest. OpenAQ doesn't support sorting locations by recency server-side
# (order_by=datetimeLast -> 422), and its default order isn't recency-based either - many
# low-cost sensor networks include long-dead stations mixed in with ones reporting today, so
# candidates are fetched, filtered to ones with a target-parameter sensor, and sorted by
# datetimeLast client-side instead of just taking the first few locations verbatim.
LOCATION_SCAN_PAGES = 5
LOCATION_PAGE_SIZE = 100

# A single, reused Session (keep-alive connection pool) instead of requests.get() per call.
# Opening a fresh connection (and therefore a fresh DNS lookup) for every one of the hundreds
# of sensor requests this pipeline makes is what was flooding Docker's embedded DNS resolver
# and causing persistent NameResolutionError failures partway through a run.
SESSION = requests.Session()
SESSION.headers.update({"X-API-Key": OPENAQ_API_KEY})


class OpenAQRequestError(Exception):
    """Raised when a single endpoint keeps failing after retries - the caller can choose to skip it."""


def _get(path: str, params: dict, max_retries: int = 5) -> dict:
    response = None
    for attempt in range(max_retries):
        try:
            response = SESSION.get(f"{BASE_URL}{path}", params=params, timeout=30)
        except requests.exceptions.RequestException as exc:
            # DNS blips, connection resets, timeouts, etc. never produce a response object,
            # so they have to be caught separately from the status-code checks below.
            if attempt == max_retries - 1:
                raise OpenAQRequestError(f"{path} failed after {max_retries} attempts: {exc}") from exc
            time.sleep(2**attempt)
            continue
        if response.status_code == 429:
            retry_after = float(response.headers.get("Retry-After", 2**attempt))
            time.sleep(retry_after)
            continue
        if response.status_code >= 500:
            # OpenAQ's backend occasionally 500s on specific sensors/queries; a few retries
            # separate transient blips from a persistently broken endpoint.
            time.sleep(2**attempt)
            continue
        response.raise_for_status()
        time.sleep(REQUEST_DELAY_SECONDS)
        return response.json()
    raise OpenAQRequestError(f"{path} failed after {max_retries} attempts (last status {response.status_code})")


def _paginate(path: str, params: dict):
    page = 1
    while True:
        data = _get(path, {**params, "limit": PAGE_SIZE, "page": page})
        results = data.get("results", [])
        if not results:
            return
        yield from results
        if len(results) < PAGE_SIZE:
            return
        page += 1


def _active_locations(country: str) -> list:
    """Scans up to LOCATION_SCAN_PAGES pages of locations for a country, keeps only ones with at
    least one target-parameter sensor, and picks MAX_LOCATIONS_PER_COUNTRY of them: greedily by
    which new target parameter each adds (so the dashboard's per-pollutant KPIs - PM2.5, PM10,
    NO2, O3 - are actually populated instead of, say, 5 locations that only ever cover PM2.5/O3),
    ties broken by most-recently-reporting first, then any remaining slots filled by recency alone.
    """
    candidates = []
    for page in range(1, LOCATION_SCAN_PAGES + 1):
        data = _get("/locations", {"iso": country, "limit": LOCATION_PAGE_SIZE, "page": page})
        results = data.get("results", [])
        if not results:
            break
        for location in results:
            parameters = {sn.get("parameter", {}).get("name") for sn in location.get("sensors", [])}
            if parameters & TARGET_PARAMETERS:
                candidates.append(location)

    candidates.sort(key=lambda loc: (loc.get("datetimeLast") or {}).get("utc") or "", reverse=True)

    selected, selected_ids, covered = [], set(), set()
    for location in candidates:
        if len(selected) >= MAX_LOCATIONS_PER_COUNTRY:
            break
        parameters = {sn.get("parameter", {}).get("name") for sn in location.get("sensors", [])} & TARGET_PARAMETERS
        if parameters - covered:
            selected.append(location)
            selected_ids.add(location["id"])
            covered |= parameters

    for location in candidates:
        if len(selected) >= MAX_LOCATIONS_PER_COUNTRY:
            break
        if location["id"] not in selected_ids:
            selected.append(location)
            selected_ids.add(location["id"])

    return selected


@dlt.resource(name="openaq_measurements", write_disposition="append")
def openaq_measurements():
    total_rows = 0

    for country in COUNTRIES:
        for location in _active_locations(country):
            # The location's own `sensors` list (from /locations) only has id/name/parameter -
            # no recency. A location's overall datetimeLast reflects whichever of its sensors is
            # still active, which is not necessarily all of them (e.g. a station's PM2.5 sensor
            # keeps reporting today while its CO/SO2 sensors quietly died years ago) - so recency
            # has to be checked per sensor via /locations/{id}/sensors, not assumed from the
            # location level.
            sensors = _get(f"/locations/{location['id']}/sensors", {"limit": 100}).get("results", [])

            for sensor in sensors:
                parameter = sensor.get("parameter", {}).get("name")
                if parameter not in TARGET_PARAMETERS:
                    continue
                last_reading = (sensor.get("datetimeLast") or {}).get("utc")
                if not last_reading:
                    continue  # sensor registered but has never reported - nothing to fetch

                window_end = datetime.fromisoformat(last_reading.replace("Z", "+00:00"))
                datetime_from = (window_end - timedelta(days=BACKFILL_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")

                try:
                    # OpenAQ returns measurements oldest-first within the window, and a
                    # BACKFILL_DAYS-wide window at hourly cadence easily fits in one page - so
                    # capping mid-stream (yield-then-break) would silently keep only the OLDEST
                    # MAX_ROWS_PER_SENSOR readings, never reaching the sensor's most recent ones
                    # (the whole point of anchoring the window to this sensor's own last
                    # reading). Materialize the window and take the tail instead.
                    window_measurements = list(
                        _paginate(f"/sensors/{sensor['id']}/measurements", {"datetime_from": datetime_from})
                    )
                except OpenAQRequestError as err:
                    print(f"WARNING: skipping sensor {sensor['id']} (location {location['id']}): {err}")
                    continue

                recent = window_measurements[-MAX_ROWS_PER_SENSOR:] if MAX_ROWS_PER_SENSOR else window_measurements
                for measurement in recent:
                    measurement["location_id"] = location["id"]
                    measurement["location_name"] = location.get("name")
                    measurement["sensor_id"] = sensor["id"]
                    measurement["parameter"] = parameter
                    measurement["country"] = country
                    yield measurement
                    total_rows += 1

    print(f"Yielded {total_rows} measurements across {len(COUNTRIES)} countries.")


def run():
    # dlt sanitizes dataset_name (slashes -> underscores), so the "bronze/" layer has to live in
    # bucket_url (a literal path, not sanitized) instead - dataset_name stays a single word.
    bucket_root = os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"]
    pipeline = dlt.pipeline(
        pipeline_name="openaq_batch",
        destination=dlt.destinations.filesystem(bucket_url=f"{bucket_root}/bronze"),
        dataset_name="openaq",
    )
    load_info = pipeline.run(openaq_measurements(), loader_file_format="parquet")
    print(load_info)


if __name__ == "__main__":
    run()
