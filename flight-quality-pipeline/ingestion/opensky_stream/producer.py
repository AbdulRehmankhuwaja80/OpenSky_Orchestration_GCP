"""Polls the OpenSky Network REST API for live aircraft state vectors over a small, fixed set of
bounding-box regions and streams each state vector into a Kafka topic - the ingestion-layer
counterpart to the OpenAQ batch (dlt) pipeline.

OpenSky retired basic (username/password) auth on March 18, 2026; this uses the OAuth2 client
credentials flow instead. Without OPENSKY_CLIENT_ID/OPENSKY_CLIENT_SECRET, requests fall back to
anonymous access with lower rate limits.
"""
import json
import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

OPENSKY_CLIENT_ID = os.environ.get("OPENSKY_CLIENT_ID")
OPENSKY_CLIENT_SECRET = os.environ.get("OPENSKY_CLIENT_SECRET")
REGIONS = [
    tuple(part.strip() for part in pair.split(","))
    for pair in os.environ["OPENSKY_BBOXES"].split("|")
]
KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC_OPENSKY"]
POLL_INTERVAL_SECONDS = int(os.environ["STREAM_TRIGGER_SECONDS"])

STATES_URL = "https://opensky-network.org/api/states/all"
TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
)
TOKEN_REFRESH_MARGIN_SECONDS = 60  # refresh a bit before the ~30-minute token actually expires

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

_token = None
_token_expires_at = 0.0


def _get_access_token() -> str | None:
    """Returns a cached OAuth2 bearer token, refreshing it shortly before expiry. Returns None
    (anonymous access) if no client credentials are configured."""
    global _token, _token_expires_at

    if not OPENSKY_CLIENT_ID or not OPENSKY_CLIENT_SECRET:
        return None

    if _token and time.time() < _token_expires_at:
        return _token

    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": OPENSKY_CLIENT_ID,
            "client_secret": OPENSKY_CLIENT_SECRET,
        },
        timeout=15,
    )
    response.raise_for_status()
    payload = response.json()
    _token = payload["access_token"]
    _token_expires_at = time.time() + payload.get("expires_in", 1800) - TOKEN_REFRESH_MARGIN_SECONDS
    return _token


def fetch_states(region: str, lamin: str, lomin: str, lamax: str, lomax: str) -> list[dict]:
    headers = {}
    token = _get_access_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"

    response = requests.get(
        STATES_URL,
        params={"lamin": lamin, "lomin": lomin, "lamax": lamax, "lomax": lomax},
        headers=headers,
        timeout=15,
    )
    response.raise_for_status()
    data = response.json()
    ingested_at = datetime.now(timezone.utc).isoformat()
    observed_at = datetime.fromtimestamp(data["time"], tz=timezone.utc).isoformat()

    records = []
    for state in data.get("states") or []:
        # Fixed field order per the OpenSky REST API state-vector spec.
        records.append(
            {
                "region": region,
                "icao24": state[0],
                "callsign": (state[1] or "").strip() or None,
                "origin_country": state[2],
                "longitude": state[5],
                "latitude": state[6],
                "baro_altitude": state[7],
                "on_ground": state[8],
                "velocity": state[9],
                "true_track": state[10],
                "vertical_rate": state[11],
                "geo_altitude": state[13],
                "squawk": state[14],
                "observed_at": observed_at,
                "ingested_at": ingested_at,
            }
        )
    return records


def run() -> None:
    while True:
        for region, lamin, lomin, lamax, lomax in REGIONS:
            try:
                for record in fetch_states(region, lamin, lomin, lamax, lomax):
                    producer.send(KAFKA_TOPIC, record)
            except requests.exceptions.RequestException as exc:
                print(f"WARNING: skipping region {region} this cycle: {exc}")
        producer.flush()
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
