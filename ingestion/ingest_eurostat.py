"""
Fetches new passenger car registrations by motor energy type from Eurostat
(dataset: ROAD_EQR_CARPDA) and lands the raw JSON response to
s3://count-electric/landing/raw/eurostat/.

Skips the upload if the fetched content is identical to the most
recent file already in S3 (compared by MD5 hash of canonical JSON).

This is the key source for showing EV vs Combustion trends,
including Romania country-level data.

Source: https://ec.europa.eu/eurostat/databrowser/view/ROAD_EQR_CARPDA/
API docs: https://wikis.ec.europa.eu/display/EUROSTATHELP/API+Statistics+-+data+query
Format: JSON-stat2
Cadence: Annual (published with ~3 month lag for the previous year)
"""

import hashlib
import json
import logging
import os
from datetime import datetime

import boto3
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

EUROSTAT_URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/ROAD_EQR_CARPDA"
    "?format=JSON"
    "&freq=A"
    "&lang=EN"
)

S3_BUCKET = os.getenv("S3_BUCKET", "count-electric")
S3_PREFIX = "landing/raw/eurostat"


def fetch_eurostat_data() -> dict:
    logger.info("Fetching Eurostat ROAD_EQR_CARPDA from API")
    response = requests.get(EUROSTAT_URL, timeout=60)
    response.raise_for_status()
    data = response.json()
    logger.info(
        "Fetched dataset. Size: %d bytes, dataset label: %s",
        len(response.content),
        data.get("label", "unknown"),
    )
    return data


def canonical_bytes(data: dict) -> bytes:
    """Stable JSON serialisation for hashing — sort_keys eliminates key-order differences."""
    return json.dumps(data, sort_keys=True, ensure_ascii=False).encode("utf-8")


def md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def latest_s3_object(s3, prefix: str) -> dict | None:
    """Return the metadata of the most recently modified object under prefix, or None."""
    result = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    objects = result.get("Contents", [])
    if not objects:
        return None
    return max(objects, key=lambda o: o["LastModified"])


def upload_to_s3(s3, data: dict, filename: str) -> None:
    key = f"{S3_PREFIX}/{filename}"
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    logger.info("Uploading to s3://%s/%s (%d bytes)", S3_BUCKET, key, len(body))
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json")
    logger.info("Upload complete.")


def main() -> None:
    s3 = boto3.client("s3")
    data = fetch_eurostat_data()
    new_hash = md5(canonical_bytes(data))

    latest = latest_s3_object(s3, S3_PREFIX)
    if latest:
        existing_bytes = s3.get_object(Bucket=S3_BUCKET, Key=latest["Key"])["Body"].read()
        existing_hash = md5(canonical_bytes(json.loads(existing_bytes)))
        if existing_hash == new_hash:
            logger.info("Data unchanged (hash match). Skipping upload. Latest: %s", latest["Key"])
            return
        logger.info("Data has changed — uploading new file.")
    else:
        logger.info("No existing files in S3 — uploading first file.")

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    upload_to_s3(s3, data, f"eurostat_road_eqr_carpda_{timestamp}.json")
    logger.info("Eurostat ingestion complete.")


if __name__ == "__main__":
    main()
