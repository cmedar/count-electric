"""
Fetches new passenger car registrations by motor energy type from Eurostat
(dataset: ROAD_EQR_CARPDA) and lands the raw JSON response to
s3://count-electric/landing/raw/eurostat/.

This is the key source for showing EV vs ICE (petrol/diesel) trends,
including Romania country-level data.

Source: https://ec.europa.eu/eurostat/databrowser/view/ROAD_EQR_CARPDA/
API docs: https://wikis.ec.europa.eu/display/EUROSTATHELP/API+Statistics+-+data+query
Format: JSON-stat2
Cadence: Annual (published with ~3 month lag for the previous year)
"""

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

# Eurostat API — ROAD_EQR_CARPDA dataset
# mot_nrg values we care about:
#   EL  = Electric (BEV)
#   PHEV = Plug-in hybrid
#   HEV  = Non-plug-in hybrid
#   LPG  = LPG
#   TOTAL = All fuel types combined (used to calculate ICE share)
#   PETROL, DIESEL = traditional ICE
EUROSTAT_URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/ROAD_EQR_CARPDA"
    "?format=JSON"
    "&freq=A"           # Annual frequency
    "&lang=EN"
)

S3_BUCKET = os.getenv("S3_BUCKET", "count-electric")
S3_PREFIX = "landing/raw/eurostat"


def fetch_eurostat_data() -> dict:
    """Download the Eurostat ROAD_EQR_CARPDA dataset and return parsed JSON."""
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


def upload_to_s3(data: dict, filename: str) -> None:
    """Upload JSON data to the S3 landing zone."""
    s3 = boto3.client("s3")
    key = f"{S3_PREFIX}/{filename}"
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    logger.info("Uploading to s3://%s/%s (%d bytes)", S3_BUCKET, key, len(body))
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json")
    logger.info("Upload complete.")


def main() -> None:
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"eurostat_road_eqr_carpda_{timestamp}.json"

    data = fetch_eurostat_data()
    upload_to_s3(data, filename)
    logger.info("Eurostat ingestion complete: %s", filename)


if __name__ == "__main__":
    main()
