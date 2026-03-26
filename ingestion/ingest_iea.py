"""
Fetches EV registration data from the IEA Global EV Data Explorer
and lands the raw CSV file to s3://count-electric/landing/raw/iea/.

Skips the upload if the fetched content is identical to the most
recent file already in S3 (compared by MD5 hash).

Source: https://www.iea.org/data-and-statistics/data-tools/global-ev-data-explorer
Format: CSV
Cadence: Annual
"""

import hashlib
import logging
import os
from datetime import datetime

import boto3
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

IEA_URL = "https://api.iea.org/evs?parameter=EV+sales&category=Historical&mode=Cars&csv=true"
S3_BUCKET = os.getenv("S3_BUCKET", "count-electric")
S3_PREFIX = "landing/raw/iea"


def fetch_iea_data() -> bytes:
    logger.info("Fetching IEA EV data from %s", IEA_URL)
    response = requests.get(IEA_URL, timeout=30)
    response.raise_for_status()
    logger.info("Fetched %d bytes", len(response.content))
    return response.content


def latest_s3_object(s3, prefix: str) -> dict | None:
    """Return the metadata of the most recently modified object under prefix, or None."""
    result = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    objects = result.get("Contents", [])
    if not objects:
        return None
    return max(objects, key=lambda o: o["LastModified"])


def md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def upload_to_s3(s3, data: bytes, filename: str) -> None:
    key = f"{S3_PREFIX}/{filename}"
    logger.info("Uploading to s3://%s/%s", S3_BUCKET, key)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=data, ContentType="text/csv")
    logger.info("Upload complete.")


def main() -> None:
    s3 = boto3.client("s3")
    data = fetch_iea_data()
    new_hash = md5(data)

    latest = latest_s3_object(s3, S3_PREFIX)
    if latest:
        existing = s3.get_object(Bucket=S3_BUCKET, Key=latest["Key"])["Body"].read()
        if md5(existing) == new_hash:
            logger.info("Data unchanged (hash match). Skipping upload. Latest: %s", latest["Key"])
            return
        logger.info("Data has changed — uploading new file.")
    else:
        logger.info("No existing files in S3 — uploading first file.")

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    upload_to_s3(s3, data, f"iea_ev_sales_{timestamp}.csv")
    logger.info("IEA ingestion complete.")


if __name__ == "__main__":
    main()
