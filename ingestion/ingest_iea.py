"""
Fetches EV registration data from the IEA Global EV Data Explorer
and lands the raw CSV file to s3://count-electric/landing/raw/iea/.

Source: https://www.iea.org/data-and-statistics/data-tools/global-ev-data-explorer
Format: CSV
Cadence: Annual
"""

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
    """Download the IEA Global EV Data CSV and return raw bytes."""
    logger.info("Fetching IEA EV data from %s", IEA_URL)
    response = requests.get(IEA_URL, timeout=30)
    response.raise_for_status()
    logger.info("Fetched %d bytes", len(response.content))
    return response.content


def upload_to_s3(data: bytes, filename: str) -> None:
    """Upload raw bytes to the S3 landing zone using the EC2 IAM role."""
    s3 = boto3.client("s3")
    key = f"{S3_PREFIX}/{filename}"
    logger.info("Uploading to s3://%s/%s", S3_BUCKET, key)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=data, ContentType="text/csv")
    logger.info("Upload complete.")


def main() -> None:
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"iea_ev_sales_{timestamp}.csv"

    data = fetch_iea_data()
    upload_to_s3(data, filename)
    logger.info("IEA ingestion complete: %s", filename)


if __name__ == "__main__":
    main()
