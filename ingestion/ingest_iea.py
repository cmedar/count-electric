"""
Fetches EV registration data from the IEA Global EV Data Explorer
and lands the raw CSV file to s3://count-electric/landing/raw/iea/.

Source: https://www.iea.org/data-and-statistics/data-tools/global-ev-data-explorer
Format: CSV download (manual export or direct URL)
Cadence: Annual
"""

import os
import boto3
from dotenv import load_dotenv

load_dotenv()


def fetch_iea_data() -> bytes:
    """Download the IEA Global EV Data CSV."""
    pass


def upload_to_s3(data: bytes, filename: str) -> None:
    """Upload raw bytes to the S3 landing zone."""
    pass


def main() -> None:
    pass


if __name__ == "__main__":
    main()
