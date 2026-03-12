"""
Fetches new car registrations by fuel type for EU countries
from the EU Open Data Portal and lands the raw file to
s3://count-electric/landing/raw/eu_open_data/.

Source: https://data.europa.eu
Format: CSV / API
Cadence: Monthly
"""

import os
import boto3
from dotenv import load_dotenv

load_dotenv()


def fetch_eu_data() -> bytes:
    """Download EU car registration data."""
    pass


def upload_to_s3(data: bytes, filename: str) -> None:
    """Upload raw bytes to the S3 landing zone."""
    pass


def main() -> None:
    pass


if __name__ == "__main__":
    main()
