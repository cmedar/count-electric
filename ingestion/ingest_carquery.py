"""
Fetches vehicle make/model/year metadata from the CarQuery API
and lands the raw JSON to s3://count-electric/landing/raw/carquery/.

Source: http://www.carqueryapi.com
Format: JSON API (no key required)
Cadence: Static (one-time or infrequent refresh)
"""

import os
import requests
import boto3
from dotenv import load_dotenv

load_dotenv()

CARQUERY_BASE_URL = "https://www.carqueryapi.com/api/0.3/"


def fetch_makes() -> dict:
    """Fetch all vehicle makes from CarQuery."""
    pass


def fetch_models(make: str) -> dict:
    """Fetch all models for a given make."""
    pass


def upload_to_s3(data: dict, filename: str) -> None:
    """Upload raw JSON to the S3 landing zone."""
    pass


def main() -> None:
    pass


if __name__ == "__main__":
    main()
