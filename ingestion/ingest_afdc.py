"""
Fetches US EV registration data by state and model from the
US DOE Alternative Fuels Data Center (AFDC) API and lands
the raw JSON to s3://count-electric/landing/raw/afdc/.

Source: https://afdc.energy.gov/api
Format: JSON API (requires free API key)
Cadence: Annual
"""

import os
import requests
import boto3
from dotenv import load_dotenv

load_dotenv()

AFDC_BASE_URL = "https://developer.nrel.gov/api/alt-fuel-stations/v1"


def fetch_afdc_data() -> dict:
    """Call the AFDC API and return the raw JSON response."""
    pass


def upload_to_s3(data: dict, filename: str) -> None:
    """Upload raw JSON to the S3 landing zone."""
    pass


def main() -> None:
    pass


if __name__ == "__main__":
    main()
