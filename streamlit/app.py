"""
Count Electric — Streamlit Dashboard

Entry point. Currently shows S3 landing zone contents for validation.
Full dashboard views will be connected to the Gold layer in Phase 4.
"""

import os

import boto3
import pandas as pd
import streamlit as st

S3_BUCKET = os.getenv("S3_BUCKET", "count-electric")

st.set_page_config(page_title="Count Electric", page_icon="⚡", layout="wide")
st.title("⚡ Count Electric")
st.caption("Tracking global EV adoption trends — by country, manufacturer, and year.")

st.header("S3 Landing Zone")

try:
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=S3_BUCKET)

    objects = response.get("Contents", [])

    if not objects:
        st.info("No files found in the bucket yet. Run an ingestion script to populate it.")
    else:
        rows = [
            {
                "Key": obj["Key"],
                "Size (KB)": round(obj["Size"] / 1024, 2),
                "Last Modified": obj["LastModified"].strftime("%Y-%m-%d %H:%M:%S"),
            }
            for obj in objects
        ]
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True)
        st.caption(f"{len(objects)} file(s) in s3://{S3_BUCKET}")

except Exception as e:
    st.error(f"Could not connect to S3: {e}")
