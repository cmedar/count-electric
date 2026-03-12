"""
Airflow DAG: count_electric_pipeline

Orchestrates the full Count Electric pipeline on a weekly cadence:
  1. Ingest raw data from all sources to S3 landing zone
  2. Load Bronze Delta tables from raw files
  3. Transform to Silver (clean & conform)
  4. Aggregate to Gold (market share, YoY growth, regional rollups)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "count-electric",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="count_electric_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    tags=["count-electric"],
) as dag:
    pass  # Tasks to be added in Phase 2
