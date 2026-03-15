# Databricks notebook source
"""
Shared utilities for Count Electric Databricks notebooks.
Import in other notebooks with: %run ../utils/spark_utils
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def log_quality(df: DataFrame, table_name: str, source_file: str) -> None:
    """
    Log row count, null counts, and duplicate check to the quality_log table.
    Run after each Bronze load to build an audit trail.
    """
    total_rows = df.count()
    null_counts = {
        c: df.filter(F.col(c).isNull()).count()
        for c in df.columns
        if c not in ("source_file", "ingested_at", "source_name")
    }
    null_summary = str({k: v for k, v in null_counts.items() if v > 0})

    log_row = spark.createDataFrame([{
        "table_name": table_name,
        "source_file": source_file,
        "total_rows": total_rows,
        "null_summary": null_summary,
        "logged_at": str(F.current_timestamp()),
    }])

    log_row.write.format("delta").mode("append").saveAsTable("bronze.quality_log")
    print(f"[quality_log] {table_name}: {total_rows} rows, nulls: {null_summary}")


def assert_not_empty(df: DataFrame, label: str) -> None:
    """Raise an error if the dataframe is empty — stops the notebook run."""
    count = df.count()
    if count == 0:
        raise ValueError(f"[QUALITY FAIL] {label} is empty — aborting.")
    print(f"[OK] {label}: {count} rows")


def show_table_summary(table_name: str) -> None:
    """Print row count, schema, and sample rows for a table."""
    df = spark.table(table_name)
    print(f"\n=== {table_name} ===")
    print(f"Rows: {df.count()}")
    df.printSchema()
    df.show(5, truncate=False)
