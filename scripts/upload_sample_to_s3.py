#!/usr/bin/env python3
"""
Script de setup: sube datos de ejemplo a S3 para un rango de fechas.
"""

import argparse
import os
from datetime import datetime, timedelta
import sys
import boto3
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass


def generate_date_range(start_date: str, end_date: str) -> list[str]:
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    if start > end:
        raise ValueError("start_date no puede ser mayor que end_date")

    dates = []
    current = start

    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    return dates


def upload_sample(dates: list[str], dry_run: bool = False):
    bucket = os.getenv("S3_BUCKET_NAME")
    prefix = os.getenv("S3_PREFIX", "telemetry/").rstrip("/")

    if not bucket:
        print("❌ ERROR: S3_BUCKET_NAME no configurado en .env")
        sys.exit(1)

    sample_path = Path(__file__).parent.parent / "data" / "raw" / "telemetry_sample.csv"
    if not sample_path.exists():
        print(f"❌ ERROR: No existe {sample_path}")
        sys.exit(1)

    client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )

    for date in dates:
        key = f"{prefix}/{date}/telemetry_{date.replace('-', '')}.csv"

        if dry_run:
            print(f"[DRY RUN] s3://{bucket}/{key}")
            continue

        print(f"Subiendo → s3://{bucket}/{key}")
        client.upload_file(str(sample_path), bucket, key)

    print("Listo.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sube datos de ejemplo a S3")

    parser.add_argument("--start-date", required=True, help="Fecha inicio YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="Fecha fin YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    dates = generate_date_range(args.start_date, args.end_date)
    upload_sample(dates, args.dry_run)