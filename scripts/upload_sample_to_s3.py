#!/usr/bin/env python3
"""
Script de setup: sube datos de ejemplo a S3 para que el pipeline los encuentre.
Ejecutar UNA VEZ después de configurar las credenciales AWS en el .env

Uso:
    python scripts/upload_sample_to_s3.py --date 2024-01-15
    python scripts/upload_sample_to_s3.py --date 2024-01-15 --date 2024-01-16
"""

import argparse
import os
from datetime import datetime
import sys
import boto3
from pathlib import Path

# Cargar .env manualmente si no está en el entorno
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass


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
            print(f"[DRY RUN] Subiría: s3://{bucket}/{key}")
            continue

        print(f"Subiendo → s3://{bucket}/{key} ... ", end="")
        client.upload_file(str(sample_path), bucket, key)
        print("✅")

    print(f"\nListo. Datos disponibles en s3://{bucket}/{prefix}/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sube datos de ejemplo a S3")
    parser.add_argument("--date", action="append", default=["2026-04-16"],
                        help="Fecha(s) a subir (YYYY-MM-DD). Repetir para múltiples fechas.")
    parser.add_argument("--dry-run", action="store_true", help="Solo muestra qué subiría")
    args = parser.parse_args()
    upload_sample(args.date, args.dry_run)