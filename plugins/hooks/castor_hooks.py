import logging
import os
from typing import Optional
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)


class CastorS3Hook(BaseHook):
    """
    Hook para extraer archivos CSV desde S3.
    Lee credenciales desde variables de entorno (inyectadas vía .env / docker-compose).
    """

    conn_name_attr = "aws_conn_id"
    default_conn_name = "aws_default"

    def __init__(self, aws_conn_id: str = "aws_default"):
        super().__init__()
        self.aws_conn_id = aws_conn_id
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = boto3.client(
                "s3",
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
            )
        return self._client

    def list_objects(self, bucket: str, prefix: str) -> list[dict]:
        """Lista objetos en S3 bajo un prefijo dado."""
        try:
            response = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            objects = response.get("Contents", [])
            logger.info(f"Encontrados {len(objects)} objetos en s3://{bucket}/{prefix}")
            return objects
        except NoCredentialsError:
            raise RuntimeError("Credenciales AWS no configuradas. Revisa el .env.")
        except ClientError as e:
            raise RuntimeError(f"Error S3: {e.response['Error']['Message']}")

    def download_file(self, bucket: str, key: str, local_path: str) -> str:
        """Descarga un objeto S3 a disco local. Retorna el path local."""
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        logger.info(f"Descargando s3://{bucket}/{key} → {local_path}")
        self.client.download_file(bucket, key, local_path)
        return local_path

    def download_files_for_date(
        self, bucket: str, prefix: str, logical_date: str, local_dir: str
    ) -> list[str]:
        """
        Descarga todos los archivos CSV de un prefijo para una fecha lógica.
        Espera estructura: s3://bucket/prefix/YYYY-MM-DD/*.csv
        """
        date_prefix = f"{prefix.rstrip('/')}/{logical_date}/"
        objects = self.list_objects(bucket, date_prefix)
        csv_objects = [o for o in objects if o["Key"].endswith(".csv")]

        local_paths = []
        for obj in csv_objects:
            filename = os.path.basename(obj["Key"])
            local_path = os.path.join(local_dir, filename)
            self.download_file(bucket, obj["Key"], local_path)
            local_paths.append(local_path)

        logger.info(f"Descargados {len(local_paths)} archivos para {logical_date}")
        return local_paths


class CastorMasterDataHook(BaseHook):
    """
    Hook para datos maestros (simula Oracle).
    Lee desde CSV local en /opt/airflow/data/raw/master_data.csv
    En producción: reemplazar get_connection() por OracleHook con credenciales en Airflow Connections.
    """

    def __init__(self, source_path: Optional[str] = None):
        super().__init__()
        self.source_path = source_path or "/opt/airflow/data/raw/master_data.csv"

    def read_master_data(self) -> list[dict]:
        """Lee datos maestros desde CSV local (simulación de Oracle)."""
        import csv

        if not os.path.exists(self.source_path):
            raise FileNotFoundError(
                f"Archivo de datos maestros no encontrado: {self.source_path}\n"
                "En producción este hook conectaría a Oracle via OracleHook."
            )

        records = []
        with open(self.source_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                records.append(dict(row))

        logger.info(f"Leídos {len(records)} registros maestros desde {self.source_path}")
        return records