"""
DAG: castor_device_telemetry_pipeline
======================================
Pipeline principal de telemetría de dispositivos.

Flujo:
  S3 (bronze) → DQ Validation → Silver (UPSERT) → Gold (Aggregation)

Características:
  - Idempotente: re-ejecuciones para la misma fecha no duplican datos
  - Backfill compatible: usa {{ ds }} como partición lógica
  - Observabilidad: cada tarea registra métricas en observability.pipeline_runs
  - DQ: falla explícitamente si > 5% de nulos en columnas críticas
  - Schema evolution: columnas nuevas en S3 se agregan automáticamente
  - SLA: alerta si el pipeline no termina antes de las 06:00 AM
"""

import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email

# Custom hooks & operators
from plugins.hooks.castor_hooks import CastorS3Hook, CastorMasterDataHook
from plugins.operators.castor_operators import (
    DataQualityOperator,
    BronzeLoaderOperator,
    SilverUpsertOperator,
    GoldRefreshOperator,
    log_pipeline_run,
)

logger = logging.getLogger(__name__)

# ── Configuración ─────────────────────────────────────────────────────────────

S3_BUCKET = os.getenv("S3_BUCKET_NAME", "castor-telemetry-dev")
S3_PREFIX = os.getenv("S3_PREFIX", "telemetry/")
POSTGRES_CONN_ID = "postgres_dwh"
CRITICAL_COLUMNS = ["device_id", "event_timestamp", "metric_name"]
NULL_THRESHOLD = float(os.getenv("DQ_NULL_THRESHOLD", "0.05"))


# ── Callbacks ─────────────────────────────────────────────────────────────────

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Alerta de SLA: se dispara si el pipeline no termina a las 06:00 AM.
    En producción: integrar con PagerDuty, Slack o email corporativo.
    """
    msg = (
        f"⚠️ SLA MISS - DAG: {dag.dag_id}\n"
        f"Tareas bloqueadas: {[t.task_id for t in blocking_task_list]}\n"
        f"Fecha lógica: {slas[0].execution_date if slas else 'N/A'}\n"
        f"El proceso crítico de Power BI no terminó antes de las 06:00 AM."
    )
    logger.critical(msg)
    # Descomentar para enviar email real:
    # send_email(to=["data-team@castor.com"], subject="[CRITICAL] SLA Miss DWH", html_content=msg)


def on_failure_callback(context):
    """Log detallado en caso de fallo de cualquier tarea."""
    logger.error(
        f"[TASK FAILURE] dag={context['dag'].dag_id} "
        f"task={context['task'].task_id} "
        f"date={context['ds']} "
        f"error={context.get('exception', 'Unknown')}"
    )


# ── Default args ──────────────────────────────────────────────────────────────

default_args = {
    "owner": "castor-data-team",
    "depends_on_past": False,
    "email_on_failure": False,        # En prod: True + email real
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}

# ── DAG Definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="castor_device_telemetry_pipeline",
    description="Pipeline incremental S3 → Bronze → Silver → Gold para telemetría de dispositivos",
    default_args=default_args,
    schedule_interval="0 4 * * *",       # Corre a las 04:00 AM, debe terminar antes 06:00
    start_date=datetime(2024, 1, 1),     # ✅ Fecha fija para permitir backfill
    catchup=True,                         # ✅ Habilita backfill para fechas pasadas
    max_active_runs=1,
    tags=["castor", "telemetry", "dwh", "daily"],
    sla_miss_callback=sla_miss_callback,
    doc_md=__doc__,
) as dag:

    # ── Task 1: Extracción S3 → local ─────────────────────────────────────────
    def extract_from_s3(**context):
        """
        Descarga archivos CSV de S3 para la fecha lógica.
        Usa CastorS3Hook (intercambiable: Azure Blob, GCS, SFTP sin cambiar el DAG).
        
        ✅ CORRECCIÓN: Usa {{ ds }} (context['ds']) en lugar de datetime.utcnow()
        """
        logical_date = context['ds']  # ✅ Fecha lógica del DAG (formato YYYY-MM-DD)
        local_dir = f"/opt/airflow/data/raw/{logical_date}"
        started_at = datetime.utcnow()
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        try:
            hook = CastorS3Hook()
            local_paths = hook.download_files_for_date(
                bucket=S3_BUCKET,
                prefix=S3_PREFIX,
                logical_date=logical_date,
                local_dir=local_dir,
            )

            if not local_paths:
                raise FileNotFoundError(
                    f"No se encontraron archivos CSV en "
                    f"s3://{S3_BUCKET}/{S3_PREFIX}{logical_date}/. "
                    f"Verifica que el archivo exista en S3 para esta fecha."
                )

            logger.info(f"[S3] {len(local_paths)} archivo(s) descargados para {logical_date}")
            context["ti"].xcom_push(key="csv_paths", value=local_paths)
            log_pipeline_run(
                pg_hook, dag.dag_id, "extract_s3", logical_date,
                started_at, datetime.utcnow(), len(local_paths), 0, "SUCCESS"
            )
            return local_paths

        except Exception as e:
            log_pipeline_run(
                pg_hook, dag.dag_id, "extract_s3", logical_date,
                started_at, datetime.utcnow(), 0, 0, "FAILED", str(e)
            )
            raise

    extract_s3 = PythonOperator(
        task_id="extract_s3",
        python_callable=extract_from_s3,
        sla=timedelta(hours=1),           # Extracción debe terminar en < 1h
    )

    # ── Task 2: Cargar datos maestros (Oracle simulado) ───────────────────────
    def load_master_data(**context):
        """
        Carga datos maestros desde CSV local (simula Oracle).
        UPSERT a silver.master_devices.
        
        ✅ CORRECCIÓN: Usa {{ ds }} (context['ds'])
        """
        import psycopg2.extras
        logical_date = context['ds']  # ✅ Fecha lógica del DAG
        started_at = datetime.utcnow()
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        try:
            hook = CastorMasterDataHook()
            records = hook.read_master_data()

            upsert_sql = """
                INSERT INTO silver.master_devices
                    (device_id, device_type, location, owner_id, active, logical_date)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (device_id)
                DO UPDATE SET
                    device_type  = EXCLUDED.device_type,
                    location     = EXCLUDED.location,
                    owner_id     = EXCLUDED.owner_id,
                    active       = EXCLUDED.active,
                    logical_date = EXCLUDED.logical_date,
                    processed_at = NOW()
            """
            rows = [
                (
                    r.get("device_id"), r.get("device_type"),
                    r.get("location"), r.get("owner_id"),
                    r.get("active", "true").lower() == "true",
                    logical_date  # ✅ Usa fecha lógica del contexto
                )
                for r in records
            ]

            conn = pg_hook.get_conn()
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, upsert_sql, rows, page_size=500)
            conn.commit()

            logger.info(f"[MASTER] {len(rows)} dispositivos maestros cargados/actualizados")
            log_pipeline_run(
                pg_hook, dag.dag_id, "load_master_data", logical_date,
                started_at, datetime.utcnow(), len(rows), 0, "SUCCESS"
            )
            return len(rows)

        except Exception as e:
            log_pipeline_run(
                pg_hook, dag.dag_id, "load_master_data", logical_date,
                started_at, datetime.utcnow(), 0, 0, "FAILED", str(e)
            )
            raise

    load_masters = PythonOperator(
        task_id="load_master_data",
        python_callable=load_master_data,
    )

    # ── Task 3: Carga a Bronze ────────────────────────────────────────────────
    load_bronze = BronzeLoaderOperator(
        task_id="load_bronze",
        table="bronze.device_telemetry",
        extract_task_id="extract_s3",
        logical_date_col="logical_date",
        postgres_conn_id=POSTGRES_CONN_ID,
        sla=timedelta(hours=1),
    )

    # ── Task 4: Validación de calidad (DQ) ───────────────────────────────────
    validate_dq = DataQualityOperator(
        task_id="validate_dq",
        extract_task_id="extract_s3",
        critical_columns=CRITICAL_COLUMNS,
        null_threshold=NULL_THRESHOLD,
        postgres_conn_id=POSTGRES_CONN_ID,
        sla=timedelta(minutes=30),
    )

    # ── Task 5: UPSERT a Silver ───────────────────────────────────────────────
    load_silver = SilverUpsertOperator(
        task_id="load_silver",
        dq_task_id="validate_dq",
        postgres_conn_id=POSTGRES_CONN_ID,
        sla=timedelta(hours=1),
    )

    # ── Task 6: Agregación a Gold ─────────────────────────────────────────────
    refresh_gold = GoldRefreshOperator(
        task_id="refresh_gold",
        postgres_conn_id=POSTGRES_CONN_ID,
        sla=timedelta(minutes=30),
    )

    # ── Dependencias ──────────────────────────────────────────────────────────
    #
    #   extract_s3 ──┬──→ load_bronze ──→ validate_dq ──→ load_silver ──→ refresh_gold
    #                │
    #   load_masters ┘  (paralelo: maestros se cargan mientras descargamos S3)
    #
    [extract_s3, load_masters] >> load_bronze >> validate_dq >> load_silver >> refresh_gold