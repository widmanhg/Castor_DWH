"""
Custom Operators - Castor DWH
Operadores reutilizables para validación de calidad de datos y carga a Postgres.
"""

import csv
import logging
import uuid
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extras
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


# ── Utilidad: Log de observabilidad ──────────────────────────────────────────

def log_pipeline_run(
    pg_hook: PostgresHook,
    dag_id: str,
    task_id: str,
    logical_date: str,
    started_at: datetime,
    finished_at: datetime,
    rows_processed: int,
    rows_rejected: int,
    status: str,
    error_message: Optional[str] = None,
):
    """Inserta un registro en observability.pipeline_runs."""
    duration = (finished_at - started_at).total_seconds()
    sql = """
        INSERT INTO observability.pipeline_runs
            (dag_id, task_id, logical_date, started_at, finished_at,
             duration_seconds, rows_processed, rows_rejected, status, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    pg_hook.run(sql, parameters=(
        dag_id, task_id, logical_date, started_at, finished_at,
        duration, rows_processed, rows_rejected, status, error_message
    ))
    logger.info(
        f"[OBSERVABILITY] dag={dag_id} task={task_id} date={logical_date} "
        f"status={status} rows_ok={rows_processed} rows_rejected={rows_rejected} "
        f"duration={duration:.1f}s"
    )


# ── Operator 1: Data Quality Validator ───────────────────────────────────────

class DataQualityOperator(BaseOperator):


    def __init__(
        self,
        critical_columns: list[str],
        extract_task_id: str = "extract_s3",
        null_threshold: float = 0.05,
        postgres_conn_id: str = "postgres_dwh",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.extract_task_id = extract_task_id
        self.critical_columns = critical_columns
        self.null_threshold = null_threshold
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        started_at = datetime.utcnow()
        logical_date = context['ds']  # ✅ Usa {{ ds }} del contexto de Airflow
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        csv_paths = context["ti"].xcom_pull(task_ids=self.extract_task_id, key="csv_paths") or []
        all_records = []
        for path in csv_paths:
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    all_records.append(row)

        total = len(all_records)
        logger.info(f"[DQ] Total registros leídos: {total}")

        if total == 0:
            raise ValueError("No se encontraron registros para procesar.")

        # ── Validación 1: Nulos en columnas críticas ──────────────────────
        null_count = sum(
            1 for r in all_records
            if any(not r.get(col) or r.get(col, "").strip() == "" for col in self.critical_columns)
        )
        null_ratio = null_count / total
        logger.info(f"[DQ] Registros con nulos críticos: {null_count} ({null_ratio:.2%})")

        if null_ratio > self.null_threshold:
            msg = (
                f"FALLO DQ: {null_ratio:.2%} de registros tienen nulos en columnas críticas "
                f"{self.critical_columns}. Umbral: {self.null_threshold:.0%}"
            )
            log_pipeline_run(
                pg_hook, context["dag"].dag_id, self.task_id, logical_date,
                started_at, datetime.utcnow(), 0, null_count, "FAILED", msg
            )
            raise ValueError(msg)

        # ── Validación 2: Registros huérfanos ─────────────────────────────
        known_devices = set(
            row[0] for row in pg_hook.get_records(
                "SELECT device_id FROM silver.master_devices"
            )
        )

        valid_records = []
        orphan_count = 0
        for r in all_records:
            device_id = r.get("device_id", "").strip()
            is_null_critical = any(
                not r.get(col) or r.get(col, "").strip() == "" for col in self.critical_columns
            )
            is_orphan = bool(known_devices) and device_id not in known_devices

            if is_null_critical or is_orphan:
                orphan_count += 1
                logger.debug(f"[DQ] Registro rechazado: device_id={device_id}")
            else:
                valid_records.append(r)

        logger.info(
            f"[DQ] Registros válidos: {len(valid_records)} | Rechazados: {orphan_count}"
        )

        log_pipeline_run(
            pg_hook, context["dag"].dag_id, self.task_id, logical_date,
            started_at, datetime.utcnow(), len(valid_records), orphan_count, "SUCCESS"
        )

        # Pasar datos válidos al siguiente operador via XCom
        context["ti"].xcom_push(key="valid_records", value=valid_records)
        context["ti"].xcom_push(key="rows_rejected", value=orphan_count)
        return len(valid_records)


# ── Operator 2: Bronze Loader ─────────────────────────────────────────────────

class BronzeLoaderOperator(BaseOperator):
    """
    Carga datos crudos a bronze con UPSERT (idempotente).
    Soporta evolución de esquema: columnas nuevas se agregan automáticamente.

    Lee csv_paths desde XCom de la tarea extract_s3 (no usa Jinja templates
    porque los operadores custom no los renderizan automáticamente).
    """

    def __init__(
        self,
        table: str,
        extract_task_id: str = "extract_s3",
        logical_date_col: str = "logical_date",
        postgres_conn_id: str = "postgres_dwh",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.table = table
        self.extract_task_id = extract_task_id
        self.logical_date_col = logical_date_col
        self.postgres_conn_id = postgres_conn_id

    def _ensure_columns_exist(self, conn, columns: list[str]):
        """Evolución de esquema: agrega columnas nuevas sin romper el pipeline."""
        schema, table = self.table.split(".")
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                """,
                (schema, table),
            )
            existing = {row[0] for row in cur.fetchall()}

        new_cols = [c for c in columns if c not in existing and not c.startswith("_")]
        for col in new_cols:
            logger.warning(f"[SCHEMA EVOLUTION] Agregando columna nueva: {self.table}.{col}")
            with conn.cursor() as cur:
                cur.execute(f'ALTER TABLE {self.table} ADD COLUMN IF NOT EXISTS "{col}" TEXT')
            conn.commit()

    def execute(self, context):
        started_at = datetime.utcnow()
        logical_date = context['ds'] 
        batch_id = str(uuid.uuid4())
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        # Leer paths desde XCom (evita problema de Jinja templates en operadores custom)
        csv_paths = context["ti"].xcom_pull(task_ids=self.extract_task_id, key="csv_paths") or []
        if not csv_paths:
            logger.info("[BRONZE] No hay archivos para procesar.")
            return 0

        all_records = []
        for path in csv_paths:
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    row["source_file"] = path
                    row["logical_date"] = logical_date 
                    row["batch_id"] = batch_id
                    all_records.append(row)

        if not all_records:
            logger.info("[BRONZE] No hay registros para cargar.")
            return 0

        conn = pg_hook.get_conn()

        # ── Evolución de esquema ──────────────────────────────────────────
        self._ensure_columns_exist(conn, list(all_records[0].keys()))

        # ── Truncate-load por partición lógica (idempotente) ─────────────
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {self.table} WHERE {self.logical_date_col} = %s",
                (logical_date,),
            )
            deleted = cur.rowcount
            logger.info(f"[BRONZE] Eliminados {deleted} registros anteriores para {logical_date}")

        # ── Bulk insert ───────────────────────────────────────────────────
        cols = [k for k in all_records[0].keys() if not k.startswith("_id")]
        col_str = ", ".join(f'"{c}"' for c in cols)
        val_str = ", ".join(["%s"] * len(cols))
        insert_sql = f"INSERT INTO {self.table} ({col_str}) VALUES ({val_str})"

        rows = [[r.get(c) for c in cols] for r in all_records]
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=1000)
        conn.commit()

        logger.info(f"[BRONZE] Insertados {len(rows)} registros en {self.table}")

        log_pipeline_run(
            pg_hook, context["dag"].dag_id, self.task_id, logical_date,
            started_at, datetime.utcnow(), len(rows), 0, "SUCCESS"
        )
        return len(rows)


# ── Operator 3: Silver Upsert ─────────────────────────────────────────────────

class SilverUpsertOperator(BaseOperator):
    """
    UPSERT de bronze → silver (idempotente, N re-ejecuciones sin duplicados).
    Toma registros validados desde XCom del DataQualityOperator.
    """

    def __init__(
        self,
        dq_task_id: str,
        postgres_conn_id: str = "postgres_dwh",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.dq_task_id = dq_task_id
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        started_at = datetime.utcnow()
        logical_date = context['ds'] 
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        valid_records = context["ti"].xcom_pull(
            task_ids=self.dq_task_id, key="valid_records"
        )

        if not valid_records:
            logger.info("[SILVER] No hay registros válidos para cargar.")
            return 0

        upsert_sql = """
            INSERT INTO silver.device_telemetry
                (device_id, event_timestamp, metric_name, metric_value, logical_date, batch_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (device_id, event_timestamp, metric_name)
            DO UPDATE SET
                metric_value  = EXCLUDED.metric_value,
                logical_date  = EXCLUDED.logical_date,
                batch_id      = EXCLUDED.batch_id,
                processed_at  = NOW()
        """

        rows = [
            (
                r.get("device_id"),
                r.get("event_timestamp"),
                r.get("metric_name"),
                r.get("metric_value"),
                logical_date,  
                r.get("batch_id", str(uuid.uuid4())),
            )
            for r in valid_records
        ]

        conn = pg_hook.get_conn()
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, upsert_sql, rows, page_size=1000)
        conn.commit()

        logger.info(f"[SILVER] UPSERT completado: {len(rows)} registros en silver.device_telemetry")

        log_pipeline_run(
            pg_hook, context["dag"].dag_id, self.task_id, logical_date,
            started_at, datetime.utcnow(), len(rows), 0, "SUCCESS"
        )
        return len(rows)


# ── Operator 4: Gold Refresh ──────────────────────────────────────────────────

class GoldRefreshOperator(BaseOperator):
    """
    Agrega métricas diarias de silver → gold (DELETE + INSERT por fecha, idempotente).
    """


    def __init__(self, postgres_conn_id: str = "postgres_dwh", **kwargs):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        started_at = datetime.utcnow()
        logical_date = context['ds']  
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        # Idempotente: elimina el día antes de recalcular
        pg_hook.run(
            "DELETE FROM gold.daily_device_metrics WHERE report_date = %s",
            parameters=(logical_date,)
        )

        gold_sql = """
            INSERT INTO gold.daily_device_metrics
                (device_id, metric_name, report_date, avg_value, max_value,
                 min_value, reading_count, device_type, location)
            SELECT
                t.device_id,
                t.metric_name,
                t.logical_date                    AS report_date,
                AVG(t.metric_value)               AS avg_value,
                MAX(t.metric_value)               AS max_value,
                MIN(t.metric_value)               AS min_value,
                COUNT(*)                          AS reading_count,
                d.device_type,
                d.location
            FROM silver.device_telemetry t
            LEFT JOIN silver.master_devices d USING (device_id)
            WHERE t.logical_date = %s
            GROUP BY t.device_id, t.metric_name, t.logical_date, d.device_type, d.location
        """
        pg_hook.run(gold_sql, parameters=(logical_date,))

        rows = pg_hook.get_first(
            "SELECT COUNT(*) FROM gold.daily_device_metrics WHERE report_date = %s",
            parameters=(logical_date,)
        )[0]

        logger.info(f"[GOLD] {rows} métricas agregadas en gold.daily_device_metrics para {logical_date}")

        log_pipeline_run(
            pg_hook, context["dag"].dag_id, self.task_id, logical_date,
            started_at, datetime.utcnow(), rows, 0, "SUCCESS"
        )
        return rows