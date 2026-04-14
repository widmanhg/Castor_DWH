-- ============================================================
-- CASTOR DWH - Inicialización de Esquemas (Medallion Architecture)
-- ============================================================

-- Medallion layers
CREATE SCHEMA IF NOT EXISTS bronze;   -- Raw / Staging
CREATE SCHEMA IF NOT EXISTS silver;   -- Clean / Normalized
CREATE SCHEMA IF NOT EXISTS gold;     -- Business aggregates

-- Pipeline observability
CREATE SCHEMA IF NOT EXISTS observability;

-- ── BRONZE: Datos crudos de S3 (telemetría de dispositivos) ──────
CREATE TABLE IF NOT EXISTS bronze.device_telemetry (
    _id                BIGSERIAL PRIMARY KEY,
    device_id          VARCHAR(100),
    event_timestamp    TIMESTAMP,
    metric_name        VARCHAR(200),
    metric_value       NUMERIC,
    raw_payload        JSONB,
    source_file        VARCHAR(500),
    ingested_at        TIMESTAMP DEFAULT NOW(),
    logical_date       DATE,          -- Partición lógica (ds de Airflow)
    batch_id           UUID
);

-- ── BRONZE: Datos maestros (simulando Oracle) ────────────────────
CREATE TABLE IF NOT EXISTS bronze.master_devices (
    _id                BIGSERIAL PRIMARY KEY,
    device_id          VARCHAR(100),
    device_type        VARCHAR(100),
    location           VARCHAR(200),
    owner_id           VARCHAR(100),
    active             BOOLEAN,
    source_file        VARCHAR(500),
    ingested_at        TIMESTAMP DEFAULT NOW(),
    logical_date       DATE,
    batch_id           UUID
);

-- ── SILVER: Telemetría limpia y normalizada ──────────────────────
CREATE TABLE IF NOT EXISTS silver.device_telemetry (
    device_id          VARCHAR(100)  NOT NULL,
    event_timestamp    TIMESTAMP     NOT NULL,
    metric_name        VARCHAR(200)  NOT NULL,
    metric_value       NUMERIC,
    logical_date       DATE          NOT NULL,
    batch_id           UUID,
    processed_at       TIMESTAMP DEFAULT NOW(),
    CONSTRAINT pk_silver_telemetry PRIMARY KEY (device_id, event_timestamp, metric_name)
);

-- ── SILVER: Dispositivos limpios ─────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.master_devices (
    device_id          VARCHAR(100)  NOT NULL PRIMARY KEY,
    device_type        VARCHAR(100),
    location           VARCHAR(200),
    owner_id           VARCHAR(100),
    active             BOOLEAN,
    logical_date       DATE,
    processed_at       TIMESTAMP DEFAULT NOW()
);

-- ── GOLD: Métricas diarias por dispositivo (agregado de negocio) ─
CREATE TABLE IF NOT EXISTS gold.daily_device_metrics (
    device_id          VARCHAR(100)  NOT NULL,
    metric_name        VARCHAR(200)  NOT NULL,
    report_date        DATE          NOT NULL,
    avg_value          NUMERIC,
    max_value          NUMERIC,
    min_value          NUMERIC,
    reading_count      INTEGER,
    device_type        VARCHAR(100),
    location           VARCHAR(200),
    refreshed_at       TIMESTAMP DEFAULT NOW(),
    CONSTRAINT pk_gold_daily PRIMARY KEY (device_id, metric_name, report_date)
);

-- ── OBSERVABILITY: Log de ejecuciones del pipeline ───────────────
CREATE TABLE IF NOT EXISTS observability.pipeline_runs (
    run_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dag_id             VARCHAR(200),
    task_id            VARCHAR(200),
    logical_date       DATE,
    started_at         TIMESTAMP,
    finished_at        TIMESTAMP,
    duration_seconds   NUMERIC,
    rows_processed     INTEGER DEFAULT 0,
    rows_rejected      INTEGER DEFAULT 0,
    status             VARCHAR(50),   -- SUCCESS | FAILED | PARTIAL
    error_message      TEXT,
    created_at         TIMESTAMP DEFAULT NOW()
);

-- Índices de performance
CREATE INDEX IF NOT EXISTS idx_bronze_telemetry_logical_date  ON bronze.device_telemetry(logical_date);
CREATE INDEX IF NOT EXISTS idx_bronze_telemetry_device_id     ON bronze.device_telemetry(device_id);
CREATE INDEX IF NOT EXISTS idx_silver_telemetry_logical_date  ON silver.device_telemetry(logical_date);
CREATE INDEX IF NOT EXISTS idx_gold_daily_report_date         ON gold.daily_device_metrics(report_date);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_dag_logical      ON observability.pipeline_runs(dag_id, logical_date);