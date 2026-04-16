# Castor DWH — Senior Data Engineer Technical Assessment

Pipeline de telemetría de dispositivos sobre arquitectura Medallion (Bronze → Silver → Gold), orquestado con Apache Airflow y almacenado en PostgreSQL.

---

## Arquitectura

```
S3 (Telemetría CSV)   ──┐
                         ├──> Bronze (crudo) ──> DQ Validation ──> Silver (limpio) ──> Gold (negocio)
Oracle (Maestros CSV) ──┘
```

| Capa | Esquema | Descripción |
|------|---------|-------------|
| Bronze | `bronze.*` | Datos crudos tal como llegan de S3, particionados por fecha de ejecución |
| Silver | `silver.*` | Datos limpios y normalizados, cargados con UPSERT idempotente |
| Gold | `gold.*` | Agregados de negocio diarios, listos para consumo en Power BI |
| Observabilidad | `observability.*` | Registro de cada ejecución: inicio, fin, filas procesadas y rechazadas |

---

## Stack Tecnológico

| Componente | Tecnología |
|-----------|-----------|
| Orquestación | Apache Airflow 2.8.1 |
| Data Warehouse | PostgreSQL 15 |
| Fuente de telemetría | AWS S3 (archivos CSV por fecha) |
| Fuente de maestros | CSV local (simula Oracle en desarrollo) |
| Infraestructura | Docker Compose |

> Nota sobre Oracle: los datos maestros se simulan con `data/raw/master_data.csv`. En producción, `CastorMasterDataHook` se reemplaza por `OracleHook` de Airflow Providers sin modificar el DAG ni los operadores downstream.

---

## Estructura del Repositorio

```
castor-dwh/
├── dags/
│   └── castor_telemetry_dag.py          # DAG principal del pipeline
├── plugins/
│   ├── hooks/
│   │   └── castor_hooks.py              # CastorS3Hook, CastorMasterDataHook
│   └── operators/
│       └── castor_operators.py          # DataQualityOperator, BronzeLoaderOperator,
│                                        # SilverUpsertOperator, GoldRefreshOperator
├── sql/
│   └── init/
│       └── 01_init_schemas.sql          # Creación de esquemas y tablas
├── data/
│   └── raw/
│       ├── telemetry_sample.csv         # Datos de telemetría de ejemplo
│       └── master_data.csv              # Datos maestros (simulación de Oracle)
├── docs/
│   └── architecture.md                 # Diagrama de arquitectura en Mermaid
├── scripts/
│   ├── upload_sample_to_s3.py          # Sube datos de ejemplo al bucket S3
│   ├── setup_airflow_connections.sh    # Registra la conexión postgres_dwh en Airflow
│   └── reset_dwh.sql                   # Limpia todas las tablas (útil en desarrollo)
├── .env                                 # Variables de entorno (completar antes de iniciar)
├── docker-compose.yml
└── requirements.txt
```

---

## Requisitos Previos

- Docker >= 20.x
- Docker Compose >= 2.x
- Cuenta AWS con un bucket S3 creado y credenciales de acceso

```bash
docker --version
docker compose version
```

---

## Pasos para Levantar el Proyecto

### 1. Configurar variables de entorno

Abre el archivo `.env` y completa las credenciales AWS:

```env
AWS_ACCESS_KEY_ID=TU_ACCESS_KEY_AQUI
AWS_SECRET_ACCESS_KEY=TU_SECRET_KEY_AQUI
S3_BUCKET_NAME=TU_BUCKET_NAME_AQUI
```

El resto de variables (PostgreSQL, Airflow) ya están configuradas para funcionar con Docker Compose sin cambios.

### 2. Subir datos de ejemplo a S3

El pipeline espera encontrar archivos en S3 bajo la estructura `s3://BUCKET/telemetry/YYYY-MM-DD/`.

```bash
pip install boto3 python-dotenv

# Subir datos para la fecha de hoy
python scripts/upload_sample_to_s3.py --date $(date +%Y-%m-%d)

# Ver que subiria sin ejecutar
python scripts/upload_sample_to_s3.py --date $(date +%Y-%m-%d) --dry-run
```

### 3. Levantar la infraestructura

```bash
docker compose up -d

# Verificar que todos los servicios esten saludables
docker compose ps
```

Estado esperado:

```
NAME                              STATUS
castor-dwh-postgres-1             running (healthy)
castor-dwh-airflow-init-1         exited (0)          <- correcto, solo inicializa
castor-dwh-airflow-webserver-1    running (healthy)
castor-dwh-airflow-scheduler-1    running
```

### 4. Registrar la conexión a PostgreSQL en Airflow

```bash
chmod +x scripts/setup_airflow_connections.sh
./scripts/setup_airflow_connections.sh
```

### 5. Acceder a la interfaz de Airflow

```
URL:      http://localhost:8080
Usuario:  admin
Password: admin
```

### 6. Ejecutar el DAG

Desde la UI: activar el toggle del DAG `castor_device_telemetry_pipeline` y usar el botón Trigger DAG.

Desde terminal:

```bash
docker exec castor-dwh-airflow-scheduler-1 \
  airflow dags trigger castor_device_telemetry_pipeline
```

### 7. Verificar los datos en PostgreSQL

```bash
docker exec -it castor-dwh-postgres-1 psql -U dwh_user -d dwh
```

```sql
-- Conteo por capa
SELECT COUNT(*) FROM bronze.device_telemetry;
SELECT COUNT(*) FROM silver.device_telemetry;
SELECT COUNT(*) FROM gold.daily_device_metrics;

-- Agregados de negocio (capa Gold)
SELECT device_id, metric_name, report_date,
       ROUND(avg_value::numeric, 2) AS promedio,
       reading_count
FROM gold.daily_device_metrics
ORDER BY report_date DESC;

-- Log de observabilidad
SELECT dag_id, task_id, logical_date, status,
       rows_processed, rows_rejected,
       ROUND(duration_seconds::numeric, 1) AS segundos
FROM observability.pipeline_runs
ORDER BY started_at DESC
LIMIT 20;
```

---

## Caracteristicas Tecnicas

### Idempotencia

El pipeline puede ejecutarse N veces para la misma fecha sin duplicar registros. Cada capa usa una estrategia distinta:

| Capa | Estrategia |
|------|-----------|
| Bronze | `DELETE WHERE logical_date = fecha` seguido de INSERT bulk |
| Silver | `INSERT ... ON CONFLICT (device_id, event_timestamp, metric_name) DO UPDATE` |
| Gold | `DELETE WHERE report_date = fecha` seguido de INSERT con agregacion |

### Data Quality

`DataQualityOperator` valida los datos antes de permitir la carga a Silver. El pipeline falla explicitamente si mas del 5% de los registros tienen nulos en columnas criticas (`device_id`, `event_timestamp`, `metric_name`). Tambien detecta y rechaza registros huerfanos (device_id que no existe en los datos maestros). Las filas rechazadas quedan registradas en `observability.pipeline_runs`.

El umbral del 5% es configurable via la variable de entorno `DQ_NULL_THRESHOLD`.

### Evolucion de Esquema

`BronzeLoaderOperator` compara las columnas del archivo CSV entrante contra las columnas existentes en la tabla. Si detecta columnas nuevas, ejecuta `ALTER TABLE ADD COLUMN IF NOT EXISTS` automaticamente antes de la carga. El pipeline no se interrumpe ante cambios de esquema en la fuente.

### SLA y Alertas

El DAG tiene `sla_miss_callback` configurado. Si el proceso no termina antes de las 06:00 AM (el schedule corre a las 04:00 AM), se dispara el callback. Para conectarlo a Slack, PagerDuty o email corporativo, editar la funcion `sla_miss_callback` en el DAG.

### Observabilidad

Cada tarea registra una fila en `observability.pipeline_runs` con los siguientes campos: tiempo de inicio, tiempo de fin, duracion en segundos, filas procesadas correctamente, filas rechazadas por calidad y estado final (`SUCCESS`, `FAILED`, `PARTIAL`). Esto permite construir un dashboard de salud del pipeline sin depender de los logs de Airflow.

### Hooks intercambiables

`CastorS3Hook` y `CastorMasterDataHook` encapsulan la logica de conexion a las fuentes. Cambiar de S3 a Azure Blob, o de CSV local a Oracle, requiere unicamente reemplazar el hook sin tocar el DAG ni los operadores downstream.

---

## Decisiones de Diseno

**Truncate-Load en Bronze, UPSERT en Silver**

Bronze es la zona de aterrizaje cruda. Re-procesar la misma fecha con DELETE + INSERT es la operacion mas simple y predecible. Silver ya tiene una llave primaria compuesta, por lo que ON CONFLICT DO UPDATE es mas eficiente y preciso que un truncate.

**CSV local en lugar de Oracle real**

La prueba evalua el patron arquitectonico, no el acceso a infraestructura enterprise. `CastorMasterDataHook` es intercambiable por `OracleHook` sin modificar el DAG. El README lo documenta explicitamente para que quede claro en produccion.

**XCom para transferir registros entre DQ y Silver**

Para los volumenes de esta prueba (hasta 50K registros por batch diario) XCom es suficiente. En produccion con 50M registros mensuales, `DataQualityOperator` escribiria los registros validados a una tabla temporal en Postgres en lugar de XCom, para evitar el limite de tamano del metastore.

**Fecha logica basada en datetime.utcnow()**

Los operadores usan la fecha real de ejecucion en lugar de `context["ds"]` de Airflow. Esto garantiza que el pipeline siempre procesa el archivo del dia en que se corre, independientemente del schedule interno de Airflow.

---

## Troubleshooting

```bash
# Ver logs del scheduler
docker compose logs airflow-scheduler --tail=50

# Reiniciar un servicio sin bajar los demas
docker compose restart airflow-scheduler

# Limpiar las tablas sin bajar la infraestructura
docker exec -i castor-dwh-postgres-1 psql -U dwh_user -d dwh < scripts/reset_dwh.sql

# Bajar todo y empezar desde cero (borra volumenes)
docker compose down -v
docker compose up -d
```