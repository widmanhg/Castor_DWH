# Guia Tecnica — Castor DWH

Este documento describe en detalle las decisiones de arquitectura, patrones de implementacion y criterios de calidad aplicados en el pipeline de telemetria de dispositivos.

---

## 1. Arquitectura de Capas (Medallion)

El pipeline sigue el patron Medallion con tres capas separadas en esquemas de PostgreSQL. Cada capa tiene una responsabilidad clara y un criterio de calidad distinto.

### Bronze — Zona de aterrizaje cruda

Bronze recibe los datos exactamente como llegan de S3, sin transformaciones. La unica informacion que se agrega es metadata de trazabilidad: el archivo de origen (`source_file`), la fecha logica de la ejecucion (`logical_date` tomada de `context["ds"]`) y un identificador de batch (`batch_id` como UUID).

La estrategia de carga es truncate-load por particion: antes de cada carga se elimina todo el contenido de la fecha logica correspondiente y luego se inserta el nuevo batch. Esto garantiza idempotencia sin necesidad de llaves primarias en esta capa.

Bronze tambien soporta evolucion de esquema. Si el archivo CSV de S3 trae columnas nuevas que no existen en la tabla, el operador las agrega automaticamente con `ALTER TABLE ADD COLUMN IF NOT EXISTS` antes de iniciar la carga. El pipeline no falla ni requiere intervencion manual ante cambios de esquema en la fuente.

### Silver — Zona de datos limpios

Silver recibe unicamente los registros que pasaron las validaciones de calidad. La carga usa `INSERT ... ON CONFLICT DO UPDATE` (UPSERT) sobre la llave primaria compuesta `(device_id, event_timestamp, metric_name)`. Esto garantiza que re-ejecuciones para la misma fecha logica actualicen los registros existentes en lugar de duplicarlos.

Los registros huerfanos (device_id que no existe en `silver.master_devices`) se rechazan en la etapa de Data Quality y no llegan a esta capa. La diferencia de conteo entre Bronze y Silver representa exactamente las filas rechazadas.

### Gold — Zona de negocio

Gold contiene los agregados diarios por dispositivo y metrica: promedio, maximo, minimo y conteo de lecturas. Esta capa se recalcula completamente en cada ejecucion usando DELETE + INSERT por fecha de reporte. El join con `silver.master_devices` enriquece los datos con el tipo de dispositivo y la ubicacion, que son los atributos que necesita Power BI para sus dimensiones.

---

## 2. Estrategia de Carga Incremental con Logical Partitioning

El pipeline procesa 50 millones de registros mensuales usando particion logica por fecha de ejecucion de Airflow (`context["ds"]`). Este patron, conocido como Logical Partitioning, divide el volumen mensual en batches diarios de aproximadamente 1.6 millones de registros, lo que hace la carga manejable en terminos de memoria y tiempo de ejecucion.

La fecha logica `context["ds"]` es la clave de este patron. A diferencia de usar la fecha del sistema (`datetime.utcnow()`), `context["ds"]` es la fecha que Airflow asigna al run. Esto tiene dos consecuencias importantes: primero, el backfill funciona correctamente porque cada run historico usa su propia fecha logica. Segundo, si un run falla y se reintenta horas despues, la particion sigue siendo la del dia original, no la del momento del reintento.

La estructura en S3 sigue el mismo patron: `s3://bucket/telemetry/YYYY-MM-DD/archivo.csv`. Cuando el operador necesita los datos de una fecha logica, busca exactamente en ese prefijo. Si no encuentra archivos, el pipeline falla explicitamente con un mensaje descriptivo en lugar de continuar con datos vacios.

---

## 3. Idempotencia del Pipeline

Cada capa implementa su propia estrategia de idempotencia:

**Bronze** usa DELETE por `logical_date` seguido de INSERT. Es la estrategia mas simple y adecuada para una zona de aterrizaje donde no hay llaves primarias definidas.

**Silver** usa `ON CONFLICT DO UPDATE`. Cuando un registro ya existe con la misma llave primaria compuesta, se actualiza con los nuevos valores en lugar de fallar o duplicar. Esto permite re-procesar datos corregidos sin necesidad de limpiar la tabla manualmente.

**Gold** usa DELETE por `report_date` seguido de INSERT con agregacion. Como Gold es un calculo derivado de Silver, es mas limpio recalcularlo completamente que hacer UPSERT sobre agregados.

Para verificar idempotencia en cualquier entorno se puede usar el comando `airflow dags test`, que ejecuta todas las tareas del DAG para una fecha logica especifica sin registrar el run en el metastore de Airflow:

```bash
docker exec castor-dwh-airflow-scheduler-1 \
  airflow dags test castor_device_telemetry_pipeline 2024-01-05T04:00:00
```

Ejecutando este comando dos veces seguidas para la misma fecha y verificando que los COUNT en cada tabla no cambian se demuestra la idempotencia del pipeline.

---

## 4. Validacion de Calidad de Datos

`DataQualityOperator` se ejecuta despues de la carga a Bronze y antes de cualquier escritura en Silver. Implementa dos tipos de validacion:

**Validacion de nulos en columnas criticas**: cuenta los registros que tienen nulo o cadena vacia en `device_id`, `event_timestamp` o `metric_name`. Si el porcentaje supera el umbral configurado (5% por defecto, ajustable via `DQ_NULL_THRESHOLD`), el operador lanza una excepcion con el porcentaje exacto y las columnas afectadas. Airflow marca la tarea como fallida y ejecuta los reintentos configurados.

**Deteccion de registros huerfanos**: cruza los `device_id` del batch contra los registros activos en `silver.master_devices`. Un registro es huerfano si su `device_id` no existe en la tabla de maestros. Estos registros se rechazan silenciosamente (no detienen el pipeline) pero quedan contabilizados en `observability.pipeline_runs` como `rows_rejected`.

Los registros que pasan ambas validaciones se pasan al siguiente operador via XCom bajo la clave `valid_records`.

---

## 5. Observabilidad

Cada operador registra una fila en `observability.pipeline_runs` al completar su ejecucion, independientemente de si fue exitosa o no. Los campos registrados son:

- `dag_id` y `task_id`: identifican que tarea del pipeline genero el registro
- `logical_date`: la fecha logica del run (`context["ds"]`)
- `started_at` y `finished_at`: timestamps UTC de inicio y fin
- `duration_seconds`: duracion calculada como diferencia entre ambos timestamps
- `rows_processed`: filas cargadas exitosamente en la tabla destino
- `rows_rejected`: filas descartadas por validaciones de calidad
- `status`: `SUCCESS`, `FAILED` o `PARTIAL`
- `error_message`: mensaje de la excepcion en caso de fallo

Con esta tabla se puede construir un dashboard de salud del pipeline que responda preguntas como: cuanto tarda en promedio cada tarea, que porcentaje de filas se rechaza por fecha, o en que tareas ocurren la mayoria de los fallos.

---

## 6. SLA y Configuracion de Alertas

El DAG esta configurado para correr a las 04:00 AM con un SLA que requiere que el proceso completo termine antes de las 06:00 AM, para que los datos esten disponibles en Power BI al inicio de la jornada laboral.

La configuracion del SLA en Airflow funciona a nivel de tarea. Cada tarea tiene un parametro `sla` que define el tiempo maximo desde el inicio del DAG run hasta que esa tarea debe completarse. Si una tarea supera su SLA, Airflow ejecuta la funcion `sla_miss_callback` definida en el DAG.

La funcion `sla_miss_callback` actual registra el evento en los logs del scheduler. Para conectarla a un sistema de alertas en produccion se modifica unicamente esa funcion, sin tocar la logica del pipeline:

```python
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    # Opcion Slack
    requests.post(SLACK_WEBHOOK_URL, json={"text": f"SLA miss en {dag.dag_id}"})

    # Opcion PagerDuty
    pagerduty_client.create_incident(title=f"SLA miss en {dag.dag_id}")

    # Opcion email
    send_email(to=["data-team@castor.com"], subject="SLA Miss DWH", html_content=msg)
```

---

## 7. Guia de Code Review — Patrones a Evitar

### SELECT * en pipelines de datos

Un Data Engineer Jr. que usa `SELECT *` introduce varios problemas en un pipeline de produccion. Primero, si la tabla de origen agrega columnas, el pipeline puede romper el esquema de la tabla destino o cargar datos que no deberian estar ahi. Segundo, `SELECT *` impide que el motor de base de datos use indices de cobertura, lo que degrada el performance en tablas grandes. Tercero, hace el codigo ilegible porque no queda claro que columnas se estan usando.

La correccion es siempre explicitar las columnas:

```sql
-- Incorrecto
SELECT * FROM bronze.device_telemetry WHERE logical_date = '2024-01-05';

-- Correcto
SELECT device_id, event_timestamp, metric_name, metric_value, logical_date
FROM bronze.device_telemetry
WHERE logical_date = '2024-01-05';
```

### Cargar todos los datos en memoria de Airflow

Airflow es un orquestador, no un motor de procesamiento de datos. Cargar datasets completos en memoria dentro de una tarea de Airflow tiene dos consecuencias graves: el worker puede quedarse sin memoria y matar el proceso, y si el dataset crece, el problema escala sin ninguna señal de advertencia previa.

El patron correcto es usar el motor de base de datos para las transformaciones y mover solo metadata entre tareas:

```python
# Incorrecto: carga todo en memoria del worker de Airflow
df = pd.read_sql("SELECT * FROM bronze.device_telemetry", conn)
df_filtered = df[df["metric_value"] > 0]
df_filtered.to_sql("silver.device_telemetry", conn)

# Correcto: el motor de Postgres hace la transformacion
pg_hook.run("""
    INSERT INTO silver.device_telemetry (device_id, event_timestamp, metric_name, metric_value)
    SELECT device_id, event_timestamp, metric_name, metric_value
    FROM bronze.device_telemetry
    WHERE logical_date = %s
      AND metric_value > 0
    ON CONFLICT (device_id, event_timestamp, metric_name) DO UPDATE
    SET metric_value = EXCLUDED.metric_value
""", parameters=(logical_date,))
```

Para volumes muy grandes (50M+ registros) donde ni siquiera una query de Postgres es suficiente, el siguiente paso es Spark o dbt corriendo fuera del worker de Airflow, con Airflow actuando solo como disparador del job.

---

## 8. Comandos de Referencia

### Administracion

```bash
# Crear usuario administrador
docker exec -it castor-dwh-airflow-scheduler-1 airflow users create \
  --username admin --firstname admin --lastname admin \
  --role Admin --email admin@admin.com --password admin

# Ver estado de todos los servicios
docker compose ps

# Ver logs en tiempo real del scheduler
docker compose logs -f airflow-scheduler

# Reiniciar scheduler sin bajar la infraestructura
docker compose restart airflow-scheduler
```

### Ejecucion del Pipeline

```bash
# Trigger manual para una fecha especifica
docker exec castor-dwh-airflow-scheduler-1 \
  airflow dags trigger castor_device_telemetry_pipeline \
  -e 2024-01-05T04:00:00

# Backfill de un dia especifico
docker exec castor-dwh-airflow-scheduler-1 \
  airflow dags backfill castor_device_telemetry_pipeline \
  --start-date 2024-01-05T04:00:00 \
  --end-date 2024-01-05T04:00:00

# Probar idempotencia sin registrar en el metastore
docker exec castor-dwh-airflow-scheduler-1 \
  airflow dags test castor_device_telemetry_pipeline 2024-01-05T04:00:00
```

### Verificacion de Datos

```bash
# Conectarse a PostgreSQL
docker exec -it castor-dwh-postgres-1 psql -U dwh_user -d dwh
```

```sql
-- Bronze: registros por fecha de particion
SELECT logical_date, COUNT(*) AS total_registros
FROM bronze.device_telemetry
GROUP BY logical_date
ORDER BY logical_date DESC;

-- Comparacion Bronze vs Silver
SELECT 'bronze' AS capa, COUNT(*) AS registros
FROM bronze.device_telemetry
UNION ALL
SELECT 'silver', COUNT(*)
FROM silver.device_telemetry;

-- Gold: agregados listos para Power BI
SELECT device_id, metric_name, report_date,
       ROUND(avg_value::numeric, 2)  AS promedio,
       ROUND(max_value::numeric, 2)  AS maximo,
       ROUND(min_value::numeric, 2)  AS minimo,
       reading_count                 AS lecturas,
       device_type, location
FROM gold.daily_device_metrics
ORDER BY report_date DESC, device_id;

-- Observabilidad: ultimas 3 ejecuciones
SELECT dag_id, task_id, logical_date, status,
       rows_processed, rows_rejected,
       duration_seconds, started_at, finished_at,
       error_message
FROM observability.pipeline_runs
ORDER BY started_at DESC
LIMIT 3;

-- Limpiar todas las tablas (desarrollo)
-- Ejecutar desde terminal:
-- docker exec -i castor-dwh-postgres-1 psql -U dwh_user -d dwh < scripts/reset_dwh.sql
```