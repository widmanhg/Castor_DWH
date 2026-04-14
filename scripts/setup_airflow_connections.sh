#!/bin/bash
# ============================================================
# Configura la conexión a PostgreSQL en Airflow
# Ejecutar DESPUÉS de que airflow-init haya terminado
# ============================================================

echo "⏳ Configurando conexión postgres_dwh en Airflow..."

docker exec castor-dwh-airflow-scheduler-1 airflow connections add postgres_dwh \
  --conn-type postgres \
  --conn-host postgres \
  --conn-schema dwh \
  --conn-login dwh_user \
  --conn-password dwh_password \
  --conn-port 5432 \
  2>/dev/null || \
docker exec castor-dwh-airflow-scheduler-1 airflow connections delete postgres_dwh && \
docker exec castor-dwh-airflow-scheduler-1 airflow connections add postgres_dwh \
  --conn-type postgres \
  --conn-host postgres \
  --conn-schema dwh \
  --conn-login dwh_user \
  --conn-password dwh_password \
  --conn-port 5432

echo "✅ Conexión postgres_dwh configurada"
echo ""
echo "Puedes verificarla en: http://localhost:8080 → Admin → Connections"