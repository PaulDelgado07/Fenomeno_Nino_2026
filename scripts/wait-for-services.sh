#!/usr/bin/env bash
# Espera a que Postgres, Kafka, HDFS y Spark estén listos antes de lanzar jobs.

set -euo pipefail

MAX_WAIT=120
ELAPSED=0

wait_for() {
  local name="$1"
  local cmd="$2"
  while ! eval "$cmd" >/dev/null 2>&1; do
    if [ "$ELAPSED" -ge "$MAX_WAIT" ]; then
      echo "  ✗ Timeout esperando $name (${MAX_WAIT}s)"
      return 1
    fi
    sleep 3
    ELAPSED=$((ELAPSED + 3))
    echo "  ... esperando $name (${ELAPSED}s)"
  done
  echo "  ✓ $name listo"
}

echo "Verificando servicios Docker..."

wait_for "Postgres" "docker exec gye_postgres pg_isready -U el_nino -d el_nino"
wait_for "Kafka" "docker exec gye_kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092"
wait_for "HDFS NameNode" "curl -sf http://localhost:9870"
wait_for "Spark Master" "curl -sf http://localhost:8080"

echo "Todos los servicios están listos."
