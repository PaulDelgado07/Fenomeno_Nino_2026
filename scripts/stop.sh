#!/usr/bin/env bash
# Detiene productores, backend y (opcionalmente) los contenedores Docker.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOGS="$ROOT/logs"

echo "Deteniendo procesos locales..."

for name in backend sst_producer risk_producer; do
  pidfile="$LOGS/${name}.pid"
  if [ -f "$pidfile" ]; then
    pid=$(cat "$pidfile")
    if kill "$pid" 2>/dev/null; then
      echo "  ✓ $name detenido (PID $pid)"
    fi
    rm -f "$pidfile"
  fi
done

# Detener jobs Spark dentro del contenedor
docker exec gye_spark_master bash -lc 'pkill -f "read_kafka.py" 2>/dev/null || true; pkill -f "calculate_risk.py" 2>/dev/null || true' 2>/dev/null || true
echo "  ✓ Jobs Spark detenidos"

read -r -p "¿Detener también los contenedores Docker? [s/N] " answer
if [[ "$answer" =~ ^[sS]$ ]]; then
  docker compose -f "$ROOT/docker/docker-compose.yml" down
  echo "  ✓ Contenedores Docker detenidos"
else
  echo "  → Contenedores Docker siguen corriendo"
fi

echo "Listo."
