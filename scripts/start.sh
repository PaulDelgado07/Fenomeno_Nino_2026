#!/usr/bin/env bash
# Arranca todo el pipeline: Docker → Spark → Kafka producers → Backend + Dashboard

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOGS="$ROOT/logs"
mkdir -p "$LOGS"

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║   EL NIÑO 2026 — Arranque del Sistema Big Data           ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# ── 0. Verificar Docker ──────────────────────────────────────
if ! docker info >/dev/null 2>&1; then
  echo "❌ Docker no está corriendo."
  echo "   Abre Docker Desktop y vuelve a ejecutar: ./scripts/start.sh"
  exit 1
fi
echo "✓ Docker activo"

# ── 1. Entorno Python ────────────────────────────────────────
if [ ! -d "$ROOT/.venv" ]; then
  echo "→ Creando entorno virtual Python..."
  python3 -m venv "$ROOT/.venv"
fi
# shellcheck disable=SC1091
source "$ROOT/.venv/bin/activate"
pip install -q -r "$ROOT/backend/requirements.txt"
echo "✓ Dependencias Python instaladas"

# ── 2. Infraestructura Docker ────────────────────────────────
echo ""
echo "→ Levantando contenedores (Kafka, Spark, HDFS, Postgres, Grafana)..."
docker compose -f "$ROOT/docker/docker-compose.yml" up -d

echo "→ Esperando que los servicios estén listos..."
bash "$ROOT/scripts/wait-for-services.sh"

# ── 3. Jobs Spark Streaming ──────────────────────────────────
echo ""
echo "→ Iniciando Spark Streaming (SST + Riesgo de inundación)..."

SPARK_CMD="/opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client"

docker exec -d gye_spark_master bash -lc \
  "$SPARK_CMD /workspace/spark/streaming/read_kafka.py > /tmp/spark_sst.log 2>&1"

docker exec -d gye_spark_master bash -lc \
  "$SPARK_CMD /workspace/spark/streaming/calculate_risk.py > /tmp/spark_risk.log 2>&1"

echo "✓ Jobs Spark lanzados (logs en contenedor: /tmp/spark_*.log)"

# ── 4. Productores Kafka ─────────────────────────────────────
echo ""
echo "→ Iniciando productores Kafka..."

# Detener productores previos si existen
for pidfile in "$LOGS"/sst_producer.pid "$LOGS"/risk_producer.pid; do
  if [ -f "$pidfile" ]; then
    kill "$(cat "$pidfile")" 2>/dev/null || true
    rm -f "$pidfile"
  fi
done

cd "$ROOT/kafka/producers"
PYTHONUNBUFFERED=1 nohup python -u simulator.py       > "$LOGS/sst_producer.log"  2>&1 &
echo $! > "$LOGS/sst_producer.pid"
PYTHONUNBUFFERED=1 nohup python -u risk_simulator.py  > "$LOGS/risk_producer.log" 2>&1 &
echo $! > "$LOGS/risk_producer.pid"
echo "✓ Productores Kafka activos (logs en logs/)"

# ── 5. Backend + Dashboard ───────────────────────────────────
echo ""
echo "→ Iniciando API y Dashboard..."

if [ -f "$LOGS/backend.pid" ]; then
  kill "$(cat "$LOGS/backend.pid")" 2>/dev/null || true
  rm -f "$LOGS/backend.pid"
fi

cd "$ROOT/backend"
nohup uvicorn main:app --host 0.0.0.0 --port 8000 > "$LOGS/backend.log" 2>&1 &
echo $! > "$LOGS/backend.pid"
sleep 2
if ! curl -sf http://localhost:8000/api/status >/dev/null; then
  echo "⚠️  El backend no respondió. Revisa logs/backend.log"
else
  echo "✓ Backend activo en puerto 8000"
fi

# ── Listo ────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║                    SISTEMA INICIADO                      ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║                                                          ║"
echo "║  🌐 DASHBOARD (abre esto en tu navegador):               ║"
echo "║     http://localhost:8000                                ║"
echo "║                                                          ║"
echo "║  📊 Grafana:     http://localhost:3000  (admin / admin)  ║"
echo "║  ⚡ Spark UI:    http://localhost:8080                   ║"
echo "║  🗄  HDFS:        http://localhost:9870                   ║"
echo "║                                                          ║"
echo "║  ⏳ Espera 1-2 minutos para que aparezcan los datos      ║"
echo "║     (mapa coloreado, gráficos, alertas SNGR).            ║"
echo "║                                                          ║"
echo "║  Para detener todo:  ./scripts/stop.sh                   ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
