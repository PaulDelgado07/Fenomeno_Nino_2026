# Plataforma Big Data — Monitoreo El Niño 2026 (Guayaquil)

Sistema de monitoreo en tiempo real de riesgo de inundación, temperatura del mar (SST) y alertas SNGR para Guayaquil, Ecuador.

## Arquitectura

```
Productores Kafka  →  Spark Streaming  →  HDFS + PostgreSQL
                                              ↓
                                    FastAPI + Dashboard Web
                                              ↓
                                         Grafana
```

## Requisitos previos

1. **Docker Desktop** instalado y corriendo
2. **Python 3.10+** instalado en tu Mac
3. Conexión a internet (para datos NOAA, INOCAR y el mapa)

## Arranque rápido (un solo comando)

```bash
cd bigdata-el-nino-2026
chmod +x scripts/*.sh
./scripts/start.sh
```

Luego abre en tu navegador:

### **http://localhost:8000**

Ahí verás el dashboard completo: mapa con zonas de riesgo coloreadas, gráficos de SST y precipitación, alertas SNGR en vivo, y rutas de evacuación.

> Espera **1-2 minutos** después del arranque para que Spark procese los primeros datos y el mapa se coloree.

## Arranque paso a paso (manual)

Si prefieres encender cada pieza por separado:

### Paso 1 — Docker (infraestructura)

```bash
cd docker
docker compose up -d
```

Esto levanta: Kafka, Spark, HDFS, PostgreSQL y Grafana.

Verifica que todo esté arriba:

```bash
docker ps
```

Debes ver 7 contenedores: `namenode`, `datanode`, `gye_kafka`, `gye_postgres`, `gye_grafana`, `gye_spark_master`, `gye_spark_worker`.

### Paso 2 — Esperar servicios (~60 segundos)

```bash
# Postgres
docker exec gye_postgres pg_isready -U el_nino -d el_nino

# Kafka
docker exec gye_kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# HDFS
open http://localhost:9870

# Spark
open http://localhost:8080
```

### Paso 3 — Jobs Spark Streaming

```bash
SPARK_CMD="/opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client"

# Job 1: procesa temperatura del mar (SST)
docker exec -d gye_spark_master bash -lc \
  "$SPARK_CMD /workspace/spark/streaming/read_kafka.py"

# Job 2: calcula riesgo de inundación por zona
docker exec -d gye_spark_master bash -lc \
  "$SPARK_CMD /workspace/spark/streaming/calculate_risk.py"
```

### Paso 4 — Productores Kafka (datos en vivo)

```bash
cd kafka/producers
python3 -m venv ../../.venv
source ../../.venv/bin/activate
pip install -r ../../backend/requirements.txt

# Terminal A: datos SST (NOAA)
python simulator.py

# Terminal B: lluvia, marea, embalse y alertas SNGR
python risk_simulator.py
```

### Paso 5 — Backend + Dashboard

```bash
cd backend
source ../.venv/bin/activate
uvicorn main:app --host 0.0.0.0 --port 8000
```

Abre **http://localhost:8000** en tu navegador.

## URLs del sistema

| Servicio | URL | Credenciales |
|---|---|---|
| **Dashboard principal** | http://localhost:8000 | — |
| Grafana | http://localhost:3000 | admin / admin |
| Spark UI | http://localhost:8080 | — |
| HDFS NameNode | http://localhost:9870 | — |
| API REST | http://localhost:8000/api/status | — |
| PostgreSQL | localhost:5433 | el_nino / el_nino_2026 |

## Detener todo

```bash
./scripts/stop.sh
```

## Solución de problemas

### El dashboard se ve "simple" o sin datos

**Causa más común:** abriste `frontend/index.html` directamente como archivo (`file://...`). Eso no carga el mapa ni los datos.

**Solución:** siempre usa **http://localhost:8000** después de ejecutar `./scripts/start.sh`.

### El mapa no tiene colores de riesgo

1. Verifica que los productores Kafka estén corriendo: `cat logs/sst_producer.log`
2. Verifica que Spark esté procesando: `docker exec gye_spark_master cat /tmp/spark_risk.log`
3. Espera 1-2 minutos; Spark necesita crear las tablas en PostgreSQL primero.

### Error "Postgres/API no responde"

```bash
# Verificar que Postgres tiene datos
docker exec -it gye_postgres psql -U el_nino -d el_nino -c "SELECT count(*) FROM riesgo_zonas;"
```

Si la tabla no existe, Spark aún no ha procesado datos. Revisa los logs de Spark.

### Docker no arranca

Abre **Docker Desktop** primero, espera a que el ícono deje de parpadear, y vuelve a ejecutar `./scripts/start.sh`.

### Ver logs

```bash
tail -f logs/backend.log
tail -f logs/sst_producer.log
tail -f logs/risk_producer.log
docker exec gye_spark_master tail -f /tmp/spark_risk.log
```

## Estructura del proyecto

```
├── backend/          → API FastAPI + sirve el dashboard
├── frontend/         → Mapa Leaflet, gráficos, alertas
├── kafka/producers/  → Simuladores que envían datos a Kafka
├── spark/streaming/  → Jobs Spark (SST + riesgo de inundación)
├── docker/           → Docker Compose (toda la infraestructura)
├── scripts/          → start.sh y stop.sh
└── docs/             → Documentación del modelo de riesgo
```
