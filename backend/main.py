from pathlib import Path
import logging

from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import get_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"

app = FastAPI(
    title="API de Monitoreo Fenómeno El Niño 2026",
    description=(
        "API REST que expone datos en tiempo real de inundaciones, SST y alertas en Guayaquil. "
        "Pipeline: Kafka (ingesta) → Spark Structured Streaming (procesamiento) → "
        "HDFS Parquet (almacenamiento) → PostgreSQL (serving) → Este API."
    ),
    version="2.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# Configuración de CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Catálogo de albergues seguros en cotas altas de Guayaquil
ALBERGUES = [
    {
        "id": 1,
        "name": "Polideportivo Samanes (Zona Norte)",
        "lat": -2.1054,
        "lon": -79.8950,
        "capacity": 300,
        "address": "Av. Francisco de Orellana, Parque Samanes",
        "safe_elevation_m": 9.0
    },
    {
        "id": 2,
        "name": "Coliseo Abel Jiménez Parra / U. de Guayaquil (Zona Suburbio/Centro)",
        "lat": -2.1850,
        "lon": -79.8970,
        "capacity": 500,
        "address": "Av. Delta y Av. Kennedy, Ciudadela Universitaria",
        "safe_elevation_m": 8.0
    },
    {
        "id": 3,
        "name": "Unidad Educativa Trinitaria / Coliseo Zona Alta (Zona Sur)",
        "lat": -2.2150,
        "lon": -79.9000,
        "capacity": 250,
        "address": "Isla Trinitaria, cooperativa cercana a la vía principal",
        "safe_elevation_m": 6.5
    }
]


@app.get("/api/status")
def api_status():
    """Estado general del sistema y listado de endpoints disponibles."""
    return {
        "project": "Plataforma de Monitoreo Fenómeno de El Niño 2026 - Guayaquil",
        "version": "2.0.0",
        "pipeline": "Kafka → Spark Structured Streaming → HDFS Parquet → PostgreSQL → FastAPI",
        "status": "Online",
        "endpoints": [
            "/api/riesgo/actual",
            "/api/sst/historico",
            "/api/alertas",
            "/api/albergues",
            "/api/enso/estado",
        ]
    }


@app.get("/api/albergues")
def get_albergues():
    """Retorna la lista de albergues seguros con su capacidad y coordenadas."""
    return ALBERGUES


@app.get("/api/riesgo/actual")
def get_riesgo_actual(db: Session = Depends(get_db)):
    """
    Retorna los datos de riesgo e hidrometeorológicos más recientes de cada zona,
    tal como fueron calculados por el job Spark Structured Streaming.
    Incluye: zone, lat, lon, elevation_m, precipitation_mm_h, tide_m,
             reservoir_pct, risk_index, risk_level.
    """
    try:
        result = db.execute(text("SELECT * FROM riesgo_zonas"))
        rows = result.fetchall()
        keys = result.keys()
        return [dict(zip(keys, row)) for row in rows]
    except Exception as e:
        logger.exception("Error al consultar riesgo_zonas: %s", e)
        return []


@app.get("/api/sst/historico")
def get_sst_historico(db: Session = Depends(get_db)):
    """
    Retorna los últimos 100 registros históricos de temperatura superficial del mar
    (región Niño 3.4), tal como los publicó el job Spark del topic sst-noaa.
    Campos: timestamp, value (°C), estado (Normal / Posible El Niño / Fría), source.
    """
    try:
        result = db.execute(
            text("SELECT * FROM sst_procesada ORDER BY timestamp DESC LIMIT 100")
        )
        rows = result.fetchall()
        keys = result.keys()
        return [dict(zip(keys, row)) for row in rows]
    except Exception as e:
        logger.exception("Error al consultar sst_procesada: %s", e)
        return []


@app.get("/api/alertas")
def get_alertas(db: Session = Depends(get_db)):
    """
    Retorna las últimas 20 alertas activas reportadas por la SNGR (simuladas
    en el producer risk_simulator.py basado en las condiciones hidrometeorológicas).
    Campos: event_time, zone, alert_level (Verde/Amarilla/Naranja/Roja), description.
    """
    try:
        result = db.execute(
            text("SELECT * FROM alertas_sngr ORDER BY event_time DESC LIMIT 20")
        )
        rows = result.fetchall()
        keys = result.keys()
        return [dict(zip(keys, row)) for row in rows]
    except Exception as e:
        logger.exception("Error al consultar alertas_sngr: %s", e)
        return []


@app.get("/api/enso/estado")
def get_enso_estado(db: Session = Depends(get_db)):
    """
    Retorna el estado ENSO más reciente para la barra KPI del dashboard.
    Lee el último registro de sst_procesada e infiere:
      - sst_c: temperatura superficial del mar (°C) de la región Niño 3.4
      - anomalia_c: diferencia respecto al umbral climatológico de 27.0°C
      - estado: Normal | Posible El Niño | La Niña
      - intensidad: calculada desde la anomalía
    """
    try:
        result = db.execute(
            text("SELECT value, estado, timestamp FROM sst_procesada ORDER BY timestamp DESC LIMIT 1")
        )
        row = result.fetchone()
        if not row:
            return {"sst_c": None, "estado": "Sin datos", "anomalia_c": None, "intensidad": "Sin datos"}

        sst    = float(row[0])
        estado = str(row[1])
        ts     = str(row[2])
        anom   = round(sst - 27.0, 2)

        if anom >= 2.0:
            intensidad = "El Niño Fuerte"
        elif anom >= 1.0:
            intensidad = "El Niño Moderado"
        elif anom >= 0.5:
            intensidad = "El Niño Débil"
        elif anom <= -0.5:
            intensidad = "La Niña"
        else:
            intensidad = "Condición Neutral"

        return {
            "sst_c": sst,
            "anomalia_c": anom,
            "estado": estado,
            "intensidad": intensidad,
            "timestamp": ts,
            "source": "NOAA CPC / Región Niño 3.4"
        }
    except Exception as e:
        logger.exception("Error al consultar estado ENSO: %s", e)
        return {"sst_c": None, "estado": "Error", "anomalia_c": None, "intensidad": "Sin datos"}


# Sirve el dashboard completo (HTML, CSS, JS, GeoJSON) desde el mismo puerto que la API.
app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
