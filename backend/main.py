from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import get_db

app = FastAPI(
    title="API de Monitoreo Fenómeno El Niño 2026",
    description="API REST que expone datos en tiempo real de inundaciones, SST y alertas en Guayaquil.",
    version="1.0.0"
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


@app.get("/")
def read_root():
    return {
        "project": "Plataforma de Monitoreo Fenómeno de El Niño 2026 - Guayaquil",
        "status": "Online",
        "endpoints": [
            "/api/riesgo/actual",
            "/api/sst/historico",
            "/api/alertas",
            "/api/albergues"
        ]
    }


@app.get("/api/albergues")
def get_albergues():
    """Retorna la lista de albergues seguros con su capacidad y coordenadas."""
    return ALBERGUES


@app.get("/api/riesgo/actual")
def get_riesgo_actual(db: Session = Depends(get_db)):
    """Retorna los datos de riesgo e hidrometeorológicos más recientes de cada zona."""
    try:
        result = db.execute(text("SELECT * FROM riesgo_zonas"))
        rows = result.fetchall()
        keys = result.keys()
        return [dict(zip(keys, row)) for row in rows]
    except Exception as e:
        # Retornar una lista vacía si la tabla aún no ha sido creada por Spark
        return []


@app.get("/api/sst/historico")
def get_sst_historico(db: Session = Depends(get_db)):
    """Retorna los últimos 100 registros históricos de temperatura superficial del mar."""
    try:
        result = db.execute(text("SELECT * FROM sst_procesada ORDER BY timestamp DESC LIMIT 100"))
        rows = result.fetchall()
        keys = result.keys()
        return [dict(zip(keys, row)) for row in rows]
    except Exception as e:
        return []


@app.get("/api/alertas")
def get_alertas(db: Session = Depends(get_db)):
    """Retorna las últimas 20 alertas activas reportadas por la SNGR."""
    try:
        result = db.execute(text("SELECT * FROM alertas_sngr ORDER BY event_time DESC LIMIT 20"))
        rows = result.fetchall()
        keys = result.keys()
        return [dict(zip(keys, row)) for row in rows]
    except Exception as e:
        return []
