"""
data_sources.py

Capa de acceso a datos reales para el pipeline de riesgo.

Por qué existe este archivo separado:
•⁠ ⁠Los scrapers (INOCAR, NOAA, etc.) NO deben llamarse cada 5 segundos,
  porque las fuentes reales no publican con esa frecuencia (INOCAR es
  trimestral, NOAA es mensual). Si golpeas su servidor cada 5 segundos
  te van a banear la IP.
•⁠  ⁠Este módulo guarda el último valor obtenido en memoria (caché) y
  solo vuelve a llamar al scraper cuando pasó REFRESH_SECONDS.
•⁠  ⁠Si el scraping falla (sitio caído, cambio de estructura, sin
  internet), NO se cae el simulador: se reusa el último valor bueno
  conocido, o un valor de respaldo si nunca se logró obtener uno.

Agrega aquí una función get_xxx() por cada fuente real que integres.
"""

import time
import random
import requests
import pandas as pd
from io import StringIO

from scrape_inocar_mareas import descargar_pdf, extraer_texto, parsear_valores_marea

# Cada cuánto se permite volver a golpear la fuente real (segundos).
# INOCAR publica trimestralmente, pero usamos 1 hora como intervalo
# prudente de refresco dentro del propio trimestre vigente.
REFRESH_SECONDS = 60 * 60  # 1 hora

# NOAA CPC actualiza este archivo una vez al mes (el 5 de cada mes),
# así que basta con refrescarlo una vez al día.
NOAA_REFRESH_SECONDS = 60 * 60 * 24  # 24 horas
NOAA_ONI_URL = "https://www.cpc.ncep.noaa.gov/data/indices/oni.ascii.txt"
OPENMETEO_URL = "https://api.open-meteo.com/v1/forecast"
OPENMETEO_REFRESH_SECONDS = 60 * 15  # cada 15 minutos (intervalo real de la API)

_cache = {
    "tide_m": {"value": None, "last_fetch": 0},
    "sst_c": {"value": None, "last_fetch": 0},
}


def get_sst_c():
    """Devuelve la SST real más reciente del Pacífico (región Niño 3.4).

    Fuente: NOAA CPC, archivo oni.ascii.txt, columna TOTAL (temperatura
    absoluta en °C — NO usar la columna ANOM, que es una anomalía y no
    encaja con los umbrales de estado 'Fría/Normal/Posible El Niño').

    Nota: es la SST promedio de la región Niño 3.4 (Pacífico ecuatorial),
    no una medición puntual frente a la costa de Guayaquil — es la mejor
    aproximación real disponible para el indicador nacional/regional
    que pide la sección 5.1 del proyecto.
    """
    entry = _cache["sst_c"]
    ahora = time.time()

    necesita_refresh = (
        entry["value"] is None
        or (ahora - entry["last_fetch"]) > NOAA_REFRESH_SECONDS
    )

    if necesita_refresh:
        try:
            resp = requests.get(NOAA_ONI_URL, timeout=15)
            resp.raise_for_status()

            df = pd.read_csv(StringIO(resp.text), sep=r"\s+")
            ultimo = df.iloc[-1]

            entry["value"] = float(ultimo["TOTAL"])
            entry["last_fetch"] = ahora
            print(f"[data_sources] SST actualizada desde NOAA CPC: {entry['value']} °C ({ultimo['SEAS']} {int(ultimo['YR'])})")

        except Exception as e:
            print(f"⚠️ [data_sources] Falló el scraping de NOAA CPC ({e}). Se usa último valor conocido.")

    if entry["value"] is None:
        entry["value"] = round(random.uniform(27.5, 30.5), 2)

    return entry["value"]

_cache["precip_mm_h"] = {"value": None, "last_fetch": 0}

def get_precip_mm_h():
    """Precipitación real actual en Guayaquil desde Open-Meteo (sin API key)."""
    entry = _cache["precip_mm_h"]
    ahora = time.time()

    if entry["value"] is None or (ahora - entry["last_fetch"]) > OPENMETEO_REFRESH_SECONDS:
        try:
            resp = requests.get(OPENMETEO_URL, params={
                "latitude": -2.17,
                "longitude": -79.92,
                "current": "precipitation",
                "timezone": "America/Guayaquil"
            }, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            entry["value"] = float(data["current"]["precipitation"])
            entry["last_fetch"] = ahora
            print(f"[data_sources] Precipitación real Open-Meteo: {entry['value']} mm")
        except Exception as e:
            print(f"⚠️ [data_sources] Falló Open-Meteo ({e}). Usando último valor.")

    if entry["value"] is None:
        entry["value"] = round(random.uniform(0, 30), 2)

    return entry["value"]

def get_tide_m(year=None, quarter=None):
    """Devuelve la altura de marea más reciente disponible.

    Usa caché: solo vuelve a descargar el PDF de INOCAR si pasó
    REFRESH_SECONDS desde la última descarga exitosa. Si el scraping
    falla, devuelve el último valor cacheado; si nunca hubo uno,
    devuelve un valor de respaldo razonable para no frenar el pipeline.
    """
    if year is None or quarter is None:
        from datetime import datetime
        now = datetime.now()
        year = now.year
        quarter = (now.month - 1) // 3 + 1

    entry = _cache["tide_m"]
    ahora = time.time()

    necesita_refresh = (
        entry["value"] is None
        or (ahora - entry["last_fetch"]) > REFRESH_SECONDS
    )

    if necesita_refresh:
        try:
            pdf_bytes = descargar_pdf(year=year, quarter=quarter)
            texto = extraer_texto(pdf_bytes)
            mareas = parsear_valores_marea(texto)

            if mareas:
                entry["value"] = mareas[-1]["value"]
                entry["last_fetch"] = ahora
                print(f"[data_sources] Marea actualizada desde INOCAR: {entry['value']} m")
            else:
                print("[data_sources] PDF de INOCAR sin datos parseables, se mantiene valor previo.")

        except Exception as e:
            print(f"⚠️ [data_sources] Falló el scraping de INOCAR ({e}). Se usa último valor conocido.")

    if entry["value"] is None:
        # Nunca se logró obtener un dato real: usamos un valor plausible
        # para que el pipeline no se detenga por completo.
        entry["value"] = round(random.uniform(2.0, 3.8), 2)

    return entry["value"]