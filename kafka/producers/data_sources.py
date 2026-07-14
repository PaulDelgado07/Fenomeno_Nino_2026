"""Capa de acceso a datos reales para el pipeline de riesgo.

Por qué existe este archivo separado:
• Los scrapers (INOCAR, NOAA, etc.) NO deben llamarse cada 5 segundos,
  porque las fuentes reales no publican con esa frecuencia (INOCAR es
  trimestral, NOAA es mensual). Si golpeas su servidor cada 5 segundos
  te van a banear la IP.
• Este módulo guarda el último valor obtenido en memoria (caché) y
  solo vuelve a llamar al scraper cuando pasó REFRESH_SECONDS.
• Si el scraping falla (sitio caído, cambio de estructura, sin
  internet), NO se cae el simulador: se reusa el último valor bueno
  conocido, o un valor de respaldo histórico si nunca se logró obtener uno.
"""

import time
import requests
import pandas as pd
from io import StringIO

from scrape_inocar_mareas import descargar_pdf, extraer_lecturas, lectura_mas_cercana_a_ahora

# Intervalos de refresco para no saturar los servidores externos
REFRESH_SECONDS = 60 * 60  # 1 hora para INOCAR
NOAA_REFRESH_SECONDS = 60 * 60 * 24  # 24 horas para NOAA (se actualiza mensual)
NOAA_ONI_URL = "https://www.cpc.ncep.noaa.gov/data/indices/oni.ascii.txt"
OPENMETEO_URL = "https://api.open-meteo.com/v1/forecast"
OPENMETEO_REFRESH_SECONDS = 60 * 15  # 15 minutos para Open-Meteo

# CONSTANTES HISTÓRICAS REALES (Solo se usan si el servidor externo está caído y no hay caché)
FALLBACK_SST_C = 0.98          # Anomalía ONI histórica reciente (región Niño 3.4)
FALLBACK_PRECIP_MM_H = 0.0     # Lo normal es que no esté lloviendo (0.0 mm/h)
FALLBACK_TIDE_M = 2.50         # Nivel medio de marea astronómica en el Puerto de Guayaquil (metros)

_cache = {
    "tide_m": {"value": None, "last_fetch": 0},
    "sst_c": {"value": None, "last_fetch": 0},
    "precip_mm_h": {}  # una entrada por (lat, lon) redondeados, para lluvia por zona
}


def get_sst_c():
    """Devuelve la anomalía de SST más reciente del Pacífico (región Niño 3.4) desde NOAA CPC (índice ONI)."""
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

            entry["value"] = float(ultimo["ANOM"])
            entry["last_fetch"] = ahora
            print(f"[data_sources] Anomalía SST actualizada desde NOAA CPC: {entry['value']} °C ({ultimo['SEAS']} {int(ultimo['YR'])})")

        except Exception as e:
            print(f"⚠️ [data_sources] Falló el scraping de NOAA CPC ({e}). Se intentará usar caché o fallback histórico.")

    if entry["value"] is None:
        entry["value"] = FALLBACK_SST_C
        print(f"ℹ️ [data_sources] Sin conexión a NOAA. Usando respaldo histórico: {FALLBACK_SST_C} °C")

    return entry["value"]


def get_precip_mm_h(lat=-2.17, lon=-79.92):
    """Devuelve la precipitación real actual en el punto (lat, lon) indicado desde Open-Meteo.

    Cada zona tiene su propia entrada de caché (clave = coordenadas redondeadas),
    así cada una consulta su propio punto real en vez de compartir un único valor.
    """
    clave = (round(lat, 3), round(lon, 3))
    entry = _cache["precip_mm_h"].setdefault(clave, {"value": None, "last_fetch": 0})
    ahora = time.time()

    necesita_refresh = (
        entry["value"] is None
        or (ahora - entry["last_fetch"]) > OPENMETEO_REFRESH_SECONDS
    )

    if necesita_refresh:
        try:
            resp = requests.get(OPENMETEO_URL, params={
                "latitude": lat,
                "longitude": lon,
                "current": "precipitation",
                "timezone": "America/Guayaquil"
            }, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            entry["value"] = float(data["current"]["precipitation"])
            entry["last_fetch"] = ahora
            print(f"[data_sources] Precipitación real Open-Meteo ({clave}): {entry['value']} mm")
        except Exception as e:
            print(f"⚠️ [data_sources] Falló Open-Meteo ({e}). Se intentará usar caché o fallback histórico.")

    if entry["value"] is None:
        entry["value"] = FALLBACK_PRECIP_MM_H
        print(f"ℹ️ [data_sources] Sin conexión a Open-Meteo. Usando respaldo histórico: {FALLBACK_PRECIP_MM_H} mm/h")

    return entry["value"]


def get_tide_m(year=None, quarter=None):
    """Devuelve la altura de marea real más reciente de INOCAR."""
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
            lecturas = extraer_lecturas(pdf_bytes, year=year, quarter=quarter)
            mas_cercana = lectura_mas_cercana_a_ahora(lecturas)

            if mas_cercana is not None:
                entry["value"] = mas_cercana["value"]
                entry["last_fetch"] = ahora
                print(
                    f"[data_sources] Marea actualizada desde INOCAR: {entry['value']} m "
                    f"(predicción más cercana: {mas_cercana['datetime'].isoformat()})"
                )
            else:
                print("[data_sources] PDF de INOCAR leído pero sin datos parseables. Manteniendo valor previo.")

        except Exception as e:
            print(f"⚠️ [data_sources] Falló el scraping de INOCAR ({e}). Se intentará usar caché o fallback histórico.")

    if entry["value"] is None:
        entry["value"] = FALLBACK_TIDE_M
        print(f"ℹ️ [data_sources] Sin conexión a INOCAR. Usando respaldo histórico: {FALLBACK_TIDE_M} m")

    return entry["value"]