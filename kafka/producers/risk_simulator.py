"""Genera lluvia, marea y nivel de embalse para el escenario de riesgo en Guayaquil.

Zonas cubiertas (6 sectores):
  - Isla Trinitaria  (crítico, cota ~2m)
  - Suburbio Oeste   (alto, cota ~3m)
  - Daule            (alto, cota ~4.5m — regulado por embalse Daule-Peripa)
  - Sauces           (medio, cota ~5m)
  - Samborondón      (medio, cota ~3.5m — delta Daule-Babahoyo)
  - Samanes          (bajo, cota ~9m)

Topics Kafka publicados:
  - precip-gpm       → NASA GPM (simulado)
  - mareas-inocar    → INOCAR tablas de marea
  - nivel-embalse-celec → CELEC EP Daule-Peripa
  - alertas-sngr     → SNGR alertas provinciales
"""

import random
import time
from datetime import datetime

from producer import send_message
from data_sources import get_tide_m, get_precip_mm_h  

TOPICS = {
    "precipitation": "precip-gpm",
    "tide":          "mareas-inocar",
    "reservoir":     "nivel-embalse-celec",
    "sngr":          "alertas-sngr",
}

# Valores representativos para una demostración académica; no son mediciones oficiales.
# elevation_m y vulnerability son los parámetros base del modelo de riesgo en Spark.
ZONES = [
    {
        "zone":              "Isla Trinitaria",
        "lat":               -2.2360,
        "lon":               -79.9100,
        "elevation_m":       2.0,
        "vulnerability":     0.95,
        "rain_range":        (22.0, 58.0),   # mm/h — zona baja muy expuesta
        "risk_note":         "Cota crítica ≤ 2m, rodeada por Estero Salado"
    },
    {
        "zone":              "Suburbio Oeste",
        "lat":               -2.1990,
        "lon":               -79.9190,
        "elevation_m":       3.0,
        "vulnerability":     0.85,
        "rain_range":        (18.0, 52.0),
        "risk_note":         "Relleno de esteros, drenaje bloqueado por marea alta"
    },
    {
        "zone":              "Daule",
        "lat":               -1.8640,
        "lon":               -79.9800,
        "elevation_m":       4.5,
        "vulnerability":     0.75,
        "rain_range":        (10.0, 45.0),
        "risk_note":         "Ribera del Río Daule, riesgo aumenta con embalse > 90%"
    },
    {
        "zone":              "Sauces",
        "lat":               -2.1500,
        "lon":               -79.9000,
        "elevation_m":       5.0,
        "vulnerability":     0.65,
        "rain_range":        (8.0, 38.0),
        "risk_note":         "Cercanía al Río Daule, anegamiento por saturación pluvial"
    },
    {
        "zone":              "Samborondón",
        "lat":               -2.0890,
        "lon":               -79.8680,
        "elevation_m":       3.5,
        "vulnerability":     0.60,
        "rain_range":        (6.0, 35.0),
        "risk_note":         "Delta Daule-Babahoyo, vulnerable a crecidas combinadas"
    },
    {
        "zone":              "Samanes",
        "lat":               -2.1200,
        "lon":               -79.9060,
        "elevation_m":       9.0,
        "vulnerability":     0.35,
        "rain_range":        (3.0, 22.0),
        "risk_note":         "Cota alta, riesgo bajo excepto en canales obstruidos"
    },
]


def build_message(zone, source, variable, value, unit):
    return {
        "timestamp":         datetime.now().isoformat(),
        "source":            source,
        "variable":          variable,
        "value":             round(value, 2),
        "unit":              unit,
        "zone":              zone["zone"],
        "elevation_m":       zone["elevation_m"],
        "base_vulnerability": zone["vulnerability"],
        "location":          {"lat": zone["lat"], "lon": zone["lon"]},
    }


def build_sngr_message(zone, alert_level, description):
    return {
        "timestamp":  datetime.now().isoformat(),
        "source":     "SNGR (simulado)",
        "province":   "Guayas",
        "canton":     zone["zone"] if zone["zone"] == "Daule" else "Guayaquil",
        "zone":       zone["zone"],
        "alert_level": alert_level,
        "description": description,
        "location":   {"lat": zone["lat"], "lon": zone["lon"]},
    }


print("Simulador hidrometeorológico iniciado (6 zonas). Ctrl+C para detenerlo.\n")

while True:
    # Marea y nivel de embalse son globales al ciclo (misma condición para todos los sectores).
    tide_m        = get_tide_m()
    reservoir_pct = random.uniform(68.0, 97.0)

    for zone in ZONES:
        rain_mm_h = random.uniform(*zone["rain_range"])

        messages = [
            (TOPICS["precipitation"],
             build_message(zone, "NASA GPM (simulado)", "precipitation_mm_h", rain_mm_h, "mm/h")),
            (TOPICS["tide"],
             build_message(zone, "INOCAR (simulado)", "tide_m", tide_m, "m")),
            (TOPICS["reservoir"],
             build_message(zone, "CELEC Daule-Peripa (simulado)", "reservoir_pct", reservoir_pct, "%")),
        ]

        # Lógica de alertas SNGR basada en condiciones del ciclo
        sngr_msg = None
        if rain_mm_h >= 45.0 and tide_m >= 3.2:
            sngr_msg = build_sngr_message(
                zone, "Roja",
                f"Peligro extremo en {zone['zone']}: inundación inminente por coincidencia de "
                f"marea máxima ({tide_m:.2f}m) y lluvias torrenciales ({rain_mm_h:.1f} mm/h). "
                f"Nota técnica: {zone['risk_note']}."
            )
        elif rain_mm_h >= 30.0 or tide_m >= 2.8:
            sngr_msg = build_sngr_message(
                zone, "Naranja",
                f"Riesgo alto en {zone['zone']}: acumulación severa ({rain_mm_h:.1f} mm/h) y "
                f"problemas en drenes con marea de {tide_m:.2f}m. Embalse Daule-Peripa al {reservoir_pct:.0f}%."
            )
        elif rain_mm_h >= 15.0 or tide_m >= 2.2:
            sngr_msg = build_sngr_message(
                zone, "Amarilla",
                f"Atención en {zone['zone']}: calzadas húmedas y posible anegamiento preventivo. "
                f"Lluvia: {rain_mm_h:.1f} mm/h · Marea: {tide_m:.2f}m."
            )
        elif random.random() < 0.12:
            sngr_msg = build_sngr_message(
                zone, "Verde",
                f"Condiciones normales en {zone['zone']}: monitoreo preventivo activo. "
                f"Lluvia: {rain_mm_h:.1f} mm/h · Marea: {tide_m:.2f}m."
            )

        if sngr_msg:
            messages.append((TOPICS["sngr"], sngr_msg))

        for topic, message in messages:
            send_message(message, topic)

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Ciclo: marea={tide_m:.2f}m · "
        f"embalse={reservoir_pct:.1f}% · {len(ZONES)} zonas publicadas."
    )
    time.sleep(5)