"""Genera lluvia, marea y nivel de embalse para el escenario de riesgo en Guayaquil."""

import random
import time
from datetime import datetime

from producer import send_message
from data_sources import get_tide_m


TOPICS = {
    "precipitation": "precip-gpm",
    "tide": "mareas-inocar",
    "reservoir": "nivel-embalse-celec",
    "sngr": "alertas-sngr",
}

# Valores representativos para una demostración académica; no son mediciones oficiales.
ZONES = [
    {"zone": "Isla Trinitaria", "lat": -2.2360, "lon": -79.9100, "elevation_m": 2.0, "vulnerability": 0.95},
    {"zone": "Suburbio Oeste", "lat": -2.1990, "lon": -79.9190, "elevation_m": 3.0, "vulnerability": 0.85},
    {"zone": "Sauces", "lat": -2.1500, "lon": -79.9000, "elevation_m": 5.0, "vulnerability": 0.65},
    {"zone": "Samanes", "lat": -2.1200, "lon": -79.9000, "elevation_m": 9.0, "vulnerability": 0.35},
]


def build_message(zone, source, variable, value, unit):
    return {
        "timestamp": datetime.now().isoformat(),
        "source": source,
        "variable": variable,
        "value": round(value, 2),
        "unit": unit,
        "zone": zone["zone"],
        "elevation_m": zone["elevation_m"],
        "base_vulnerability": zone["vulnerability"],
        "location": {"lat": zone["lat"], "lon": zone["lon"]},
    }


def build_sngr_message(zone, alert_level, description):
    return {
        "timestamp": datetime.now().isoformat(),
        "source": "SNGR (simulado)",
        "province": "Guayas",
        "canton": "Guayaquil",
        "zone": zone["zone"],
        "alert_level": alert_level,
        "description": description,
        "location": {"lat": zone["lat"], "lon": zone["lon"]},
    }


print("Simulador hidrometeorológico iniciado. Ctrl+C para detenerlo.\n")

while True:
    # El mismo escenario de marea y embalse aplica a todos los sectores del ciclo.
    tide_m = get_tide_m()
    reservoir_pct = random.uniform(72.0, 96.0)

    for zone in ZONES:
        # Las zonas bajas reciben lluvia algo más intensa para mostrar el escenario crítico.
        rain_mm_h = random.uniform(20.0, 55.0) if zone["vulnerability"] >= 0.8 else random.uniform(5.0, 35.0)

        messages = [
            (
                TOPICS["precipitation"],
                build_message(zone, "NASA GPM (simulado)", "precipitation_mm_h", rain_mm_h, "mm/h"),
            ),
            (
                TOPICS["tide"],
                build_message(zone, "INOCAR (simulado)", "tide_m", tide_m, "m"),
            ),
            (
                TOPICS["reservoir"],
                build_message(zone, "CELEC Daule-Peripa (simulado)", "reservoir_pct", reservoir_pct, "%"),
            ),
        ]

        # Lógica de alertas de la SNGR basada en condiciones del ciclo
        sngr_msg = None
        if rain_mm_h >= 45.0 and tide_m >= 3.2:
            sngr_msg = build_sngr_message(
                zone,
                "Roja",
                f"Peligro extremo en {zone['zone']}: Inundación inminente por coincidencia de marea máxima ({tide_m:.2f}m) y lluvias torrenciales ({rain_mm_h:.2f} mm/h)."
            )
        elif rain_mm_h >= 30.0 or tide_m >= 2.8:
            sngr_msg = build_sngr_message(
                zone,
                "Naranja",
                f"Riesgo alto en {zone['zone']}: Acumulación severa de agua de lluvia ({rain_mm_h:.2f} mm/h) y problemas en drenes con marea de {tide_m:.2f}m."
            )
        elif rain_mm_h >= 15.0 or tide_m >= 2.2:
            sngr_msg = build_sngr_message(
                zone,
                "Amarilla",
                f"Atención en {zone['zone']}: Calzadas húmedas y posible anegamiento preventivo por lluvias locales."
            )
        elif random.random() < 0.15:
            sngr_msg = build_sngr_message(
                zone,
                "Verde",
                f"Condiciones normales en {zone['zone']}: Monitoreo preventivo activo en el cantón."
            )

        if sngr_msg:
            messages.append((TOPICS["sngr"], sngr_msg))

        for topic, message in messages:
            send_message(message, topic)

    print(
        f"Ciclo enviado: lluvia, marea={tide_m:.2f} m y "
        f"embalse={reservoir_pct:.2f}% para {len(ZONES)} zonas."
    )
    time.sleep(5)
