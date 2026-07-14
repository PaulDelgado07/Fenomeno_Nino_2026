"""
Recolector 2: INOCAR - Tabla de mareas de Guayaquil (Río Guayas)
Fuente: PDF trimestral, ej:
https://www.inocar.mil.ec/mareas/TM/2025/trimestral/GUAYAQUIL_RIO_3.pdf

Caso INTERMEDIO: no hay HTML que leer directamente ni una API,
sino un PDF publicado en una URL con patrón predecible
(año + número de trimestre 1-4). Hay que descargarlo y extraer
la tabla con pdfplumber.

La tabla del PDF muestra los 3 meses del trimestre uno al lado del
otro, y cada mes se divide en dos columnas (días 1-16 y 17-31). El
texto plano de pdfplumber intercala esas 6 columnas línea por línea,
así que NO se puede usar el orden de aparición del texto para saber
a qué día pertenece cada lectura (una lectura del 1 de julio puede
aparecer en el texto justo antes que una del 30 de septiembre).
Por eso extraemos las palabras con su posición (x, y) y reconstruimos
cada columna por separado.
"""

import re
import requests
import pdfplumber
from io import BytesIO
from datetime import datetime

# El número de trimestre (1,2,3,4) corresponde a Ene-Mar, Abr-Jun, Jul-Sep, Oct-Dic
URL_TEMPLATE = "https://www.inocar.mil.ec/mareas/TM/{year}/trimestral/GUAYAQUIL_RIO_{q}.pdf"
MESES_POR_TRIMESTRE = {
    1: [1, 2, 3],
    2: [4, 5, 6],
    3: [7, 8, 9],
    4: [10, 11, 12],
}


def descargar_pdf(year, quarter):
    url = URL_TEMPLATE.format(year=year, q=quarter)
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    return BytesIO(resp.content)


def _columna_de(x0, limites):
    for i, limite in enumerate(limites):
        if x0 < limite:
            return i
    return len(limites)


def extraer_lecturas(pdf_bytes, year, quarter):
    """Extrae todas las lecturas de marea del PDF con su fecha/hora real.

    Devuelve una lista de dicts: {"datetime": datetime, "value": float}.
    """
    meses = MESES_POR_TRIMESTRE[quarter]
    lecturas = []

    with pdfplumber.open(pdf_bytes) as pdf:
        for page in pdf.pages:
            words = page.extract_words()
            horas = sorted(w["x0"] for w in words if w["text"] == "HORA")
            if len(horas) != 6:
                # Formato inesperado en esta página: no arriesgamos a
                # inventar columnas, simplemente la saltamos.
                continue

            # Límites = punto medio entre cada par de columnas consecutivas
            limites = [(horas[i] + horas[i + 1]) / 2 for i in range(5)]

            columnas = {i: [] for i in range(6)}
            for w in words:
                texto = w["text"]
                if re.fullmatch(r"\d{1,2}", texto) or re.fullmatch(r"\d{4}", texto) or re.fullmatch(r"\d\.\d{2}", texto):
                    col = _columna_de(w["x0"], limites)
                    columnas[col].append((w["top"], texto))

            for col_idx, tokens in columnas.items():
                tokens.sort(key=lambda t: t[0])
                mes = meses[col_idx // 2]
                dia_actual = None
                hora_pendiente = None
                for _, texto in tokens:
                    if re.fullmatch(r"\d{1,2}", texto) and 1 <= int(texto) <= 31:
                        dia_actual = int(texto)
                    elif re.fullmatch(r"\d{4}", texto):
                        hora_pendiente = texto
                    elif re.fullmatch(r"\d\.\d{2}", texto):
                        if dia_actual is not None and hora_pendiente is not None:
                            try:
                                fecha = datetime(
                                    year, mes, dia_actual,
                                    int(hora_pendiente[:2]), int(hora_pendiente[2:]),
                                )
                            except ValueError:
                                fecha = None
                            if fecha is not None:
                                lecturas.append({"datetime": fecha, "value": float(texto)})
                        hora_pendiente = None

    return lecturas


def lectura_mas_cercana_a_ahora(lecturas, ahora=None):
    """De todas las lecturas (predicciones de pleamar/bajamar del trimestre),
    devuelve la más cercana en el tiempo a `ahora`. Como INOCAR publica
    predicciones puntuales (no una serie continua), esta es la mejor
    aproximación a "la marea actual" sin interpolar entre dos extremos.
    """
    if not lecturas:
        return None
    if ahora is None:
        ahora = datetime.now()
    return min(lecturas, key=lambda l: abs((l["datetime"] - ahora).total_seconds()))


if __name__ == "__main__":
    pdf_bytes = descargar_pdf(year=2026, quarter=3)
    lecturas = extraer_lecturas(pdf_bytes, year=2026, quarter=3)

    print(f"Se extrajeron {len(lecturas)} lecturas de marea.")
    mas_cercana = lectura_mas_cercana_a_ahora(lecturas)
    print("Lectura más cercana a ahora:", mas_cercana)
