"""
Recolector 2: INOCAR - Tabla de mareas de Guayaquil (Río Guayas)
Fuente: PDF trimestral, ej:
https://www.inocar.mil.ec/mareas/TM/2025/trimestral/GUAYAQUIL_RIO_3.pdf

Caso INTERMEDIO: no hay HTML que leer directamente ni una API,
sino un PDF publicado en una URL con patrón predecible
(año + número de trimestre 1-4). Hay que descargarlo y extraer
el texto/tabla con pdfplumber.
"""

import re
import requests
import pdfplumber
from io import BytesIO
from datetime import datetime

# El número de trimestre (1,2,3,4) corresponde a Ene-Mar, Abr-Jun, Jul-Sep, Oct-Dic
URL_TEMPLATE = "https://www.inocar.mil.ec/mareas/TM/{year}/trimestral/GUAYAQUIL_RIO_{q}.pdf"


def descargar_pdf(year, quarter):
    url = URL_TEMPLATE.format(year=year, q=quarter)
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    return BytesIO(resp.content)


def extraer_texto(pdf_bytes):
    """Extrae todo el texto del PDF. Cada fila representa un día,
    con valores hora:altura repetidos para las mareas del día."""
    texto_completo = []
    with pdfplumber.open(pdf_bytes) as pdf:
        for page in pdf.pages:
            texto = page.extract_text()
            if texto:
                texto_completo.append(texto)
    return "\n".join(texto_completo)


def parsear_valores_marea(texto):
    """
    Busca patrones tipo 'HHMM ALTURA' (ej: 0404 0.22) que es como
    INOCAR reporta cada pleamar/bajamar. Ajusta la regex según lo
    que veas al inspeccionar el texto extraído real.
    """
    patron = re.compile(r"(\d{4})\s+(\d\.\d{2})")
    resultados = patron.findall(texto)

    mensajes = []
    for hora, altura in resultados:
        mensajes.append({
            "timestamp": datetime.utcnow().isoformat(),
            "source": "INOCAR",
            "variable": "tide_m",
            "hora_reportada": hora,
            "value": float(altura),
            "unit": "m",
            "zone": "Guayaquil (Río Guayas)",
        })
    return mensajes


if __name__ == "__main__":
    pdf_bytes = descargar_pdf(year=2025, quarter=3)
    texto = extraer_texto(pdf_bytes)
    mensajes = parsear_valores_marea(texto)

    print(f"Se extrajeron {len(mensajes)} lecturas de marea.")
    for m in mensajes[:5]:
        print(m)
    # En tu pipeline real, aquí iterarías enviando cada uno con send_message()