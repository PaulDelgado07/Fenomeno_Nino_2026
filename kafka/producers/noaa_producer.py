import time
import json
import requests
from kafka import KafkaProducer

# 1. Inicializar el productor de Kafka apuntando a tu broker en Docker
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# REGLA CLAVE: Ajusta este nombre al topic que lea tu script "read_kafka.py"
TOPIC_NAME = "clima"  
NOAA_URL = "https://www.cpc.ncep.noaa.gov/data/indices/sstoi.indices"

def iniciar_streaming_historico():
    print("[+] Conectando con los servidores oficiales de la NOAA...")
    try:
        response = requests.get(NOAA_URL, timeout=10)
        if response.status_code != 200:
            print("[-] No se pudo descargar el archivo de índices de la NOAA.")
            return

        # Filtramos las líneas del archivo de texto para limpiar espacios vacíos
        lineas = [l.strip() for l in response.text.split('\n') if l.strip()]
        
        # Saltamos la primera línea que contiene los encabezados (YR MON NINO1+2...)
        datos_historicos = lineas[1:] 

        print("[+] ¡Conexión exitosa! Iniciando transmisión en tiempo real del historial climático...")
        
        while True:
            for registro in datos_historicos:
                columnas = registro.split()
                if len(columnas) < 6:
                    continue
                
                anio = columnas[0]
                mes = columnas[1]
                sst_anomalia = float(columnas[5]) # Columna 5: Anomalía de la Región Niño 1+2 (Costa del Pacífico de Ecuador)

                # Formateamos el JSON idéntico a lo que espera tu pipeline actual
                payload = {
                    "fuente": "NOAA CPC",
                    "parametro": "Anomalia SST",
                    "valor": sst_anomalia,
                    "zona": "Niño 1+2 (Ecuador)",
                    "fecha": f"{anio}-{mes.zfill(2)}"
                }

                # Enviamos el registro al topic de Kafka
                producer.send(TOPIC_NAME, value=payload)
                print(f"[ KAFKA STREAMING HISTÓRICO ] -> Enviado: {anio}-{mes.zfill(2)} | Anomalía: {sst_anomalia}°C")
                
                # Pausa de 3 segundos entre registros para que tu frontend dibuje la curva
                # de manera progresiva y dinámica durante la exposición.
                time.sleep(3)
                
            print("[*] Fin del historial alcanzado. Reiniciando bucle de transmisión...")

    except Exception as e:
        print(f"[-] Error crítico en el pipeline de la NOAA: {e}")

if __name__ == "__main__":
    iniciar_streaming_historico()