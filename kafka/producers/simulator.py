import time

from producer import send_message
from utils import create_message
from data_sources import get_sst_c


print("Simulador iniciado...\n")

while True:

    temperatura = get_sst_c()

    mensaje = create_message(temperatura)

    print(mensaje)

    send_message(mensaje)

    time.sleep(5)