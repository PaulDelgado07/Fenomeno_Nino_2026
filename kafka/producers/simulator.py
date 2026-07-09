import random
import time

from producer import send_message
from utils import create_message


print("Simulador iniciado...\n")

while True:

    temperatura = round(random.uniform(27.5, 30.5), 2)

    mensaje = create_message(temperatura)

    print(mensaje)

    send_message(mensaje)

    time.sleep(5)