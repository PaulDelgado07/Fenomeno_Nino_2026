import json
from kafka import KafkaProducer

from config import KAFKA_BROKER, TOPIC_NAME


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def send_message(message):
    future = producer.send(TOPIC_NAME, message)

    metadata = future.get(timeout=10)

    print(
        f"✅ Enviado -> "
        f"Topic: {metadata.topic}, "
        f"Partición: {metadata.partition}, "
        f"Offset: {metadata.offset}"
    )

    producer.flush()