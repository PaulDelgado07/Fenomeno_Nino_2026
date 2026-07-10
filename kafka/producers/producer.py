import json
from kafka import KafkaProducer

from config import KAFKA_BROKER, TOPIC_NAME


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def send_message(message, topic=TOPIC_NAME):
    """Envía un mensaje JSON al topic indicado de forma segura."""
    try:
        future = producer.send(topic, message)
        metadata = future.get(timeout=15)
        print(
            f"✅ Enviado -> "
            f"Topic: {metadata.topic}, "
            f"Partición: {metadata.partition}, "
            f"Offset: {metadata.offset}"
        )
        producer.flush()
    except Exception as e:
        print(f"⚠️ [WARNING] No se pudo enviar el mensaje al topic {topic} debido a un error: {e}")

