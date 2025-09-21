import os
import json
from confluent_kafka import Producer
import random

KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL', 'kafka:9092')
KAFKA_TOPIC_INGESTION = 'telegram_messages'

def send_message_to_kafka(chat_id: int, text: str):
    producer_conf = {'bootstrap.servers': KAFKA_BROKER_URL}
    producer = Producer(producer_conf)

    message = {
        'chat_id': chat_id,
        'text': text
    }

    encoded_message = json.dumps(message).encode('utf-8')

    try:
        producer.produce(KAFKA_TOPIC_INGESTION, value=encoded_message)
        producer.flush()
        print(f"Messaggio inviato a Kafka: {message}")
    except Exception as e:
        print(f"Errore nell'invio a Kafka: {e}")

def main():
    # Lista dei possibili chat_id
    chat_ids = [160254588]

    # Legge gli eventi da file
    with open('events.txt', 'r', encoding='utf-8') as f:
        texts = [line.strip() for line in f if line.strip()]

    for text in texts:
        chat_id = random.choice(chat_ids)
        send_message_to_kafka(chat_id, text)

if __name__ == "__main__":
    main()


