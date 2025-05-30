from confluent_kafka import Producer
import json
from data import get_registered_user
import time

# Configuration for Kafka
conf = {
    'bootstrap.servers': 'localhost:9092'  # Change to your Kafka broker
}

# Create Producer instance
producer = Producer(conf)

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

# Optional callback for delivery confirmation
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

if __name__ == "__main__":
    while True:
        registered_user = get_registered_user()
        producer.produce("test-topic", key="key1", value=json_serializer(registered_user), callback=delivery_report)
        # Wait for all messages to be delivered
        producer.flush()
        time.sleep(2)
