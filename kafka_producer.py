from confluent_kafka import Producer
import json
from data import get_live_data
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
        live_data = get_live_data()
        producer.produce("test-topic", key="key1", value=json_serializer(live_data), callback=delivery_report)
        # Wait for all messages to be delivered
        producer.flush()
        time.sleep(2)
