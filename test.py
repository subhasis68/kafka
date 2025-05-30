from confluent_kafka import Producer

# Configuration for Kafka
conf = {
    'bootstrap.servers': 'localhost:9092'  # Change to your Kafka broker
}

# Create Producer instance
producer = Producer(conf)

# Optional callback for delivery confirmation
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Produce message
producer.produce('test-topic', key='key1', value='Hello Kafka!', callback=delivery_report)

# Wait for all messages to be delivered
producer.flush()
