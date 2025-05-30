from confluent_kafka import Consumer, KafkaException

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'test-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['test-topic'])

try:
    while True:
        msg = consumer.poll(1.0)  # 1 second timeout
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        print(f'Received message: {msg.value().decode("utf-8")}')
finally:
    consumer.close()
