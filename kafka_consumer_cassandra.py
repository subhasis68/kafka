import json
from confluent_kafka import Consumer, KafkaException
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from datetime import datetime

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'test-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['test-topic'])

# Connect to Cassandra
cluster = Cluster(['localhost'], port=9042)  # Replace with your Cassandra nodes if needed
session = cluster.connect()
session.set_keyspace('kafkadb')

insert_stmt = session.prepare("""
    INSERT INTO vehicle_data (vehicle_id, lat, long, speed, temperature, humidity, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)
""")

if __name__ == "__main__":
    try:
        while True:
            msg = consumer.poll(1.0)  # 1 second timeout
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            
            message_value = msg.value().decode("utf-8")
            print(f'Received message: {message_value}')

            
            try:
                data = json.loads(message_value)
                vehicle_id = data.get("vehicle_id")
                lat = data.get("lat")
                long = data.get("long")
                speed = data.get("speed")
                temperature = data.get("temperature")
                humidity = data.get("humidity")
                created_at = datetime.fromisoformat( data.get("created_at") )
                

                # Insert into Cassandra
                session.execute(insert_stmt, (vehicle_id, lat, long, speed, temperature, humidity, created_at))

            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON: {e}")
            except Exception as e:
                print(f"Error inserting into Cassandra: {e}")




    finally:
        consumer.close()
        cluster.shutdown()
