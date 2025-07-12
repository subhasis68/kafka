import json
import psycopg2
from confluent_kafka import Consumer, KafkaException

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'test-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['test-topic'])


# TimescaleDB connection
conn = psycopg2.connect(
    host="localhost",
    port=5431,
    dbname="kafkadb",
    user="postgres",
    password="mysecretpassword"
)
cur = conn.cursor()

insert_stmt = """
    INSERT INTO vehicle_data (vehicle_id, lat, long, speed, temperature, humidity, created_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""


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
                data = json.loads(msg.value().decode('utf-8'))
                cur.execute(insert_stmt, (
                    data['vehicle_id'],
                    data['lat'],
                    data['long'],
                    data['speed'],
                    data['temperature'],
                    data['humidity'],
                    data['created_at']
                ))
                conn.commit()
                print(f"Inserted: {data}")
            except (json.JSONDecodeError, KeyError, psycopg2.Error) as e:
                print(f"Error processing message: {e}")
                conn.rollback()


    finally:
        consumer.close()
        cur.close()
        conn.close()


