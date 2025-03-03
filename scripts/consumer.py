import json
from confluent_kafka import Consumer, KafkaError
import psycopg2
from time import sleep
import time

# Initializes Kafka consumer with retry logic
def initialize_consumer():
    for attempt in range(5):
        try:
            consumer = Consumer({
                'bootstrap.servers': 'kafka:9092',
                'group.id': 'weather-group',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
                'session.timeout.ms': 300000,
                'metadata.max.age.ms': 360000
            })
            consumer.subscribe(['weather_raw'])
            print("Connected to Kafka successfully!")
            return consumer
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            sleep(30)  # Retry after delay
    raise Exception("Failed to connect to Kafka after 5 attempts")

# Sets up Postgres connection and creates weather table
def initialize_db():
    for attempt in range(5):
        try:
            conn = psycopg2.connect(
                dbname="weather_db",
                user="weather_user",
                password="weather_pass",
                host="postgres",
                port="5432"
            )
            cursor = conn.cursor()
            # Create weather table if it doesn’t exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS weather (
                    id SERIAL PRIMARY KEY,
                    city VARCHAR(50),
                    temp_celsius REAL,
                    humidity INTEGER,
                    timestamp BIGINT
                )
            """)
            conn.commit()
            print("Connected to PostgreSQL successfully!")
            return conn, cursor
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            sleep(30)  # Retry after delay
    raise Exception("Failed to connect to PostgreSQL after 5 attempts")

# Polls Kafka messages and stores them in Postgres
def process_and_store():
    consumer = initialize_consumer()
    conn, cursor = initialize_db()
    messages_processed = 0
    max_poll_duration = 60

    start_time = time.time()
    while (time.time() - start_time) < max_poll_duration:
        msg = consumer.poll(10.0)  # Wait for message up to 10s
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue 
            else:
                print(f"Consumer error: {msg.error()}")
                break
        try:
            data = json.loads(msg.value().decode('utf-8'))
            temp_celsius = data["main"]["temp"] - 273.15  # Convert to Celsius
            cursor.execute("""
                INSERT INTO weather (city, temp_celsius, humidity, timestamp)
                VALUES (%s, %s, %s, %s)
            """, (data["name"], round(temp_celsius, 2), data["main"]["humidity"], data["dt"]))
            conn.commit()
            consumer.commit()
            print(f"Stored: {data['name']}")
            messages_processed += 1
        except Exception as e:
            print(f"Error processing message: {e}")
            conn.rollback()

    consumer.close()
    conn.close()
    print(f"Processed {messages_processed} messages.")

if __name__ == "__main__":
    process_and_store()