import os
import requests
import json
from confluent_kafka import Producer
from time import sleep

# Fetch API key from environment variable
API_KEY = os.getenv("OPENWEATHER_API_KEY")  # Default to None if not set
CITIES = ["New York", "London", "Tokyo"]  # Cities to fetch data for
BASE_URL = "http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"

# Handles Kafka message delivery feedback
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Initializes Kafka producer with retry logic
def initialize_producer():
    for attempt in range(5):
        try:
            producer = Producer({
                'bootstrap.servers': 'kafka:9092',  # Kafka broker
                'client.id': 'weather-producer',  # Producer ID
                'acks': 'all',  # Wait for all replicas
                'retry.backoff.ms': 1000,  # Delay between retries
                'retries': 5,  # Retry attempts
                'request.timeout.ms': 300000,
                'max.in.flight.requests.per.connection': 1
            })
            print("Connected to Kafka successfully!")
            return producer
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            sleep(30)  # Retry after delay
    raise Exception("Failed to connect to Kafka after 5 attempts")

# Fetches weather data and sends it to Kafka
def fetch_and_send():
    if not API_KEY:
        raise ValueError("OPENWEATHER_API_KEY environment variable not set")
    producer = initialize_producer()
    for city in CITIES:
        try:
            response = requests.get(BASE_URL.format(city=city, api_key=API_KEY))
            if response.status_code == 200:
                data = response.json()
                producer.produce("weather_raw", value=json.dumps(data).encode('utf-8'), callback=delivery_report)
                producer.poll(1.0)  # Process callbacks
                print(f"Sent data for {city}")
            else:
                print(f"Failed to fetch data for {city}: {response.status_code}")
        except Exception as e:
            print(f"Error fetching data for {city}: {e}")
    producer.flush(timeout=30.0)  # Ensure all messages are sent
    print("Producer finished sending all messages.")

if __name__ == "__main__":
    fetch_and_send()