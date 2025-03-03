# Weather Pipeline

A real-time data pipeline that fetches weather data from OpenWeatherMap, streams it through Kafka, processes it with Airflow, stores it in PostgreSQL, and visualizes it in Grafana.

## Overview
This project demonstrates a modern data engineering workflow:
- **Data Source**: OpenWeatherMap API (weather data for New York, London, Tokyo).
- **Streaming**: Kafka with Zookeeper for message brokering.
- **Orchestration**: Airflow schedules data ingestion and processing every 10 minutes.
- **Storage**: PostgreSQL stores processed weather data.
- **Visualization**: Grafana dashboard displays real-time weather metrics.

## Features
- Fetches weather data every 10 minutes.
- Streams data through Kafka for scalability.
- Processes and stores data in PostgreSQL with error handling.
- Visualizes temperature, humidity, and timestamps in Grafana.

## Prerequisites
- Docker & Docker Compose
- Python 3.8+
- OpenWeatherMap API key 
