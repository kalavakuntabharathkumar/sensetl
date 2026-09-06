# Automated ETL Pipeline for Sensor Data Anomaly Detection

Portfolio-ready simulated IoT data pipeline using Kafka, Airflow and PostgreSQL.

## Stack
Python, Apache Airflow, Kafka, PostgreSQL, SQL

## Architecture
IoT simulator -> Kafka -> Airflow -> PostgreSQL -> SQL aggregation -> anomaly flags

## Quick start
```bash
docker compose up --build
```

- Airflow: http://localhost:8080
- Kafka: localhost:9092
- PostgreSQL: localhost:5433
- Airflow login: airflow / airflow

The DAG `sensor_anomaly_pipeline` initializes the schema, consumes Kafka events, aggregates readings, and flags energy spikes.

The repository intentionally does not contain a multi-million-row dataset. The producer generates synthetic events on demand.

The resume figures in the prompt are target/simulated benchmark figures unless independently measured.
