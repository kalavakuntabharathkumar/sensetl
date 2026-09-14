from datetime import datetime, timedelta
import json
import psycopg
from airflow import DAG
from airflow.operators.python import PythonOperator

DB = "postgresql://sensor:sensor@postgres:5432/sensors"

def init_db():
    with psycopg.connect(DB) as conn:
        with conn.cursor() as cur:
            with open("/opt/airflow/sql/schema.sql", encoding="utf-8") as f:
                cur.execute(f.read())

def consume_kafka():
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(
        "sensor-events",
        bootstrap_servers="kafka:29092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=3000,
        value_deserializer=lambda x: json.loads(x.decode()),
    )
    rows = []
    for msg in consumer:
        value = msg.value
        rows.append((
            value["timestamp"],
            value["device_id"],
            value["energy_kw"],
            value["temperature_c"],
        ))
        if len(rows) >= 5000:
            break

    if not rows:
        return

    with psycopg.connect(DB) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO sensor_readings
                (timestamp,device_id,energy_kw,temperature_c)
                VALUES (%s,%s,%s,%s)""",
                rows,
            )

def aggregate():
    with psycopg.connect(DB) as conn:
        with conn.cursor() as cur:
            with open("/opt/airflow/sql/aggregations.sql", encoding="utf-8") as f:
                cur.execute(f.read())

def detect():
    with psycopg.connect(DB) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO anomaly_flags(timestamp,device_id,energy_kw,reason)
                   SELECT timestamp,device_id,energy_kw,'Energy spike'
                   FROM sensor_readings
                   WHERE energy_kw > 200
                   ON CONFLICT DO NOTHING"""
            )

with DAG(
    "sensor_anomaly_pipeline",
    start_date=datetime(2025,1,1),
    schedule=timedelta(hours=1),
    catchup=False,
    default_args={"retries":2,"retry_delay":timedelta(minutes=1)},
) as dag:
    a = PythonOperator(task_id="init_db", python_callable=init_db)
    b = PythonOperator(task_id="consume_kafka", python_callable=consume_kafka)
    c = PythonOperator(task_id="aggregate", python_callable=aggregate)
    d = PythonOperator(task_id="detect_anomalies", python_callable=detect)
    a >> b >> c >> d
