FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python==2.0.2
COPY producer/producer.py /app/producer.py
