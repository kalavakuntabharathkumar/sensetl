import argparse
import json
import math
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=10000)
    parser.add_argument("--interval", type=float, default=0.01)
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers="kafka:29092",
        value_serializer=lambda x: json.dumps(x).encode(),
    )

    for i in range(args.events):
        spike = i % 1000 == 0
        energy = 125 + 20 * math.sin(i / 50) + random.uniform(-8, 8)
        if spike:
            energy += 110

        event = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "device_id": f"IOT-{i % 20:03d}",
            "energy_kw": round(energy, 2),
            "temperature_c": round(55 + energy / 25 + random.uniform(-2, 2), 2),
        }
        producer.send("sensor-events", event)

        if i % 1000 == 0:
            producer.flush()
        time.sleep(args.interval)

    producer.flush()

if __name__ == "__main__":
    main()
