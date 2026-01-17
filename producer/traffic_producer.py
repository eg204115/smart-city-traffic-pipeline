import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Kafka configuration
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "traffic-data"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Simulated traffic sensors (4 junctions)
SENSORS = ["JUNCTION_1", "JUNCTION_2", "JUNCTION_3", "JUNCTION_4"]

print("🚦 Traffic Sensor Simulator Started...")

while True:
    sensor_id = random.choice(SENSORS)

    # Occasionally generate congestion (low speed)
    if random.random() < 0.15:   # 15% chance
        avg_speed = random.randint(5, 9)
    else:
        avg_speed = random.randint(20, 60)

    traffic_event = {
        "sensor_id": sensor_id,
        "timestamp": datetime.utcnow().isoformat(),
        "vehicle_count": random.randint(50, 200),
        "avg_speed": avg_speed
    }

    producer.send(TOPIC_NAME, traffic_event)
    print(f"Sent: {traffic_event}")

    time.sleep(1)
