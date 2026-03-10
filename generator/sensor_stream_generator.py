import json
import random
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:

    data = {
        "sensor_id": f"sensor_{random.randint(1,1000)}",
        "temperature": random.uniform(20,80),
        "pressure": random.uniform(90,120),
        "vibration": random.uniform(0,1),
        "timestamp": int(time.time())
    }

    producer.send("sensor_topic", data)

    time.sleep(0.01)