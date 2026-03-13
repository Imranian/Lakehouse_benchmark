import json
import random
import time
import argparse
from kafka import KafkaProducer


def create_producer(bootstrap_servers):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )


def generate_sensor_event(sensor_count):
    sensor_id = f"sensor_{random.randint(1, sensor_count)}"

    return {
        "sensor_id": sensor_id,
        "temperature": round(random.uniform(20, 90), 2),
        "pressure": round(random.uniform(90, 120), 2),
        "vibration": round(random.uniform(0, 1.5), 3),
        "timestamp": int(time.time())
    }


def run_stream(producer, topic, sensor_count, events_per_second, duration):

    interval = 1.0 / events_per_second
    end_time = time.time() + duration if duration else None

    sent = 0

    while True:

        event = generate_sensor_event(sensor_count)

        producer.send(topic, event)

        sent += 1

        if sent % 1000 == 0:
            print(f"Generated {sent} events")

        time.sleep(interval)

        if end_time and time.time() > end_time:
            break


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--topic", default="sensor_topic")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--sensors", type=int, default=1000)
    parser.add_argument("--rate", type=int, default=100)
    parser.add_argument("--duration", type=int, default=0)

    args = parser.parse_args()

    print("Starting sensor stream generator")
    print(f"Sensors: {args.sensors}")
    print(f"Rate: {args.rate} events/sec")

    producer = create_producer(args.bootstrap)

    run_stream(
        producer,
        args.topic,
        args.sensors,
        args.rate,
        args.duration
    )


if __name__ == "__main__":
    main()