import json
import random
import time
import argparse
import sys

import six

sys.modules.setdefault("kafka.vendor.six", six)
sys.modules.setdefault("kafka.vendor.six.moves", six.moves)

from kafka import KafkaProducer


def create_producer(bootstrap_servers):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )


def generate_sensor_event(sensor_count):
    sensor_id = f"sensor_{random.randint(1, sensor_count)}"
    produced_at_ms = int(time.time() * 1000)

    return {
        "event_id": f"{sensor_id}_{produced_at_ms}_{random.randint(1000, 9999)}",
        "sensor_id": sensor_id,
        "temperature": round(random.uniform(20, 90), 2),
        "pressure": round(random.uniform(90, 120), 2),
        "vibration": round(random.uniform(0, 1.5), 3),
        "timestamp": int(time.time()),
        "produced_at_ms": produced_at_ms,
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


def run_burst_stream(producer, topic, sensor_count, phase_rates, phase_durations):
    if len(phase_rates) != len(phase_durations):
        raise ValueError("phase_rates and phase_durations must have the same length.")

    total_sent = 0
    for phase_index, (rate, duration) in enumerate(zip(phase_rates, phase_durations), start=1):
        print(
            f"Starting burst phase {phase_index}/{len(phase_rates)}: "
            f"{rate} events/sec for {duration} seconds"
        )
        interval = 1.0 / rate
        end_time = time.time() + duration

        while time.time() <= end_time:
            event = generate_sensor_event(sensor_count)
            producer.send(topic, event)
            total_sent += 1

            if total_sent % 1000 == 0:
                print(f"Generated {total_sent} events")

            time.sleep(interval)


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--topic", default="sensor_topic")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--sensors", type=int, default=1000)
    parser.add_argument("--rate", type=int, default=100)
    parser.add_argument("--duration", type=int, default=0)
    parser.add_argument(
        "--mode",
        choices=["constant", "burst"],
        default="constant",
    )
    parser.add_argument(
        "--phase-rates",
        default="",
        help="Comma-separated rates for burst mode, for example 100,2000,100",
    )
    parser.add_argument(
        "--phase-durations",
        default="",
        help="Comma-separated durations in seconds for burst mode, for example 45,60,45",
    )

    args = parser.parse_args()

    print("Starting sensor stream generator")
    print(f"Sensors: {args.sensors}")
    print(f"Mode: {args.mode}")
    if args.mode == "constant":
        print(f"Rate: {args.rate} events/sec")
    else:
        print(f"Phase rates: {args.phase_rates}")
        print(f"Phase durations: {args.phase_durations}")

    producer = create_producer(args.bootstrap)

    if args.mode == "burst":
        phase_rates = [int(value.strip()) for value in args.phase_rates.split(",") if value.strip()]
        phase_durations = [
            int(value.strip()) for value in args.phase_durations.split(",") if value.strip()
        ]
        if not phase_rates or not phase_durations:
            raise ValueError("Burst mode requires --phase-rates and --phase-durations.")
        run_burst_stream(producer, args.topic, args.sensors, phase_rates, phase_durations)
    else:
        run_stream(
            producer,
            args.topic,
            args.sensors,
            args.rate,
            args.duration
        )


if __name__ == "__main__":
    main()
