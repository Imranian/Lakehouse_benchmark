#!/usr/bin/env bash

set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-/home/imran/kafka}"
TOPIC_NAME="${TOPIC_NAME:-sensor_topic}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"

usage() {
  cat <<'EOF'
Usage:
  scripts/start_kafka.sh zookeeper
  scripts/start_kafka.sh broker
  scripts/start_kafka.sh topic
  scripts/start_kafka.sh all-background
  scripts/start_kafka.sh list-topics

Run `zookeeper` in one terminal, `broker` in another, and `topic` once after Kafka starts.
EOF
}

if [[ ! -d "$KAFKA_HOME" ]]; then
  echo "KAFKA_HOME does not exist: $KAFKA_HOME"
  exit 1
fi

case "${1:-}" in
  zookeeper)
    exec "$KAFKA_HOME/bin/zookeeper-server-start.sh" "$KAFKA_HOME/config/zookeeper.properties"
    ;;
  broker)
    exec "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_HOME/config/server.properties"
    ;;
  topic)
    "$KAFKA_HOME/bin/kafka-topics.sh" \
      --create \
      --if-not-exists \
      --topic "$TOPIC_NAME" \
      --bootstrap-server "$BOOTSTRAP_SERVER" \
      --partitions 3 \
      --replication-factor 1
    ;;
  all-background)
    mkdir -p /tmp/lakehouse_benchmark_logs
    nohup "$KAFKA_HOME/bin/zookeeper-server-start.sh" \
      "$KAFKA_HOME/config/zookeeper.properties" \
      >/tmp/lakehouse_benchmark_logs/zookeeper.log 2>&1 &
    sleep 5
    nohup "$KAFKA_HOME/bin/kafka-server-start.sh" \
      "$KAFKA_HOME/config/server.properties" \
      >/tmp/lakehouse_benchmark_logs/kafka.log 2>&1 &
    sleep 10
    "$KAFKA_HOME/bin/kafka-topics.sh" \
      --create \
      --if-not-exists \
      --topic "$TOPIC_NAME" \
      --bootstrap-server "$BOOTSTRAP_SERVER" \
      --partitions 3 \
      --replication-factor 1
    echo "Kafka and Zookeeper started in background. Logs: /tmp/lakehouse_benchmark_logs"
    ;;
  list-topics)
    "$KAFKA_HOME/bin/kafka-topics.sh" \
      --list \
      --bootstrap-server "$BOOTSTRAP_SERVER"
    ;;
  *)
    usage
    exit 1
    ;;
esac
