#!/usr/bin/env bash

set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-/home/imran/kafka}"
TOPIC_NAME="${TOPIC_NAME:-sensor_topic}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"
LOG_DIR="${LOG_DIR:-/tmp/lakehouse_benchmark_logs}"
ZOOKEEPER_PID_FILE="$LOG_DIR/zookeeper.pid"
KAFKA_PID_FILE="$LOG_DIR/kafka.pid"


is_running() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

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

mkdir -p "$LOG_DIR"

case "${1:-}" in
  zookeeper)
    if [[ -f "$ZOOKEEPER_PID_FILE" ]] && is_running "$(cat "$ZOOKEEPER_PID_FILE")"; then
      echo "Zookeeper is already running with PID $(cat "$ZOOKEEPER_PID_FILE")"
      exit 0
    fi
    exec "$KAFKA_HOME/bin/zookeeper-server-start.sh" "$KAFKA_HOME/config/zookeeper.properties"
    ;;
  broker)
    if [[ -f "$KAFKA_PID_FILE" ]] && is_running "$(cat "$KAFKA_PID_FILE")"; then
      echo "Kafka broker is already running with PID $(cat "$KAFKA_PID_FILE")"
      exit 0
    fi
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
    if [[ -f "$ZOOKEEPER_PID_FILE" ]] && is_running "$(cat "$ZOOKEEPER_PID_FILE")"; then
      echo "Zookeeper already running with PID $(cat "$ZOOKEEPER_PID_FILE")"
    else
      nohup "$KAFKA_HOME/bin/zookeeper-server-start.sh" \
        "$KAFKA_HOME/config/zookeeper.properties" \
        >"$LOG_DIR/zookeeper.log" 2>&1 &
      echo $! >"$ZOOKEEPER_PID_FILE"
      echo "Started Zookeeper with PID $(cat "$ZOOKEEPER_PID_FILE")"
      sleep 5
    fi

    if [[ -f "$KAFKA_PID_FILE" ]] && is_running "$(cat "$KAFKA_PID_FILE")"; then
      echo "Kafka already running with PID $(cat "$KAFKA_PID_FILE")"
    else
      nohup "$KAFKA_HOME/bin/kafka-server-start.sh" \
        "$KAFKA_HOME/config/server.properties" \
        >"$LOG_DIR/kafka.log" 2>&1 &
      echo $! >"$KAFKA_PID_FILE"
      echo "Started Kafka with PID $(cat "$KAFKA_PID_FILE")"
      sleep 10
    fi

    "$KAFKA_HOME/bin/kafka-topics.sh" \
      --create \
      --if-not-exists \
      --topic "$TOPIC_NAME" \
      --bootstrap-server "$BOOTSTRAP_SERVER" \
      --partitions 3 \
      --replication-factor 1
    echo "Kafka and Zookeeper are ready. Logs: $LOG_DIR"
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
