#!/usr/bin/env bash

set -euo pipefail

LOG_DIR="${LOG_DIR:-/tmp/lakehouse_benchmark_logs}"
ZOOKEEPER_PID_FILE="$LOG_DIR/zookeeper.pid"
KAFKA_PID_FILE="$LOG_DIR/kafka.pid"
GENERATOR_PID_FILE="$LOG_DIR/generator.pid"


stop_pid_file() {
  local label="$1"
  local pid_file="$2"

  if [[ ! -f "$pid_file" ]]; then
    echo "$label PID file not found, nothing to stop"
    return
  fi

  local pid
  pid="$(cat "$pid_file")"
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    sleep 2
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
    fi
    echo "Stopped $label PID $pid"
  else
    echo "$label PID $pid is not running"
  fi

  rm -f "$pid_file"
}


stop_pid_file "generator" "$GENERATOR_PID_FILE"
stop_pid_file "kafka" "$KAFKA_PID_FILE"
stop_pid_file "zookeeper" "$ZOOKEEPER_PID_FILE"
