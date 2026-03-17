#!/usr/bin/env bash

set -euo pipefail

LOG_DIR="${LOG_DIR:-/tmp/lakehouse_benchmark_logs}"
ZOOKEEPER_PID_FILE="$LOG_DIR/zookeeper.pid"
KAFKA_PID_FILE="$LOG_DIR/kafka.pid"
GENERATOR_PID_FILE="$LOG_DIR/generator.pid"


stop_by_pattern() {
  local label="$1"
  local pattern="$2"
  local pids

  pids="$(pgrep -f "$pattern" || true)"
  if [[ -z "$pids" ]]; then
    echo "$label is not running"
    return
  fi

  echo "$pids" | xargs -r kill 2>/dev/null || true
  sleep 2
  pids="$(pgrep -f "$pattern" || true)"
  if [[ -n "$pids" ]]; then
    echo "$pids" | xargs -r kill -9 2>/dev/null || true
  fi
  echo "Stopped $label via process lookup"
}


stop_pid_file() {
  local label="$1"
  local pid_file="$2"
  local fallback_pattern="${3:-}"

  if [[ ! -f "$pid_file" ]]; then
    if [[ -n "$fallback_pattern" ]]; then
      stop_by_pattern "$label" "$fallback_pattern"
    else
      echo "$label PID file not found, nothing to stop"
    fi
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
    if [[ -n "$fallback_pattern" ]]; then
      echo "$label PID $pid is stale, trying process lookup"
      stop_by_pattern "$label" "$fallback_pattern"
    else
      echo "$label PID $pid is not running"
    fi
  fi

  rm -f "$pid_file"
}


stop_pid_file "generator" "$GENERATOR_PID_FILE" "sensor_stream_generator.py"
stop_pid_file "kafka" "$KAFKA_PID_FILE" "kafka.Kafka"
stop_pid_file "zookeeper" "$ZOOKEEPER_PID_FILE" "org.apache.zookeeper.server.quorum.QuorumPeerMain"
