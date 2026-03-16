#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOPIC_NAME="${TOPIC_NAME:-sensor_topic}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"
EVENT_RATE="${EVENT_RATE:-100}"
SENSOR_COUNT="${SENSOR_COUNT:-1000}"
DURATION_SECONDS="${DURATION_SECONDS:-120}"
LOG_DIR="${LOG_DIR:-/tmp/lakehouse_benchmark_logs}"
GENERATOR_PID_FILE="$LOG_DIR/generator.pid"

if [[ -n "${PYTHON_BIN:-}" ]]; then
  PROJECT_PYTHON="$PYTHON_BIN"
elif [[ -x "$ROOT_DIR/venv/bin/python" ]]; then
  PROJECT_PYTHON="$ROOT_DIR/venv/bin/python"
elif [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PROJECT_PYTHON="$ROOT_DIR/.venv/bin/python"
else
  PROJECT_PYTHON="python3"
fi

mkdir -p "$LOG_DIR"

if [[ -f "$GENERATOR_PID_FILE" ]] && kill -0 "$(cat "$GENERATOR_PID_FILE")" 2>/dev/null; then
  echo "Sensor generator already running with PID $(cat "$GENERATOR_PID_FILE")"
  exit 0
fi

nohup "$PROJECT_PYTHON" "$ROOT_DIR/generator/sensor_stream_generator.py" \
  --topic "$TOPIC_NAME" \
  --bootstrap "$BOOTSTRAP_SERVER" \
  --rate "$EVENT_RATE" \
  --sensors "$SENSOR_COUNT" \
  --duration "$DURATION_SECONDS" \
  >"$LOG_DIR/generator.log" 2>&1 &

echo $! >"$GENERATOR_PID_FILE"

echo "Sensor generator started with PID $(cat "$GENERATOR_PID_FILE"). Logs: $LOG_DIR/generator.log"
