#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOPIC_NAME="${TOPIC_NAME:-sensor_topic}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"
EVENT_RATE="${EVENT_RATE:-100}"
SENSOR_COUNT="${SENSOR_COUNT:-1000}"
DURATION_SECONDS="${DURATION_SECONDS:-120}"

if [[ -n "${PYTHON_BIN:-}" ]]; then
  PROJECT_PYTHON="$PYTHON_BIN"
elif [[ -x "$ROOT_DIR/venv/bin/python" ]]; then
  PROJECT_PYTHON="$ROOT_DIR/venv/bin/python"
elif [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PROJECT_PYTHON="$ROOT_DIR/.venv/bin/python"
else
  PROJECT_PYTHON="python3"
fi

mkdir -p /tmp/lakehouse_benchmark_logs

nohup "$PROJECT_PYTHON" "$ROOT_DIR/generator/sensor_stream_generator.py" \
  --topic "$TOPIC_NAME" \
  --bootstrap "$BOOTSTRAP_SERVER" \
  --rate "$EVENT_RATE" \
  --sensors "$SENSOR_COUNT" \
  --duration "$DURATION_SECONDS" \
  >/tmp/lakehouse_benchmark_logs/generator.log 2>&1 &

echo "Sensor generator started in background. Logs: /tmp/lakehouse_benchmark_logs/generator.log"
