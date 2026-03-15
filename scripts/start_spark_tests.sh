#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"
TOPIC_NAME="${TOPIC_NAME:-sensor_topic}"
TIMEOUT="${TIMEOUT:-60}"

if [[ -d "/usr/lib/jvm/java-11-openjdk-amd64" ]]; then
  export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}"
  export PATH="$JAVA_HOME/bin:$PATH"
fi

COMMON_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4"

usage() {
  cat <<'EOF'
Usage:
  scripts/start_spark_tests.sh delta
  scripts/start_spark_tests.sh hudi
  scripts/start_spark_tests.sh iceberg
  scripts/start_spark_tests.sh all

Optional environment variables:
  TIMEOUT=90
  BOOTSTRAP_SERVER=localhost:9092
  TOPIC_NAME=sensor_topic
EOF
}

run_test() {
  local format="$1"
  local packages="$2"
  local script_path="$3"

  echo "Starting ${format} test for ${TIMEOUT} seconds..."
  "$SPARK_SUBMIT" \
    --packages "$packages" \
    "$script_path" \
    --bootstrap "$BOOTSTRAP_SERVER" \
    --topic "$TOPIC_NAME" \
    --timeout "$TIMEOUT"
}

case "${1:-}" in
  delta)
    run_test \
      "delta" \
      "${COMMON_PACKAGES},io.delta:delta-spark_2.12:3.2.0" \
      "$ROOT_DIR/tests/spark_kafka_delta_test.py"
    ;;
  hudi)
    run_test \
      "hudi" \
      "${COMMON_PACKAGES},org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0" \
      "$ROOT_DIR/tests/spark_kafka_hudi_test.py"
    ;;
  iceberg)
    run_test \
      "iceberg" \
      "${COMMON_PACKAGES},org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2" \
      "$ROOT_DIR/tests/spark_kafka_iceberg_test.py"
    ;;
  all)
    run_test \
      "delta" \
      "${COMMON_PACKAGES},io.delta:delta-spark_2.12:3.2.0" \
      "$ROOT_DIR/tests/spark_kafka_delta_test.py"
    run_test \
      "hudi" \
      "${COMMON_PACKAGES},org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0" \
      "$ROOT_DIR/tests/spark_kafka_hudi_test.py"
    run_test \
      "iceberg" \
      "${COMMON_PACKAGES},org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2" \
      "$ROOT_DIR/tests/spark_kafka_iceberg_test.py"
    ;;
  *)
    usage
    exit 1
    ;;
esac
