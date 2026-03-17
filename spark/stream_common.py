import json
import os
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import yaml
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import DoubleType, LongType, StringType, StructType


def load_config(config_path):
    with open(config_path, "r", encoding="utf-8") as config_file:
        return yaml.safe_load(config_file)


def sensor_schema():
    return (
        StructType()
        .add("event_id", StringType())
        .add("sensor_id", StringType())
        .add("temperature", DoubleType())
        .add("pressure", DoubleType())
        .add("vibration", DoubleType())
        .add("timestamp", LongType())
        .add("produced_at_ms", LongType())
    )


def build_sensor_stream(spark, bootstrap_servers, topic, max_offsets_per_trigger=None):
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
    )
    if max_offsets_per_trigger is not None:
        reader = reader.option("maxOffsetsPerTrigger", str(max_offsets_per_trigger))

    kafka_df = reader.load()

    parsed_df = (
        kafka_df.selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), sensor_schema()).alias("data"))
        .select(
            col("data.event_id").alias("event_id"),
            col("data.sensor_id").alias("sensor_id"),
            col("data.temperature").alias("temperature"),
            col("data.pressure").alias("pressure"),
            col("data.vibration").alias("vibration"),
            col("data.timestamp").alias("event_ts"),
            col("data.produced_at_ms").alias("produced_at_ms"),
        )
        .filter("event_id IS NOT NULL")
        .filter("sensor_id IS NOT NULL")
        .withColumn(
            "ingested_at_ms",
            (current_timestamp().cast("double") * 1000).cast("long"),
        )
    )

    return parsed_df


def uri_to_path(uri_or_path):
    if uri_or_path.startswith("file://"):
        return uri_or_path.replace("file://", "", 1)
    return uri_or_path


def write_json(output_path, payload):
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as file_handle:
        json.dump(payload, file_handle, indent=2)


def get_run_label():
    return os.environ.get("BENCHMARK_RUN_LABEL", "").strip()


def get_metrics_dir(config):
    base_dir = Path(config["metrics"]["output_dir"])
    run_label = get_run_label()
    if run_label:
        return base_dir / run_label
    return base_dir


def get_metrics_file_path(config, metric_key):
    metrics_dir = get_metrics_dir(config)
    configured_path = Path(config["metrics"][metric_key])
    return str(metrics_dir / configured_path.name)


def _insert_run_label_into_path_string(path_string, run_label):
    path_obj = Path(path_string)
    return str(path_obj.parent / run_label / path_obj.name)


def get_run_storage_path(path_or_uri):
    run_label = get_run_label()
    if not run_label:
        return path_or_uri

    if path_or_uri.startswith("file://"):
        parsed = urlparse(path_or_uri)
        new_path = _insert_run_label_into_path_string(parsed.path, run_label)
        return urlunparse(parsed._replace(path=new_path))

    return _insert_run_label_into_path_string(path_or_uri, run_label)


def get_configured_storage_path(config, config_key):
    return get_run_storage_path(config["paths"][config_key])


def get_compacted_storage_path(path_or_uri):
    if path_or_uri.startswith("file://"):
        parsed = urlparse(path_or_uri)
        compacted_path = f"{parsed.path}_compacted"
        return urlunparse(parsed._replace(path=compacted_path))
    return f"{path_or_uri}_compacted"


def count_parquet_files(base_path):
    file_count = 0
    total_bytes = 0
    local_path = uri_to_path(base_path)

    if not Path(local_path).exists():
        return {"data_file_count": 0, "total_size_bytes": 0}

    for file_path in Path(local_path).rglob("*.parquet"):
        if file_path.is_file():
            file_count += 1
            total_bytes += file_path.stat().st_size

    return {"data_file_count": file_count, "total_size_bytes": total_bytes}
