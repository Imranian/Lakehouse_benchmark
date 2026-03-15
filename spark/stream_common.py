import json
from pathlib import Path

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


def build_sensor_stream(spark, bootstrap_servers, topic):
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

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
