import argparse
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark.stream_common import (
    build_sensor_stream,
    get_configured_storage_path,
    get_metrics_file_path,
    load_config,
    write_json,
)


def build_spark_session():
    spark = (
        SparkSession.builder.appName("DeltaStreaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="configs/pipeline_config.yaml")
    parser.add_argument("--timeout", type=int, default=None)
    args = parser.parse_args()

    config = load_config(args.config)
    timeout = args.timeout or config["benchmark"]["duration_seconds"]
    delta_table_path = get_configured_storage_path(config, "delta_table")
    delta_checkpoint_path = get_configured_storage_path(config, "delta_checkpoint")
    spark = build_spark_session()
    print(
        f"[delta] Starting stream from topic={config['kafka']['topic']} "
        f"to path={delta_table_path} for {timeout} seconds"
    )
    parsed_df = build_sensor_stream(
        spark,
        config["kafka"]["bootstrap_servers"],
        config["kafka"]["topic"],
    )

    start_time = time.time()
    query = (
        parsed_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", delta_checkpoint_path)
        .start(delta_table_path)
    )

    query.awaitTermination(timeout)
    if query.isActive:
        query.stop()

    duration_seconds = round(time.time() - start_time, 2)
    result_df = spark.read.format("delta").load(delta_table_path)
    summary = (
        result_df.selectExpr(
            "COUNT(*) AS row_count",
            "AVG(ingested_at_ms - produced_at_ms) AS avg_ingestion_latency_ms",
            "MAX(ingested_at_ms - produced_at_ms) AS max_ingestion_latency_ms",
            "percentile_approx(ingested_at_ms - produced_at_ms, 0.95) AS p95_ingestion_latency_ms",
        ).first()
    )

    metrics = {
        "format": "delta",
        "table_path": delta_table_path,
        "duration_seconds": duration_seconds,
        "row_count": int(summary["row_count"]),
        "avg_ingestion_latency_ms": float(summary["avg_ingestion_latency_ms"] or 0),
        "max_ingestion_latency_ms": float(summary["max_ingestion_latency_ms"] or 0),
        "p95_ingestion_latency_ms": float(summary["p95_ingestion_latency_ms"] or 0),
        "write_throughput_rows_per_sec": round(
            summary["row_count"] / duration_seconds, 2
        )
        if duration_seconds
        else 0,
    }

    metrics_output_path = get_metrics_file_path(config, "delta_ingestion")
    write_json(metrics_output_path, metrics)
    print(f"[delta] Summary: {metrics}")
    print(f"[delta] Metrics written to {metrics_output_path}")
    result_df.orderBy(col("ingested_at_ms").desc()).select(
        "event_id",
        "sensor_id",
        "temperature",
        "produced_at_ms",
        "ingested_at_ms",
    ).show(5, truncate=False)
    spark.stop()
    print("[delta] Stream completed successfully")


if __name__ == "__main__":
    main()
