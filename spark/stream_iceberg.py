import argparse
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark.stream_common import build_sensor_stream, load_config, write_json


def build_spark_session(warehouse_path):
    spark = (
        SparkSession.builder.appName("IcebergStreaming")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse_path)
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
    spark = build_spark_session(config["paths"]["iceberg_warehouse"])
    parsed_df = build_sensor_stream(
        spark,
        config["kafka"]["bootstrap_servers"],
        config["kafka"]["topic"],
    )
    full_table_name = (
        f"local.{config['iceberg']['namespace']}.{config['iceberg']['table_name']}"
    )

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS local.{config['iceberg']['namespace']}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            event_id STRING,
            sensor_id STRING,
            temperature DOUBLE,
            pressure DOUBLE,
            vibration DOUBLE,
            event_ts BIGINT,
            produced_at_ms BIGINT,
            ingested_at_ms BIGINT
        )
        USING iceberg
        """
    )

    start_time = time.time()
    query = (
        parsed_df.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", config["paths"]["iceberg_checkpoint"])
        .toTable(full_table_name)
    )

    query.awaitTermination(timeout)
    if query.isActive:
        query.stop()

    duration_seconds = round(time.time() - start_time, 2)
    result_df = spark.table(full_table_name)
    summary = (
        result_df.selectExpr(
            "COUNT(*) AS row_count",
            "AVG(ingested_at_ms - produced_at_ms) AS avg_ingestion_latency_ms",
            "MAX(ingested_at_ms - produced_at_ms) AS max_ingestion_latency_ms",
            "percentile_approx(ingested_at_ms - produced_at_ms, 0.95) AS p95_ingestion_latency_ms",
        ).first()
    )

    metrics = {
        "format": "iceberg",
        "table_name": full_table_name,
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

    write_json(config["metrics"]["iceberg_ingestion"], metrics)
    spark.stop()
    print(metrics)


if __name__ == "__main__":
    main()
