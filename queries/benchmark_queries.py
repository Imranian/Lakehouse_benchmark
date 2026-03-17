import csv
import argparse
import sys
import time
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark.stream_common import (
    get_compacted_storage_path,
    get_configured_storage_path,
    get_metrics_dir,
)


def load_config(config_path="configs/pipeline_config.yaml"):
    with open(config_path, "r", encoding="utf-8") as config_file:
        return yaml.safe_load(config_file)


def build_spark_session(warehouse_path):
    spark = (
        SparkSession.builder.appName("BenchmarkQueries")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.sql.catalog.local",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse_path)
        .config("spark.sql.iceberg.vectorization.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def time_query(spark, sql_text):
    start = time.time()
    spark.sql(sql_text).show(10, truncate=False)
    return round(time.time() - start, 4)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["before", "after"], default="after")
    args = parser.parse_args()

    config = load_config()
    original_iceberg_warehouse_path = get_configured_storage_path(config, "iceberg_warehouse")
    original_delta_table_path = get_configured_storage_path(config, "delta_table")
    original_hudi_table_path = get_configured_storage_path(config, "hudi_table")

    if args.mode == "after":
        iceberg_warehouse_path = get_compacted_storage_path(original_iceberg_warehouse_path)
        delta_table_path = get_compacted_storage_path(original_delta_table_path)
        hudi_table_path = get_compacted_storage_path(original_hudi_table_path)
        output_name = "query_benchmark_after_compaction.csv"
    else:
        iceberg_warehouse_path = original_iceberg_warehouse_path
        delta_table_path = original_delta_table_path
        hudi_table_path = original_hudi_table_path
        output_name = "query_benchmark_before_compaction.csv"

    spark = build_spark_session(iceberg_warehouse_path)
    query_runs = config["benchmark"]["query_runs"]
    iceberg_table = (
        f"local.{config['iceberg']['namespace']}.{config['iceberg']['table_name']}"
    )
    local_hudi_table_path = hudi_table_path.replace("file://", "", 1)

    if not Path(local_hudi_table_path).exists():
        spark.stop()
        raise RuntimeError(
            f"Hudi table path does not exist, so {args.mode}-compaction query benchmarking cannot run."
        )

    spark.read.format("hudi").load(hudi_table_path).createOrReplaceTempView(
        "hudi_sensor_table"
    )

    sql_by_format = {
        "delta": f"""
            SELECT sensor_id, AVG(temperature) AS avg_temperature
            FROM delta.`{delta_table_path}`
            GROUP BY sensor_id
        """,
        "hudi": f"""
            SELECT sensor_id, AVG(temperature) AS avg_temperature
            FROM hudi_sensor_table
            GROUP BY sensor_id
        """,
        "iceberg": f"""
            SELECT sensor_id, AVG(temperature) AS avg_temperature
            FROM {iceberg_table}
            GROUP BY sensor_id
        """,
    }

    output_path = get_metrics_dir(config) / output_name
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["format", "run_number", "query_latency_seconds"])

        for table_format, sql_text in sql_by_format.items():
            for run_number in range(1, query_runs + 1):
                latency = time_query(spark, sql_text)
                writer.writerow([table_format, run_number, latency])
                print(
                    f"{table_format} query run {run_number} latency: {latency} seconds"
                )

    spark.stop()
    print(f"Wrote query benchmark results to {output_path}")


if __name__ == "__main__":
    main()
