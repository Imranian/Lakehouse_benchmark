import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark.stream_common import (
    count_parquet_files,
    get_compacted_storage_path,
    get_configured_storage_path,
    get_metrics_file_path,
    load_config,
    write_json,
)


def build_spark_session(warehouse_path):
    spark = (
        SparkSession.builder.appName("IcebergCompaction")
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
    config = load_config("configs/pipeline_config.yaml")
    source_warehouse = get_configured_storage_path(config, "iceberg_warehouse")
    compacted_warehouse = get_compacted_storage_path(source_warehouse)
    metrics_path = get_metrics_file_path(config, "iceberg_compaction")
    target_file_count = config["compaction"]["target_file_count"]
    table_name = f"local.{config['iceberg']['namespace']}.{config['iceberg']['table_name']}"

    spark = build_spark_session(source_warehouse)
    source_df = spark.table(table_name)
    before_metrics = count_parquet_files(source_warehouse)

    print(
        f"[iceberg-compaction] Rewriting {table_name} into warehouse {compacted_warehouse}"
        f"with target_file_count={target_file_count}"
    )

    source_rows = source_df.collect()
    source_schema = source_df.schema
    spark.stop()

    compacted_spark = build_spark_session(compacted_warehouse)
    compacted_spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS local.{config['iceberg']['namespace']}"
    )
    compacted_spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
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
    compacted_df = compacted_spark.createDataFrame(source_rows, schema=source_schema)
    compacted_df.repartition(target_file_count).writeTo(table_name).overwritePartitions()
    duration_seconds = round(time.time() - start_time, 2)
    after_metrics = count_parquet_files(compacted_warehouse)

    metrics = {
        "format": "iceberg",
        "source_path": source_warehouse,
        "compacted_path": compacted_warehouse,
        "target_file_count": target_file_count,
        "duration_seconds": duration_seconds,
        "before_file_count": before_metrics["data_file_count"],
        "after_file_count": after_metrics["data_file_count"],
        "before_total_size_bytes": before_metrics["total_size_bytes"],
        "after_total_size_bytes": after_metrics["total_size_bytes"],
    }

    write_json(metrics_path, metrics)
    print(f"[iceberg-compaction] Summary: {metrics}")
    compacted_spark.stop()


if __name__ == "__main__":
    main()
