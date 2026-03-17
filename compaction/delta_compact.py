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


def build_spark_session():
    spark = (
        SparkSession.builder.appName("DeltaCompaction")
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
    config = load_config("configs/pipeline_config.yaml")
    source_path = get_configured_storage_path(config, "delta_table")
    compacted_path = get_compacted_storage_path(source_path)
    metrics_path = get_metrics_file_path(config, "delta_compaction")
    target_file_count = config["compaction"]["target_file_count"]

    spark = build_spark_session()
    source_df = spark.read.format("delta").load(source_path)
    before_metrics = count_parquet_files(source_path)

    print(
        f"[delta-compaction] Rewriting {source_path} to {compacted_path}"
        f"with target_file_count={target_file_count}"
    )
    start_time = time.time()
    (
        source_df.repartition(target_file_count)
        .write.format("delta")
        .mode("overwrite")
        .save(compacted_path)
    )
    duration_seconds = round(time.time() - start_time, 2)
    after_metrics = count_parquet_files(compacted_path)

    metrics = {
        "format": "delta",
        "source_path": source_path,
        "compacted_path": compacted_path,
        "target_file_count": target_file_count,
        "duration_seconds": duration_seconds,
        "before_file_count": before_metrics["data_file_count"],
        "after_file_count": after_metrics["data_file_count"],
        "before_total_size_bytes": before_metrics["total_size_bytes"],
        "after_total_size_bytes": after_metrics["total_size_bytes"],
    }

    write_json(metrics_path, metrics)
    print(f"[delta-compaction] Summary: {metrics}")
    spark.stop()


if __name__ == "__main__":
    main()
