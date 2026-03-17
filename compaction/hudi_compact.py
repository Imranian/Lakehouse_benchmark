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
        SparkSession.builder.appName("HudiCompaction")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    config = load_config("configs/pipeline_config.yaml")
    source_path = get_configured_storage_path(config, "hudi_table")
    compacted_path = get_compacted_storage_path(source_path)
    metrics_path = get_metrics_file_path(config, "hudi_compaction")
    target_file_count = config["compaction"]["target_file_count"]

    spark = build_spark_session()
    source_df = spark.read.format("hudi").load(source_path)
    before_metrics = count_parquet_files(source_path)

    hudi_options = {
        "hoodie.table.name": f"{config['hudi']['table_name']}_compacted",
        "hoodie.datasource.write.table.type": config["hudi"]["table_type"],
        "hoodie.datasource.write.operation": "bulk_insert",
        "hoodie.datasource.write.recordkey.field": "event_id",
        "hoodie.datasource.write.precombine.field": "produced_at_ms",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
    }

    print(
        f"[hudi-compaction] Rewriting {source_path} to {compacted_path} "
        f"with target_file_count={target_file_count}"
    )
    start_time = time.time()
    (
        source_df.repartition(target_file_count)
        .write.format("hudi")
        .options(**hudi_options)
        .mode("overwrite")
        .save(compacted_path)
    )
    duration_seconds = round(time.time() - start_time, 2)
    after_metrics = count_parquet_files(compacted_path)

    metrics = {
        "format": "hudi",
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
    print(f"[hudi-compaction] Summary: {metrics}")
    spark.stop()


if __name__ == "__main__":
    main()
