import argparse
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark.stream_common import build_sensor_stream, load_config, write_json


def build_spark_session():
    spark = (
        SparkSession.builder.appName("HudiStreaming")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
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
    spark = build_spark_session()
    parsed_df = build_sensor_stream(
        spark,
        config["kafka"]["bootstrap_servers"],
        config["kafka"]["topic"],
    )

    hudi_options = {
        "hoodie.table.name": config["hudi"]["table_name"],
        "hoodie.datasource.write.table.type": config["hudi"]["table_type"],
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": "event_id",
        "hoodie.datasource.write.precombine.field": "produced_at_ms",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
    }

    def write_hudi_batch(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return

        (
            batch_df.write.format("hudi")
            .options(**hudi_options)
            .mode("append")
            .save(config["paths"]["hudi_table"])
        )
        print(f"Wrote Hudi batch {batch_id}")

    start_time = time.time()
    query = (
        parsed_df.writeStream.foreachBatch(write_hudi_batch)
        .outputMode("append")
        .option("checkpointLocation", config["paths"]["hudi_checkpoint"])
        .start()
    )

    query.awaitTermination(timeout)
    if query.isActive:
        query.stop()

    hudi_table_path = config["paths"]["hudi_table"].replace("file://", "", 1)
    if not Path(hudi_table_path).exists():
        spark.stop()
        raise RuntimeError(
            "Hudi table path was not created. This usually means no Kafka records were "
            "written during the run. Check that Kafka is running and the generator is "
            "producing events."
        )

    duration_seconds = round(time.time() - start_time, 2)
    result_df = spark.read.format("hudi").load(config["paths"]["hudi_table"])
    summary = (
        result_df.selectExpr(
            "COUNT(*) AS row_count",
            "AVG(ingested_at_ms - produced_at_ms) AS avg_ingestion_latency_ms",
            "MAX(ingested_at_ms - produced_at_ms) AS max_ingestion_latency_ms",
            "percentile_approx(ingested_at_ms - produced_at_ms, 0.95) AS p95_ingestion_latency_ms",
        ).first()
    )

    metrics = {
        "format": "hudi",
        "table_path": config["paths"]["hudi_table"],
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

    write_json(config["metrics"]["hudi_ingestion"], metrics)
    spark.stop()
    print(metrics)


if __name__ == "__main__":
    main()
