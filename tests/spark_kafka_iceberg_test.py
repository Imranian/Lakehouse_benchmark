import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, LongType, StringType, StructType


def build_spark_session(warehouse_path):
    spark = (
        SparkSession.builder.appName("KafkaIcebergTest")
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


def build_sensor_stream(spark, bootstrap_servers, topic):
    schema = (
        StructType()
        .add("sensor_id", StringType())
        .add("temperature", DoubleType())
        .add("pressure", DoubleType())
        .add("vibration", DoubleType())
        .add("timestamp", LongType())
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    return (
        kafka_df.selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), schema).alias("data"))
        .select(
            col("data.sensor_id").alias("sensor_id"),
            col("data.temperature").alias("temperature"),
            col("data.pressure").alias("pressure"),
            col("data.vibration").alias("vibration"),
            col("data.timestamp").alias("event_ts"),
        )
        .filter("sensor_id IS NOT NULL")
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="sensor_topic")
    parser.add_argument(
        "--warehouse-path",
        default="file:///home/imran/lakehouse_test/iceberg_warehouse",
    )
    parser.add_argument(
        "--checkpoint-path",
        default="file:///home/imran/lakehouse_test/checkpoints/iceberg",
    )
    parser.add_argument("--namespace", default="benchmark")
    parser.add_argument("--table-name", default="sensor_iceberg_test")
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()

    spark = build_spark_session(args.warehouse_path)
    parsed_df = build_sensor_stream(spark, args.bootstrap, args.topic)
    full_table_name = f"local.{args.namespace}.{args.table_name}"

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS local.{args.namespace}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            sensor_id STRING,
            temperature DOUBLE,
            pressure DOUBLE,
            vibration DOUBLE,
            event_ts BIGINT
        )
        USING iceberg
        """
    )

    query = (
        parsed_df.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint_path)
        .toTable(full_table_name)
    )

    query.awaitTermination(args.timeout)
    if query.isActive:
        query.stop()

    result_df = spark.table(full_table_name)
    row_count = result_df.count()

    print(f"Iceberg rows written: {row_count}")
    result_df.orderBy(col("event_ts").desc()).show(10, truncate=False)

    spark.stop()

    if row_count == 0:
        print("No rows were written to Iceberg. Make sure the generator is running.")
        sys.exit(1)


if __name__ == "__main__":
    main()
