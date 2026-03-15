import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, LongType, StringType, StructType


def build_spark_session():
    spark = (
        SparkSession.builder.appName("KafkaHudiTest")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
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
        "--output-path",
        default="file:///home/imran/lakehouse_test/hudi",
    )
    parser.add_argument(
        "--checkpoint-path",
        default="file:///home/imran/lakehouse_test/checkpoints/hudi",
    )
    parser.add_argument("--table-name", default="sensor_hudi_test")
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()

    spark = build_spark_session()
    parsed_df = build_sensor_stream(spark, args.bootstrap, args.topic)

    hudi_options = {
        "hoodie.table.name": args.table_name,
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": "sensor_id",
        "hoodie.datasource.write.precombine.field": "event_ts",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
    }

    def write_hudi_batch(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return

        (
            batch_df.write.format("hudi")
            .options(**hudi_options)
            .mode("append")
            .save(args.output_path)
        )
        print(f"Wrote Hudi batch {batch_id}")

    query = (
        parsed_df.writeStream.foreachBatch(write_hudi_batch)
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint_path)
        .start()
    )

    query.awaitTermination(args.timeout)
    if query.isActive:
        query.stop()

    result_df = spark.read.format("hudi").load(args.output_path)
    row_count = result_df.count()

    print(f"Hudi rows written: {row_count}")
    result_df.orderBy(col("event_ts").desc()).show(10, truncate=False)

    spark.stop()

    if row_count == 0:
        print("No rows were written to Hudi. Make sure the generator is running.")
        sys.exit(1)


if __name__ == "__main__":
    main()
