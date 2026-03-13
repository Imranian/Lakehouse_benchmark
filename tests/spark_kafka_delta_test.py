from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType

# Create Spark session with Delta support
spark = SparkSession.builder \
    .appName("KafkaDeltaTest") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for sensor data
schema = StructType() \
    .add("sensor_id", StringType()) \
    .add("temperature", DoubleType())

# Read stream from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_topic") \
    .option("startingOffsets", "latest") \
    .load()

# Convert Kafka value from binary to string
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json")

# Parse JSON into columns
parsed_df = json_df \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*") \
    .filter("sensor_id IS NOT NULL")

# Write streaming data to Delta table (LOCAL filesystem)
query = parsed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "file:///home/imran/checkpoint_delta") \
    .start("file:///home/imran/delta_table")

query.awaitTermination()