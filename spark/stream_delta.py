from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DeltaStreaming") \
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
    .option("subscribe","sensor_topic") \
    .load()

query = df.writeStream \
    .format("delta") \
    .option("checkpointLocation","checkpoints/delta") \
    .start("tables/delta")

query.awaitTermination()