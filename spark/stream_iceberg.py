from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IcebergStreaming") \
    .config("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type","hadoop") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
    .option("subscribe","sensor_topic") \
    .load()

df.writeStream \
    .format("iceberg") \
    .option("checkpointLocation","checkpoints/iceberg") \
    .start("tables/iceberg")