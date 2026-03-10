from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HudiStreaming") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
    .option("subscribe","sensor_topic") \
    .load()

df.writeStream \
    .format("org.apache.hudi") \
    .option("hoodie.table.name","sensor_hudi") \
    .option("checkpointLocation","checkpoints/hudi") \
    .start("tables/hudi")