from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DeltaCompaction").getOrCreate()

spark.sql("OPTIMIZE delta.`tables/delta`")