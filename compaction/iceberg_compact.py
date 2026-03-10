from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IcebergCompaction").getOrCreate()

spark.sql("CALL system.rewrite_data_files('tables.iceberg')")