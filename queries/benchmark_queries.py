from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("BenchmarkQueries").getOrCreate()

start = time.time()

df = spark.sql("""
SELECT sensor_id, AVG(temperature)
FROM delta.`tables/delta`
GROUP BY sensor_id
""")

df.show()

latency = time.time() - start

print("Query latency:", latency)