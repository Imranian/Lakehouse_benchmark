import csv
import time

import yaml
from pyspark.sql import SparkSession


def load_config(config_path="configs/pipeline_config.yaml"):
    with open(config_path, "r", encoding="utf-8") as config_file:
        return yaml.safe_load(config_file)


def build_spark_session(warehouse_path):
    spark = (
        SparkSession.builder.appName("BenchmarkQueries")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.sql.catalog.local",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse_path)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def time_query(spark, sql_text):
    start = time.time()
    spark.sql(sql_text).show(10, truncate=False)
    return round(time.time() - start, 4)


def main():
    config = load_config()
    spark = build_spark_session(config["paths"]["iceberg_warehouse"])
    query_runs = config["benchmark"]["query_runs"]
    iceberg_table = (
        f"local.{config['iceberg']['namespace']}.{config['iceberg']['table_name']}"
    )

    sql_by_format = {
        "delta": f"""
            SELECT sensor_id, AVG(temperature) AS avg_temperature
            FROM delta.`{config["paths"]["delta_table"]}`
            GROUP BY sensor_id
        """,
        "hudi": f"""
            SELECT sensor_id, AVG(temperature) AS avg_temperature
            FROM hudi.`{config["paths"]["hudi_table"]}`
            GROUP BY sensor_id
        """,
        "iceberg": f"""
            SELECT sensor_id, AVG(temperature) AS avg_temperature
            FROM {iceberg_table}
            GROUP BY sensor_id
        """,
    }

    output_path = f"{config['metrics']['output_dir']}/query_benchmark_results.csv"
    with open(output_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["format", "run_number", "query_latency_seconds"])

        for table_format, sql_text in sql_by_format.items():
            for run_number in range(1, query_runs + 1):
                latency = time_query(spark, sql_text)
                writer.writerow([table_format, run_number, latency])
                print(
                    f"{table_format} query run {run_number} latency: {latency} seconds"
                )

    spark.stop()
    print(f"Wrote query benchmark results to {output_path}")


if __name__ == "__main__":
    main()
