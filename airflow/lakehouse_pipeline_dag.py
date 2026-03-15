from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator


ROOT_DIR = Path("/home/imran/lakehouse_benchmark")
COMMON_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4"


with DAG(
    dag_id="lakehouse_benchmark_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lakehouse", "benchmark"],
) as dag:
    start_kafka = BashOperator(
        task_id="start_kafka",
        bash_command=f"cd {ROOT_DIR} && ./scripts/start_kafka.sh all-background",
    )

    start_generator = BashOperator(
        task_id="start_generator",
        bash_command=(
            f"cd {ROOT_DIR} && "
            "DURATION_SECONDS=150 EVENT_RATE=100 SENSOR_COUNT=1000 "
            "./scripts/start_generator.sh"
        ),
    )

    delta_stream = BashOperator(
        task_id="delta_stream",
        bash_command=(
            f"cd {ROOT_DIR} && "
            f"spark-submit --packages {COMMON_PACKAGES},io.delta:delta-spark_2.12:3.2.0 "
            "spark/stream_delta.py"
        ),
    )

    hudi_stream = BashOperator(
        task_id="hudi_stream",
        bash_command=(
            f"cd {ROOT_DIR} && "
            f"spark-submit --packages {COMMON_PACKAGES},org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 "
            "spark/stream_hudi.py"
        ),
    )

    iceberg_stream = BashOperator(
        task_id="iceberg_stream",
        bash_command=(
            f"cd {ROOT_DIR} && "
            f"spark-submit --packages {COMMON_PACKAGES},org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 "
            "spark/stream_iceberg.py"
        ),
    )

    collect_metrics = BashOperator(
        task_id="collect_metrics",
        bash_command=f"cd {ROOT_DIR} && python3 metrics/metrics_collector.py",
    )

    query_benchmark = BashOperator(
        task_id="query_benchmark",
        bash_command=(
            f"cd {ROOT_DIR} && "
            f"spark-submit --packages {COMMON_PACKAGES},io.delta:delta-spark_2.12:3.2.0,org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 "
            "queries/benchmark_queries.py"
        ),
    )

    start_kafka >> start_generator >> [delta_stream, hudi_stream, iceberg_stream]
    [delta_stream, hudi_stream, iceberg_stream] >> collect_metrics >> query_benchmark
