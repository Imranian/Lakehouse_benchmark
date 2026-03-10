from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    "lakehouse_benchmark_pipeline",
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False
) as dag:

    start_kafka = BashOperator(
        task_id="start_kafka",
        bash_command="bash scripts/start_kafka.sh"
    )

    start_generator = BashOperator(
        task_id="sensor_generator",
        bash_command="python generator/sensor_stream_generator.py"
    )

    start_stream = BashOperator(
        task_id="spark_streaming",
        bash_command="spark-submit spark/stream_delta.py"
    )

    compaction = BashOperator(
        task_id="compaction",
        bash_command="python compaction/delta_compact.py"
    )

    queries = BashOperator(
        task_id="benchmark_queries",
        bash_command="python queries/benchmark_queries.py"
    )

    start_kafka >> start_generator >> start_stream >> compaction >> queries