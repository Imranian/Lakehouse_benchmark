from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.task.trigger_rule import TriggerRule


ROOT_DIR = Path("/home/imran/lakehouse_benchmark")
COMMON_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4"


def shell(command):
    return f"set -e\ncd {ROOT_DIR}\n{command}\n"


RUN_LABEL = "run_{{ ts_nodash }}"


with DAG(
    dag_id="lakehouse_benchmark_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lakehouse", "benchmark"],
) as dag:
    start_kafka = BashOperator(
        task_id="start_kafka",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            "./scripts/start_kafka.sh all-background"
        ),
    )

    start_generator = BashOperator(
        task_id="start_generator",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            "DURATION_SECONDS=150 EVENT_RATE=100 SENSOR_COUNT=1000 "
            "./scripts/start_generator.sh "
        ),
    )

    delta_stream = BashOperator(
        task_id="delta_stream",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            f"spark-submit --packages {COMMON_PACKAGES},io.delta:delta-spark_2.12:3.2.0 "
            "spark/stream_delta.py"
        ),
    )

    hudi_stream = BashOperator(
        task_id="hudi_stream",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            f"spark-submit --packages {COMMON_PACKAGES},org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 "
            "spark/stream_hudi.py"
        ),
    )

    iceberg_stream = BashOperator(
        task_id="iceberg_stream",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            f"spark-submit --packages {COMMON_PACKAGES},org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 "
            "spark/stream_iceberg.py"
        ),
    )

    collect_metrics = BashOperator(
        task_id="collect_metrics",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            "python3 metrics/metrics_collector.py"
        ),
    )

    delta_compaction = BashOperator(
        task_id="delta_compaction",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            f"spark-submit --packages {COMMON_PACKAGES},io.delta:delta-spark_2.12:3.2.0 "
            "compaction/delta_compact.py"
        ),
    )

    hudi_compaction = BashOperator(
        task_id="hudi_compaction",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            f"spark-submit --packages {COMMON_PACKAGES},org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 "
            "compaction/hudi_compact.py"
        ),
    )

    iceberg_compaction = BashOperator(
        task_id="iceberg_compaction",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            f"spark-submit --packages {COMMON_PACKAGES},org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 "
            "compaction/iceberg_compact.py"
        ),
    )

    collect_compaction_metrics = BashOperator(
        task_id="collect_compaction_metrics",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            "python3 metrics/compaction_metrics_collector.py"
        ),
    )

    query_benchmark_before = BashOperator(
        task_id="query_benchmark_before",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            f"spark-submit --packages {COMMON_PACKAGES},io.delta:delta-spark_2.12:3.2.0,org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 "
            "queries/benchmark_queries.py --mode before"
        ),
    )

    prediction_benchmark_before = BashOperator(
        task_id="prediction_benchmark_before",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            f"spark-submit --packages {COMMON_PACKAGES},io.delta:delta-spark_2.12:3.2.0,org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 "
            "ml/prediction_benchmark.py --mode before"
        ),
    )

    query_benchmark_after = BashOperator(
        task_id="query_benchmark_after",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            f"spark-submit --packages {COMMON_PACKAGES},io.delta:delta-spark_2.12:3.2.0,org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 "
            "queries/benchmark_queries.py --mode after"
        ),
    )

    prediction_benchmark_after = BashOperator(
        task_id="prediction_benchmark_after",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            f"spark-submit --packages {COMMON_PACKAGES},io.delta:delta-spark_2.12:3.2.0,org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 "
            "ml/prediction_benchmark.py --mode after"
        ),
    )

    stop_services = BashOperator(
        task_id="stop_services",
        bash_command=shell("./scripts/stop_services.sh"),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start_kafka >> start_generator >> [delta_stream, hudi_stream, iceberg_stream]
    [delta_stream, hudi_stream, iceberg_stream] >> collect_metrics
    collect_metrics >> query_benchmark_before
    query_benchmark_before >> prediction_benchmark_before
    prediction_benchmark_before >> [delta_compaction, hudi_compaction, iceberg_compaction]
    [delta_compaction, hudi_compaction, iceberg_compaction] >> collect_compaction_metrics
    collect_compaction_metrics >> query_benchmark_after
    query_benchmark_after >> prediction_benchmark_after >> stop_services
