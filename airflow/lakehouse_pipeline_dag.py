from datetime import datetime
import json
from pathlib import Path

import yaml
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.bash import BashOperator
from airflow.task.trigger_rule import TriggerRule


ROOT_DIR = Path("/home/imran/lakehouse_benchmark")
COMMON_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4"
PIPELINE_CONFIG = yaml.safe_load((ROOT_DIR / "configs/pipeline_config.yaml").read_text(encoding="utf-8"))
WORKLOADS = Variable.get(
    "benchmark_workloads",
    default_var=json.dumps(PIPELINE_CONFIG["workloads"]),
    deserialize_json=True,
)
DEFAULT_WORKLOAD_PROFILE = Variable.get(
    "default_workload_profile",
    default_var="low",
)


def shell(command):
    return f"set -e\ncd {ROOT_DIR}\n{command}\n"


WORKLOAD_PROFILE = '{{ dag_run.conf.get("workload_profile", "' + DEFAULT_WORKLOAD_PROFILE + '") }}'
RUN_LABEL = "run_{{ dag_run.conf.get('workload_profile', '" + DEFAULT_WORKLOAD_PROFILE + "') }}_{{ ts_nodash }}"


def workload_exports():
    lines = [
        f'export WORKLOAD_PROFILE={WORKLOAD_PROFILE}',
        'case "$WORKLOAD_PROFILE" in',
    ]
    for profile_name, profile_config in WORKLOADS.items():
        lines.append(f"  {profile_name})")
        lines.append(f"    export SENSOR_COUNT={profile_config['sensors']}")
        lines.append(f"    export GENERATOR_MODE={profile_config['mode']}")
        if profile_config["mode"] == "burst":
            phase_rates = ",".join(str(rate) for rate in profile_config["phase_rates"])
            phase_durations = ",".join(
                str(duration) for duration in profile_config["phase_durations"]
            )
            total_duration = sum(profile_config["phase_durations"])
            lines.append(f"    export PHASE_RATES={phase_rates}")
            lines.append(f"    export PHASE_DURATIONS={phase_durations}")
            lines.append(f"    export DURATION_SECONDS={total_duration}")
        else:
            lines.append(f"    export EVENT_RATE={profile_config['event_rate']}")
            lines.append(f"    export DURATION_SECONDS={profile_config['duration_seconds']}")
        lines.append("    ;;")
    lines.append('  *) echo "Unsupported workload profile: $WORKLOAD_PROFILE"; exit 1 ;;')
    lines.append("esac")
    return "\n".join(lines)


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
            f"export BENCHMARK_WORKLOAD_PROFILE={WORKLOAD_PROFILE}\n"
            "./scripts/start_kafka.sh all-background"
        ),
    )

    start_generator = BashOperator(
        task_id="start_generator",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            f"export BENCHMARK_WORKLOAD_PROFILE={WORKLOAD_PROFILE}\n"
            f"{workload_exports()}\n"
            "./scripts/start_generator.sh "
        ),
    )

    delta_stream = BashOperator(
        task_id="delta_stream",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            f"export BENCHMARK_WORKLOAD_PROFILE={WORKLOAD_PROFILE}\n"
            f"spark-submit --packages {COMMON_PACKAGES},io.delta:delta-spark_2.12:3.2.0 "
            "spark/stream_delta.py"
        ),
    )

    hudi_stream = BashOperator(
        task_id="hudi_stream",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            f"export BENCHMARK_WORKLOAD_PROFILE={WORKLOAD_PROFILE}\n"
            f"spark-submit --packages {COMMON_PACKAGES},org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 "
            "spark/stream_hudi.py"
        ),
    )

    iceberg_stream = BashOperator(
        task_id="iceberg_stream",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            f"export BENCHMARK_WORKLOAD_PROFILE={WORKLOAD_PROFILE}\n"
            f"spark-submit --packages {COMMON_PACKAGES},org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 "
            "spark/stream_iceberg.py"
        ),
    )

    collect_metrics = BashOperator(
        task_id="collect_metrics",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            f"export BENCHMARK_WORKLOAD_PROFILE={WORKLOAD_PROFILE}\n"
            "python3 metrics/metrics_collector.py"
        ),
    )

    delta_compaction = BashOperator(
        task_id="delta_compaction",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            f"export BENCHMARK_WORKLOAD_PROFILE={WORKLOAD_PROFILE}\n"
            f"spark-submit --packages {COMMON_PACKAGES},io.delta:delta-spark_2.12:3.2.0 "
            "compaction/delta_compact.py"
        ),
    )

    hudi_compaction = BashOperator(
        task_id="hudi_compaction",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            f"export BENCHMARK_WORKLOAD_PROFILE={WORKLOAD_PROFILE}\n"
            f"spark-submit --packages {COMMON_PACKAGES},org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 "
            "compaction/hudi_compact.py"
        ),
    )

    iceberg_compaction = BashOperator(
        task_id="iceberg_compaction",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            f"export BENCHMARK_WORKLOAD_PROFILE={WORKLOAD_PROFILE}\n"
            f"spark-submit --packages {COMMON_PACKAGES},org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 "
            "compaction/iceberg_compact.py"
        ),
    )

    collect_compaction_metrics = BashOperator(
        task_id="collect_compaction_metrics",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            f"export BENCHMARK_WORKLOAD_PROFILE={WORKLOAD_PROFILE}\n"
            "python3 metrics/compaction_metrics_collector.py"
        ),
    )

    query_benchmark_before = BashOperator(
        task_id="query_benchmark_before",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            f"export BENCHMARK_WORKLOAD_PROFILE={WORKLOAD_PROFILE}\n"
            f"spark-submit --packages {COMMON_PACKAGES},io.delta:delta-spark_2.12:3.2.0,org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 "
            "queries/benchmark_queries.py --mode before"
        ),
    )

    prediction_benchmark_before = BashOperator(
        task_id="prediction_benchmark_before",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            f"export BENCHMARK_WORKLOAD_PROFILE={WORKLOAD_PROFILE}\n"
            f"spark-submit --packages {COMMON_PACKAGES},io.delta:delta-spark_2.12:3.2.0,org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 "
            "ml/prediction_benchmark.py --mode before"
        ),
    )

    query_benchmark_after = BashOperator(
        task_id="query_benchmark_after",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            f"export BENCHMARK_WORKLOAD_PROFILE={WORKLOAD_PROFILE}\n"
            f"spark-submit --packages {COMMON_PACKAGES},io.delta:delta-spark_2.12:3.2.0,org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 "
            "queries/benchmark_queries.py --mode after"
        ),
    )

    prediction_benchmark_after = BashOperator(
        task_id="prediction_benchmark_after",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            f"export BENCHMARK_WORKLOAD_PROFILE={WORKLOAD_PROFILE}\n"
            f"spark-submit --packages {COMMON_PACKAGES},io.delta:delta-spark_2.12:3.2.0,org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 "
            "ml/prediction_benchmark.py --mode after"
        ),
    )

    send_results_email = BashOperator(
        task_id="send_results_email",
        bash_command=shell(
            f"export BENCHMARK_RUN_LABEL={RUN_LABEL}\n"
            f"export BENCHMARK_WORKLOAD_PROFILE={WORKLOAD_PROFILE}\n"
            "export SMTP_HOST='{{ var.value.get(\"smtp_host\", \"\") }}'\n"
            "export SMTP_PORT='{{ var.value.get(\"smtp_port\", \"587\") }}'\n"
            "export SMTP_USERNAME='{{ var.value.get(\"smtp_username\", \"\") }}'\n"
            "export SMTP_PASSWORD='{{ var.value.get(\"smtp_password\", \"\") }}'\n"
            "export SMTP_FROM='{{ var.value.get(\"smtp_from\", \"\") }}'\n"
            "export SMTP_TO='{{ var.value.get(\"smtp_to\", \"\") }}'\n"
            "python3 notifications/send_results_email.py"
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
    query_benchmark_after >> prediction_benchmark_after >> send_results_email >> stop_services
