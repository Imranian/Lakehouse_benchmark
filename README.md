# Lakehouse Streaming Benchmark

An end-to-end benchmark project for comparing **Delta Lake**, **Apache Hudi**, and **Apache Iceberg** in a real-time data pipeline.

Instead of evaluating these formats only on static analytical datasets, this project pushes them through a more practical workflow:

- generate IIoT-style sensor events
- publish them to Kafka
- ingest them with Spark Structured Streaming
- write them into Delta, Hudi, and Iceberg in parallel
- compact the resulting files
- benchmark query performance before and after compaction
- benchmark a simple downstream ML workload

The goal is to help data engineers, platform teams, and students understand how these lakehouse formats behave under **live ingestion**, **file-fragmentation pressure**, and **downstream analytical use**.

## What This Project Shows

- How ingestion latency changes across `low`, `medium`, and `burst` stream workloads
- Which format captures more rows during the same benchmark window
- How much small-file growth each format creates
- How much compaction costs and how much it helps
- How downstream query and ML behavior change after compaction

## High-Level Flow

![System architecture](papers/Model_Arch.drawio.png)

The pipeline is orchestrated with **Airflow**, uses **Kafka** as the event backbone, and runs **format-specific Spark jobs** so each sink can be tuned independently.

## Benchmark Snapshots

### Ingestion Overview

![Ingestion overview](analysis/paper_figures/figure_ingestion_overview.png)

### Compaction and Query Overview

![Compaction and query overview](analysis/paper_figures/figure_query_compaction_overview.png)

### Query Trends Before vs After Compaction

![Query run trends](analysis/paper_figures/figure_query_run_trends.png)

### Predictive Benchmark Overview

![Prediction overview](analysis/paper_figures/figure_prediction_overview.png)

## Why It’s Useful

This project is useful if you are trying to answer questions like:

- Which lakehouse format keeps up best with a live stream?
- Which one creates the most small files?
- Is compaction just maintenance overhead, or does it materially improve query speed?
- Does the storage choice still matter when the data is used for downstream ML?
- How can I build a reproducible benchmark instead of relying on feature checklists or vendor claims?

For a **data engineer**, the benchmark helps reason about ingestion stability, compaction cost, and operational tradeoffs.

For a **data architect**, it helps connect storage-format choice with analytics, notebook use, and ML-readiness across the same platform.

## Workload Profiles

The benchmark currently supports these workload styles through Airflow variables and DAG config:

| Profile | Pattern | Purpose |
| --- | --- | --- |
| `low` | constant low-rate stream | baseline comparison |
| `medium` | sustained higher-rate stream | scalability and degradation behavior |
| `burst` | low → high → low | spike/backlog behavior |
| `high` | aggressive stress load | optional stress testing |

Default workload definitions live in [configs/pipeline_config.yaml](configs/pipeline_config.yaml), and can also be overridden from Airflow Variables.

## Project Structure

```text
lakehouse_benchmark
├── airflow/        Airflow DAG for orchestration
├── analysis/       Plot generation and paper figures
├── compaction/     Format-specific compaction jobs
├── configs/        Pipeline and workload configuration
├── generator/      Synthetic sensor stream generator
├── metrics/        Metrics aggregation and run outputs
├── ml/             Prediction benchmark
├── notifications/  Email results utility
├── papers/         Literature, architecture image, review deck
├── queries/        Query benchmark jobs
├── scripts/        Kafka/generator/service helper scripts
├── spark/          Delta, Hudi, Iceberg streaming jobs
└── tests/          Smoke tests for each table format
```

## Main Components

### 1. Synthetic Sensor Generator

The generator emits structured IIoT-like records with fields such as:

- `event_id`
- `sensor_id`
- `temperature`
- `pressure`
- `vibration`
- `timestamp`
- `produced_at_ms`

This makes it possible to measure ingestion latency later as:

`ingested_at_ms - produced_at_ms`

### 2. Kafka + Spark Streaming

Kafka provides the event stream, and Spark Structured Streaming reads the topic and writes the same event flow into:

- Delta Lake
- Apache Hudi
- Apache Iceberg

Each sink has its own job so format-specific behavior can be observed and tuned independently.

### 3. Metrics and Compaction

For each run, the project records:

- row count
- write throughput
- average / p95 / max ingestion latency
- data file count
- total data size
- compaction duration
- query latency before compaction
- query latency after compaction

### 4. Prediction Benchmark

After ingestion, the project runs a simple downstream ML benchmark using synthetic anomaly labels and compares:

- load time
- training time
- inference time
- prediction throughput
- accuracy / precision / recall / F1 / AUC

This extends the benchmark from storage behavior into downstream usability.

## Quick Start

### 1. Start Airflow

```bash
source venv/bin/activate
export AIRFLOW_HOME=/home/imran/lakehouse_benchmark/.airflow
export AIRFLOW__CORE__DAGS_FOLDER=/home/imran/lakehouse_benchmark/airflow
airflow standalone
```

### 2. Open the Airflow UI

Enable the DAG:

- `lakehouse_benchmark_pipeline`

### 3. Trigger a Run

You can trigger the DAG with a workload profile like:

```json
{"workload_profile": "low"}
```

or:

```json
{"workload_profile": "medium"}
```

or:

```json
{"workload_profile": "burst"}
```
#### 3.1 Airflow variables used
```
benchmark_workloads: {"low": {"mode": "constant", "event_rate": 100, "duration_seconds": 150, "sensors": 1000}, "medium": {"mode": "constant", "event_rate": 500, "duration_seconds": 250, "sensors": 1000}, "high": {"mode": "constant", "event_rate": 5000, "duration_seconds": 150, "sensors": 1000}, "burst": {"mode": "burst", "sensors": 1000, "phase_rates": [100, 2000, 100], "phase_durations": [45, 60, 45]}}
```
```
smtp_from: from_mail
	
smtp_host: smtp.gmail.com

smtp_password: Google App password

smtp_port: 587
	
smtp_to: to_mail

smtp_username: name 
```

### 4. Review the Output

Each run writes results into a run-scoped folder such as:

```text
metrics/results/run_low_20260317T091045/
```

Typical files include:

- `benchmark_results.csv`
- `compaction_results.csv`
- `query_benchmark_before_compaction.csv`
- `query_benchmark_after_compaction.csv`
- `prediction_benchmark_before_compaction.csv`
- `prediction_benchmark_after_compaction.csv`

## Example Findings From This Project

Across the completed benchmark runs so far, a few patterns appear consistently:

- **Iceberg** often ingests fastest and captures the most rows under pressure.
- **Iceberg** also tends to create the most small files before compaction.
- **Hudi** often keeps file counts lower, but may require more careful tuning under heavier streaming load.
- **Delta** is usually the most balanced option across ingestion, compaction, and downstream use.
- **Compaction** matters a lot for read behavior, especially when file fragmentation is high.
- **Storage format choice** still affects downstream ML runtime even when model quality remains similar.

These are not absolute claims for every environment, but they are useful comparative observations for a controlled local benchmark.

## Rebuilding the Figures

To regenerate the analysis figures:

```bash
source venv/bin/activate
python analysis/data_analysis.py
```

Generated figures are written to:

- [analysis/paper_figures](analysis/paper_figures)

## Tests

The project includes format-specific smoke tests in:

- [tests/spark_kafka_delta_test.py](tests/spark_kafka_delta_test.py)
- [tests/spark_kafka_hudi_test.py](tests/spark_kafka_hudi_test.py)
- [tests/spark_kafka_iceberg_test.py](tests/spark_kafka_iceberg_test.py)

These are useful for validating the basic Kafka → Spark → lakehouse write path before running the full DAG.


## Future Scope

- run the benchmark on a distributed cluster instead of a local WSL environment
- add more workload profiles or longer-duration experiments
- compare native optimization features more deeply
- extend the downstream ML benchmark with additional models
- convert key results into a dashboard or notebook summary

---
