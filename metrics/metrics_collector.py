import csv
import json
import os
from pathlib import Path

import yaml


def load_config(config_path="configs/pipeline_config.yaml"):
    with open(config_path, "r", encoding="utf-8") as config_file:
        return yaml.safe_load(config_file)


def uri_to_path(uri_or_path):
    if uri_or_path.startswith("file://"):
        return uri_or_path.replace("file://", "", 1)
    return uri_or_path


def load_json(path):
    with open(path, "r", encoding="utf-8") as file_handle:
        return json.load(file_handle)


def collect_file_metrics(base_path, small_file_threshold_bytes):
    parquet_files = []
    total_bytes = 0

    for root, _, files in os.walk(base_path):
        for file_name in files:
            if not file_name.endswith(".parquet"):
                continue
            file_path = os.path.join(root, file_name)
            file_size = os.path.getsize(file_path)
            parquet_files.append(file_size)
            total_bytes += file_size

    return {
        "data_file_count": len(parquet_files),
        "small_file_count": sum(
            1 for file_size in parquet_files if file_size < small_file_threshold_bytes
        ),
        "total_size_bytes": total_bytes,
    }


def main():
    config = load_config()
    metrics_dir = Path(config["metrics"]["output_dir"])
    metrics_dir.mkdir(parents=True, exist_ok=True)
    threshold_bytes = config["benchmark"]["small_file_threshold_mb"] * 1024 * 1024

    ingestion_metrics = {
        "delta": load_json(config["metrics"]["delta_ingestion"]),
        "hudi": load_json(config["metrics"]["hudi_ingestion"]),
        "iceberg": load_json(config["metrics"]["iceberg_ingestion"]),
    }

    storage_metrics = {
        "delta": collect_file_metrics(
            uri_to_path(config["paths"]["delta_table"]), threshold_bytes
        ),
        "hudi": collect_file_metrics(
            uri_to_path(config["paths"]["hudi_table"]), threshold_bytes
        ),
        "iceberg": collect_file_metrics(
            uri_to_path(config["paths"]["iceberg_warehouse"]), threshold_bytes
        ),
    }

    output_path = metrics_dir / "benchmark_results.csv"
    with open(output_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            [
                "format",
                "row_count",
                "duration_seconds",
                "avg_ingestion_latency_ms",
                "p95_ingestion_latency_ms",
                "max_ingestion_latency_ms",
                "write_throughput_rows_per_sec",
                "data_file_count",
                "small_file_count",
                "total_size_bytes",
            ]
        )

        for table_format in ["delta", "hudi", "iceberg"]:
            writer.writerow(
                [
                    table_format,
                    ingestion_metrics[table_format]["row_count"],
                    ingestion_metrics[table_format]["duration_seconds"],
                    ingestion_metrics[table_format]["avg_ingestion_latency_ms"],
                    ingestion_metrics[table_format]["p95_ingestion_latency_ms"],
                    ingestion_metrics[table_format]["max_ingestion_latency_ms"],
                    ingestion_metrics[table_format]["write_throughput_rows_per_sec"],
                    storage_metrics[table_format]["data_file_count"],
                    storage_metrics[table_format]["small_file_count"],
                    storage_metrics[table_format]["total_size_bytes"],
                ]
            )

    print(f"Wrote benchmark metrics to {output_path}")


if __name__ == "__main__":
    main()
