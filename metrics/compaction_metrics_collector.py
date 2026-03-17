import csv
import json
import sys
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark.stream_common import get_metrics_dir, get_metrics_file_path


def load_config(config_path="configs/pipeline_config.yaml"):
    with open(config_path, "r", encoding="utf-8") as config_file:
        return yaml.safe_load(config_file)


def load_json(path):
    with open(path, "r", encoding="utf-8") as file_handle:
        return json.load(file_handle)


def main():
    config = load_config()
    metrics_dir = get_metrics_dir(config)
    metrics_dir.mkdir(parents=True, exist_ok=True)

    compaction_metrics = {
        "delta": load_json(get_metrics_file_path(config, "delta_compaction")),
        "hudi": load_json(get_metrics_file_path(config, "hudi_compaction")),
        "iceberg": load_json(get_metrics_file_path(config, "iceberg_compaction")),
    }

    output_path = metrics_dir / "compaction_results.csv"
    with open(output_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            [
                "format",
                "duration_seconds",
                "target_file_count",
                "before_file_count",
                "after_file_count",
                "before_total_size_bytes",
                "after_total_size_bytes",
            ]
        )

        for table_format in ["delta", "hudi", "iceberg"]:
            writer.writerow(
                [
                    table_format,
                    compaction_metrics[table_format]["duration_seconds"],
                    compaction_metrics[table_format]["target_file_count"],
                    compaction_metrics[table_format]["before_file_count"],
                    compaction_metrics[table_format]["after_file_count"],
                    compaction_metrics[table_format]["before_total_size_bytes"],
                    compaction_metrics[table_format]["after_total_size_bytes"],
                ]
            )

    print(f"Wrote compaction benchmark metrics to {output_path}")


if __name__ == "__main__":
    main()
