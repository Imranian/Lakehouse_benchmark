import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]


RUN_IDS = [
    "run_low_20260317T091045",
    "run_medium_20260317T100003",
    "run_burst_20260317T093043",
]

FORMAT_ORDER = ["delta", "hudi", "iceberg"]
WORKLOAD_ORDER = ["low", "medium", "burst"]
MODEL_ORDER = ["logistic_regression", "random_forest"]
HATCHES = {"delta": "//", "hudi": "\\\\", "iceberg": "xx"}


def extract_workload(run_id):
    for workload in WORKLOAD_ORDER:
        if run_id.startswith(f"run_{workload}_"):
            return workload
    return "unknown"


def load_run_csv(base_dir, run_id, file_name):
    file_path = Path(base_dir) / run_id / file_name
    df = pd.read_csv(file_path)
    df["run_id"] = run_id
    df["workload"] = extract_workload(run_id)
    return df


def load_all_results(base_dir, run_ids):
    ingestion = pd.concat(
        [load_run_csv(base_dir, run_id, "benchmark_results.csv") for run_id in run_ids],
        ignore_index=True,
    )
    compaction = pd.concat(
        [load_run_csv(base_dir, run_id, "compaction_results.csv") for run_id in run_ids],
        ignore_index=True,
    )
    query_before = pd.concat(
        [
            load_run_csv(base_dir, run_id, "query_benchmark_before_compaction.csv")
            for run_id in run_ids
        ],
        ignore_index=True,
    )
    query_after = pd.concat(
        [
            load_run_csv(base_dir, run_id, "query_benchmark_after_compaction.csv")
            for run_id in run_ids
        ],
        ignore_index=True,
    )
    prediction_before = pd.concat(
        [
            load_run_csv(base_dir, run_id, "prediction_benchmark_before_compaction.csv")
            for run_id in run_ids
        ],
        ignore_index=True,
    )
    prediction_after = pd.concat(
        [
            load_run_csv(base_dir, run_id, "prediction_benchmark_after_compaction.csv")
            for run_id in run_ids
        ],
        ignore_index=True,
    )
    return ingestion, compaction, query_before, query_after, prediction_before, prediction_after


def aggregate_mean_std(df, group_cols, value_col):
    summary = (
        df.groupby(group_cols, as_index=False)
        .agg(mean=(value_col, "mean"), std=(value_col, "std"))
    )
    summary["std"] = summary["std"].fillna(0)
    return summary


def style_axes(ax, title, ylabel):
    ax.set_title(title, fontsize=10)
    ax.set_ylabel(ylabel, fontsize=9)
    ax.grid(axis="y", linestyle="--", linewidth=0.6, alpha=0.8)
    ax.set_axisbelow(True)
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)


def save_figure(fig, output_dir, file_stem):
    output_dir.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(output_dir / f"{file_stem}.png", dpi=300, bbox_inches="tight")
    fig.savefig(output_dir / f"{file_stem}.pdf", bbox_inches="tight")
    plt.close(fig)


def plot_single_metric(summary, output_dir, metric_col, title, ylabel, file_stem):
    fig, ax = plt.subplots(figsize=(5.5, 3.5))
    x_positions = range(len(FORMAT_ORDER))
    values = []
    errors = []

    for table_format in FORMAT_ORDER:
        row = summary[summary["format"] == table_format].iloc[0]
        values.append(row["mean"])
        errors.append(row["std"])

    bars = ax.bar(
        list(x_positions),
        values,
        yerr=errors,
        color="white",
        edgecolor="black",
        linewidth=1.0,
        capsize=4,
    )
    for bar, table_format in zip(bars, FORMAT_ORDER):
        bar.set_hatch(HATCHES[table_format])

    ax.set_xticks(list(x_positions), FORMAT_ORDER)
    style_axes(ax, title, ylabel)
    save_figure(fig, output_dir, file_stem)


def plot_workload_metric(summary, output_dir, title, ylabel, file_stem):
    fig, ax = plt.subplots(figsize=(7.2, 4.0))
    width = 0.22
    positions = list(range(len(WORKLOAD_ORDER)))

    for format_index, table_format in enumerate(FORMAT_ORDER):
        offsets = [p + (format_index - 1) * width for p in positions]
        values = []
        errors = []
        for workload in WORKLOAD_ORDER:
            row = summary[
                (summary["workload"] == workload) & (summary["format"] == table_format)
            ].iloc[0]
            values.append(row["mean"])
            errors.append(row["std"])

        bars = ax.bar(
            offsets,
            values,
            width=width,
            yerr=errors,
            color="white",
            edgecolor="black",
            linewidth=1.0,
            capsize=4,
            label=table_format,
        )
        for bar in bars:
            bar.set_hatch(HATCHES[table_format])

    ax.set_xticks(positions, WORKLOAD_ORDER)
    ax.legend(frameon=False, fontsize=8)
    style_axes(ax, title, ylabel)
    save_figure(fig, output_dir, file_stem)


def plot_query_comparison(query_before, query_after, output_dir):
    before_summary = aggregate_mean_std(query_before, ["format"], "query_latency_seconds")
    after_summary = aggregate_mean_std(query_after, ["format"], "query_latency_seconds")

    fig, ax = plt.subplots(figsize=(6.4, 3.8))
    width = 0.35
    positions = list(range(len(FORMAT_ORDER)))

    before_values = [before_summary[before_summary["format"] == fmt]["mean"].iloc[0] for fmt in FORMAT_ORDER]
    before_errors = [before_summary[before_summary["format"] == fmt]["std"].iloc[0] for fmt in FORMAT_ORDER]
    after_values = [after_summary[after_summary["format"] == fmt]["mean"].iloc[0] for fmt in FORMAT_ORDER]
    after_errors = [after_summary[after_summary["format"] == fmt]["std"].iloc[0] for fmt in FORMAT_ORDER]

    before_bars = ax.bar(
        [p - width / 2 for p in positions],
        before_values,
        width=width,
        yerr=before_errors,
        color="white",
        edgecolor="black",
        linewidth=1.0,
        capsize=4,
        label="Before Compaction",
    )
    after_bars = ax.bar(
        [p + width / 2 for p in positions],
        after_values,
        width=width,
        yerr=after_errors,
        color="0.75",
        edgecolor="black",
        linewidth=1.0,
        capsize=4,
        label="After Compaction",
    )

    for bar, table_format in zip(before_bars, FORMAT_ORDER):
        bar.set_hatch(HATCHES[table_format])
    for bar in after_bars:
        bar.set_hatch("..")

    ax.set_xticks(positions, FORMAT_ORDER)
    ax.legend(frameon=False, fontsize=8)
    style_axes(ax, "Query Latency Before vs After Compaction", "Latency (seconds)")
    save_figure(fig, output_dir, "query_latency_before_after_compaction")


def plot_prediction_metric(prediction_df, metric_col, title, ylabel, file_stem, output_dir):
    summary = aggregate_mean_std(prediction_df, ["format", "model"], metric_col)
    fig, ax = plt.subplots(figsize=(7.0, 4.0))
    width = 0.35
    positions = list(range(len(FORMAT_ORDER)))

    for model_index, model_name in enumerate(MODEL_ORDER):
        offsets = [p + (model_index - 0.5) * width for p in positions]
        values = []
        errors = []
        for table_format in FORMAT_ORDER:
            row = summary[
                (summary["format"] == table_format) & (summary["model"] == model_name)
            ].iloc[0]
            values.append(row["mean"])
            errors.append(row["std"])

        bars = ax.bar(
            offsets,
            values,
            width=width,
            yerr=errors,
            color="white" if model_name == "logistic_regression" else "0.75",
            edgecolor="black",
            linewidth=1.0,
            capsize=4,
            label=model_name.replace("_", " ").title(),
        )
        for bar, table_format in zip(bars, FORMAT_ORDER):
            bar.set_hatch(HATCHES[table_format] if model_name == "logistic_regression" else "..")

    ax.set_xticks(positions, FORMAT_ORDER)
    ax.legend(frameon=False, fontsize=8)
    style_axes(ax, title, ylabel)
    save_figure(fig, output_dir, file_stem)


def plot_prediction_metric_by_workload(
    prediction_df,
    metric_col,
    model_name,
    title,
    ylabel,
    file_stem,
    output_dir,
):
    filtered = prediction_df[prediction_df["model"] == model_name]
    summary = aggregate_mean_std(filtered, ["workload", "format"], metric_col)
    plot_workload_metric(summary, output_dir, title, ylabel, file_stem)


def add_row_capture_ratio(ingestion):
    run_max = ingestion.groupby("run_id")["row_count"].transform("max")
    enriched = ingestion.copy()
    enriched["row_capture_ratio"] = enriched["row_count"] / run_max
    return enriched


def write_summary_tables(output_dir, ingestion, compaction, query_before, query_after, prediction_after):
    output_dir.mkdir(parents=True, exist_ok=True)

    ingestion = add_row_capture_ratio(ingestion)
    ingestion_summary = aggregate_mean_std(
        ingestion, ["workload", "format"], "avg_ingestion_latency_ms"
    ).rename(columns={"mean": "avg_ingestion_latency_ms_mean", "std": "avg_ingestion_latency_ms_std"})
    throughput_summary = aggregate_mean_std(
        ingestion, ["workload", "format"], "write_throughput_rows_per_sec"
    ).rename(columns={"mean": "throughput_mean", "std": "throughput_std"})
    file_summary = aggregate_mean_std(
        ingestion, ["workload", "format"], "data_file_count"
    ).rename(columns={"mean": "file_count_mean", "std": "file_count_std"})
    row_summary = aggregate_mean_std(
        ingestion, ["workload", "format"], "row_count"
    ).rename(columns={"mean": "row_count_mean", "std": "row_count_std"})
    capture_summary = aggregate_mean_std(
        ingestion, ["workload", "format"], "row_capture_ratio"
    ).rename(columns={"mean": "row_capture_ratio_mean", "std": "row_capture_ratio_std"})

    combined_ingestion = (
        ingestion_summary
        .merge(throughput_summary, on=["workload", "format"])
        .merge(file_summary, on=["workload", "format"])
        .merge(row_summary, on=["workload", "format"])
        .merge(capture_summary, on=["workload", "format"])
    )
    combined_ingestion.to_csv(output_dir / "summary_ingestion_by_workload.csv", index=False)

    compaction_summary = aggregate_mean_std(compaction, ["workload", "format"], "duration_seconds")
    compaction_summary.to_csv(output_dir / "summary_compaction_by_workload.csv", index=False)

    query_before_summary = aggregate_mean_std(query_before, ["workload", "format"], "query_latency_seconds")
    query_after_summary = aggregate_mean_std(query_after, ["workload", "format"], "query_latency_seconds")
    query_before_summary["phase"] = "before"
    query_after_summary["phase"] = "after"
    pd.concat([query_before_summary, query_after_summary], ignore_index=True).to_csv(
        output_dir / "summary_query_by_workload.csv", index=False
    )

    prediction_summary = aggregate_mean_std(
        prediction_after, ["workload", "format", "model"], "accuracy"
    ).rename(columns={"mean": "accuracy_mean", "std": "accuracy_std"})
    prediction_summary.to_csv(output_dir / "summary_prediction_by_workload.csv", index=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--results-dir",
        default="metrics/results",
        help="Base results directory containing run folders.",
    )
    parser.add_argument(
        "--run-ids",
        nargs="+",
        default=RUN_IDS,
        help="Run IDs to include in the paper figures.",
    )
    parser.add_argument(
        "--output-dir",
        default="analysis/paper_figures",
        help="Directory where figure files will be saved.",
    )
    args = parser.parse_args()

    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.size": 9,
            "axes.edgecolor": "black",
            "axes.linewidth": 0.8,
            "savefig.facecolor": "white",
            "figure.facecolor": "white",
        }
    )

    results_dir = Path(args.results_dir)
    if not results_dir.is_absolute():
        results_dir = PROJECT_ROOT / results_dir

    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = PROJECT_ROOT / output_dir

    (
        ingestion,
        compaction,
        query_before,
        query_after,
        prediction_before,
        prediction_after,
    ) = load_all_results(results_dir, args.run_ids)
    ingestion = add_row_capture_ratio(ingestion)

    plot_workload_metric(
        aggregate_mean_std(ingestion, ["workload", "format"], "avg_ingestion_latency_ms"),
        output_dir,
        "Average Ingestion Latency by Workload",
        "Latency (ms)",
        "ingestion_latency_by_workload",
    )
    plot_workload_metric(
        aggregate_mean_std(ingestion, ["workload", "format"], "write_throughput_rows_per_sec"),
        output_dir,
        "Write Throughput by Workload",
        "Rows per second",
        "write_throughput_by_workload",
    )
    plot_workload_metric(
        aggregate_mean_std(ingestion, ["workload", "format"], "row_count"),
        output_dir,
        "Captured Row Count by Workload",
        "Rows captured",
        "row_count_by_workload",
    )
    plot_workload_metric(
        aggregate_mean_std(ingestion, ["workload", "format"], "row_capture_ratio"),
        output_dir,
        "Row Capture Ratio by Workload",
        "Capture ratio (best format = 1.0)",
        "row_capture_ratio_by_workload",
    )
    plot_workload_metric(
        aggregate_mean_std(ingestion, ["workload", "format"], "data_file_count"),
        output_dir,
        "Small File Generation by Workload",
        "Number of data files",
        "small_file_generation_by_workload",
    )
    plot_workload_metric(
        aggregate_mean_std(compaction, ["workload", "format"], "duration_seconds"),
        output_dir,
        "Compaction Duration by Workload",
        "Duration (seconds)",
        "compaction_duration_by_workload",
    )
    plot_workload_metric(
        aggregate_mean_std(query_before, ["workload", "format"], "query_latency_seconds"),
        output_dir,
        "Query Latency Before Compaction by Workload",
        "Latency (seconds)",
        "query_latency_before_by_workload",
    )
    plot_workload_metric(
        aggregate_mean_std(query_after, ["workload", "format"], "query_latency_seconds"),
        output_dir,
        "Query Latency After Compaction by Workload",
        "Latency (seconds)",
        "query_latency_after_by_workload",
    )
    plot_prediction_metric(
        prediction_before[prediction_before["workload"].isin(WORKLOAD_ORDER)],
        "accuracy",
        "Prediction Accuracy Before Compaction",
        "Accuracy",
        "prediction_accuracy_before_compaction",
        output_dir,
    )
    plot_prediction_metric(
        prediction_after[prediction_after["workload"].isin(WORKLOAD_ORDER)],
        "accuracy",
        "Prediction Accuracy After Compaction",
        "Accuracy",
        "prediction_accuracy_after_compaction",
        output_dir,
    )
    plot_prediction_metric(
        prediction_after[prediction_after["workload"].isin(WORKLOAD_ORDER)],
        "inference_time_seconds",
        "Prediction Inference Time After Compaction",
        "Inference time (seconds)",
        "prediction_inference_after_compaction",
        output_dir,
    )
    plot_prediction_metric_by_workload(
        prediction_after,
        "accuracy",
        "random_forest",
        "Random Forest Accuracy After Compaction by Workload",
        "Accuracy",
        "random_forest_accuracy_after_by_workload",
        output_dir,
    )
    plot_prediction_metric_by_workload(
        prediction_after,
        "inference_time_seconds",
        "random_forest",
        "Random Forest Inference Time After Compaction by Workload",
        "Inference time (seconds)",
        "random_forest_inference_after_by_workload",
        output_dir,
    )

    write_summary_tables(
        output_dir,
        ingestion,
        compaction,
        query_before,
        query_after,
        prediction_after,
    )

    print(f"Saved paper-ready figures to {output_dir}")


if __name__ == "__main__":
    main()
