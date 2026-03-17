import argparse
import csv
import sys
import time
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark.stream_common import (
    get_compacted_storage_path,
    get_configured_storage_path,
    get_metrics_dir,
    load_config,
)


def build_spark_session(warehouse_path):
    spark = (
        SparkSession.builder.appName("PredictionBenchmark")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse_path)
        .config("spark.sql.iceberg.vectorization.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def add_label(df):
    return df.withColumn(
        "label",
        when(
            (col("temperature") > 75)
            | (col("pressure") < 95)
            | (col("pressure") > 115)
            | (col("vibration") > 1.1),
            1.0,
        ).otherwise(0.0),
    )


def load_table(spark, table_format, delta_path, hudi_path, iceberg_table):
    if table_format == "delta":
        return spark.read.format("delta").load(delta_path)
    if table_format == "hudi":
        return spark.read.format("hudi").load(hudi_path)
    return spark.table(iceberg_table)


def build_pipeline(model_name):
    assembler = VectorAssembler(
        inputCols=["temperature", "pressure", "vibration"],
        outputCol="features",
    )

    if model_name == "logistic_regression":
        classifier = LogisticRegression(
            featuresCol="features",
            labelCol="label",
            maxIter=20,
            regParam=0.0,
        )
    elif model_name == "random_forest":
        classifier = RandomForestClassifier(
            featuresCol="features",
            labelCol="label",
            numTrees=30,
            maxDepth=8,
            seed=42,
        )
    else:
        raise ValueError(f"Unsupported model: {model_name}")

    return Pipeline(stages=[assembler, classifier])


def benchmark_format(spark, table_format, model_name, delta_path, hudi_path, iceberg_table):
    load_start = time.time()
    df = load_table(spark, table_format, delta_path, hudi_path, iceberg_table)
    df = add_label(df).select("temperature", "pressure", "vibration", "label")
    row_count = df.count()
    load_time = round(time.time() - load_start, 4)

    prep_start = time.time()
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    pipeline = build_pipeline(model_name)
    prep_time = round(time.time() - prep_start, 4)

    train_start = time.time()
    model = pipeline.fit(train_df)
    train_time = round(time.time() - train_start, 4)

    inference_start = time.time()
    predictions = model.transform(test_df).cache()
    prediction_count = predictions.count()
    inference_time = round(time.time() - inference_start, 4)

    accuracy = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy"
    ).evaluate(predictions)
    f1 = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="f1"
    ).evaluate(predictions)
    precision = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="weightedPrecision"
    ).evaluate(predictions)
    recall = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="weightedRecall"
    ).evaluate(predictions)
    auc = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    ).evaluate(predictions)

    predictions_with_probability = predictions.withColumn(
        "positive_probability",
        vector_to_array(col("probability"))[1],
    )

    predictions_with_probability.orderBy(col("positive_probability").desc()).select(
        "temperature",
        "pressure",
        "vibration",
        "label",
        "prediction",
        "positive_probability",
    ).show(5, truncate=False)

    return {
        "format": table_format,
        "model": model_name,
        "row_count": row_count,
        "prediction_row_count": prediction_count,
        "load_time_seconds": load_time,
        "feature_prep_time_seconds": prep_time,
        "train_time_seconds": train_time,
        "inference_time_seconds": inference_time,
        "predictions_per_second": round(prediction_count / inference_time, 2)
        if inference_time
        else 0,
        "accuracy": round(accuracy, 4),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "area_under_roc": round(auc, 4),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["before", "after"], default="after")
    parser.add_argument(
        "--models",
        default="logistic_regression,random_forest",
        help="Comma-separated list of models: logistic_regression,random_forest",
    )
    args = parser.parse_args()

    config = load_config("configs/pipeline_config.yaml")
    delta_path = get_configured_storage_path(config, "delta_table")
    hudi_path = get_configured_storage_path(config, "hudi_table")
    iceberg_warehouse = get_configured_storage_path(config, "iceberg_warehouse")

    if args.mode == "after":
        delta_path = get_compacted_storage_path(delta_path)
        hudi_path = get_compacted_storage_path(hudi_path)
        iceberg_warehouse = get_compacted_storage_path(iceberg_warehouse)
        output_name = "prediction_benchmark_after_compaction.csv"
    else:
        output_name = "prediction_benchmark_before_compaction.csv"

    iceberg_table = (
        f"local.{config['iceberg']['namespace']}.{config['iceberg']['table_name']}"
    )

    spark = build_spark_session(iceberg_warehouse)
    output_path = get_metrics_dir(config) / output_name
    output_path.parent.mkdir(parents=True, exist_ok=True)
    selected_models = [model.strip() for model in args.models.split(",") if model.strip()]

    results = []
    for model_name in selected_models:
        for table_format in ["delta", "hudi", "iceberg"]:
            print(
                f"[prediction] Running {args.mode}-compaction benchmark for "
                f"{table_format} with model={model_name}"
            )
            result = benchmark_format(
                spark,
                table_format,
                model_name,
                delta_path,
                hudi_path,
                iceberg_table,
            )
            print(f"[prediction] Summary for {table_format}/{model_name}: {result}")
            results.append(result)

    with open(output_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=[
                "format",
                "model",
                "row_count",
                "prediction_row_count",
                "load_time_seconds",
                "feature_prep_time_seconds",
                "train_time_seconds",
                "inference_time_seconds",
                "predictions_per_second",
                "accuracy",
                "precision",
                "recall",
                "f1",
                "area_under_roc",
            ],
        )
        writer.writeheader()
        writer.writerows(results)

    spark.stop()
    print(f"Wrote prediction benchmark results to {output_path}")


if __name__ == "__main__":
    main()
