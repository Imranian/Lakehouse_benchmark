import os
import smtplib
import sys
import tempfile
from email.message import EmailMessage
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark.stream_common import get_metrics_dir, get_run_label, load_config


def get_env(name, default=""):
    value = os.environ.get(name, default)
    return value.strip() if isinstance(value, str) else value


def required_env(name):
    value = get_env(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def zip_directory(source_dir, output_zip):
    with ZipFile(output_zip, "w", ZIP_DEFLATED) as zip_file:
        for file_path in source_dir.rglob("*"):
            if file_path.is_file():
                arcname = file_path.relative_to(source_dir.parent)
                zip_file.write(file_path, arcname)


def main():
    config = load_config("configs/pipeline_config.yaml")
    run_label = get_run_label()
    if not run_label:
        raise RuntimeError("BENCHMARK_RUN_LABEL is not set.")

    results_dir = get_metrics_dir(config)
    if not results_dir.exists():
        raise RuntimeError(f"Results directory does not exist: {results_dir}")

    smtp_host = get_env("SMTP_HOST")
    smtp_to = get_env("SMTP_TO")
    if not smtp_host or not smtp_to:
        print(
            "Skipping email notification because SMTP_HOST or SMTP_TO is not configured."
        )
        return

    smtp_port = int(get_env("SMTP_PORT", "587"))
    smtp_username = required_env("SMTP_USERNAME")
    smtp_password = required_env("SMTP_PASSWORD")
    smtp_from = required_env("SMTP_FROM")

    with tempfile.TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / f"{run_label}_results.zip"
        zip_directory(results_dir, zip_path)

        message = EmailMessage()
        message["Subject"] = f"Lakehouse benchmark results: {run_label}"
        message["From"] = smtp_from
        message["To"] = smtp_to
        message.set_content(
            f"Attached are the benchmark results for run {run_label}.\n"
            f"Results folder: {results_dir}\n"
        )

        with open(zip_path, "rb") as attachment_file:
            message.add_attachment(
                attachment_file.read(),
                maintype="application",
                subtype="zip",
                filename=zip_path.name,
            )

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            server.send_message(message)

    print(f"Sent results email for {run_label} with attachment {results_dir}")


if __name__ == "__main__":
    main()
