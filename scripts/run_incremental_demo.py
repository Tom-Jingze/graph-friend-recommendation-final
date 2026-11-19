"""Run a small self-contained real Spark baseline + incremental batch via Docker."""
import argparse
from datetime import datetime, timezone
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", default=datetime.now(timezone.utc).strftime("demo-%Y%m%dT%H%M%S"))
    parser.add_argument("--format", choices=("parquet", "iceberg"), default="parquet")
    parser.add_argument("--image", default="graph-friend-recommendation-spark:3.5.7")
    args = parser.parse_args()
    import sys
    sys.path.insert(0, str(ROOT))
    from src.spark_pipeline import validate_run_id
    validate_run_id(args.prefix)
    common = ["docker", "run", "--rm", "--pull=never", "--cpus", "2", "--memory", "6g",
        "--mount", f"type=bind,source={ROOT},target=/app", "--workdir", "/app",
        "--env", "SPARK_LOCAL_IP=127.0.0.1", "--entrypoint", "/opt/spark/bin/spark-submit",
        args.image, "--driver-memory", "2g", "--conf", "spark.ui.enabled=false"]
    output = f"/app/artifacts/demo/{args.prefix}"
    options = ["--output", output, "--format", args.format, "--iceberg-package=",
        "--warehouse", "file:" + output + "/warehouse"]
    subprocess.run(common + ["/app/src/spark_pipeline.py", "--input", "/app/data/incremental_fixture.txt",
        "--run-id", "base"] + options, check=True)
    batch = common + ["/app/src/incremental_pipeline.py", "--base-run", output + "/runs/base",
        "--changes", "/app/data/incremental_changes.txt", "--run-id", "batch-1"] + options
    subprocess.run(batch, check=True)
    subprocess.run(batch, check=True)  # Must return already_published without Spark startup.


if __name__ == "__main__":
    main()
