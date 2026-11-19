"""Plan or execute NEW baseline partition/AQE runs. Default: dry-run only.

Spark starts only with --execute. Run after other heavy experiments finish.
One Docker container at a time; no image pulling or persistent services.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from report_partition_benchmark import build_comparison, markdown, report_run

PROJECT = Path(__file__).resolve().parents[1]


def sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def command(project: Path, image: str, relative_input: str, relative_output: str,
            run_id: str, variant: str, driver_memory: str = "2g") -> list[str]:
    arguments = ["docker", "run", "--rm", "--pull=never", "--name", "graph-v3-" + run_id,
                 "--cpus", "2", "--memory", "6g", "--mount", f"type=bind,source={project},target=/workspace",
                 "--workdir", "/workspace", "--entrypoint", "/opt/spark/bin/spark-submit", image,
                 "--master", "local[2]", "--driver-memory", driver_memory, "--conf", "spark.ui.enabled=false",
                 "/workspace/src/spark_pipeline.py", "--input", "/workspace/" + relative_input,
                 "--output", "/workspace/" + relative_output, "--run-id", run_id,
                 "--master", "local[2]", "--mode", "baseline", "--format", "parquet",
                 "--partitions", "4" if variant == "p4-default-aqe" else "16"]
    if variant == "p16-no-coalesce":
        arguments.append("--disable-adaptive-coalesce")
    return arguments


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("data/youtube_sample_200k.txt"))
    parser.add_argument("--output", type=Path, default=Path("artifacts/partition-benchmark"))
    parser.add_argument("--evidence", type=Path, default=Path("evidence/performance"))
    parser.add_argument("--image", default="apache/spark:3.5.7-python3")
    parser.add_argument("--driver-memory", default="2g", help="Fixed JVM heap for BOTH variants; container limit stays 6g")
    parser.add_argument("--prefix", default=datetime.now(timezone.utc).strftime("aqe-%Y%m%dT%H%M%S"))
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--skip-parity", action="store_true", help="Explicitly omit memory-heavy full table parity; report will mark it unverified")
    args = parser.parse_args()
    if args.repetitions < 1:
        parser.error("repetitions must be positive")
    sys.path.insert(0, str(PROJECT))
    from src.spark_pipeline import validate_run_id
    validate_run_id(args.prefix)
    source = (PROJECT / args.input).resolve()
    output = (PROJECT / args.output).resolve()
    evidence = (PROJECT / args.evidence / args.prefix).resolve()
    try:
        relative_input = source.relative_to(PROJECT).as_posix()
        relative_output = output.relative_to(PROJECT).as_posix()
        evidence.relative_to(PROJECT)
    except ValueError:
        parser.error("Input/output/evidence must be inside the mounted V3 project")
    if not source.is_file():
        parser.error(f"Missing sample input: {source}")
    input_digest = sha256(source)
    source_digest = sha256(PROJECT / "src/spark_pipeline.py")
    runs = []
    for repetition in range(1, args.repetitions + 1):
        variants = ["p4-default-aqe", "p16-no-coalesce"]
        if repetition % 2 == 0:
            variants.reverse()
        for variant in variants:
            run_id = f"{args.prefix}-{repetition}-{variant}"
            validate_run_id(run_id)
            runs.append({"run_id": run_id, "variant": variant, "repetition": repetition,
                         "run_directory": str(output / "runs" / run_id),
                         "command": command(PROJECT, args.image, relative_input, relative_output,
                                            run_id, variant, args.driver_memory)})
    if not args.execute:
        print(json.dumps({"status": "DRY_RUN_NO_SPARK_STARTED", "input_sha256": input_digest,
                          "driver_memory": args.driver_memory, "container_cpus": 2,
                          "container_memory_bytes": 6 * 1024**3, "runs": runs}, indent=2))
        return
    if evidence.exists():
        raise FileExistsError(f"Refusing to overwrite suite evidence: {evidence}")
    for run in runs:
        if Path(run["run_directory"]).exists():
            raise FileExistsError(f"Refusing an already-published benchmark run: {run['run_directory']}")
    image_id = subprocess.run(["docker", "image", "inspect", args.image, "--format", "{{.Id}}"],
                              check=True, capture_output=True, text=True).stdout.strip()
    evidence.mkdir(parents=True)
    suite = {"scope": "NEW baseline runs only; serial containers", "runs": [], "parity_reports": []}
    for run in runs:
        if sha256(source) != input_digest or sha256(PROJECT / "src/spark_pipeline.py") != source_digest:
            raise ValueError("Input or pipeline changed during suite")
        controls = {"variant": run["variant"], "repetition": run["repetition"],
                    "input_sha256": input_digest, "pipeline_source_sha256": source_digest,
                    "image_id": image_id, "container_cpus": 2, "container_memory_bytes": 6 * 1024**3,
                    "driver_memory": args.driver_memory, "command": run["command"]}
        print("Starting", run["run_id"], flush=True)
        with (evidence / (run["run_id"] + ".log")).open("w", encoding="utf-8") as handle:
            subprocess.run(run["command"], check=True, stdout=handle, stderr=subprocess.STDOUT)
        if sha256(source) != input_digest or sha256(PROJECT / "src/spark_pipeline.py") != source_digest:
            raise ValueError("Input or pipeline changed during a run")
        completed = {**run, "controls": controls}
        suite["runs"].append(completed)
        (evidence / "suite.json").write_text(json.dumps(suite, indent=2) + "\n", encoding="utf-8")
    if not args.skip_parity:
        for repetition in range(1, args.repetitions + 1):
            pair = sorted((run for run in suite["runs"] if run["repetition"] == repetition), key=lambda run: run["variant"])
            target = evidence / f"parity-{repetition}.json"
            subprocess.run([sys.executable, str(PROJECT / "scripts/verify_results.py"), "--input", str(source),
                            "--first-run", pair[0]["run_directory"], "--second-run", pair[1]["run_directory"],
                            "--output", str(target)], check=True)
            suite["parity_reports"].append(str(target))
        (evidence / "suite.json").write_text(json.dumps(suite, indent=2) + "\n", encoding="utf-8")
    rows = [report_run(Path(run["run_directory"]), run["controls"]) for run in suite["runs"]]
    parity = [json.loads(Path(path).read_text(encoding="utf-8")) for path in suite["parity_reports"]]
    result = build_comparison(rows, parity)
    (evidence / "comparison.json").write_text(json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    (evidence / "comparison.md").write_text(markdown(result), encoding="utf-8")
    print("Saved measured comparison:", evidence / "comparison.json")


if __name__ == "__main__":
    main()
