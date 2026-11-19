"""Real Spark fault injection: source changes AFTER freezing, batch reads snapshot."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))
from src import incremental_pipeline as inc
from src import spark_pipeline as full
from accept_incremental import compare, independent_reference


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--suite", default="graph-final-accept-02")
    args = parser.parse_args()
    full.validate_run_id(args.prefix)
    old_evidence = PROJECT / "evidence/incremental" / args.suite
    old_output = PROJECT / "artifacts/incremental" / args.suite
    evidence = PROJECT / "evidence/incremental" / args.prefix
    output = PROJECT / "artifacts/incremental" / args.prefix
    if evidence.exists() or output.exists():
        raise FileExistsError("Use a new prefix")
    evidence.mkdir(parents=True)
    output.mkdir(parents=True)
    event_dir = output / "event-logs"
    event_dir.mkdir()
    source_bytes = (old_evidence / "changes.txt").read_bytes()
    source = evidence / "changes-original.txt"
    source.write_bytes(source_bytes)
    (evidence / "input-before.txt").write_bytes(source_bytes)
    cfg = inc.argument_parser().parse_args(["--base-run", str(old_output / "runs/fixture-base"),
        "--changes", str(source), "--output", str(output), "--run-id", "frozen-input"])
    cfg.warehouse = (output / "warehouse").as_uri()
    base = inc.load_manifest(cfg.base_run)
    staging, final, lock, identity, existing = inc.prepare_batch(cfg, base)
    real_snapshot = inc.snapshot_changes
    def mutate_after_snapshot(source, staging, expected):
        snapshot = real_snapshot(source, staging, expected)
        source.write_text("ADD 500 501\n", encoding="utf-8")
        (evidence / "input-after.txt").write_bytes(source.read_bytes())
        assert inc.sha256(source) != expected
        return snapshot
    inc.snapshot_changes = mutate_after_snapshot
    spark = full.build_spark(cfg, event_dir)
    spark.sparkContext.setLogLevel("WARN")
    report = {"status": "RUNNING"}
    try:
        manifest = inc.execute_incremental(spark, cfg, staging, final, event_dir, base, identity)
        full.publish_run(staging, final, manifest)
        for name in ("manifest.json", "quality.json", "timings.json", "changes_input.txt", "incremental_score_plan.txt"):
            (evidence / name).write_bytes((final / name).read_bytes())
        assert inc.sha256(final / "changes_input.txt") == identity["changes_sha256"]
        report["source_mutated_after_snapshot"] = True
        report["full_parity"] = compare(spark, final, old_output / "runs/mixed-full")
        expected_edges = {tuple(map(int, line.split())) for line in (old_evidence / "updated.txt").read_text().splitlines()}
        report["independent_python"] = independent_reference(spark, final, expected_edges)
        try:
            inc.prepare_batch(cfg, base)
            raise AssertionError("Changed source accepted under committed batch ID")
        except ValueError:
            report["mutated_request_rejected"] = True
        source.write_bytes(source_bytes)
        assert inc.prepare_batch(cfg, base)[4] is not None
        report["original_request_idempotent"] = True
        report.update(status="PASS", request=identity,
            source_sha256={name: hashlib.sha256((PROJECT / name).read_bytes()).hexdigest() for name in
                ("src/incremental_pipeline.py", "scripts/accept_incremental.py", "scripts/verify_frozen_batch.py")},
            environment={"spark": spark.version, "master": spark.sparkContext.master,
                "application_id": spark.sparkContext.applicationId})
        (evidence / "manifest.json").write_bytes((final / "manifest.json").read_bytes())
    except BaseException as exc:
        report.update(status="FAILED", error=str(exc))
        raise
    finally:
        full.write_json(evidence / "acceptance.json", report)
        lock.unlink(missing_ok=True)
        spark.stop()
    print(json.dumps({"status": report["status"], "independent_python": report["independent_python"]}))


if __name__ == "__main__":
    main()
