"""Guard experimental controls and prevent unsupported benchmark conclusions."""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / "scripts"))
from benchmark_partitions import command
from report_partition_benchmark import build_comparison, markdown


def observation(variant, spill=10):
    return {"variant": variant, "run_id": variant, "run_directory": str(PROJECT / "fixture" / variant),
            "controls": {"variant": variant, "repetition": 1, "input_sha256": "same-input",
                         "pipeline_source_sha256": "same-source", "image_id": "same-image",
                         "container_cpus": 2, "container_memory_bytes": 6 * 1024**3,
                         "driver_memory": "2g"},
            "k": 10, "tie_policy": "same", "quality": {"nodes": 10, "clean_edges": 20,
                                                        "candidate_pairs": 30, "recommendation_rows": 40},
            "topk_all_attempts": {"disk_spill_bytes": spill,
                                  "successful_task_duration_ms": {"p95": 100}},
            "timings_seconds": {"topk_materialize_and_write_seconds": 5,
                                "pipeline_seconds": 10, "end_to_end_seconds": 12}}


def test_run_commands_hold_resources_and_algorithm_fixed():
    first = command(PROJECT, "image", "sample.txt", "artifacts", "first", "p4-default-aqe")
    second = command(PROJECT, "image", "sample.txt", "artifacts", "second", "p16-no-coalesce")
    for arguments in (first, second):
        assert arguments[arguments.index("--cpus") + 1] == "2"
        assert arguments[arguments.index("--memory") + 1] == "6g"
        assert arguments[arguments.index("--driver-memory") + 1] == "2g"
        assert arguments[arguments.index("--mode") + 1] == "baseline"
        assert "--pull=never" in arguments
    assert first[first.index("--partitions") + 1] == "4"
    assert "--disable-adaptive-coalesce" not in first
    assert second[second.index("--partitions") + 1] == "16"
    assert "--disable-adaptive-coalesce" in second


def test_report_does_not_call_cardinality_checks_full_table_parity():
    report = build_comparison([observation("p4-default-aqe", 10), observation("p16-no-coalesce", 20)])
    assert report["topk_disk_spill_reduction_fraction"] == -1.0
    assert not report["full_table_parity_verified"]
    assert "尚未提供" in markdown(report)
    assert "single observation" in " ".join(report["limitations"])


@pytest.mark.parametrize("control", ["input_sha256", "pipeline_source_sha256", "image_id", "container_cpus", "container_memory_bytes", "driver_memory"])
def test_report_refuses_changed_experimental_controls(control):
    first, second = observation("p4-default-aqe"), observation("p16-no-coalesce")
    second["controls"][control] = "different"
    with pytest.raises(ValueError, match="controls differ"):
        build_comparison([first, second])


def test_report_does_not_assume_spill_improved_when_baseline_has_no_spill():
    report = build_comparison([observation("p4-default-aqe", 0), observation("p16-no-coalesce", 0)])
    assert report["topk_disk_spill_reduction_fraction"] is None


def test_unrelated_passing_parity_report_cannot_support_current_benchmark():
    with pytest.raises(ValueError, match="exact benchmark run pairs"):
        build_comparison([observation("p4-default-aqe"), observation("p16-no-coalesce")],
                         [{"status": "PASS", "first_run": "old-a", "second_run": "old-b"}])


def test_default_cli_dry_run_cannot_start_spark():
    run = subprocess.run([sys.executable, str(PROJECT / "scripts/benchmark_partitions.py"),
                          "--prefix", "test-plan"], cwd=PROJECT, capture_output=True, text=True, check=True)
    report = json.loads(run.stdout)
    assert report["status"] == "DRY_RUN_NO_SPARK_STARTED"
    assert len(report["runs"]) == 2
    assert all("baseline" in row["command"] for row in report["runs"])
