"""Hand-authored Spark event fixtures; these tests do not require Spark."""

from pathlib import Path
import gzip
import json
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
from eventlog_metrics import summarize_event_logs


def write_events(path, events):
    path.write_text("\n".join(json.dumps(event) for event in events) + "\n", encoding="utf-8")
    return path


def task(task_id, partition, *, stage=0, stage_attempt=0, attempt=0,
         reason="Success", duration=10, read=10, write=20, speculative=False):
    return {
        "Event": "SparkListenerTaskEnd", "Stage ID": stage,
        "Stage Attempt ID": stage_attempt, "Task End Reason": {"Reason": reason},
        "Task Info": {"Task ID": task_id, "Index": partition, "Attempt": attempt,
                      "Launch Time": 100, "Finish Time": 100 + duration,
                      "Speculative": speculative},
        "Task Metrics": {"Executor Run Time": 8, "Executor CPU Time": 5_000_000,
                         "JVM GC Time": 1, "Memory Bytes Spilled": 3,
                         "Disk Bytes Spilled": 2,
                         "Shuffle Read Metrics": {"Remote Bytes Read": read,
                                                  "Local Bytes Read": 5,
                                                  "Remote Bytes Read To Disk": 4,
                                                  "Total Records Read": 2},
                         "Shuffle Write Metrics": {"Shuffle Bytes Written": write,
                                                   "Shuffle Write Time": 1_000_000,
                                                   "Shuffle Records Written": 3}},
    }


def completed(stage=0, attempt=0, failure=None):
    return {"Event": "SparkListenerStageCompleted", "Stage Info": {
        "Stage ID": stage, "Stage Attempt ID": attempt, "Number of Tasks": 2,
        "Stage Name": "test", "Completion Time": 300, "Failure Reason": failure,
    }}


def test_real_resource_totals_include_failed_and_speculative_attempts_but_logical_totals_do_not(tmp_path):
    events = [
        {"Event": "SparkListenerApplicationStart", "App ID": "app-test", "App Name": "test"},
        {"Event": "SparkListenerJobStart", "Job ID": 0, "Stage IDs": [0], "Submission Time": 1},
        task(0, 0, reason="ExceptionFailure", duration=5, read=2, write=3),
        task(1, 0, attempt=1, duration=20),
        task(2, 0, attempt=1, duration=30, speculative=True),
        task(3, 1, duration=40), completed(),
        {"Event": "SparkListenerJobEnd", "Job ID": 0,
         "Job Result": {"Result": "JobSucceeded"}, "Completion Time": 400},
        {"Event": "SparkListenerApplicationEnd", "Timestamp": 500},
    ]
    result = summarize_event_logs(write_events(tmp_path / "eventlog", events))
    summary = result["summary"]
    assert summary["task_outcome_counts"] == {"failed": 1, "success": 3}
    assert summary["all_attempts"]["task_attempt_count"] == 4
    assert summary["all_attempts"]["shuffle_read_bytes"] == 52  # (2+5) + 3*(10+5)
    assert summary["all_attempts"]["shuffle_write_bytes"] == 63
    assert summary["all_attempts"]["executor_cpu_time_ns"] == 20_000_000
    assert summary["all_attempts"]["disk_spill_bytes"] == 8
    assert summary["by_task_outcome"]["failed"]["shuffle_write_bytes"] == 3
    assert summary["logical_successful_tasks"]["task_attempt_count"] == 2
    assert summary["logical_successful_tasks"]["shuffle_write_bytes"] == 40
    assert summary["all_attempts"]["successful_task_duration_ms"]["p50"] == 30
    assert summary["all_attempts"]["successful_task_duration_ms"]["p95"] == 39
    assert summary["logical_successful_tasks"]["successful_task_duration_ms"]["p50"] == 30
    assert result["jobs"][0]["status"] == "success"
    assert result["stage_attempts"][0]["by_task_outcome"]["failed"]["task_attempt_count"] == 1
    assert result["warnings"] == []


def test_repeated_stage_uses_latest_success_per_partition_and_retains_reused_outputs(tmp_path):
    events = [task(0, 0, write=100), task(1, 1, reason="FetchFailed", write=10),
              completed(failure="fetch failure"),
              task(2, 1, stage_attempt=1, write=200), completed(attempt=1)]
    result = summarize_event_logs(write_events(tmp_path / "log", events))
    assert result["summary"]["stage_attempt_counts"] == {"failed": 1, "success": 1}
    assert result["summary"]["all_attempts"]["shuffle_write_bytes"] == 310
    assert result["summary"]["logical_successful_tasks"]["shuffle_write_bytes"] == 300


def test_recomputed_partition_replaces_old_success_in_logical_totals(tmp_path):
    events = [task(0, 0, write=100), completed(failure="lost executor"),
              task(1, 0, stage_attempt=1, write=200), completed(attempt=1)]
    result = summarize_event_logs(write_events(tmp_path / "log", events))
    assert result["summary"]["all_attempts"]["shuffle_write_bytes"] == 300
    assert result["summary"]["logical_successful_tasks"]["shuffle_write_bytes"] == 200


def test_rolling_logs_ignore_exact_duplicate_events_and_sort_segments_numerically(tmp_path):
    event = task(0, 0)
    write_events(tmp_path / "events_10_app", [event, completed()])
    write_events(tmp_path / "events_2_app", [event])
    (tmp_path / "appstatus_app").write_text("not an event", encoding="utf-8")
    result = summarize_event_logs(tmp_path)
    assert Path(result["input_files"][0]).name == "events_2_app"
    assert result["duplicate_task_end_events_ignored"] == 1
    assert result["summary"]["all_attempts"]["task_attempt_count"] == 1


def test_conflicting_duplicate_task_end_is_not_silently_accepted(tmp_path):
    with pytest.raises(ValueError, match="Conflicting"):
        summarize_event_logs(write_events(tmp_path / "log", [task(0, 0), task(0, 0, write=99)]))


def test_failed_job_and_stage_have_separate_status_and_no_committed_logical_totals(tmp_path):
    events = [{"Event": "SparkListenerJobStart", "Job ID": 1, "Stage IDs": [0]},
              task(0, 0, reason="TaskKilled"), completed(failure="cancelled"),
              {"Event": "SparkListenerJobEnd", "Job ID": 1, "Job Result": {"Result": "JobFailed"}}]
    result = summarize_event_logs(write_events(tmp_path / "log", events))
    assert result["jobs"][0]["status"] == "failed"
    assert result["stage_attempts"][0]["status"] == "failed"
    assert result["summary"]["killed_task_attempts"] == 1
    assert result["summary"]["failed_task_attempts"] == 0
    assert result["summary"]["logical_successful_tasks"]["task_attempt_count"] == 0
    assert result["summary"]["all_attempts"]["successful_task_duration_ms"]["p50"] is None


def test_shared_stage_job_totals_are_not_summed_into_application_total(tmp_path):
    events = [{"Event": "SparkListenerJobStart", "Job ID": job, "Stage IDs": [0]} for job in (1, 2)]
    events += [task(0, 0), completed()]
    result = summarize_event_logs(write_events(tmp_path / "log", events))
    assert sum(job["all_attempts"]["shuffle_write_bytes"] for job in result["jobs"]) == 40
    assert result["summary"]["all_attempts"]["shuffle_write_bytes"] == 20


def test_group_resources_use_execution_stage_properties_not_shared_job_references(tmp_path):
    events = [
        {"Event": "SparkListenerJobStart", "Job ID": 1, "Stage IDs": [0],
         "Properties": {"spark.jobGroup.id": "clean"}},
        {"Event": "SparkListenerStageSubmitted", "Stage Info": {
            "Stage ID": 0, "Stage Attempt ID": 0, "Submission Time": 50},
         "Properties": {"spark.jobGroup.id": "clean"}},
        task(0, 0, write=100), completed(),
        {"Event": "SparkListenerJobStart", "Job ID": 2, "Stage IDs": [0, 1],
         "Properties": {"spark.jobGroup.id": "score"}},
        {"Event": "SparkListenerStageSubmitted", "Stage Info": {
            "Stage ID": 1, "Stage Attempt ID": 0, "Submission Time": 400},
         "Properties": {"spark.jobGroup.id": "score"}},
        task(1, 0, stage=1, write=200), completed(stage=1),
    ]
    report = summarize_event_logs(write_events(tmp_path / "log", events))
    groups = {entry["job_group_id"]: entry for entry in report["job_groups"]}
    assert groups["clean"]["all_attempts"]["shuffle_write_bytes"] == 100
    assert groups["score"]["all_attempts"]["shuffle_write_bytes"] == 200
    assert sum(group["all_attempts"]["shuffle_write_bytes"] for group in groups.values()) == 300
    assert report["jobs"][1]["all_attempts"]["shuffle_write_bytes"] == 300
    assert report["stage_attempts"][0]["job_group_source"] == "StageSubmitted.Properties"


def test_group_fallback_is_unambiguous_or_unassigned(tmp_path):
    events = [{"Event": "SparkListenerJobStart", "Job ID": job, "Stage IDs": [0],
               "Properties": {"spark.jobGroup.id": group}} for job, group in [(1, "clean"), (2, "score")]]
    events += [task(0, 0), completed()]
    report = summarize_event_logs(write_events(tmp_path / "log", events))
    assert report["job_groups"][0]["job_group_id"] is None
    events[1]["Properties"]["spark.jobGroup.id"] = "clean"
    report = summarize_event_logs(write_events(tmp_path / "log", events))
    assert report["job_groups"][0]["job_group_id"] == "clean"


def test_gzip_magic_detection_and_cli_write(tmp_path):
    event_file = tmp_path / "eventlog.compressed"
    with gzip.open(event_file, "wt", encoding="utf-8") as stream:
        stream.write(json.dumps(task(0, 0)) + "\n" + json.dumps(completed()) + "\n")
    output = tmp_path / "out" / "metrics.json"
    project = Path(__file__).parents[1]
    run = subprocess.run([sys.executable, "-m", "src.eventlog_metrics", "--input", str(event_file),
                          "--output", str(output)], cwd=project, capture_output=True, text=True)
    assert run.returncode == 0, run.stderr
    assert json.loads(output.read_text(encoding="utf-8"))["summary"]["all_attempts"]["shuffle_write_bytes"] == 20


def test_missing_metrics_and_incomplete_logs_report_limits(tmp_path):
    event = task(0, 0)
    del event["Task Metrics"]
    result = summarize_event_logs(write_events(tmp_path / "log", [event]))
    assert not result["application_end_observed"]
    assert result["summary"]["tasks_missing_metrics"] == 1
    assert len(result["warnings"]) == 2


def test_multiple_applications_and_invalid_json_are_rejected(tmp_path):
    events = [{"Event": "SparkListenerApplicationStart", "App ID": app} for app in ("one", "two")]
    with pytest.raises(ValueError, match="multiple Spark applications"):
        summarize_event_logs(write_events(tmp_path / "log", events))
    malformed = tmp_path / "malformed"
    malformed.write_text('{"Event":', encoding="utf-8")
    with pytest.raises(ValueError, match="invalid JSON event"):
        summarize_event_logs(malformed)
