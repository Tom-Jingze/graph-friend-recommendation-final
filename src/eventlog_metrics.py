"""Summarize actual Spark JSON event logs without double-counting retries.

Run: python -m src.eventlog_metrics --input EVENT_LOG --output metrics.json
Only Python's standard library is required. Plain JSON-lines and gzip logs are
supported; pass a rolling-log directory to read its events_* segments.
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import re
from collections import Counter
from pathlib import Path
from typing import Any


COUNTERS = (
    "shuffle_read_bytes", "shuffle_write_bytes", "shuffle_read_records",
    "shuffle_write_records", "memory_spill_bytes", "disk_spill_bytes",
    "executor_run_time_ms", "executor_cpu_time_ns", "jvm_gc_time_ms",
    "shuffle_fetch_wait_time_ms", "shuffle_write_time_ns",
)
OUTCOMES = ("success", "failed", "killed", "unknown")


def percentile(values: list[float], probability: float) -> float | None:
    """Linearly interpolated quantile (R-7 / numpy's default method)."""
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower, upper = math.floor(position), math.ceil(position)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def _files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    if not path.is_dir():
        raise ValueError(f"Event log does not exist: {path}")
    segments = [p for p in path.iterdir() if p.is_file() and p.name.startswith("events_")
                and not p.name.endswith(".crc")]
    if not segments:
        raise ValueError("Directory must contain Spark rolling-log events_* segments; "
                         "otherwise pass a single event-log file")
    def segment_order(item: Path) -> tuple[int, str]:
        match = re.match(r"events_(\d+)", item.name)
        return (int(match.group(1)) if match else 0, item.name)
    return sorted(segments, key=segment_order)


def _events(path: Path):
    with path.open("rb") as raw:
        compressed = raw.read(2) == b"\x1f\x8b"
    opener = gzip.open if compressed else open
    with opener(path, "rt", encoding="utf-8-sig") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except (ValueError, UnicodeError) as error:
                raise ValueError(f"{path}:{line_number}: invalid JSON event; "
                                 "use an uncompressed or gzip Spark JSON log") from error
            if not isinstance(event, dict) or "Event" not in event:
                raise ValueError(f"{path}:{line_number}: not a Spark JSON event")
            yield event


def _number(value: Any) -> int:
    return int(value) if isinstance(value, (int, float)) and math.isfinite(value) else 0


def _task(event: dict[str, Any]) -> dict[str, Any]:
    info = event.get("Task Info", {})
    metrics = event.get("Task Metrics") or {}
    read = metrics.get("Shuffle Read Metrics") or {}
    write = metrics.get("Shuffle Write Metrics") or {}
    reason = event.get("Task End Reason", {})
    reason = reason.get("Reason", "unknown") if isinstance(reason, dict) else str(reason)
    if reason == "Success":
        outcome = "success"
    elif reason in ("TaskKilled", "Killed") or info.get("Killed"):
        outcome = "killed"
    elif reason == "unknown":
        outcome = "unknown"
    else:
        outcome = "failed"
    launch, finish = info.get("Launch Time"), info.get("Finish Time")
    duration = (finish - launch if isinstance(launch, (int, float)) and
                isinstance(finish, (int, float)) and finish >= launch else None)
    task_id = info.get("Task ID")
    if task_id is None or "Stage ID" not in event:
        raise ValueError("SparkListenerTaskEnd must have Stage ID and Task Info.Task ID")
    partition = info.get("Partition ID", info.get("Index"))
    return {
        "stage_id": int(event["Stage ID"]),
        "stage_attempt_id": int(event.get("Stage Attempt ID", 0)),
        "task_id": int(task_id), "partition_index": partition,
        "attempt": int(info.get("Attempt", 0)), "outcome": outcome,
        "reason": reason, "speculative": bool(info.get("Speculative", False)),
        "finish_time_ms": _number(finish), "duration_ms": duration,
        "metrics_present": bool(metrics),
        "shuffle_read_bytes": _number(read.get("Remote Bytes Read")) + _number(read.get("Local Bytes Read")),
        "shuffle_write_bytes": _number(write.get("Shuffle Bytes Written")),
        "shuffle_read_records": _number(read.get("Total Records Read")),
        "shuffle_write_records": _number(write.get("Shuffle Records Written")),
        "memory_spill_bytes": _number(metrics.get("Memory Bytes Spilled")),
        "disk_spill_bytes": _number(metrics.get("Disk Bytes Spilled")),
        "executor_run_time_ms": _number(metrics.get("Executor Run Time")),
        "executor_cpu_time_ns": _number(metrics.get("Executor CPU Time")),
        "jvm_gc_time_ms": _number(metrics.get("JVM GC Time")),
        "shuffle_fetch_wait_time_ms": _number(read.get("Fetch Wait Time")),
        "shuffle_write_time_ns": _number(write.get("Shuffle Write Time")),
    }


def _aggregate(tasks: list[dict[str, Any]]) -> dict[str, Any]:
    durations = [task["duration_ms"] for task in tasks if task["outcome"] == "success"
                 and task["duration_ms"] is not None]
    return {
        "task_attempt_count": len(tasks),
        **{key: sum(task[key] for task in tasks) for key in COUNTERS},
        "successful_task_duration_ms": {
            "count": len(durations), "p50": percentile(durations, 0.5),
            "p95": percentile(durations, 0.95),
            "max": max(durations) if durations else None,
        },
    }


def _by_outcome(tasks: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {outcome: _aggregate([task for task in tasks if task["outcome"] == outcome])
            for outcome in OUTCOMES}


def summarize_event_logs(path: str | Path) -> dict[str, Any]:
    inputs = _files(Path(path))
    stages: dict[tuple[int, int], dict[str, Any]] = {}
    jobs: dict[int, dict[str, Any]] = {}
    tasks: dict[tuple[int, int, int], dict[str, Any]] = {}
    application_ids: set[str] = set()
    event_counts: Counter[str] = Counter()
    duplicate_task_events = 0
    app_ended = False
    app_name = None

    def stage(stage_id: int, attempt: int) -> dict[str, Any]:
        return stages.setdefault((stage_id, attempt), {
            "stage_id": stage_id, "attempt_id": attempt, "name": None,
            "status": "unknown", "submitted_time_ms": None,
            "completion_time_ms": None, "failure_reason": None,
            "expected_tasks": None, "job_group_id": None,
            "job_group_source": None,
        })

    for file_path in inputs:
        for event in _events(file_path):
            kind = event["Event"]
            event_counts[kind] += 1
            if kind == "SparkListenerApplicationStart":
                if event.get("App ID") is not None:
                    application_ids.add(str(event["App ID"]))
                    if len(application_ids) > 1:
                        raise ValueError("Input contains multiple Spark applications; summarize each separately")
                app_name = event.get("App Name", app_name)
            elif kind == "SparkListenerApplicationEnd":
                app_ended = True
            elif kind == "SparkListenerJobStart":
                job_id = int(event["Job ID"])
                entry = jobs.setdefault(job_id, {"job_id": job_id, "status": "unknown"})
                entry.update({"stage_ids": sorted(set(event.get("Stage IDs", []))),
                              "submission_time_ms": event.get("Submission Time")})
                properties = event.get("Properties") or {}
                entry.update({"job_group_id": properties.get("spark.jobGroup.id"),
                              "description": properties.get("spark.job.description"),
                              "sql_execution_id": properties.get("spark.sql.execution.id")})
            elif kind == "SparkListenerJobEnd":
                job_id = int(event["Job ID"])
                entry = jobs.setdefault(job_id, {"job_id": job_id, "stage_ids": []})
                result = event.get("Job Result", {})
                result = result.get("Result", "unknown") if isinstance(result, dict) else str(result)
                entry.update({"status": "success" if result == "JobSucceeded" else
                              "failed" if result == "JobFailed" else "unknown",
                              "completion_time_ms": event.get("Completion Time"), "result": result})
            elif kind in ("SparkListenerStageSubmitted", "SparkListenerStageCompleted"):
                info = event["Stage Info"]
                entry = stage(int(info["Stage ID"]), int(info.get("Stage Attempt ID", 0)))
                properties = event.get("Properties") or {}
                if kind == "SparkListenerStageSubmitted" and properties.get("spark.jobGroup.id") is not None:
                    entry["job_group_id"] = properties["spark.jobGroup.id"]
                    entry["job_group_source"] = "StageSubmitted.Properties"
                entry.update({"name": info.get("Stage Name", entry["name"]),
                              "expected_tasks": info.get("Number of Tasks", entry["expected_tasks"])})
                if info.get("Submission Time") is not None:
                    entry["submitted_time_ms"] = info["Submission Time"]
                if kind == "SparkListenerStageCompleted":
                    failure = info.get("Failure Reason")
                    entry.update({"status": "failed" if failure else "success",
                                  "failure_reason": failure,
                                  "completion_time_ms": info.get("Completion Time")})
                elif entry["status"] == "unknown":
                    entry["status"] = "running"
            elif kind == "SparkListenerTaskEnd":
                task = _task(event)
                key = (task["stage_id"], task["stage_attempt_id"], task["task_id"])
                if key in tasks:
                    if tasks[key] != task:
                        raise ValueError(f"Conflicting TaskEnd events for stage/attempt/task {key}")
                    duplicate_task_events += 1
                else:
                    tasks[key] = task
                stage(task["stage_id"], task["stage_attempt_id"])

    if not event_counts:
        raise ValueError("No Spark events found")
    all_tasks = list(tasks.values())
    for entry in stages.values():
        if entry["job_group_id"] is None:
            referencing_groups = {job.get("job_group_id") for job in jobs.values()
                                  if entry["stage_id"] in job.get("stage_ids", [])
                                  and job.get("job_group_id") is not None}
            if len(referencing_groups) == 1:
                entry["job_group_id"] = next(iter(referencing_groups))
                entry["job_group_source"] = "unambiguous referencing JobStart.Properties"
    # A repeated stage can reuse valid outputs from an earlier failed attempt.
    # For each completed stage ID, select the latest observed success per
    # partition up to its final successful attempt; ties choose earliest finish.
    successful_stage_attempts: dict[int, int] = {}
    for entry in stages.values():
        if entry["status"] == "success":
            successful_stage_attempts[entry["stage_id"]] = max(
                successful_stage_attempts.get(entry["stage_id"], -1), entry["attempt_id"])
    selected: dict[tuple[int, Any], dict[str, Any]] = {}
    missing_partition_count = 0
    for task in all_tasks:
        if task["outcome"] != "success" or task["stage_id"] not in successful_stage_attempts:
            continue
        if task["stage_attempt_id"] > successful_stage_attempts[task["stage_id"]]:
            continue
        if task["partition_index"] is None:
            missing_partition_count += 1
            continue
        key = (task["stage_id"], task["partition_index"])
        previous = selected.get(key)
        preference = lambda item: (-item["stage_attempt_id"], item["finish_time_ms"], item["task_id"])
        if previous is None or preference(task) < preference(previous):
            selected[key] = task
    logical_tasks = list(selected.values())
    stage_rows = []
    for key, entry in sorted(stages.items()):
        subset = [task for task in all_tasks
                  if (task["stage_id"], task["stage_attempt_id"]) == key]
        stage_rows.append({**entry, "all_attempts": _aggregate(subset),
                           "by_task_outcome": _by_outcome(subset),
                           "failure_reasons": dict(Counter(task["reason"] for task in subset
                                                           if task["outcome"] != "success"))})
    job_rows = []
    for _, entry in sorted(jobs.items()):
        ids = set(entry.get("stage_ids", []))
        subset = [task for task in all_tasks if task["stage_id"] in ids]
        logical_subset = [task for task in logical_tasks if task["stage_id"] in ids]
        job_rows.append({**entry, "all_attempts": _aggregate(subset),
                         "by_task_outcome": _by_outcome(subset),
                         "logical_successful_tasks": _aggregate(logical_subset)})
    # Phase accounting assigns each *executed stage attempt* to its submission
    # group, rather than adding job metrics whose referenced stages can overlap.
    group_rows = []
    groups = {entry["job_group_id"] for entry in stages.values()}
    for group_id in sorted(groups, key=lambda value: (value is None, str(value))):
        entries = [entry for entry in stages.values() if entry["job_group_id"] == group_id]
        keys = {(entry["stage_id"], entry["attempt_id"]) for entry in entries}
        subset = [task for task in all_tasks if (task["stage_id"], task["stage_attempt_id"]) in keys]
        logical_subset = [task for task in logical_tasks if (task["stage_id"], task["stage_attempt_id"]) in keys]
        group_jobs = [entry for entry in jobs.values() if entry.get("job_group_id") == group_id]
        submitted = [entry["submitted_time_ms"] for entry in entries if entry["submitted_time_ms"] is not None]
        completed = [entry["completion_time_ms"] for entry in entries if entry["completion_time_ms"] is not None]
        group_rows.append({
            "job_group_id": group_id, "job_ids": sorted(entry["job_id"] for entry in group_jobs),
            "job_counts": dict(Counter(entry["status"] for entry in group_jobs)),
            "stage_attempt_counts": dict(Counter(entry["status"] for entry in entries)),
            "stage_attempts": [[entry["stage_id"], entry["attempt_id"]] for entry in entries],
            "observed_stage_window_ms": max(completed) - min(submitted) if submitted and completed else None,
            "all_attempts": _aggregate(subset), "by_task_outcome": _by_outcome(subset),
            "logical_successful_tasks": _aggregate(logical_subset),
        })
    warnings = []
    if not app_ended:
        warnings.append("No ApplicationEnd event: log may be live, truncated, or filtered; metrics are observed lower bounds.")
    if missing_partition_count:
        warnings.append(f"Excluded {missing_partition_count} successful tasks without partition/index from logical totals.")
    if any(not task["metrics_present"] for task in all_tasks):
        warnings.append("Some TaskEnd events have no Task Metrics; unavailable resource counters are zero, not proof of no cost.")
    return {
        "schema_version": 1, "application_ids": sorted(application_ids),
        "application_name": app_name, "application_end_observed": app_ended,
        "input_files": [str(item.resolve()) for item in inputs],
        "counting_policy": {
            "all_attempts": "Sum every distinct (stage ID, stage attempt ID, task ID) TaskEnd once; includes failed, killed and speculative attempts and unsuccessful stages.",
            "logical_successful_tasks": "Observed approximation: for each successfully completed stage ID and partition, select the highest stage attempt not later than its last successful completion, then earliest successful finish/task ID. This includes reusable earlier-attempt outputs and excludes duplicate speculative successes; it is not a measurement of bytes physically saved.",
            "jobs": "Job metrics include referenced stage IDs; shared/reused stages can appear in multiple jobs. Job totals are NOT additive. Application totals count stages only once.",
            "job_groups": "Each executed stage attempt is assigned to its StageSubmitted.Properties spark.jobGroup.id, falling back only to one unambiguous referencing JobStart group. Group resources are disjoint across stage attempts; jobs are NOT summed. observed_stage_window_ms is the first stage submission to last completion, not Python pipeline wall time.",
            "duration": "Finish Time minus Launch Time, milliseconds; p50/p95 use linear interpolation and successful attempts only.",
            "shuffle_read": "Remote Bytes Read + Local Bytes Read; Remote Bytes Read To Disk is a subset and is not added twice.",
            "units": "Bytes and records are integers; executor CPU and shuffle-write time are nanoseconds; executor run, GC, fetch wait and task duration are milliseconds.",
            "missing_metrics": "Absent resource counters contribute zero; inspect warnings and tasks_missing_metrics before treating totals as complete.",
        },
        "event_counts": dict(event_counts), "warnings": warnings,
        "duplicate_task_end_events_ignored": duplicate_task_events,
        "summary": {
            "job_counts": dict(Counter(entry["status"] for entry in jobs.values())),
            "stage_attempt_counts": dict(Counter(entry["status"] for entry in stages.values())),
            "task_outcome_counts": dict(Counter(task["outcome"] for task in all_tasks)),
            "failed_task_attempts": sum(task["outcome"] == "failed" for task in all_tasks),
            "killed_task_attempts": sum(task["outcome"] == "killed" for task in all_tasks),
            "retried_task_attempts": sum(task["attempt"] > 0 for task in all_tasks),
            "speculative_task_attempts": sum(task["speculative"] for task in all_tasks),
            "tasks_missing_metrics": sum(not task["metrics_present"] for task in all_tasks),
            "all_attempts": _aggregate(all_tasks), "by_task_outcome": _by_outcome(all_tasks),
            "logical_successful_tasks": _aggregate(logical_tasks),
        },
        "jobs": job_rows, "stage_attempts": stage_rows, "job_groups": group_rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True, help="JSON/gzip event log or rolling events_* directory")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = summarize_event_logs(args.input)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    if str(args.output.resolve()) in result["input_files"]:
        parser.error("--output must not overwrite an input event log")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    print(f"Saved Spark event-log metrics to {args.output}")


if __name__ == "__main__":
    main()
