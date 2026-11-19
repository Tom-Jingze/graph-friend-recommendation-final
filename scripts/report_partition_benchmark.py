"""Report completed, controlled V3 runs without treating V2 timings as baseline."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import statistics
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.eventlog_metrics import summarize_event_logs


def report_run(run: Path, controls: dict) -> dict:
    if not (run / "_SUCCESS").is_file():
        raise ValueError(f"Run is not published: {run}")
    manifest = json.loads((run / "manifest.json").read_text(encoding="utf-8"))
    if manifest["status"] != "success" or manifest["mode"] != "baseline" or manifest["format"] != "parquet":
        raise ValueError("Performance comparison requires NEW successful baseline/Parquet runs")
    application_id = manifest["environment"]["application_id"]
    eventlog = run.parents[1] / "event-logs" / application_id
    metrics = summarize_event_logs(eventlog)
    if metrics["application_ids"] != [application_id] or not metrics["application_end_observed"]:
        raise ValueError("Event log must be a completed exact application match")
    if metrics["warnings"] or any(metrics["summary"]["task_outcome_counts"].get(key, 0)
                                  for key in ("failed", "killed", "unknown")):
        raise ValueError("Incomplete/failed attempts require separate investigation before comparing")
    groups = {group["job_group_id"]: group for group in metrics["job_groups"]}
    if "topk" not in groups:
        raise ValueError("No executed topk job-group evidence")
    topk = groups["topk"]["all_attempts"]
    expected_partitions = 4 if controls["variant"] == "p4-default-aqe" else 16
    expected_coalesce = "true" if controls["variant"] == "p4-default-aqe" else "false"
    env = manifest["environment"]
    if env["shuffle_partitions"] != expected_partitions or env["adaptive_coalesce_enabled"] != expected_coalesce:
        raise ValueError("Actual Spark partition/coalesce settings differ from the requested variant")
    if env["master"] != "local[2]" or env["driver_memory"] != controls["driver_memory"]:
        raise ValueError("Actual Spark CPU/master or driver-memory settings differ from controls")
    return {
        "run_id": manifest["run_id"], "run_directory": str(run), "application_id": application_id,
        "variant": controls["variant"], "controls": controls,
        "environment": env, "execution_parameters": manifest["execution_parameters"],
        "quality": manifest["quality"], "k": manifest["k"], "tie_policy": manifest["tie_policy"],
        "timings_seconds": manifest["timings"], "topk_all_attempts": topk,
        "application_all_attempts": metrics["summary"]["all_attempts"],
        "topk_stage_attempts": [stage for stage in metrics["stage_attempts"] if stage.get("job_group_id") == "topk"],
        "eventlog_sha256": hashlib.sha256(eventlog.read_bytes()).hexdigest(),
        "metrics": metrics,
    }


def build_comparison(rows: list[dict], parity_reports: list[dict] | None = None) -> dict:
    if not rows or {row["variant"] for row in rows} != {"p4-default-aqe", "p16-no-coalesce"}:
        raise ValueError("Need both newly executed variants")
    first = rows[0]
    control_keys = ("input_sha256", "pipeline_source_sha256", "image_id", "container_cpus",
                    "container_memory_bytes", "driver_memory")
    for row in rows[1:]:
        if any(row["controls"][key] != first["controls"][key] for key in control_keys):
            raise ValueError("Input/source/image/resource controls differ; comparison refused")
        if row["k"] != first["k"] or row["tie_policy"] != first["tie_policy"]:
            raise ValueError("Ranking contract differs")
        for key in ("nodes", "clean_edges", "candidate_pairs", "recommendation_rows"):
            if row["quality"][key] != first["quality"][key]:
                raise ValueError(f"Output cardinality differs: {key}")
    repetitions = {row["controls"]["repetition"] for row in rows}
    expected_pairs = set()
    for repetition in repetitions:
        pair = [row for row in rows if row["controls"]["repetition"] == repetition]
        if len(pair) != 2 or {row["variant"] for row in pair} != {"p4-default-aqe", "p16-no-coalesce"}:
            raise ValueError("Each repetition must have exactly one observation per variant")
        expected_pairs.add(frozenset(str(Path(row["run_directory"]).resolve()) for row in pair))
    supplied_pairs = set()
    for parity in parity_reports or []:
        supplied_pairs.add(frozenset(str(Path(parity[key]).resolve()) for key in ("first_run", "second_run")))
    if parity_reports and (supplied_pairs != expected_pairs or len(parity_reports) != len(expected_pairs)):
        raise ValueError("Parity reports do not cover these exact benchmark run pairs")
    summary = {}
    for variant in ("p4-default-aqe", "p16-no-coalesce"):
        selected = [row for row in rows if row["variant"] == variant]
        summary[variant] = {
            "observations": len(selected),
            "median_topk_disk_spill_bytes": statistics.median(row["topk_all_attempts"]["disk_spill_bytes"] for row in selected),
            "median_topk_task_p95_ms": statistics.median(row["topk_all_attempts"]["successful_task_duration_ms"]["p95"] for row in selected),
            "median_topk_seconds": statistics.median(row["timings_seconds"]["topk_materialize_and_write_seconds"] for row in selected),
            "median_pipeline_seconds": statistics.median(row["timings_seconds"]["pipeline_seconds"] for row in selected),
            "median_end_to_end_seconds": statistics.median(row["timings_seconds"]["end_to_end_seconds"] for row in selected),
        }
    default, candidate = (summary[key] for key in ("p4-default-aqe", "p16-no-coalesce"))
    denominator = default["median_topk_disk_spill_bytes"]
    return {
        "status": "MEASURED", "comparison": summary, "runs": rows,
        "topk_disk_spill_reduction_fraction": (denominator - candidate["median_topk_disk_spill_bytes"]) / denominator if denominator else None,
        "full_table_parity_reports": parity_reports or [],
        "full_table_parity_verified": bool(parity_reports) and supplied_pairs == expected_pairs and all(report.get("status") == "PASS" for report in parity_reports),
        "limitations": [
            "V2 optimized 97-second run is historical localization evidence only; both measured variants here use V3 baseline.",
            "Partitions and coalescing change together: this is a combined layout experiment and cannot isolate their individual causal effects.",
            "A single observation per variant does not establish stable speedup; file cache/host load are not fully controlled.",
            "Top-K task p95 is a task distribution, not phase wall time. Top-K phase time includes materialization and output writes.",
            "Container CPU/memory limits and driver heap are distinct controls; neither implies physical multi-machine scalability.",
        ],
    }


def markdown(report: dict) -> str:
    lines = ["# V3 分区与 AQE 对照", "", "以下只比较本轮新执行的 baseline。旧 V2 optimized 运行仅用于定位 spill。", "",
             "| 配置 | 次数 | Top-K disk spill B | Top-K task p95 ms | Top-K 含落表 s | pipeline s | end-to-end s |",
             "|---|---:|---:|---:|---:|---:|---:|"]
    for variant, item in report["comparison"].items():
        lines.append(f"| {variant} | {item['observations']} | {item['median_topk_disk_spill_bytes']:,.0f} | {item['median_topk_task_p95_ms']:,.2f} | {item['median_topk_seconds']:.3f} | {item['median_pipeline_seconds']:.3f} | {item['median_end_to_end_seconds']:.3f} |")
    lines += ["", "表中为各次运行统计量的中位数。输入哈希、源代码、镜像与资源控制均经一致性检查。",
              "", f"完整结果表等价验证：{'已通过' if report['full_table_parity_verified'] else '尚未提供；计数一致不能代替逐行等价'}。", "",
              "本实验同时改变 shuffle 分区数和 AQE 合并策略，不能分别归因。单次观测不支撑稳定加速结论。",
              "", "各运行的环境、原始日志哈希、topk stage 及详细指标见同名 JSON。", ""]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", required=True, type=Path, help="suite.json from benchmark_partitions.py")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    suite = json.loads(args.suite.read_text(encoding="utf-8"))
    rows = [report_run(Path(run["run_directory"]), run["controls"]) for run in suite["runs"]]
    parity = [json.loads(Path(path).read_text(encoding="utf-8")) for path in suite.get("parity_reports", [])]
    report = build_comparison(rows, parity)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    args.output.with_suffix(".md").write_text(markdown(report), encoding="utf-8")
    print(json.dumps(report["comparison"], indent=2))


if __name__ == "__main__":
    main()
