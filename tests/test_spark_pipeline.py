"""DataFrame integration checks plus dependency-free publication checks.

Set RUN_SPARK_TESTS=1 to enable tests that start one local Spark session.  This
keeps correctness/publication unit tests usable before Java/Spark are installed.
"""

from __future__ import annotations

import importlib.util
import inspect
from decimal import Decimal, ROUND_HALF_EVEN
import json
import math
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).parents[1]))

from src.spark_pipeline import (
    adaptive_spark_settings,
    argument_parser,
    canonical_edges,
    enforce_wedge_budget,
    graph_tables,
    parse_edge_lines,
    prepare_run,
    publish_run,
    score_candidates,
    topk_recommendations,
    validate_run_id,
)


def test_default_mode_uses_measured_lower_shuffle_baseline():
    args = argument_parser().parse_args([
        "--input", "edges.txt", "--output", "artifacts/test", "--run-id", "default-choice",
    ])
    assert args.mode == "baseline"
    assert inspect.signature(score_candidates).parameters["mode"].default == "baseline"


def test_default_aqe_controls_preserve_distribution_coalescing_policy():
    args = argument_parser().parse_args(["--input", "x", "--output", "out", "--run-id", "defaults"])
    settings = adaptive_spark_settings(args)
    assert settings["spark.sql.adaptive.enabled"] == "true"
    assert "spark.sql.adaptive.coalescePartitions.enabled" not in settings
    assert "spark.sql.adaptive.advisoryPartitionSizeInBytes" not in settings
    assert "spark.sql.adaptive.coalescePartitions.parallelismFirst" not in settings
    assert args.mode == "baseline" and args.partitions == 4


def test_explicit_aqe_controls_keep_baseline_and_validate_target_size():
    args = argument_parser().parse_args([
        "--input", "x", "--output", "out", "--run-id", "layout", "--partitions", "16",
        "--disable-adaptive-coalesce", "--adaptive-advisory-partition-bytes", "33554432",
        "--adaptive-coalesce-size-first",
    ])
    settings = adaptive_spark_settings(args)
    assert settings["spark.sql.adaptive.coalescePartitions.enabled"] == "false"
    assert settings["spark.sql.adaptive.advisoryPartitionSizeInBytes"] == "33554432"
    assert settings["spark.sql.adaptive.coalescePartitions.parallelismFirst"] == "false"
    assert args.mode == "baseline"
    args.adaptive_advisory_partition_bytes = 0
    with pytest.raises(ValueError, match="must be positive"):
        adaptive_spark_settings(args)


@pytest.fixture(scope="module")
def spark():
    if os.environ.get("RUN_SPARK_TESTS") != "1":
        pytest.skip("Set RUN_SPARK_TESTS=1 to start local Spark integration tests")
    if importlib.util.find_spec("pyspark") is None:
        pytest.skip("Install requirements-spark.txt first")
    from pyspark.sql import SparkSession

    session = SparkSession.builder.master("local[2]").appName("friend-recommendation-tests").config(
        "spark.sql.shuffle.partitions", "2"
    ).config("spark.ui.enabled", "false").config("spark.sql.ansi.enabled", "true").getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()


def test_retry_replaces_only_owned_staging_and_success_is_immutable(tmp_path):
    staging, final, lock = prepare_run(tmp_path, "retry_1")
    (staging / "failure.json").write_text("failed", encoding="utf-8")
    lock.unlink()  # Same cleanup performed by main() after an injected failure.
    retry_staging, retry_final, retry_lock = prepare_run(tmp_path, "retry_1")
    assert retry_staging == staging and retry_final == final
    assert not (retry_staging / "failure.json").exists()
    publish_run(retry_staging, retry_final, {"run_id": "retry_1", "status": "success"})
    retry_lock.unlink()
    assert not staging.exists()
    assert (final / "_SUCCESS").exists()
    assert json.loads((final / "manifest.json").read_text(encoding="utf-8"))["status"] == "success"
    with pytest.raises(FileExistsError):
        prepare_run(tmp_path, "retry_1")


def test_unowned_staging_and_concurrent_run_are_protected(tmp_path):
    staging = tmp_path / ".staging" / "unsafe"
    staging.mkdir(parents=True)
    (staging / "keep.txt").write_text("keep", encoding="utf-8")
    with pytest.raises(RuntimeError, match="unowned"):
        prepare_run(tmp_path, "unsafe")
    assert (staging / "keep.txt").exists()
    _, _, lock = prepare_run(tmp_path, "active")
    try:
        with pytest.raises(FileExistsError):
            prepare_run(tmp_path, "active")
    finally:
        lock.unlink()


@pytest.mark.parametrize("value", ["../x", "a/b", "a\\b", "", "x" * 64, "中文", "."])
def test_run_id_cannot_escape_output(value):
    with pytest.raises(ValueError):
        validate_run_id(value)


def test_wedge_budget_refuses_before_expansion():
    enforce_wedge_budget(50_000_000, 50_000_000)
    with pytest.raises(ValueError, match="theoretical_wedges=50,000,001, max_wedges=50,000,000"):
        enforce_wedge_budget(50_000_001, 50_000_000)


def test_dirty_input_classification_and_canonicalization(spark):
    values = [
        "", "  # comment", "0 1", "1 0", "\t+0002\t3\t", "2 2", "-1 4",
        "oops", "2 word", "1 2 3", "9223372036854775808 2", "9" * 100 + " 2",
        "9223372036854775807 0",
    ]
    parsed = parse_edge_lines(spark.createDataFrame([(value,) for value in values], ["value"]))
    counts = {row.status: row["count"] for row in parsed.groupBy("status").count().collect()}
    assert counts == {"ignored": 2, "valid": 4, "self_loop": 1, "negative_id": 1, "malformed": 3, "overflow": 2}
    edges = {tuple(row) for row in canonical_edges(parsed).collect()}
    assert edges == {(0, 1), (2, 3), (0, 9223372036854775807)}


def python_reference(edges):
    graph = {}
    for left, right in edges:
        graph.setdefault(left, set()).add(right)
        graph.setdefault(right, set()).add(left)
    result = {}
    for left in graph:
        for right in graph:
            if left >= right or right in graph[left]:
                continue
            common = graph[left] & graph[right]
            if not common:
                continue
            result[(left, right)] = {
                "common_neighbors": len(common),
                "resource_allocation": sum(1 / len(graph[bridge]) for bridge in common),
                "adamic_adar": sum(1 / math.log(len(graph[bridge])) for bridge in common),
                "jaccard": len(common) / len(graph[left] | graph[right]),
            }
    return result


@pytest.mark.parametrize("mode", ["baseline", "optimized"])
def test_distributed_scores_and_topk_match_independent_reference(spark, mode):
    # Includes triangles (existing-edge exclusion), two bridges, and an isolated
    # component. The reference computes set intersections, not Spark's wedge join.
    raw_edges = [(0, 1), (0, 2), (1, 2), (1, 3), (2, 3), (3, 4), (4, 5), (10, 11)]
    edges = spark.createDataFrame(raw_edges, "left long, right long")
    directed, degree = graph_tables(edges)
    scores = score_candidates(edges, directed, degree, mode=mode, partitions=2)
    expected = python_reference(raw_edges)
    actual = {(row.left, row.right): row.asDict() for row in scores.collect()}
    assert actual.keys() == expected.keys()
    for pair, metrics in expected.items():
        for method, value in metrics.items():
            assert actual[pair][method] == pytest.approx(value, abs=1e-12)
    topk = topk_recommendations(scores, k=2)
    actual_ranks = {(row.user, row.method, row.rank): row.candidate for row in topk.collect()}
    expected_ranks = {}
    for user in sorted({node for edge in raw_edges for node in edge}):
        for method in ("common_neighbors", "jaccard", "adamic_adar", "resource_allocation"):
            candidates = []
            for (left, right), metrics in expected.items():
                if user == left:
                    candidates.append((right, metrics[method]))
                elif user == right:
                    candidates.append((left, metrics[method]))
            rank_key = lambda item: (-Decimal(str(item[1])).quantize(Decimal("1e-12"), rounding=ROUND_HALF_EVEN), item[0])
            for rank, (candidate, _) in enumerate(sorted(candidates, key=rank_key)[:2], 1):
                expected_ranks[(user, method, rank)] = candidate
    assert actual_ranks == expected_ranks


def test_float_aggregation_noise_does_not_change_tie_order(spark):
    scores = spark.createDataFrame(
        [(0, 2, 1, 0.50000000000001, 0.70000000000001, 0.20000000000001),
         (0, 3, 1, 0.50000000000002, 0.70000000000002, 0.20000000000002)],
        "left long, right long, common_neighbors long, resource_allocation double, adamic_adar double, jaccard double",
    )
    rows = topk_recommendations(scores, k=1).where("user = 0").collect()
    assert len(rows) == 4
    assert all(row.candidate == 2 for row in rows)
    ra = next(row for row in rows if row.method == "resource_allocation")
    assert ra.score == 0.50000000000001
    assert ra.ranking_score == 0.5


def test_empty_candidate_graph(spark):
    edges = spark.createDataFrame([(0, 1), (1, 2), (0, 2)], "left long, right long")
    directed, degrees = graph_tables(edges)
    scores = score_candidates(edges, directed, degrees)
    assert scores.count() == 0
    assert topk_recommendations(scores, k=10).count() == 0
