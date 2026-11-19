"""Distributed, exact two-hop friend recommendation with reviewable run artifacts.

The driver collects only single-row aggregates and table snapshot metadata.  Local
run directories are atomically published; Iceberg tables are run-specific, and
the manifest is the publication boundary (not a multi-table transaction).
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import sys
import time
from typing import Any


SPARK_VERSION = "3.5.7"
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0"
METHODS = ("common_neighbors", "jaccard", "adamic_adar", "resource_allocation")
STATUSES = ("ignored", "malformed", "overflow", "negative_id", "self_loop", "valid")
DEFAULT_MAX_WEDGES = 50_000_000
TIE_POLICY = "ranking_score = bround(raw score, 12), HALF_EVEN; descending ranking_score, ascending candidate ID; raw score preserved"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def validate_run_id(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,62}", value):
        raise ValueError("run-id must be 1-63 ASCII letters/digits/underscores/hyphens, starting with a letter or digit")
    return value


def local_path(value: str) -> Path:
    if "://" in value or value.startswith(("\\\\", "//")):
        raise ValueError("output/event-log directories must be local filesystem paths; remote atomic publication is not implemented")
    return Path(value).expanduser().resolve()


def enforce_wedge_budget(theoretical_wedges: int, max_wedges: int) -> None:
    if max_wedges < 1:
        raise ValueError("max-wedges must be positive")
    if theoretical_wedges > max_wedges:
        raise ValueError(
            f"Wedge budget exceeded: theoretical_wedges={theoretical_wedges:,}, max_wedges={max_wedges:,}. "
            "The exact self-join was not started. Use a smaller graph or explicitly raise --max-wedges after checking resources."
        )


def prepare_run(output: Path, run_id: str) -> tuple[Path, Path, Path]:
    """Acquire a same-run lock and create an owned, retryable staging directory."""
    validate_run_id(run_id)
    output = output.resolve()
    staging = output / ".staging" / run_id
    final = output / "runs" / run_id
    lock = output / ".locks" / (run_id + ".lock")
    lock.parent.mkdir(parents=True, exist_ok=True)
    final.parent.mkdir(parents=True, exist_ok=True)
    if final.exists():
        raise FileExistsError(f"Run already published and cannot be overwritten: {final}")
    descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump({"pid": os.getpid(), "created_at": utc_now(), "run_id": run_id}, handle)
    owner = {"run_id": run_id, "output": str(output)}
    try:
        if staging.exists():
            marker = staging / "_RUN_OWNER.json"
            if staging.is_symlink() or not marker.is_file() or json.loads(marker.read_text(encoding="utf-8")) != owner:
                raise RuntimeError(f"Refusing to remove an unowned staging directory: {staging}")
            resolved = staging.resolve()
            expected_parent = (output / ".staging").resolve()
            if resolved.parent != expected_parent or resolved.name != run_id:
                raise RuntimeError("Staging path escaped the run root")
            shutil.rmtree(resolved)
        staging.mkdir(parents=True)
        write_json(staging / "_RUN_OWNER.json", owner)
    except BaseException:
        lock.unlink(missing_ok=True)
        raise
    return staging, final, lock


def publish_run(staging: Path, final: Path, manifest: dict[str, Any]) -> None:
    if final.exists():
        raise FileExistsError(f"Run already exists: {final}")
    write_json(staging / "manifest.json", manifest)
    (staging / "_SUCCESS").write_text("", encoding="utf-8")
    # Both directories are below the same local output root / filesystem.
    os.replace(staging, final)


def parse_edge_lines(lines: Any) -> Any:
    """Classify every raw row without Python UDFs; signed 64-bit, nonnegative IDs."""
    from pyspark.sql import functions as F

    normalized = F.regexp_replace(F.regexp_replace(F.col("value"), "^\ufeff", ""), r"^\s+|\s+$", "")
    parsed = lines.select(F.col("value").alias("raw_line"), normalized.alias("line"))
    parsed = parsed.withColumn("tokens", F.split("line", r"\s+"))
    parsed = parsed.withColumn("left_token", F.expr("try_element_at(tokens, 1)")).withColumn("right_token", F.expr("try_element_at(tokens, 2)"))
    # try_cast remains safe with ANSI mode enabled and handles arbitrarily long integers.
    parsed = parsed.withColumn("raw_left", F.expr("try_cast(left_token AS BIGINT)"))
    parsed = parsed.withColumn("raw_right", F.expr("try_cast(right_token AS BIGINT)"))
    integer_syntax = F.col("left_token").rlike(r"^[+-]?[0-9]+$") & F.col("right_token").rlike(r"^[+-]?[0-9]+$")
    status = (
        F.when((F.length("line") == 0) | F.col("line").startswith("#"), F.lit("ignored"))
        .when((F.size("tokens") != 2) | ~F.coalesce(integer_syntax, F.lit(False)), F.lit("malformed"))
        .when(F.col("raw_left").isNull() | F.col("raw_right").isNull(), F.lit("overflow"))
        .when((F.col("raw_left") < 0) | (F.col("raw_right") < 0), F.lit("negative_id"))
        .when(F.col("raw_left") == F.col("raw_right"), F.lit("self_loop"))
        .otherwise(F.lit("valid"))
    )
    return parsed.withColumn("status", status).select("raw_line", "raw_left", "raw_right", "status")


def canonical_edges(parsed: Any) -> Any:
    from pyspark.sql import functions as F

    return parsed.where(F.col("status") == "valid").select(
        F.least("raw_left", "raw_right").alias("left"),
        F.greatest("raw_left", "raw_right").alias("right"),
    ).dropDuplicates(["left", "right"])


def graph_tables(edges: Any) -> tuple[Any, Any]:
    from pyspark.sql import functions as F

    directed = edges.select(F.col("left").alias("src"), F.col("right").alias("dst")).unionByName(
        edges.select(F.col("right").alias("src"), F.col("left").alias("dst"))
    )
    degrees = directed.groupBy("src").agg(F.count("dst").alias("degree"))
    return directed, degrees


def score_candidates(edges: Any, directed: Any, degrees: Any, mode: str = "baseline", partitions: int = 4) -> Any:
    """Exact CN/RA/AA/Jaccard on non-edge two-hop pairs, with no degree cap.

    Baseline aggregates all wedges before removing existing edges.  Optimized
    removes existing pairs earlier, then aggregates bridge-hash buckets before
    the final pair aggregate.  Spark already performs partial aggregation for
    baseline: this experiment is not a guaranteed speedup or a hub-join fix.
    """
    from pyspark.sql import functions as F

    if mode not in {"baseline", "optimized"} or partitions < 1:
        raise ValueError("mode must be baseline/optimized and partitions positive")
    left = directed.select(F.col("src").alias("bridge"), F.col("dst").alias("left")).alias("a")
    right = directed.select(F.col("src").alias("bridge"), F.col("dst").alias("right")).alias("b")
    wedges = left.join(right, (F.col("a.bridge") == F.col("b.bridge")) & (F.col("a.left") < F.col("b.right")), "inner").select(
        F.col("a.bridge").alias("bridge"), F.col("a.left").alias("left"), F.col("b.right").alias("right")
    )
    if mode == "optimized":
        wedges = wedges.join(edges, ["left", "right"], "left_anti")
    weighted = wedges.join(degrees.select(F.col("src").alias("bridge"), "degree"), "bridge").select(
        "left", "right", "bridge", (F.lit(1.0) / F.col("degree")).alias("ra"),
        F.when(F.col("degree") > 1, F.lit(1.0) / F.log("degree")).otherwise(F.lit(0.0)).alias("aa"),
    )
    if mode == "optimized":
        partial = weighted.withColumn("bridge_bucket", F.pmod(F.xxhash64("bridge"), F.lit(partitions))).groupBy(
            "left", "right", "bridge_bucket"
        ).agg(F.count(F.lit(1)).alias("cn"), F.sum("ra").alias("ra"), F.sum("aa").alias("aa"))
        scored = partial.groupBy("left", "right").agg(
            F.sum("cn").cast("long").alias("common_neighbors"),
            F.sum("ra").alias("resource_allocation"), F.sum("aa").alias("adamic_adar"),
        )
    else:
        scored = weighted.groupBy("left", "right").agg(
            F.count(F.lit(1)).alias("common_neighbors"),
            F.sum("ra").alias("resource_allocation"), F.sum("aa").alias("adamic_adar"),
        ).join(edges, ["left", "right"], "left_anti")
    left_degrees = degrees.select(F.col("src").alias("left"), F.col("degree").alias("left_degree"))
    right_degrees = degrees.select(F.col("src").alias("right"), F.col("degree").alias("right_degree"))
    return scored.join(left_degrees, "left").join(right_degrees, "right").withColumn(
        "jaccard", F.col("common_neighbors") / (F.col("left_degree") + F.col("right_degree") - F.col("common_neighbors"))
    ).select("left", "right", "common_neighbors", "resource_allocation", "adamic_adar", "jaccard")


def topk_recommendations(scores: Any, k: int) -> Any:
    from pyspark.sql import Window, functions as F

    if k < 1:
        raise ValueError("k must be positive")
    forward = scores.select(F.col("left").alias("user"), F.col("right").alias("candidate"), *METHODS)
    backward = scores.select(F.col("right").alias("user"), F.col("left").alias("candidate"), *METHODS)
    methods = F.array(*[F.struct(F.lit(method).alias("method"), F.col(method).cast("double").alias("score")) for method in METHODS])
    long_scores = forward.unionByName(backward).select("user", "candidate", F.explode(methods).alias("metric")).select(
        "user", "candidate", "metric.method", "metric.score"
    )
    # Different aggregation trees can change the last floating-point bits. Use
    # a documented comparison precision and keep the unrounded score for audit.
    long_scores = long_scores.withColumn("ranking_score", F.bround("score", 12))
    window = Window.partitionBy("user", "method").orderBy(F.col("ranking_score").desc(), F.col("candidate").asc())
    return long_scores.withColumn("rank", F.row_number().over(window)).where(F.col("rank") <= k)


class TableWriter:
    def __init__(self, spark: Any, args: argparse.Namespace, staging: Path, final: Path):
        self.spark, self.args, self.staging, self.final = spark, args, staging, final
        self.tables: dict[str, Any] = {}
        digest = hashlib.sha256((str(final.parent.resolve()) + ":" + args.run_id).encode()).hexdigest()[:12]
        self.prefix = "run_" + re.sub(r"[^a-z0-9_]", "_", args.run_id.lower()) + "_" + digest

    def write(self, name: str, frame: Any) -> None:
        started = time.perf_counter()
        if self.args.format == "parquet":
            destination = self.staging / "tables" / name
            frame.write.mode("errorifexists").parquet(destination.as_uri())
            record = {"format": "parquet", "relative_path": "tables/" + name, "path": (self.final / "tables" / name).as_uri()}
        else:
            identifier = f"local.{self.args.namespace}.{self.prefix}_{name}"
            frame.writeTo(identifier).using("iceberg").createOrReplace()
            snapshot = self.spark.sql(f"SELECT snapshot_id, committed_at FROM {identifier}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
            record = {"format": "iceberg", "table": identifier, "snapshot_id": int(snapshot["snapshot_id"]), "committed_at": str(snapshot["committed_at"])}
        record["write_seconds"] = time.perf_counter() - started
        self.tables[name] = record
        write_json(self.staging / "table_progress.json", self.tables)


def adaptive_spark_settings(args: argparse.Namespace) -> dict[str, str]:
    """Optional execution controls; defaults retain Spark's existing AQE policy.

    Partition/coalesce experiments change execution layout, never graph scores.
    Unspecified target/minimum policies remain the Spark distribution defaults.
    """
    settings = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
    }
    if args.disable_adaptive_coalesce:
        settings["spark.sql.adaptive.coalescePartitions.enabled"] = "false"
    if args.adaptive_advisory_partition_bytes is not None:
        if args.adaptive_advisory_partition_bytes < 1:
            raise ValueError("adaptive-advisory-partition-bytes must be positive")
        settings["spark.sql.adaptive.advisoryPartitionSizeInBytes"] = str(args.adaptive_advisory_partition_bytes)
    if args.adaptive_coalesce_size_first:
        settings["spark.sql.adaptive.coalescePartitions.parallelismFirst"] = "false"
    return settings


def build_spark(args: argparse.Namespace, event_dir: Path) -> Any:
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(f"friend-recommendation-{args.run_id}").master(args.master)
    settings = {
        "spark.sql.shuffle.partitions": str(args.partitions),
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": event_dir.as_uri(),
        "spark.eventLog.compress": "false",
        "spark.eventLog.rolling.enabled": "false",
    }
    settings.update(adaptive_spark_settings(args))
    if args.format == "iceberg":
        settings.update({
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.local.type": "hadoop",
            "spark.sql.catalog.local.warehouse": args.warehouse,
        })
        if args.iceberg_package:
            settings["spark.jars.packages"] = args.iceberg_package
    for key, value in settings.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def execute_pipeline(spark: Any, args: argparse.Namespace, staging: Path, final: Path, event_dir: Path) -> dict[str, Any]:
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    started = time.perf_counter()
    timings: dict[str, float] = {}
    writer = TableWriter(spark, args, staging, final)
    cached = []

    def persist(frame: Any) -> Any:
        frame.persist(StorageLevel.MEMORY_AND_DISK)
        cached.append(frame)
        return frame

    try:
        if args.format == "iceberg":
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS local.{args.namespace}")
        phase = time.perf_counter()
        spark.sparkContext.setJobGroup("clean", "Read, classify, deduplicate and build graph tables")
        parsed = persist(parse_edge_lines(spark.read.text(args.input)))
        counts = parsed.agg(*[F.sum(F.when(F.col("status") == status, 1).otherwise(0)).alias(status) for status in STATUSES], F.count(F.lit(1)).alias("input_rows")).first().asDict()
        counts = {key: int(value or 0) for key, value in counts.items()}
        edges = persist(canonical_edges(parsed))
        edge_count = edges.count()
        if edge_count == 0:
            raise ValueError("Input contains no valid, non-self edges")
        directed, degrees = graph_tables(edges)
        directed, degrees = persist(directed), persist(degrees)
        directed_count = directed.count()
        degree_summary = degrees.agg(
            F.count(F.lit(1)).alias("nodes"), F.max("degree").alias("max_degree"),
            F.sum((F.col("degree").cast("decimal(38,0)") * (F.col("degree") - 1) / 2).cast("decimal(38,0)")).alias("wedges"),
        ).first()
        quality = {
            "input_rows": counts["input_rows"], "classification_counts": {status: counts[status] for status in STATUSES},
            "duplicate_valid_rows": counts["valid"] - edge_count,
            "clean_edges": edge_count, "nodes": int(degree_summary["nodes"]),
            "directed_edges": directed_count, "max_degree": int(degree_summary["max_degree"]),
            "theoretical_wedges": int(degree_summary["wedges"] or 0),
            "quarantined_rows": sum(counts[status] for status in ("malformed", "overflow", "negative_id", "self_loop")),
            "input_accounting_passed": sum(counts[status] for status in STATUSES) == counts["input_rows"],
            "adjacency_count_passed": directed_count == edge_count * 2,
            "id_policy": "exactly two signed-integer tokens; IDs must fit signed int64 and be nonnegative; self-loops rejected; blank/#comment rows ignored",
            "wedge_budget": {"max_wedges": args.max_wedges, "passed": int(degree_summary["wedges"] or 0) <= args.max_wedges},
        }
        # Persist diagnostics before refusing a graph that would explode at the
        # self-join. Raising the limit is an explicit operator decision.
        write_json(staging / "quality.json", quality)
        enforce_wedge_budget(quality["theoretical_wedges"], args.max_wedges)
        if not quality["input_accounting_passed"] or not quality["adjacency_count_passed"]:
            raise AssertionError("Input/adjacency accounting failed")
        for name, frame in (("clean_edges", edges), ("quarantine", parsed.where(~F.col("status").isin("valid", "ignored"))), ("adjacency", directed), ("degree", degrees)):
            writer.write(name, frame)
        timings["clean_materialize_and_write_seconds"] = time.perf_counter() - phase
        write_json(staging / "quality.json", quality)
        if args.fail_after == "clean":
            raise RuntimeError("Injected failure after clean; this run is unpublished and retryable")

        phase = time.perf_counter()
        spark.sparkContext.setJobGroup("score", f"Exact candidate scoring ({args.mode})")
        scores = persist(score_candidates(edges, directed, degrees, args.mode, args.partitions))
        score_count = scores.count()
        (staging / "score_plan.txt").write_text(scores._jdf.queryExecution().toString(), encoding="utf-8")
        writer.write("candidate_scores", scores)
        timings["score_materialize_and_write_seconds"] = time.perf_counter() - phase
        quality["candidate_pairs"] = score_count
        write_json(staging / "quality.json", quality)
        if args.fail_after == "score":
            raise RuntimeError("Injected failure after score; this run is unpublished and retryable")

        phase = time.perf_counter()
        spark.sparkContext.setJobGroup("topk", "Per-user, per-method stable Top-K")
        recommendations = persist(topk_recommendations(scores, args.k))
        recommendation_count = recommendations.count()
        (staging / "topk_plan.txt").write_text(recommendations._jdf.queryExecution().toString(), encoding="utf-8")
        writer.write("recommendations", recommendations)
        timings["topk_materialize_and_write_seconds"] = time.perf_counter() - phase

        phase = time.perf_counter()
        spark.sparkContext.setJobGroup("validate", "Validate result invariants")
        existing_candidates = scores.join(edges, ["left", "right"], "inner").limit(1).count()
        invalid_scores = scores.where(
            (F.col("left") >= F.col("right")) | (F.col("common_neighbors") < 1)
            | (F.col("resource_allocation") <= 0) | (F.col("adamic_adar") <= 0)
            | (F.col("jaccard") <= 0) | (F.col("jaccard") > 1)
            | F.isnan("resource_allocation") | F.isnan("adamic_adar") | F.isnan("jaccard")
        ).limit(1).count()
        checks = {
            "no_existing_edges_in_candidates": existing_candidates == 0,
            "candidate_score_invariants": invalid_scores == 0,
            "recommendation_row_upper_bound": recommendation_count <= quality["nodes"] * args.k * len(METHODS),
        }
        quality.update({"recommendation_rows": recommendation_count, "result_checks": checks})
        if not all(checks.values()):
            raise AssertionError(f"Result invariants failed: {checks}")
        timings["validation_seconds"] = time.perf_counter() - phase
        timings["pipeline_seconds"] = time.perf_counter() - started
        write_json(staging / "quality.json", quality)
        write_json(staging / "timings.json", timings)
        java = spark.sparkContext._jvm.java.lang.System
        environment = {
            "python": sys.version, "platform": platform.platform(), "spark": spark.version,
            "java_version": java.getProperty("java.version"), "java_vendor": java.getProperty("java.vendor"),
            "application_id": spark.sparkContext.applicationId, "master": spark.sparkContext.master,
            "default_parallelism": spark.sparkContext.defaultParallelism,
            "shuffle_partitions": args.partitions, "adaptive_enabled": spark.conf.get("spark.sql.adaptive.enabled"),
            "adaptive_coalesce_enabled": spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled"),
            "adaptive_advisory_partition_size": spark.conf.get("spark.sql.adaptive.advisoryPartitionSizeInBytes"),
            "adaptive_coalesce_parallelism_first": spark.conf.get("spark.sql.adaptive.coalescePartitions.parallelismFirst"),
            "adaptive_coalesce_min_partition_size": spark.conf.get("spark.sql.adaptive.coalescePartitions.minPartitionSize"),
            "driver_memory": spark.sparkContext.getConf().get("spark.driver.memory", "<unset>"),
            "execution_control_requests": {
                "disable_adaptive_coalesce": args.disable_adaptive_coalesce,
                "adaptive_advisory_partition_bytes": args.adaptive_advisory_partition_bytes,
                "adaptive_coalesce_size_first": args.adaptive_coalesce_size_first,
            },
            "storage_level": "MEMORY_AND_DISK", "iceberg_package": args.iceberg_package if args.format == "iceberg" else None,
        }
        write_json(staging / "environment.json", environment)
        return {
            "schema_version": 1, "status": "success", "run_id": args.run_id,
            "completed_at": utc_now(), "input": args.input, "mode": args.mode, "k": args.k,
            "max_wedges": args.max_wedges, "tie_policy": TIE_POLICY,
            "format": args.format, "tables": writer.tables, "quality": quality, "timings": timings,
            "environment": environment, "event_log_directory": str(event_dir),
            "execution_parameters": {
                "shuffle_partitions": args.partitions,
                "requested_adaptive_settings": adaptive_spark_settings(args),
                "effective_coalesce_enabled": environment["adaptive_coalesce_enabled"],
                "effective_advisory_partition_size": environment["adaptive_advisory_partition_size"],
                "effective_coalesce_parallelism_first": environment["adaptive_coalesce_parallelism_first"],
            },
            "publication": {"type": "local_atomic_directory_rename", "manifest_is_visibility_boundary": True,
                "iceberg_cross_table_transaction": False, "successful_run_is_immutable": True},
            "limits": [
                "Exact self-join enumerates O(sum(degree^2)) wedges; extreme-degree hubs can still exhaust resources.",
                "Optimized mode is a measurable hypothesis; baseline already benefits from Spark partial aggregation.",
                "No speedup, cluster scalability, online recommendation benefit or temporal accuracy is asserted by this run.",
                "An OS-killed process can leave a lock; verify the PID is no longer running before manually removing its .locks file.",
            ],
        }
    finally:
        for frame in reversed(cached):
            frame.unpersist(blocking=False)
        # SparkContext has no clearJobGroup() method in the pinned PySpark API.
        for key in ("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel"):
            spark.sparkContext.setLocalProperty(key, None)


def argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Spark-readable txt/txt.gz path, directory or glob")
    parser.add_argument("--output", required=True, help="Local output root; successful runs appear under runs/<run-id>")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--partitions", type=int, default=4)
    parser.add_argument("--disable-adaptive-coalesce", action="store_true",
                        help="Experimental: keep configured shuffle partitions instead of AQE coalescing")
    parser.add_argument("--adaptive-advisory-partition-bytes", type=int,
                        help="Optional AQE advisory target bytes; omitted keeps the Spark default")
    parser.add_argument("--adaptive-coalesce-size-first", action="store_true",
                        help="Optional: set AQE coalesce parallelismFirst=false so target size drives coalescing")
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--max-wedges", type=int, default=DEFAULT_MAX_WEDGES, help="Preflight exact self-join budget; explicitly increase only after checking resources")
    parser.add_argument("--mode", choices=("baseline", "optimized"), default="baseline",
                        help="baseline is the measured lower-shuffle default; optimized is an experimental comparison")
    parser.add_argument("--format", choices=("parquet", "iceberg"), default="parquet")
    parser.add_argument("--event-log-dir", help="Local directory; defaults to <output>/event-logs")
    parser.add_argument("--warehouse", help="Iceberg HadoopCatalog warehouse URI; defaults to local <output>/warehouse")
    parser.add_argument("--namespace", default="friend_recommendation")
    parser.add_argument("--iceberg-package", default=ICEBERG_PACKAGE, help="Maven coordinate, or empty string for a preinstalled runtime JAR")
    parser.add_argument("--fail-after", choices=("clean", "score"), help="Deliberately fail before publication, to demonstrate retry")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = argument_parser()
    args = parser.parse_args(argv)
    if args.partitions < 1 or args.k < 1 or args.max_wedges < 1:
        parser.error("partitions, k and max-wedges must be positive")
    if args.adaptive_advisory_partition_bytes is not None and args.adaptive_advisory_partition_bytes < 1:
        parser.error("adaptive-advisory-partition-bytes must be positive")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.namespace):
        parser.error("namespace must be one SQL identifier (letters, digits, underscores)")
    output = local_path(args.output)
    event_dir = local_path(args.event_log_dir) if args.event_log_dir else output / "event-logs"
    args.warehouse = args.warehouse or (output / "warehouse").as_uri()
    event_dir.mkdir(parents=True, exist_ok=True)
    staging, final, lock = prepare_run(output, args.run_id)
    spark = None
    started = time.perf_counter()
    try:
        startup = time.perf_counter()
        spark = build_spark(args, event_dir)
        spark.sparkContext.setLogLevel("WARN")
        startup_seconds = time.perf_counter() - startup
        manifest = execute_pipeline(spark, args, staging, final, event_dir)
        spark.stop()  # Finalize event log and release local file handles before publish.
        spark = None
        manifest["timings"]["spark_startup_seconds"] = startup_seconds
        manifest["timings"]["end_to_end_seconds"] = time.perf_counter() - started
        write_json(staging / "timings.json", manifest["timings"])
        publish_run(staging, final, manifest)
        print(json.dumps({"status": "success", "run_id": args.run_id, "manifest": str(final / "manifest.json")}, ensure_ascii=False))
        return 0
    except BaseException as exc:
        if staging.exists():
            write_json(staging / "failure.json", {"run_id": args.run_id, "status": "failed_unpublished", "failed_at": utc_now(), "error_type": type(exc).__name__, "error": str(exc)})
        raise
    finally:
        try:
            if spark is not None:
                spark.stop()
        finally:
            lock.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
