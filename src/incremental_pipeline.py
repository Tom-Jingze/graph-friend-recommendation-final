"""Exact micro-batch edge updates; immutable snapshots, bounded affected-user work.

An edge update can change Jaccard for *all* two-hop partners of its endpoints.
Therefore one-hop invalidation is insufficient: the affected recommendation
users are the endpoints and their <=2-hop neighbors in the old/new union graph.
Only candidate pairs incident to these users are recomputed. Unaffected scores
and recommendation rows are retained. Snapshot files are still fully written;
this is incremental computation, not a claim of incremental physical I/O.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import sys
import time

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.spark_pipeline import (METHODS, TIE_POLICY, DEFAULT_MAX_WEDGES, TableWriter,
    build_spark, graph_tables, local_path, prepare_run, publish_run,
    topk_recommendations, utc_now, validate_run_id, write_json)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_manifest(path: Path) -> dict:
    if not (path / "_SUCCESS").is_file():
        raise ValueError("Base must be an atomically published successful run")
    manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("status") != "success" or manifest.get("tie_policy") != TIE_POLICY:
        raise ValueError("Incompatible base status or tie policy")
    if manifest.get("format") not in ("parquet", "iceberg"):
        raise ValueError("Unsupported base format")
    for table in ("clean_edges", "candidate_scores", "recommendations"):
        if table not in manifest["tables"]:
            raise ValueError("Missing base table: " + table)
    return manifest


def request_identity(args: argparse.Namespace, base: dict) -> dict:
    # Byte hashes bind a retry to the same committed base and exact input file.
    payload = {"base_manifest_sha256": sha256(args.base_run / "manifest.json"),
        "changes_sha256": sha256(args.changes), "k": base["k"],
        "format": args.format, "tie_policy": TIE_POLICY, "algorithm": "two-hop-invalidation-v1"}
    return {**payload, "request_sha256": hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()}


def prepare_batch(args: argparse.Namespace, base: dict):
    if args.format != base["format"]:
        raise ValueError("Base and batch formats must match; cross-format migration is not supported")
    identity = request_identity(args, base)
    try:
        staging, final, lock = prepare_run(local_path(args.output), args.run_id)
        return staging, final, lock, identity, None
    except FileExistsError:
        final = local_path(args.output) / "runs" / validate_run_id(args.run_id)
        if not final.exists():
            raise  # Concurrent active lock, not an already committed request.
        existing = load_manifest(final)
        if existing.get("request", {}).get("request_sha256") != identity["request_sha256"]:
            raise ValueError("Batch ID already committed for a DIFFERENT base or change file")
        return None, final, None, identity, existing


def read_table(spark, base_path: Path, manifest: dict, name: str):
    record = manifest["tables"][name]
    if record["format"] == "parquet":
        # Relative references make local snapshots portable as a directory tree.
        return spark.read.parquet((base_path / record["relative_path"]).as_uri())
    return spark.read.format("iceberg").option("snapshot-id", str(record["snapshot_id"])).load(record["table"])


def snapshot_changes(source: Path, staging: Path, expected_sha256: str) -> Path:
    """Bind Spark's eventual lazy read to the exact bytes identifying this batch."""
    snapshot = staging / "changes_input.txt"
    shutil.copyfile(source, snapshot)
    if sha256(snapshot) != expected_sha256:
        raise ValueError("Change input mutated after request hashing; batch rejected")
    return snapshot


def parse_changes(spark, path: Path):
    from pyspark.sql import functions as F
    raw = spark.read.text(path.as_uri()).withColumn("line", F.trim("value"))
    rows = raw.where((F.length("line") > 0) & ~F.col("line").startswith("#"))
    rows = rows.withColumn("tokens", F.split("line", r"\s+"))
    rows = rows.withColumn("op", F.upper(F.expr("try_element_at(tokens, 1)")))
    rows = rows.withColumn("a", F.expr("try_cast(try_element_at(tokens, 2) AS BIGINT)"))
    rows = rows.withColumn("b", F.expr("try_cast(try_element_at(tokens, 3) AS BIGINT)"))
    valid = ((F.size("tokens") == 3) & F.col("op").isin("ADD", "DELETE")
        & F.expr("try_element_at(tokens, 2)").rlike(r"^[+]?[0-9]+$")
        & F.expr("try_element_at(tokens, 3)").rlike(r"^[+]?[0-9]+$")
        & F.col("a").isNotNull() & F.col("b").isNotNull()
        & (F.col("a") >= 0) & (F.col("b") >= 0) & (F.col("a") != F.col("b")))
    invalid = rows.where(~F.coalesce(valid, F.lit(False))).select("value")
    changes = rows.where(valid).select("op", F.least("a", "b").alias("left"),
        F.greatest("a", "b").alias("right")).dropDuplicates()
    return changes, invalid


def apply_changes(edges, changes):
    adds = changes.where("op = 'ADD'").select("left", "right").join(edges, ["left", "right"], "left_anti")
    deletes = changes.where("op = 'DELETE'").select("left", "right").join(edges, ["left", "right"], "inner")
    updated = edges.join(deletes, ["left", "right"], "left_anti").unionByName(adds)
    return updated, adds, deletes


def affected_users(old_edges, new_edges, changed_edges):
    """Conservative exact invalidation, including endpoint-degree effects."""
    from pyspark.sql import functions as F
    endpoints = changed_edges.select(F.col("left").alias("user")).unionByName(
        changed_edges.select(F.col("right").alias("user"))).distinct()
    union_edges = old_edges.unionByName(new_edges).distinct()
    adjacency, _ = graph_tables(union_edges)
    users = endpoints
    for _ in range(2):
        reached = adjacency.join(users, adjacency.src == users.user, "left_semi").select(F.col("dst").alias("user"))
        # Materialized reliable checkpoints bound Catalyst lineage across hops.
        # Without this, repeated old/new joins create large serialized plans.
        users = users.unionByName(reached).distinct().checkpoint(eager=True)
    return users


def incident_pairs(scores, users, how="inner"):
    """Select scores with either endpoint in users, or their complement."""
    from pyspark.sql import functions as F
    a = users.select(F.col("user").alias("left"))
    b = users.select(F.col("user").alias("right"))
    if how == "left_anti":
        return scores.join(a, "left", "left_anti").join(b, "right", "left_anti")
    first = scores.join(a, "left", "left_semi")
    second = scores.join(a, "left", "left_anti").join(b, "right", "left_semi")
    return first.unionByName(second)


def score_affected(edges, directed, degrees, users):
    """Generate wedges anchored only at affected users, with one pair orientation."""
    from pyspark.sql import functions as F
    anchors = directed.join(users, directed.src == users.user, "left_semi").select(
        F.col("src").alias("user"), F.col("dst").alias("bridge"))
    other = directed.select(F.col("src").alias("bridge"), F.col("dst").alias("candidate"))
    wedges = anchors.join(other, "bridge").where("user != candidate")
    candidate_flag = users.select(F.col("user").alias("candidate"), F.lit(True).alias("candidate_affected"))
    wedges = wedges.join(candidate_flag, "candidate", "left").where(
        (F.col("user") < F.col("candidate")) | F.col("candidate_affected").isNull())
    wedges = wedges.select("bridge", F.least("user", "candidate").alias("left"),
        F.greatest("user", "candidate").alias("right")).join(edges, ["left", "right"], "left_anti")
    weighted = wedges.join(degrees.select(F.col("src").alias("bridge"), "degree"), "bridge")
    scored = weighted.groupBy("left", "right").agg(F.count(F.lit(1)).alias("common_neighbors"),
        F.sum(F.lit(1.0) / F.col("degree")).alias("resource_allocation"),
        F.sum(F.lit(1.0) / F.log("degree")).alias("adamic_adar"))
    return scored.join(degrees.select(F.col("src").alias("left"), F.col("degree").alias("ld")), "left").join(
        degrees.select(F.col("src").alias("right"), F.col("degree").alias("rd")), "right").withColumn(
        "jaccard", F.col("common_neighbors") / (F.col("ld") + F.col("rd") - F.col("common_neighbors"))).select(
        "left", "right", *METHODS)


def execute_incremental(spark, args, staging, final, event_dir, base, identity):
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    started = time.perf_counter()
    cached = []
    def cache(frame):
        frame.persist(StorageLevel.MEMORY_AND_DISK)
        cached.append(frame)
        return frame
    args.k = int(base["k"])
    spark.sparkContext.setCheckpointDir((staging / "lineage-checkpoints").as_uri())
    writer = TableWriter(spark, args, staging, final)
    try:
        if args.format == "iceberg":
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS local.{args.namespace}")
        spark.sparkContext.setJobGroup("incremental-graph", "Validate and apply edge changes")
        snapshot = snapshot_changes(args.changes, staging, identity["changes_sha256"])
        changes, invalid = parse_changes(spark, snapshot)
        changes = cache(changes)
        invalid_count = invalid.count()
        if invalid_count:
            writer.write("rejected_changes", invalid)
            raise ValueError(f"Invalid change rows: {invalid_count}; batch rejected without publication")
        if changes.groupBy("left", "right").count().where("count > 1").limit(1).count():
            raise ValueError("Conflicting ADD and DELETE for the same edge in one batch")
        old_edges = cache(read_table(spark, args.base_run, base, "clean_edges"))
        edges, adds, deletes = apply_changes(old_edges, changes)
        adds, deletes, edges = cache(adds), cache(deletes), cache(edges)
        directed, degrees = graph_tables(edges)
        directed, degrees = cache(directed), cache(degrees)
        affected = cache(affected_users(old_edges, edges, adds.unionByName(deletes)))
        counts = {"requested_unique_changes": changes.count(), "effective_adds": adds.count(),
            "effective_deletes": deletes.count(), "affected_users": affected.count(),
            "clean_edges": edges.count(), "nodes": degrees.count()}
        counts["noop_changes"] = counts["requested_unique_changes"] - counts["effective_adds"] - counts["effective_deletes"]
        # This is the exact oriented wedge work before canonical-orientation pruning.
        work = directed.join(affected, directed.src == affected.user, "left_semi").join(
            degrees.select(F.col("src").alias("dst"), "degree"), "dst").agg(
            F.sum((F.col("degree") - 1).cast("decimal(38,0)")).alias("work")).first()["work"]
        counts["affected_oriented_wedges_upper_bound"] = int(work or 0)
        if counts["affected_oriented_wedges_upper_bound"] > args.max_wedges:
            raise ValueError("Affected wedge budget exceeded before expansion")
        for name, frame in (("clean_edges", edges), ("adjacency", directed), ("degree", degrees),
                ("changes", changes), ("effective_adds", adds), ("effective_deletes", deletes), ("affected_users", affected)):
            writer.write(name, frame)
        if args.fail_after == "graph":
            raise RuntimeError("Injected failure after graph, before publication")
        graph_seconds = time.perf_counter() - started
        spark.sparkContext.setJobGroup("incremental-score", "Score only pairs incident to affected users")
        old_scores = cache(read_table(spark, args.base_run, base, "candidate_scores"))
        recomputed = cache(score_affected(edges, directed, degrees, affected))
        retained = cache(incident_pairs(old_scores, affected, "left_anti"))
        scores = cache(retained.unionByName(recomputed))
        counts.update({"recomputed_candidate_pairs": recomputed.count(), "retained_candidate_pairs": retained.count(),
            "candidate_pairs": scores.count()})
        (staging / "incremental_score_plan.txt").write_text(recomputed._jdf.queryExecution().toString(), encoding="utf-8")
        writer.write("candidate_scores", scores)
        if args.fail_after == "score":
            raise RuntimeError("Injected failure after score, before publication")
        spark.sparkContext.setJobGroup("incremental-topk", "Replace only affected users' Top-K rows")
        old_topk = read_table(spark, args.base_run, base, "recommendations")
        retained_topk = old_topk.join(affected, "user", "left_anti")
        replaced_topk = topk_recommendations(recomputed, args.k).join(affected, "user", "left_semi")
        topk = cache(retained_topk.unionByName(replaced_topk))
        counts["recommendation_rows"] = topk.count()
        writer.write("recommendations", topk)
        elapsed = time.perf_counter() - started
        manifest = {"schema_version": 2, "kind": "incremental", "status": "success", "run_id": args.run_id,
            "completed_at": utc_now(), "base_run_id": base["run_id"], "base_run": str(args.base_run),
            "request": identity, "k": args.k, "format": args.format, "tie_policy": TIE_POLICY,
            "tables": writer.tables, "quality": counts, "timings": {"graph_seconds": graph_seconds, "pipeline_seconds": elapsed},
            "environment": {"spark": spark.version, "application_id": spark.sparkContext.applicationId,
                "master": spark.sparkContext.master, "partitions": args.partitions},
            "event_log_directory": str(event_dir),
            "publication": {"type": "local_atomic_directory_rename", "manifest_is_visibility_boundary": True,
                "iceberg_cross_table_transaction": False, "successful_run_is_immutable": True},
            "limits": ["Static graph change replay, not real historical social events.",
                "Conservative two-hop invalidation can cover most users for a hub update; no speedup guarantee.",
                "Complete snapshot tables are rewritten; computation is incremental, physical output I/O is not.",
                "Iceberg commits are per table; readers must enter through the successful manifest."]}
        write_json(staging / "quality.json", counts)
        write_json(staging / "timings.json", manifest["timings"])
        if args.fail_after == "publish":
            raise RuntimeError("Injected failure before atomic publication")
        return manifest
    finally:
        for frame in reversed(cached):
            frame.unpersist(blocking=False)
        for key in ("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel"):
            spark.sparkContext.setLocalProperty(key, None)


def argument_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-run", type=Path, required=True)
    parser.add_argument("--changes", type=Path, required=True, help="Local UTF-8: ADD/DELETE left right; # comments allowed")
    parser.add_argument("--output", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--partitions", type=int, default=4)
    parser.add_argument("--format", choices=("parquet", "iceberg"), default="parquet")
    parser.add_argument("--warehouse")
    parser.add_argument("--namespace", default="friend_recommendation")
    parser.add_argument("--iceberg-package", default="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0")
    parser.add_argument("--max-wedges", type=int, default=DEFAULT_MAX_WEDGES)
    parser.add_argument("--fail-after", choices=("graph", "score", "publish"))
    parser.set_defaults(disable_adaptive_coalesce=False, adaptive_advisory_partition_bytes=None, adaptive_coalesce_size_first=False)
    return parser


def main(argv=None):
    args = argument_parser().parse_args(argv)
    import re
    if args.partitions < 1 or args.max_wedges < 1 or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.namespace):
        raise ValueError("Invalid partition count, wedge budget or SQL namespace")
    args.base_run, args.changes = args.base_run.resolve(), args.changes.resolve()
    output = local_path(args.output)
    args.warehouse = args.warehouse or (output / "warehouse").as_uri()
    base = load_manifest(args.base_run)
    staging, final, lock, identity, committed = prepare_batch(args, base)
    if committed is not None:
        print(json.dumps({"status": "already_published", "manifest": str(final / "manifest.json")}))
        return 0
    spark = None
    event_dir = output / "event-logs"
    event_dir.mkdir(parents=True, exist_ok=True)
    try:
        spark = build_spark(args, event_dir)
        spark.sparkContext.setLogLevel("WARN")
        manifest = execute_incremental(spark, args, staging, final, event_dir, base, identity)
        spark.stop()
        spark = None
        publish_run(staging, final, manifest)
        print(json.dumps({"status": "success", "manifest": str(final / "manifest.json")}))
        return 0
    except BaseException as exc:
        write_json(staging / "failure.json", {"status": "failed_unpublished", "error": str(exc), "failed_at": utc_now()})
        raise
    finally:
        try:
            if spark is not None:
                spark.stop()
        finally:
            lock.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
