"""Leakage-aware link prediction benchmark for the SNAP Facebook graph."""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import random
import time
from collections import defaultdict, deque
from pathlib import Path


METHODS = ("common_neighbors", "jaccard", "adamic_adar", "resource_allocation")


def load_edges(path: Path) -> tuple[set[int], list[tuple[int, int]]]:
    opener = gzip.open if path.suffix == ".gz" else open
    nodes: set[int] = set()
    edges: set[tuple[int, int]] = set()
    with opener(path, "rt", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            fields = line.split()
            try:
                if len(fields) < 2:
                    raise ValueError("expected two node IDs")
                left, right = map(int, fields[:2])
            except ValueError as error:
                raise ValueError(f"{path}:{line_number}: invalid edge: {line.strip()!r}") from error
            if left == right:
                continue
            edge = (left, right) if left < right else (right, left)
            nodes.update(edge)
            edges.add(edge)
    return nodes, sorted(edges)


def adjacency(nodes: set[int], edges: list[tuple[int, int]]) -> dict[int, set[int]]:
    graph = {node: set() for node in nodes}
    for left, right in edges:
        graph[left].add(right)
        graph[right].add(left)
    return graph


def spanning_tree_edges(graph: dict[int, set[int]], start: int) -> set[tuple[int, int]]:
    seen = {start}
    queue = deque([start])
    tree: set[tuple[int, int]] = set()
    while queue:
        node = queue.popleft()
        for neighbor in sorted(graph[node]):
            if neighbor in seen:
                continue
            seen.add(neighbor)
            queue.append(neighbor)
            tree.add((node, neighbor) if node < neighbor else (neighbor, node))
    if len(seen) != len(graph):
        raise ValueError("Input graph is not connected")
    return tree


def split_edges(
    nodes: set[int], edges: list[tuple[int, int]], seed: int, test_fraction: float
) -> tuple[list[tuple[int, int]], list[tuple[int, int]]]:
    if not nodes:
        raise ValueError("Cannot split an empty graph")
    if not math.isfinite(test_fraction) or not 0.0 < test_fraction < 1.0:
        raise ValueError("test_fraction must be strictly between zero and one")
    if any(left not in nodes or right not in nodes or left == right for left, right in edges):
        raise ValueError("Edges must connect two distinct nodes in nodes")
    # Treat undirected duplicates/orientations consistently before protecting a tree.
    edges = sorted({tuple(sorted(edge)) for edge in edges})
    full_graph = adjacency(nodes, edges)
    protected = spanning_tree_edges(full_graph, min(nodes))
    removable = [edge for edge in edges if edge not in protected]
    rng = random.Random(seed)
    rng.shuffle(removable)
    test_size = round(len(edges) * test_fraction)
    if not 1 <= test_size <= len(removable):
        raise ValueError(
            f"Requested {test_size} held-out edges; a nonempty connected split allows "
            f"at most {len(removable)}. Change test_fraction or use a graph with cycles."
        )
    test_edges = removable[:test_size]
    test_set = set(test_edges)
    train_edges = [edge for edge in edges if edge not in test_set]
    return train_edges, test_edges


def score_pair(graph: dict[int, set[int]], left: int, right: int) -> dict[str, float]:
    common = graph[left] & graph[right]
    cn = float(len(common))
    denominator = len(graph[left]) + len(graph[right]) - cn
    return {
        "common_neighbors": cn,
        "jaccard": cn / denominator if denominator else 0.0,
        "adamic_adar": sum(1.0 / math.log(len(graph[node])) for node in common if len(graph[node]) > 1),
        "resource_allocation": sum(1.0 / len(graph[node]) for node in common if graph[node]),
    }


def sample_negative_edges(
    nodes: set[int], full_edges: set[tuple[int, int]], count: int, seed: int
) -> list[tuple[int, int]]:
    if not isinstance(count, int) or count < 0:
        raise ValueError("count must be a nonnegative integer")
    forbidden = {
        tuple(sorted((left, right))) for left, right in full_edges
        if left in nodes and right in nodes and left != right
    }
    available = len(nodes) * (len(nodes) - 1) // 2 - len(forbidden)
    if count > available:
        raise ValueError(f"Requested {count} negatives, but only {available} non-edges exist")
    if count == 0:
        return []
    rng = random.Random(seed)
    node_list = sorted(nodes)
    negatives: set[tuple[int, int]] = set()
    # Rejection sampling is cheap for sparse graphs. Bound it so dense graphs or
    # requests near the complement size cannot hang; reservoir-sample the rest.
    for _ in range(max(100, 20 * count)):
        if len(negatives) == count:
            break
        left, right = rng.sample(node_list, 2)
        edge = (left, right) if left < right else (right, left)
        if edge not in forbidden:
            negatives.add(edge)
    if len(negatives) < count:
        remaining = count - len(negatives)
        reservoir: list[tuple[int, int]] = []
        seen = 0
        for index, left in enumerate(node_list):
            for right in node_list[index + 1:]:
                edge = (left, right)
                if edge in forbidden or edge in negatives:
                    continue
                seen += 1
                if len(reservoir) < remaining:
                    reservoir.append(edge)
                else:
                    slot = rng.randrange(seen)
                    if slot < remaining:
                        reservoir[slot] = edge
        negatives.update(reservoir)
    return sorted(negatives)


def sample_hard_negative_edges(
    graph: dict[int, set[int]], full_edges: set[tuple[int, int]], count: int, seed: int
) -> list[tuple[int, int]]:
    """Sample non-edges that share at least one neighbor in the training graph."""
    if not isinstance(count, int) or count < 0:
        raise ValueError("count must be a nonnegative integer")
    if count == 0:
        return []
    forbidden = {tuple(sorted(edge)) for edge in full_edges}
    candidates: set[tuple[int, int]] = set()
    for neighbors in graph.values():
        ordered = sorted(neighbors)
        for index, left in enumerate(ordered):
            for right in ordered[index + 1 :]:
                edge = (left, right)
                if edge not in forbidden:
                    candidates.add(edge)
    if count > len(candidates):
        raise ValueError(f"Requested {count} hard negatives, but only {len(candidates)} exist")
    rng = random.Random(seed)
    return rng.sample(sorted(candidates), count)


def _validate_binary_scores(labels: list[int], scores: list[float]) -> None:
    if len(labels) != len(scores) or not labels:
        raise ValueError("labels and scores must be nonempty and have equal lengths")
    if any(label not in (0, 1) for label in labels):
        raise ValueError("labels must be binary (zero or one)")
    if any(not math.isfinite(score) for score in scores):
        raise ValueError("scores must be finite numbers")


def auc_score(labels: list[int], scores: list[float]) -> float:
    _validate_binary_scores(labels, scores)
    ordered = sorted(zip(scores, labels), key=lambda item: item[0])
    positive_count = sum(labels)
    negative_count = len(labels) - positive_count
    if not positive_count or not negative_count:
        raise ValueError("ROC AUC requires at least one positive and one negative")
    positive_rank_sum = 0.0
    index = 0
    while index < len(ordered):
        end = index + 1
        while end < len(ordered) and ordered[end][0] == ordered[index][0]:
            end += 1
        average_rank = ((index + 1) + end) / 2.0
        positive_rank_sum += average_rank * sum(label for _, label in ordered[index:end])
        index = end
    return (
        positive_rank_sum - positive_count * (positive_count + 1) / 2.0
    ) / (positive_count * negative_count)


def average_precision(labels: list[int], scores: list[float]) -> float:
    """Non-interpolated threshold AP, grouping equal scores (as in sklearn).

    AP = sum over distinct thresholds of delta-recall * precision. Arbitrarily
    ordering positives before negatives within a tie would inflate this metric.
    A sample containing no positives has AP zero; empty input is invalid.
    """
    _validate_binary_scores(labels, scores)
    order = sorted(range(len(scores)), key=lambda index: (-scores[index], index))
    positives = sum(labels)
    if not positives:
        return 0.0
    hits = 0
    precision_sum = 0.0
    start = 0
    while start < len(order):
        end = start + 1
        while end < len(order) and scores[order[end]] == scores[order[start]]:
            end += 1
        new_hits = sum(labels[index] for index in order[start:end])
        hits += new_hits
        precision_sum += new_hits * hits / end
        start = end
    return precision_sum / positives


def candidate_scores(graph: dict[int, set[int]], user: int) -> dict[str, dict[int, float]]:
    cn: dict[int, float] = defaultdict(float)
    aa: dict[int, float] = defaultdict(float)
    ra: dict[int, float] = defaultdict(float)
    neighbors = graph[user]
    for bridge in neighbors:
        degree = len(graph[bridge])
        aa_weight = 1.0 / math.log(degree) if degree > 1 else 0.0
        ra_weight = 1.0 / degree if degree else 0.0
        for candidate in graph[bridge]:
            if candidate == user or candidate in neighbors:
                continue
            cn[candidate] += 1.0
            aa[candidate] += aa_weight
            ra[candidate] += ra_weight
    jaccard = {
        candidate: value / (len(neighbors) + len(graph[candidate]) - value)
        for candidate, value in cn.items()
    }
    return {
        "common_neighbors": cn,
        "jaccard": jaccard,
        "adamic_adar": aa,
        "resource_allocation": ra,
    }


def ranking_metrics(
    graph: dict[int, set[int]], test_edges: list[tuple[int, int]], k: int
) -> dict[str, dict[str, float]]:
    if not isinstance(k, int) or k <= 0:
        raise ValueError("k must be a positive integer")
    if not test_edges:
        raise ValueError("Ranking evaluation requires at least one held-out edge")
    if any(left not in graph or right not in graph or left == right or right in graph[left]
           for left, right in test_edges):
        raise ValueError("Held-out edges must be valid non-edges in the training graph")
    held_out: dict[int, set[int]] = defaultdict(set)
    for left, right in test_edges:
        held_out[left].add(right)
        held_out[right].add(left)
    totals = {method: defaultdict(float) for method in METHODS}
    users = sorted(held_out)
    for user in users:
        method_scores = candidate_scores(graph, user)
        for method in METHODS:
            ranked = sorted(method_scores[method], key=lambda node: (-method_scores[method][node], node))[:k]
            truth = held_out[user]
            hits = [1 if node in truth else 0 for node in ranked]
            hit_count = sum(hits)
            totals[method]["precision_at_k"] += hit_count / k
            totals[method]["recall_at_k"] += hit_count / len(truth)
            dcg = sum(hit / math.log2(rank + 2) for rank, hit in enumerate(hits))
            ideal = sum(1.0 / math.log2(rank + 2) for rank in range(min(len(truth), k)))
            totals[method]["ndcg_at_k"] += dcg / ideal if ideal else 0.0
            totals[method]["hit_rate_at_k"] += float(hit_count > 0)
            totals[method]["candidate_coverage"] += float(bool(method_scores[method]))
    return {
        method: {metric: value / len(users) for metric, value in totals[method].items()}
        for method in METHODS
    }


def shuffle_statistics(graph: dict[int, set[int]], partitions: int = 4) -> dict[str, int | float]:
    """Local wedge-record simulation; these are not measured Spark shuffle bytes."""
    if not isinstance(partitions, int) or partitions <= 0:
        raise ValueError("partitions must be a positive integer")
    naive_emissions = 0
    local_pairs = [set() for _ in range(partitions)]
    for bridge, neighbors in graph.items():
        ordered = sorted(neighbors)
        naive_emissions += len(ordered) * (len(ordered) - 1) // 2
        bucket = local_pairs[bridge % partitions]
        for index, left in enumerate(ordered):
            for right in ordered[index + 1 :]:
                bucket.add((left, right))
    combined_emissions = sum(map(len, local_pairs))
    return {
        "naive_wedge_records": naive_emissions,
        "combiner_records": combined_emissions,
        "record_reduction": 1.0 - combined_emissions / naive_emissions if naive_emissions else 0.0,
        "partitions": partitions,
    }


def run_seed(
    nodes: set[int], edges: list[tuple[int, int]], seed: int, test_fraction: float, k: int
) -> tuple[list[dict[str, float | int | str]], dict[str, int | float]]:
    train_edges, test_edges = split_edges(nodes, edges, seed, test_fraction)
    graph = adjacency(nodes, train_edges)
    full_edge_set = set(edges)
    random_negatives = sample_negative_edges(nodes, full_edge_set, len(test_edges), seed + 10_000)
    hard_negatives = sample_hard_negative_edges(graph, full_edge_set, len(test_edges), seed + 20_000)
    labels = [1] * len(test_edges) + [0] * len(random_negatives)
    random_pair_scores = {method: [] for method in METHODS}
    hard_pair_scores = {method: [] for method in METHODS}
    started = time.perf_counter()
    for left, right in test_edges + random_negatives:
        scores = score_pair(graph, left, right)
        for method in METHODS:
            random_pair_scores[method].append(scores[method])
    for left, right in test_edges + hard_negatives:
        scores = score_pair(graph, left, right)
        for method in METHODS:
            hard_pair_scores[method].append(scores[method])
    ranking = ranking_metrics(graph, test_edges, k)
    elapsed = time.perf_counter() - started
    rows = []
    for method in METHODS:
        rows.append(
            {
                "seed": seed,
                "method": method,
                "random_auc": auc_score(labels, random_pair_scores[method]),
                "random_average_precision": average_precision(labels, random_pair_scores[method]),
                "hard_auc": auc_score(labels, hard_pair_scores[method]),
                "hard_average_precision": average_precision(labels, hard_pair_scores[method]),
                **ranking[method],
                "evaluation_seconds": elapsed,
                "train_edges": len(train_edges),
                "test_edges": len(test_edges),
                "ranking_users": len({node for edge in test_edges for node in edge}),
            }
        )
    return rows, shuffle_statistics(graph)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seeds", type=int, nargs="+", default=[11, 22, 33, 44, 55])
    parser.add_argument("--test-fraction", type=float, default=0.10)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--source-name", default="User-provided undirected edge list")
    parser.add_argument("--source-url", default=None)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    nodes, edges = load_edges(args.data)
    all_rows: list[dict[str, float | int | str]] = []
    shuffle_rows = []
    for seed in args.seeds:
        rows, shuffle = run_seed(nodes, edges, seed, args.test_fraction, args.k)
        all_rows.extend(rows)
        shuffle_rows.append({"seed": seed, **shuffle})
        print(f"seed={seed} complete")
    with (args.output / "seed_results.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(all_rows[0]))
        writer.writeheader()
        writer.writerows(all_rows)
    with (args.output / "shuffle_results.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(shuffle_rows[0]))
        writer.writeheader()
        writer.writerows(shuffle_rows)
    metadata = {
        "nodes": len(nodes),
        "edges": len(edges),
        "seeds": args.seeds,
        "test_fraction": args.test_fraction,
        "k": args.k,
        "source": args.source_name,
        "source_url": args.source_url,
        "input_file": str(args.data.resolve()),
        "average_precision_definition": "non-interpolated threshold AP; all equal scores grouped",
        "ranking_tie_break": "score descending, node ID ascending",
        "split_protocol": "static hidden-edge evaluation with a protected spanning tree; not temporal forecasting",
        "shuffle_statistics_type": "local record-count simulation, not measured Spark shuffle bytes",
    }
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
