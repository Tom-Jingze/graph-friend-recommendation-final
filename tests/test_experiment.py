from pathlib import Path
import sys
import itertools
import math

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from experiment import (
    adjacency, auc_score, average_precision, load_edges, ranking_metrics,
    sample_hard_negative_edges, sample_negative_edges, shuffle_statistics, split_edges,
)


def test_split_preserves_connectivity_and_has_no_overlap() -> None:
    nodes = set(range(6))
    edges = [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 0), (0, 3), (1, 4)]
    train, test = split_edges(nodes, edges, seed=7, test_fraction=0.25)
    assert set(train).isdisjoint(test)
    graph = adjacency(nodes, train)
    seen, frontier = {0}, [0]
    while frontier:
        node = frontier.pop()
        for neighbor in graph[node] - seen:
            seen.add(neighbor)
            frontier.append(neighbor)
    assert seen == nodes


def test_auc_handles_ties() -> None:
    assert auc_score([1, 1, 0, 0], [2.0, 1.0, 1.0, 0.0]) == 0.875


def test_threshold_ap_for_constant_predictions_is_prevalence_and_order_invariant() -> None:
    for labels in set(itertools.permutations([1, 1, 0, 0, 0])):
        assert average_precision(list(labels), [0.3] * 5) == pytest.approx(0.4)


def test_threshold_ap_matches_hand_computed_precision_recall_steps() -> None:
    # At score 0.9: recall=1/2, precision=1/2; at 0.2: recall=1,
    # precision=2/3. AP = (1/2)*(1/2) + (1/2)*(2/3) = 7/12.
    assert average_precision([1, 0, 1], [0.9, 0.9, 0.2]) == pytest.approx(7 / 12)
    assert average_precision([1, 1], [0.0, 0.0]) == 1.0
    assert average_precision([0, 0], [1.0, 0.0]) == 0.0


@pytest.mark.parametrize("metric", [auc_score, average_precision])
@pytest.mark.parametrize("labels,scores", [([], []), ([1], [1.0, 2.0]),
                                        ([2, 0], [1.0, 0.0]),
                                        ([1, 0], [math.nan, 0.0]),
                                        ([1, 0], [math.inf, 0.0])])
def test_metrics_reject_undefined_or_malformed_inputs(metric, labels, scores) -> None:
    with pytest.raises(ValueError):
        metric(labels, scores)


def test_auc_requires_both_classes() -> None:
    with pytest.raises(ValueError, match="positive and one negative"):
        auc_score([1, 1], [0.2, 0.1])


def test_dense_graph_negative_sampling_returns_complement_and_rejects_impossible_count() -> None:
    nodes = set(range(30))
    complement = {(0, 29), (4, 17)}
    edges = set(itertools.combinations(sorted(nodes), 2)) - complement
    assert set(sample_negative_edges(nodes, edges, 2, seed=7)) == complement
    with pytest.raises(ValueError, match="only 2"):
        sample_negative_edges(nodes, edges, 3, seed=7)
    assert sample_negative_edges(set(), set(), 0, seed=7) == []


def test_negative_sampling_normalizes_undirected_edges() -> None:
    assert sample_negative_edges({-3, -1, 5}, {(-1, -3), (5, -1)}, 1, 1) == [(-3, 5)]


def test_hard_negatives_support_arbitrary_integer_ids_and_clear_capacity_errors() -> None:
    graph = adjacency({-3, -1, 5}, [(-3, -1), (-1, 5)])
    assert sample_hard_negative_edges(graph, {(-1, -3), (5, -1)}, 1, 1) == [(-3, 5)]
    with pytest.raises(ValueError, match="only 1"):
        sample_hard_negative_edges(graph, {(-3, -1), (-1, 5)}, 2, 1)


def test_connected_split_rejects_unachievable_holdout_and_normalizes_edges() -> None:
    with pytest.raises(ValueError, match="at most 0"):
        split_edges({0, 1, 2}, [(0, 1), (1, 2)], 1, 0.5)
    with pytest.raises(ValueError, match="empty graph"):
        split_edges(set(), [], 1, 0.1)
    train, test = split_edges({0, 1, 2}, [(1, 0), (0, 1), (2, 1), (2, 0)], 1, 0.34)
    assert len(train) == 2 and len(test) == 1
    assert all(left < right for left, right in train + test)


@pytest.mark.parametrize("fraction", [0, 1, -0.1, math.nan])
def test_split_validates_fraction(fraction) -> None:
    with pytest.raises(ValueError, match="strictly between"):
        split_edges({0, 1, 2}, [(0, 1), (1, 2), (0, 2)], 1, fraction)


def test_ranking_requires_held_out_edges_and_positive_k() -> None:
    graph = adjacency({0, 1, 2}, [(0, 1), (1, 2)])
    with pytest.raises(ValueError, match="held-out"):
        ranking_metrics(graph, [], 1)
    with pytest.raises(ValueError, match="positive integer"):
        ranking_metrics(graph, [(0, 2)], 0)
    with pytest.raises(ValueError, match="non-edges"):
        ranking_metrics(graph, [(0, 1)], 1)


def test_empty_wedge_simulation_is_finite() -> None:
    assert shuffle_statistics({})["record_reduction"] == 0.0
    with pytest.raises(ValueError, match="positive integer"):
        shuffle_statistics({}, 0)


def test_edge_reader_has_line_diagnostics_and_accepts_indented_comments(tmp_path) -> None:
    data = tmp_path / "edges.txt"
    data.write_text("  # header\n4 1\n1 4\n2 2\n", encoding="utf-8")
    assert load_edges(data) == ({1, 4}, [(1, 4)])
    data.write_text("1 2\nbad\n", encoding="utf-8")
    with pytest.raises(ValueError, match=r"edges.txt:2:"):
        load_edges(data)
