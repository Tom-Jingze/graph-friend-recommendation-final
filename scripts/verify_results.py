"""Independently check published Parquet runs against each other and Python sets."""
from __future__ import annotations

import argparse
from decimal import Decimal, ROUND_HALF_EVEN
import json
from pathlib import Path
import random
import sys

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.experiment import METHODS, adjacency, candidate_scores, load_edges, score_pair


def rounded(value):
    return Decimal(str(value)).quantize(Decimal('0.000000000001'), rounding=ROUND_HALF_EVEN)


def require_integer_keys(frame, columns, table_name):
    """Never coerce graph IDs through float64 (which loses int64 IDs > 2**53)."""
    for column in columns:
        if not pd.api.types.is_integer_dtype(frame[column].dtype) or frame[column].isna().any():
            raise AssertionError(f'{table_name}.{column} must be a nonmissing integer column')


def verify(input_path, first, second, user_samples=64, pair_samples=1000):
    manifests = []
    for path in (first, second):
        if not (path / '_SUCCESS').is_file():
            raise ValueError(f'Not a published successful run: {path}')
        manifest = json.loads((path / 'manifest.json').read_text(encoding='utf-8'))
        if manifest.get('status') != 'success' or manifest.get('format') != 'parquet':
            raise ValueError(f'Expected a successful Parquet run: {path}')
        manifests.append(manifest)
    if manifests[0]['k'] != manifests[1]['k'] or manifests[0].get('tie_policy') != manifests[1].get('tie_policy'):
        raise AssertionError('Runs must use the same k and tie policy')
    left = pd.read_parquet(first / 'tables/candidate_scores').sort_values(['left', 'right'], kind='mergesort').reset_index(drop=True)
    right = pd.read_parquet(second / 'tables/candidate_scores').sort_values(['left', 'right'], kind='mergesort').reset_index(drop=True)
    for frame in (left, right):
        require_integer_keys(frame, ['left', 'right', 'common_neighbors'], 'candidate_scores')
        if frame.duplicated(['left', 'right']).any() or (frame['left'] >= frame['right']).any():
            raise AssertionError('Candidate keys must be unique canonical pairs')
        if not np.isfinite(frame[list(METHODS)].to_numpy(dtype=float)).all():
            raise AssertionError('Candidate scores must be finite')
    pd.testing.assert_frame_equal(left[['left', 'right']], right[['left', 'right']])
    errors = {}
    for method in METHODS:
        np.testing.assert_allclose(left[method], right[method], atol=1e-12, rtol=1e-12)
        errors[method] = float(np.max(np.abs(left[method].to_numpy() - right[method].to_numpy()), initial=0))
    rank_keys = ['user', 'method', 'rank']
    top_a = pd.read_parquet(first / 'tables/recommendations').sort_values(rank_keys, kind='mergesort').reset_index(drop=True)
    top_b = pd.read_parquet(second / 'tables/recommendations').sort_values(rank_keys, kind='mergesort').reset_index(drop=True)
    for frame in (top_a, top_b):
        require_integer_keys(frame, ['user', 'candidate', 'rank'], 'recommendations')
        if frame.duplicated(rank_keys).any() or frame.duplicated(['user', 'method', 'candidate']).any():
            raise AssertionError('Recommendation rank/candidate keys must be unique')
        if not frame['method'].isin(METHODS).all() or not frame['rank'].between(1, manifests[0]['k']).all():
            raise AssertionError('Recommendation method/rank violates its contract')
    pd.testing.assert_frame_equal(top_a[rank_keys + ['candidate']], top_b[rank_keys + ['candidate']])
    nodes, edges = load_edges(input_path)
    graph = adjacency(nodes, edges)
    rng = random.Random(42)
    sampled_indices = rng.sample(range(len(left)), min(pair_samples, len(left)))
    max_reference_error = 0.0
    for index in sampled_indices:
        # iloc[index] creates a mixed numeric Series and silently promotes int64
        # IDs to float64. Select each ID from its original typed column instead.
        left_id = int(left['left'].iat[index])
        right_id = int(left['right'].iat[index])
        expected = score_pair(graph, left_id, right_id)
        for method in METHODS:
            value = float(left[method].iat[index])
            np.testing.assert_allclose(value, expected[method], atol=1e-12, rtol=1e-12)
            max_reference_error = max(max_reference_error, abs(value - expected[method]))
    k = manifests[0]['k']
    sampled_users = rng.sample(sorted(nodes), min(user_samples, len(nodes)))
    for user in sampled_users:
        scores = candidate_scores(graph, user)
        for method in METHODS:
            expected = sorted(scores[method], key=lambda candidate: (-rounded(scores[method][candidate]), candidate))[:k]
            actual = top_a.loc[(top_a['user'] == user) & (top_a['method'] == method)].sort_values('rank')['candidate'].tolist()
            if expected != actual:
                raise AssertionError(f'Top-K differs for user={user}, method={method}: {expected} != {actual}')
    return {'status': 'PASS', 'candidate_pairs_compared_between_modes': len(left),
            'recommendation_rows_compared_between_modes': len(top_a),
            'max_score_error_between_modes': errors,
            'independent_reference_pairs': len(sampled_indices),
            'independent_reference_users': len(sampled_users),
            'max_reference_score_error': max_reference_error,
            'pair_tolerance': {'atol': 1e-12, 'rtol': 1e-12},
            'topk_tie_policy': 'HALF_EVEN score quantized to 12 decimal places, then candidate ID',
            'validation_scope': 'Local pandas loads both result tables and the input graph; score equivalence is full-table, independent Python reference checks are sampled',
            'first_run': str(first), 'second_run': str(second),
            'pandas': pd.__version__, 'numpy': np.__version__}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True, type=Path)
    parser.add_argument('--first-run', required=True, type=Path)
    parser.add_argument('--second-run', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    report = verify(args.input, args.first_run, args.second_run)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps(report, indent=2))
