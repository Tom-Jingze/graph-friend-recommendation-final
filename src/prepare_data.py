"""Reproducible edge-record sampling and an honest two-hop workload preflight."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import random
from collections import Counter
from pathlib import Path


def open_text(path):
    return gzip.open(path, 'rt', encoding='utf-8') if str(path).endswith('.gz') else open(path, encoding='utf-8')


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def prepare(source: Path, output: Path, sample_edges: int, seed: int):
    if sample_edges <= 0:
        raise ValueError('sample_edges must be positive')
    if output.exists() or output.with_suffix(output.suffix + '.json').exists():
        raise FileExistsError(f'Refusing to overwrite sample or metadata: {output}')
    rng = random.Random(seed)
    reservoir = []
    counts = Counter()
    with open_text(source) as handle:
        for line in handle:
            counts['source_lines'] += 1
            if not line.strip() or line.lstrip().startswith('#'):
                counts['comment_or_blank'] += 1
                continue
            fields = line.split()
            try:
                if len(fields) != 2:
                    raise ValueError('not two columns')
                left, right = map(int, fields)
                if min(left, right) < 0 or max(left, right) > 2**63 - 1:
                    raise ValueError('node outside nonnegative int64')
            except ValueError:
                counts['invalid_lines'] += 1
                continue
            if left == right:
                counts['self_loops'] += 1
                continue
            counts['valid_edge_records'] += 1
            edge = (min(left, right), max(left, right))
            index = counts['valid_edge_records']
            if len(reservoir) < sample_edges:
                reservoir.append(edge)
            else:
                replace = rng.randrange(index)
                if replace < sample_edges:
                    reservoir[replace] = edge
    edges = sorted(set(reservoir))
    degrees = Counter()
    for left, right in edges:
        degrees[left] += 1
        degrees[right] += 1
    wedges = sum(d * (d - 1) // 2 for d in degrees.values())
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open('x', encoding='utf-8', newline='\n') as handle:
        handle.write('# Uniform reservoir sample of valid input edge records, then deduplicated.\n')
        for left, right in edges:
            handle.write(f'{left}\t{right}\n')
    metadata = {
        'source': str(source.resolve()),
        'source_sha256': sha256_file(source),
        'sampling': 'uniform reservoir of valid edge records; canonicalize and deduplicate sampled records',
        'seed': seed, 'requested_records': sample_edges, 'source_counts': dict(counts),
        'sampled_records': len(reservoir), 'unique_edges': len(edges), 'nodes': len(degrees),
        'max_degree': max(degrees.values(), default=0), 'two_hop_wedge_records': wedges,
        'top10_degree_wedge_fraction': sum(math.comb(d, 2) for _, d in degrees.most_common(10)) / wedges if wedges else 0,
        'sample_sha256': sha256_file(output),
        'limitations': ['Sampled graph metrics do not estimate full-graph recommendation quality.',
                        'Source is a static graph without real event timestamps.'],
    }
    output.with_suffix(output.suffix + '.json').write_text(json.dumps(metadata, indent=2), encoding='utf-8')
    return metadata


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--sample-edges', type=int, default=200_000)
    parser.add_argument('--seed', type=int, default=42)
    args = parser.parse_args()
    print(json.dumps(prepare(args.input, args.output, args.sample_edges, args.seed), indent=2))


if __name__ == '__main__':
    main()
