"""Summarize experiment CSVs and produce dependency-free SVG charts."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def summarize(rows: list[dict[str, str]]) -> list[dict[str, str | float]]:
    metrics = (
        "random_auc",
        "random_average_precision",
        "hard_auc",
        "hard_average_precision",
        "precision_at_k",
        "recall_at_k",
        "ndcg_at_k",
        "hit_rate_at_k",
    )
    groups: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        groups[row["method"]].append(row)
    output = []
    for method, method_rows in groups.items():
        record: dict[str, str | float] = {"method": method}
        for metric in metrics:
            values = [float(row[metric]) for row in method_rows]
            record[f"{metric}_mean"] = statistics.mean(values)
            record[f"{metric}_std"] = statistics.stdev(values)
        output.append(record)
    return sorted(output, key=lambda row: float(row["ndcg_at_k_mean"]), reverse=True)


def write_csv(path: Path, rows: list[dict[str, str | float]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def svg_bar(path: Path, rows: list[dict[str, str | float]], metric: str, title: str, subtitle: str) -> None:
    width, height = 960, 560
    left, right, top, bottom = 225, 70, 105, 70
    plot_width = width - left - right
    row_height = (height - top - bottom) / len(rows)
    maximum = max(float(row[metric]) for row in rows) * 1.12
    colors = ["#2457A6", "#E19728", "#6B7A3A", "#C05A78"]
    labels = {
        "common_neighbors": "Common Neighbors",
        "jaccard": "Jaccard",
        "adamic_adar": "Adamic-Adar",
        "resource_allocation": "Resource Allocation",
    }
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#FFFFFF"/>',
        f'<text x="40" y="42" font-family="Arial" font-size="25" font-weight="700" fill="#20242A">{title}</text>',
        f'<text x="40" y="70" font-family="Arial" font-size="14" fill="#616975">{subtitle}</text>',
    ]
    for tick in range(6):
        value = maximum * tick / 5
        x = left + plot_width * tick / 5
        parts.append(f'<line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{height-bottom}" stroke="#E6E8EB"/>')
        parts.append(f'<text x="{x:.1f}" y="{height-bottom+28}" text-anchor="middle" font-family="Arial" font-size="12" fill="#5D6670">{value:.3f}</text>')
    for index, row in enumerate(rows):
        value = float(row[metric])
        y = top + index * row_height + row_height * 0.22
        bar_height = row_height * 0.56
        bar_width = plot_width * value / maximum
        label = labels[str(row["method"])]
        parts.append(f'<text x="{left-14}" y="{y+bar_height*0.68:.1f}" text-anchor="end" font-family="Arial" font-size="15" fill="#20242A">{label}</text>')
        parts.append(f'<rect x="{left}" y="{y:.1f}" width="{bar_width:.1f}" height="{bar_height:.1f}" fill="{colors[index]}"/>')
        parts.append(f'<text x="{left+bar_width+9:.1f}" y="{y+bar_height*0.68:.1f}" font-family="Arial" font-size="14" font-weight="700" fill="#20242A">{value:.4f}</text>')
    parts.append('</svg>')
    path.write_text("\n".join(parts), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results", type=Path, required=True)
    args = parser.parse_args()
    rows = read_csv(args.results / "seed_results.csv")
    summary = summarize(rows)
    write_csv(args.results / "summary.csv", summary)
    metadata = json.loads((args.results / "metadata.json").read_text(encoding="utf-8"))
    svg_bar(
        args.results / "ndcg_at_10.svg",
        summary,
        "ndcg_at_k_mean",
        "Link-prediction ranking quality",
        f"Mean NDCG@{metadata['k']} across {len(metadata['seeds'])} connectivity-preserving edge splits; error values in summary.csv",
    )
    svg_bar(
        args.results / "auc.svg",
        summary,
        "hard_auc_mean",
        "Held-out edge discrimination",
        f"Mean hard-negative AUC across {len(metadata['seeds'])} splits; negatives share at least one training neighbor",
    )


if __name__ == "__main__":
    main()
