"""Build and execute a dependency-free reader-facing experiment notebook."""

from __future__ import annotations

import contextlib
import csv
import io
import json
import os
from pathlib import Path


ROOT = Path(__file__).parent
RESULTS = ROOT / "results"
NOTEBOOK = ROOT / "notebooks" / "link_prediction_experiment.ipynb"


def code_cell(source: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": source.splitlines(keepends=True),
    }


def markdown_cell(source: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": source.splitlines(keepends=True)}


def execute(cells: list[dict]) -> None:
    namespace = {"__name__": "__notebook__"}
    execution_count = 0
    for cell in cells:
        if cell["cell_type"] != "code":
            continue
        execution_count += 1
        stream = io.StringIO()
        source = "".join(cell["source"])
        with contextlib.redirect_stdout(stream):
            exec(compile(source, f"cell-{execution_count}", "exec"), namespace)
        cell["execution_count"] = execution_count
        output = stream.getvalue()
        if output:
            cell["outputs"] = [{"name": "stdout", "output_type": "stream", "text": output.splitlines(keepends=True)}]


def main() -> None:
    summary_rows = list(csv.DictReader((RESULTS / "summary.csv").open(encoding="utf-8")))
    best = summary_rows[0]
    baseline = next(row for row in summary_rows if row["method"] == "common_neighbors")
    ndcg_gain = float(best["ndcg_at_k_mean"]) - float(baseline["ndcg_at_k_mean"])
    recall_gain = float(best["recall_at_k_mean"]) - float(baseline["recall_at_k_mean"])
    cells = [
        markdown_cell(
            "# 社交图好友推荐与链接预测实验\n\n"
            "## tl;dr\n\n"
            f"在SNAP Facebook图的5个连通性保护划分上，Resource Allocation取得最佳NDCG@10="
            f"{float(best['ndcg_at_k_mean']):.4f}，相对Common Neighbors提高{ndcg_gain*100:.2f}个百分点；"
            f"Recall@10提高{recall_gain*100:.2f}个百分点。"
        ),
        markdown_cell(
            "## Context & Methods\n\n"
            "目标是把共同好友排序升级为可离线评价的链接预测实验。每个随机种子保护完整图的一棵生成树，"
            "再隐藏10%的非树边，确保训练图连通且测试边不泄漏。主AUC使用共享至少一个训练邻居的困难负边。\n\n"
            "### Key Assumptions\n\n"
            "- SNAP合并图是静态无向图，没有时间戳，因此结果反映结构恢复能力，不代表线上因果收益。\n"
            "- Top-K候选来自训练图二跳邻居，符合朋友的朋友召回场景。\n"
        ),
        markdown_cell("## Data\n"),
        code_cell(
            "import csv, json\n"
            "from pathlib import Path\n"
            "root = Path('..').resolve()\n"
            "metadata = json.loads((root / 'results' / 'metadata.json').read_text(encoding='utf-8'))\n"
            "print(metadata)\n"
        ),
        markdown_cell("## Results\n"),
        code_cell(
            "with (root / 'results' / 'summary.csv').open(encoding='utf-8') as handle:\n"
            "    rows = list(csv.DictReader(handle))\n"
            "for row in rows:\n"
            "    print(row['method'], 'hard_auc=', round(float(row['hard_auc_mean']), 4), "
            "'recall@10=', round(float(row['recall_at_k_mean']), 4), "
            "'ndcg@10=', round(float(row['ndcg_at_k_mean']), 4))\n"
        ),
        markdown_cell(
            "![NDCG comparison](../results/ndcg_at_10.svg)\n\n"
            "![Hard-negative AUC comparison](../results/auc.svg)\n"
        ),
        code_cell(
            "with (root / 'results' / 'shuffle_results.csv').open(encoding='utf-8') as handle:\n"
            "    shuffle = list(csv.DictReader(handle))\n"
            "mean_reduction = sum(float(row['record_reduction']) for row in shuffle) / len(shuffle)\n"
            "print('mean combiner record reduction:', f'{mean_reduction:.2%}')\n"
        ),
        markdown_cell(
            "## Takeaways\n\n"
            "1. 对高聚类社交图，降低高连接公共邻居权重的Resource Allocation优于原始共同好友计数。\n"
            "2. 随机负边使AUC接近1，主报告应使用二跳困难负边和Top-K排序指标。\n"
            "3. 本地聚合显著减少中间记录，但这里只模拟记录量；原四节点Spark/Hadoop运行时间需在原集群环境复测。\n"
        ),
    ]
    previous_directory = Path.cwd()
    NOTEBOOK.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.chdir(NOTEBOOK.parent)
        execute(cells)
    finally:
        os.chdir(previous_directory)
    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.10+"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    NOTEBOOK.write_text(json.dumps(notebook, ensure_ascii=False, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
