# V3 分区与 AQE 执行布局实验

本轮只调整 Spark 执行参数，默认仍是 baseline、4 个 shuffle 分区和 Spark 原有 AQE 合并策略。CN、Jaccard、AA、RA、候选集合、排序规则和 K 均保持原定义。实验结果未生成前不承诺减少 spill 或加速。

历史定位证据见 [spill-localization.json](evidence/performance/spill-localization.json)：原 YouTube 20 万边运行的全部 317,327,309 B 磁盘 spill 位于 topk 组、job 29、stage 71/attempt 0，8 个 ShuffleMapTask 各写约 38.5–40.8 MB spill。SQL execution 10 的 AQE 更新计划将 accumulator 2166 定位到 `Sort(user, method, ranking_score, candidate)`；其各 task 更新量合计 2,415,917,952 B，与 memory spill 一致。该值是内存记录量估计，不能再加到物理 disk spill 上。

该 Sort 位于正反向 Union、四方法 Generate 之后、partial WindowGroupLimit 和 shuffle 之前。瓶颈切口是展开后每个 map task 的排序压力。增加保留分区数可能减小单 task 负荷，也可能增加调度与 shuffle 成本，必须实测。历史记录使用 optimized，其约 97 秒耗时只属于旧运行，不能充当本轮 baseline。

## 本轮实测与选择

两组均为同一镜像、2 CPU / 6 GiB 容器上限、2g driver heap 的新 baseline，各跑一次；完整候选与推荐表等价通过。对照报告见 [comparison.md](evidence/performance/partition-accept-01/comparison.md)和 [comparison.json](evidence/performance/partition-accept-01/comparison.json)，原始压缩日志与 manifest 见同目录 evidence-index.json。

| 指标 | p4 默认 AQE | p16 关闭 coalesce |
|---|---:|---:|
| Top-K disk spill | 299,547,389 B | 0 B |
| Top-K task p95 | 8,622 ms | 1,562 ms |
| Top-K shuffle read/write，各 | 166,535,585 B | 369,225,175 B |
| Top-K task attempts | 21 | 81 |
| Top-K 含落表 | 41.536 s | 58.379 s |
| 完整 end-to-end | 91.235 s | 151.640 s |

本次观察到 spill 消失且单 task 尾时长降低，但 shuffle 与任务数增加，Top-K 和完整耗时均更差。前置更多分区会改变局部 Top-K 的裁剪与 shuffle 规模；日志证实了 shuffle 增长，具体耗时不能仅由一个指标归因。因此保留 baseline/p4/默认 AQE，不将实验配置升为默认，也不声称整体加速。p95 来自数量不同的 task 分布，不能替代阶段时间；各一遍的结果不构成稳定性能结论。

两组共比较 6,639,492 个候选和 3,783,816 条推荐行，最大分数误差 3.55e-15；另对 1,000 个 pair、64 个用户做独立参考核对。没有把旧 V2 optimized 运行充当新 baseline。

## 参数与观测

新增 CLI：

- `--disable-adaptive-coalesce`：显式关闭 AQE shuffle 分区合并。
- `--adaptive-advisory-partition-bytes N`：设置 AQE advisory 目标字节；省略时继承 Spark 默认。
- `--adaptive-coalesce-size-first`：设置 `parallelismFirst=false`；省略时继承 Spark 默认。

请求参数与实际生效的 coalesce、advisory size、parallelismFirst、最小分区大小写入 environment 和 manifest；同时保存 topk_plan.txt。仅设置目标大小时，Spark 默认的 parallelismFirst 仍可能主导合并行为，不能把 advisory 值当成严格分区大小。

本地 PowerShell 入口支持对应的 `-DisableAdaptiveCoalesce`、`-AdaptiveAdvisoryPartitionBytes` 和 `-AdaptiveCoalesceSizeFirst`。

## 固定资源的全新对照

两组都重新运行 V3 baseline、同一 `data/youtube_sample_200k.txt`、Parquet、K=10、local[2]。容器 CPU 上限 2、内存上限 6 GiB；JVM driver heap 默认固定为 2g，两组一致。容器内存上限和 JVM heap 是两个不同参数。

| 组别 | shuffle 分区 | AQE coalesce |
|---|---:|---|
| p4-default-aqe | 4 | 默认启用 |
| p16-no-coalesce | 16 | 显式关闭 |

先查看计划，不启动 Spark：

```powershell
python scripts/benchmark_partitions.py --prefix aqe-trial
```

待其他重负载实验结束后执行；镜像必须已存在，本脚本不拉取镜像：

```powershell
python scripts/benchmark_partitions.py --prefix aqe-trial --execute
```

执行器一次只运行一个临时容器，逐次记录命令、输入/代码/镜像哈希、CPU/内存配置及完整 stdout/stderr。默认各跑一次；有足够时间时用 `--repetitions 3`，相邻重复交换两组执行顺序。文件缓存和宿主机负载仍未完全受控，不能由单次观测推断稳定加速。

Spark 两组完成后，使用同一 Python 环境里的 pandas/pyarrow 做完整候选分数和 Top-K 表等价检查，并抽样比对独立 Python 参考。该验证不计入 Spark 性能测量，可能需要较多宿主机内存。显式加 `--skip-parity` 可以跳过，但报告会标成尚未提供完整结果等价证据。

证据写到 `evidence/performance/<prefix>/`，大表写到 `artifacts/partition-benchmark/runs/`。重新生成报告：

```powershell
python scripts/report_partition_benchmark.py --suite evidence/performance/aqe-trial/suite.json --output evidence/performance/aqe-trial/comparison.json
```

报告按实际执行 stage 的 jobGroup 统计 topk 的 disk spill、shuffle、task p50/p95，区分 Top-K 含落表时间、pipeline 时间和完整 end-to-end 时间。共享 stage 的多个 job 指标不相加。输入/代码/镜像/资源或输出计数不同、旧 optimized 结果、未完成日志都会被拒绝作为本轮对照。计数相等本身不会被标成完整结果等价。

本实验同时调整分区数与合并策略，因此即使观察到收益，也只能归于组合布局，不能分别断言某一个参数导致收益。最终是否采用新配置由实测结果决定；本次变更不自动切换默认策略。
