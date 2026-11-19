# 好友推荐 V3 性能定位与实测取舍

本轮对原有Spark批处理做了一次完整的性能调查和受控参数实验。核心评分算法与业务规则保留；新增AQE合并策略与目标大小参数，保存输入/源码/镜像哈希、实际容器资源、完整Top-K物理计划和eventlog。

旧YouTube样本的317MB磁盘spill被精确定位到Top-K前的排序。它发生在Generate四方法和正反向Union之后、partial WindowGroupLimit之前，不是模型训练或最终reduce。定位记录保留在evidence/performance/spill-localization.json。

为避免混用旧的optimized算法成绩，新跑了两组baseline：4分区/默认AQE，与16分区/关闭合并。两组均为2CPU、6GiB容器、2GiB driver堆、相同20万条真实采样边和同一镜像/代码。两组各运行一次，未声称统计显著或通用速度优势。

| 指标 | 4分区＋默认AQE | 16分区＋关闭合并 |
|---|---:|---:|
| Top-K disk spill字节 | 299,547,389 | 0 |
| Top-K task p95毫秒 | 8622 | 1562 |
| Top-K含落表秒 | 41.536 | 58.379 |
| 应用端到端秒 | 91.235 | 151.640 |
| Top-K shuffle read/write，各字节 | 166,535,585 | 369,225,175 |
| 全应用task数 | 144 | 540 |

16分区减少了单task内存压力，但伴随额外shuffle、任务与落表开销，整体更慢。p95是Spark task时长，不是用户请求延迟。原默认配置保留；新增设置仅作为可选实验，不把减少spill包装为业务加速。

两组6,639,492个候选对和3,783,816条推荐结果做了完整等价检查，最大分数差3.55e-15，排名逐行一致；独立Python参考另查1000个候选对和64个用户，通过。原始Spark计算没有collect全图；本地结果审查工具会加载结果表，需相应内存。

本轮62项依赖无关单测通过，5个小型Spark集成用例未重复启动；两次实际全样本Spark运行和完整结果对照已通过。配置仅新增可选参数，不改默认行为。源代码和参数说明见PERFORMANCE_V3.md，真实命令、资源证据、完整原始压缩日志及SHA索引见evidence/performance/partition-accept-01/。

包内省略大Parquet表；本机完整表在artifacts/partition-benchmark/中。V2既有双worker与Iceberg验证为历史工作，本轮没有再作物理多机或新集群扩展性声明。V3 Compose使用独立项目名，避免与保留的V2服务混用。

历史实施记录见IMPLEMENTATION_REPORT_V2_zh.md。
