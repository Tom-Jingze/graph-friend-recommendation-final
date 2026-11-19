# 社交图增量推荐与分布式计算系统

项目包含真实 Spark 全量批处理、精确边变更增量、Parquet/Iceberg 版本输出、批次幂等与失败重跑，以及完整结果对账。原 V3 代码与实测证据继续保留；本轮验收见 [最终实施报告](IMPLEMENTATION_REPORT_zh.md)。

## 快速演示

需要 Python 3.10+ 与 Docker Desktop；源码运行依赖见 requirements-spark.txt。交付包附 Iceberg runtime JAR，可使用离线构建：

```powershell
docker build --pull=false -f Dockerfile.offline -t graph-friend-recommendation-spark:3.5.7 .
python scripts/run_incremental_demo.py --prefix my-demo-001
# 可改用真实 Iceberg 存储；每次完整演示使用新 prefix。
python scripts/run_incremental_demo.py --prefix my-iceberg-001 --format iceberg
```

演示先完成小图全量，再应用新增/删除批次，最后重复同一批次验证 already_published。每次计算仅启动一个临时 local[2] 容器，不占宿主 Web 端口。全量大图与双 worker 运行方式见 [V3说明](README_V3.md)。

## 增量语义

变更文件为 UTF-8，每行 `ADD left right` 或 `DELETE left right`，允许空行与 # 注释。节点为非负 int64，自环及溢出拒绝。相同操作去重；同一边同时 ADD/DELETE 则整批拒绝。添加已有边、删除不存在的边按无操作处理。

新增入口 `src/incremental_pipeline.py` 接收 `--base-run`、`--changes`、`--output`、`--run-id`。base 必须带成功 manifest；存储格式须与 batch 相同。Iceberg 的 base 与 batch 必须使用同一 `--warehouse` 配置，按 manifest 的 snapshot_id 精确读取。

边端点的度数变化会影响所有二跳伙伴的 Jaccard，桥接点度数变化也会影响 RA/AA。因此在旧图与新图的并图上取变更端点及其两跳邻域，保守圈定受影响用户。只重新展开这些用户相关的候选对，并替换这些用户的 Top-K，其余结果复用。

这实现了增量计算；输出为完整不可变快照，仍会重写完整结果文件。高连接度变更可能覆盖大部分图，不承诺每批更快。内部可靠 checkpoint 用于截断计算计划，不是已实现断点续算。

同一 batch ID 与相同 base manifest/原始变更文件哈希重复提交直接返回已发布结果；同 ID 不同请求拒绝。变更输入先冻结到暂存目录，复制件SHA必须与请求一致，Spark只读冻结件，防止哈希后原始文件改变。成功 manifest 是可见性边界，单机目录原子改名；Iceberg 每表独立提交，不提供跨表事务。失败保留诊断，同 ID 从头重跑；强杀留锁需先核实原进程已退出。

## 完整验收与复现

```powershell
python -m pip install -r requirements-spark.txt -r requirements-validation.txt
python -m pytest -q tests
docker run --rm --cpus 2 --memory 6g --mount "type=bind,source=${PWD},target=/app" --workdir /app --env SPARK_LOCAL_IP=127.0.0.1 --entrypoint /opt/spark/bin/spark-submit graph-friend-recommendation-spark:3.5.7 --driver-memory 2g --conf spark.ui.enabled=false /app/scripts/accept_incremental.py --prefix my-accept-001
```

验收实际运行 Spark，并比较增量与全量的完整候选、四种打分及排名；覆盖 int64 大节点 ID、无效/冲突批次、重复提交、故障重试、无操作重放、删除全部边及 Iceberg 快照。默认包含 Facebook 全图；`--fixture-only` 仅做边界小图。

算法效果是静态留边实验，不能当作在线好友接受率。YouTube 历史证据是20万边样本，不是其全量图。Docker 多容器运行于一台主机；历次性能结果没有证明整体加速。计数、资源和耗时口径见各实测报告。

## 文件

- `src/spark_pipeline.py`：全量六表管道；`src/incremental_pipeline.py`：增量批次。
- `scripts/accept_incremental.py`：分布式完整对账与故障验收。
- `evidence/incremental/`：本轮报告、manifest、输入、实际环境与压缩日志。
- `PERFORMANCE_V3.md`、`evidence/performance/`：既有20万边性能定位与负收益取舍。
- `artifacts/`：本机完整计算表、warehouse 与 eventlog，不打入发布 ZIP。


