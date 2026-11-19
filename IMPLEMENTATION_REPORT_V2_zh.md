# 好友推荐工程升级实施报告

## 本轮已完成

原始 ZIP 保留。新增可运行的 Spark SQL/DataFrame 批处理、异常行隔离、关系规范化与去重、邻接/度数/候选/Top-K 六层输出，支持 Parquet 与 Iceberg。评分不把已有好友作为候选；保存原始分数，按 HALF_EVEN 保留12位的排序分数和候选ID确定顺序。

成功运行通过本地目录原子发布 manifest；成功 run-id 不可覆盖。异常退出保留失败诊断，同一 run-id 可从头重算。Iceberg 每张表独立提交，成功 manifest 是跨表读取的发布边界，不声称跨表事务。

修正原评估 AP 对同分样本偏乐观的问题，补充输入和负采样边界。历史 results/ 与原三份文档保留并标注旧口径，未伪造新 AP 或模型效果。原始代码差异见 changes.patch。

## 实际执行结果

| 运行 | 原始唯一边 | 节点 | 候选对 | 四方法Top-K行数 | 应用端到端秒 |
|---|---:|---:|---:|---:|---:|
| Facebook local 基线 | 88,234 | 4,039 | 1,358,067 | 161,536 | 63.09 |
| Facebook local 两阶段实验 | 88,234 | 4,039 | 1,358,067 | 161,536 | 58.13 |
| YouTube 20万边采样 | 200,000 | 201,482 | 6,639,492 | 3,783,816 | 97.13 |
| Facebook 两个worker | 88,234 | 4,039 | 1,358,067 | 161,536 | 73.43 |

local 对照均使用官方 Apache Spark 3.5.7 镜像、Java 11.0.27、local[2]、4个shuffle分区、2 CPU/6GB容器限制、2GB driver堆。表中应用端到端时间不含拉镜像和启动容器。

集群由同一 Docker 主机上的 master、两个worker和submit driver组成；每个worker提供1核/1GB容量，executor堆768MB、driver堆1GB。两个executor分别完成3和141个成功task，任务明显不均，见 validation/cluster-executors.json。该记录证明跨容器执行，不是四台物理机器测试，也不用于推断均衡扩展或集群加速比。服务已在验证后停止，数据保留。

YouTube 原图由 SNAP 官方下载，2,987,624条边。固定 seed=42 对有效边记录做蓄水池采样，得到200,000条唯一边、201,482个节点、6,657,697条理论楔形。来源、SHA256和采样方法见 data/youtube_sample_200k.txt.json。本轮没有执行全量 YouTube 候选计算，也未把采样质量当全图推荐质量。

该采样运行实际Shuffle读写各289,881,519字节，disk spill为317,327,309字节，成功task的p50/p95为193/5683ms。已暴露窗口排序和内存压力，见validation/youtube-event-metrics.json；这为后续分区、候选存储及Top-K策略优化提供了实测基线，当前没有宣称这些瓶颈已解决。

## 性能取舍

| score阶段真实指标 | baseline | 两阶段实验 |
|---|---:|---:|
| Shuffle read，各字节 | 26,924,883 | 56,147,761 |
| Shuffle write，各字节 | 26,924,883 | 56,147,761 |
| score计算及落表秒 | 12.376 | 13.052 |
| 磁盘spill字节 | 0 | 0 |

两阶段聚合使score shuffle增加108.53%，此次没有体现优化收益，因此代码、Compose和本地脚本默认已切回baseline。实验方案仍可显式选用。完整端到端时间受clean阶段、缓存与启动差异影响，单次对照没有统计置信度，不宣称加速。baseline本身已有Spark部分聚合，增加聚合层不是自动收益。

原来的70.7%仍只表示本地中间记录模拟，不与本轮实测Shuffle字节混用。真实Spark事件日志按应用ID匹配；同时报告所有task尝试的实际开销与去重后的逻辑任务口径，job复用stage不重复汇总。原始日志已gzip压缩保存在evidence/eventlogs/。

## 正确性和恢复验证

- 图项目当前54项测试，电机项目28项，共82个唯一测试用例已通过。其中5项实际Spark DataFrame集成测试在Linux容器执行；其余77项在宿主执行。新增parser/默认入口检查后复跑了相关单测，没有重复耗费Spark集成任务。
- Facebook两方案完整比较1,358,067个候选对、161,536条排名记录；评分在1e-12容差内一致，Top-K逐行一致。另抽1000个候选对和64个用户，与独立Python集合实现核对通过。
- local与两个worker集群的完整结果再次比较通过，四种分数在本次结果中逐值相同。验证器是本地pandas审查工具，会加载结果表；生产计算本身没有collect全图。
- 受控脏数据中识别1条重复关系、5条异常行，保留8条唯一边。Iceberg在clean后注入失败，日志确认为预设异常；同run-id重跑成功，第三次拒绝覆盖。
- 实际查询6张Iceberg表的manifest快照，VERSION AS OF均读取成功；重跑过的4张清洗层表各保留2个快照。结果见validation/iceberg-snapshots.json。
- 默认5,000万理论楔形预算在自连接前检查，避免直接对超级节点图盲目展开。当前实验没有实现消除超级节点成本的算法。

## 复现入口

交付包内已附官方、SHA256固定的Iceberg运行JAR，完整来源与许可证在vendor/。已有基础镜像时，可用已验证的快速构建：

```powershell
docker build --pull=false -f Dockerfile.offline -t graph-friend-recommendation-spark:3.5.7 .
docker compose up -d --no-build spark-master spark-worker-1 spark-worker-2
$env:RUN_ID = 'my-first-run'
docker compose run --rm --use-aliases --service-ports submit
docker compose down
```

也已验证Dockerfile在线构建。第一次在线构建遇到Dockerfile外部前端认证连接失败，去掉不必要的syntax下载后重建成功；最终Compose健康检查也已按master实际监听地址修正并实跑。

压缩包包含代码、原Facebook数据、YouTube采样、文档、JAR及验证证据；为避免冗余，没有打包批量Parquet表与Iceberg warehouse。本机完整表仍保留在artifacts/中。evidence/runs/保存了它们的运行清单，清单中的原绝对路径只作为本次记录，不保证搬到另一台机器后可直接读取；用新run-id复现即可。

## 可用于简历的已完成表述

基于Spark SQL与Iceberg构建社交图候选推荐批处理管道，实现关系清洗、异常隔离、四种链接评分和Top-K分层输出；在20万条真实采样关系、约20万节点上生成约664万候选对，并完成本地与双worker集群结果一致性、Iceberg快照查询及失败重跑验证。结合真实Spark事件日志比较聚合策略，识别两阶段聚合的Shuffle回退，选择资源开销更低的默认方案。

该表述描述工程验证，不包含线上用户效果、未来关系预测或未经实测的加速比例。
