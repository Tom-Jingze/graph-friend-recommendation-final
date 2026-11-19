# 大规模社交图好友推荐与链接预测：实验报告

## 实验结论

在Stanford SNAP Facebook匿名社交图（4,039个节点、88,234条边）上，Resource Allocation在5个连通性保护的随机划分中取得最佳结果：困难负样本AUC为0.9772±0.0007，Recall@10为0.5617±0.0054，NDCG@10为0.5283±0.0019。相较原始Common Neighbors，Recall@10提高3.75个百分点，NDCG@10提高3.56个百分点。

四分区Combiner记录量模拟中，中间楔形记录由约774万条降至约227万条，平均减少70.7%。该数字衡量待Shuffle记录数，不等同于真实集群运行时间；原四节点Hadoop/Spark性能仍应以集群日志为准。

## 实验设计

- 数据：Stanford SNAP ego-Facebook combined graph。
- 划分：先保护完整图的一棵生成树，再从非树边中隐藏10%，保证训练图始终连通。
- 随机种子：11、22、33、44、55。
- 方法：Common Neighbors、Jaccard、Adamic-Adar、Resource Allocation。
- 成对指标：随机负边AUC/AP，以及共享至少一个训练邻居的困难负边AUC/AP。
- 排序指标：Precision@10、Recall@10、NDCG@10、HitRate@10。

## 方法对比（5个随机种子均值）

| 方法 | 困难AUC | Precision@10 | Recall@10 | NDCG@10 | HitRate@10 |
|---|---:|---:|---:|---:|---:|
| Resource Allocation | 0.9772 | 0.2627 | 0.5617 | 0.5283 | 0.8699 |
| Adamic-Adar | 0.9705 | 0.2535 | 0.5431 | 0.5102 | 0.8587 |
| Common Neighbors | 0.9657 | 0.2477 | 0.5242 | 0.4927 | 0.8436 |
| Jaccard | 0.9655 | 0.2473 | 0.5072 | 0.4807 | 0.8198 |

## 可信度与限制

随机负边AUC达到约0.99，是因为随机非边通常没有共同邻居，任务过于容易。因此简历和报告不使用随机AUC作为主结果，而采用困难负边和Top-K指标。数据没有时间戳，所以隐藏边实验验证的是静态图结构恢复能力，不能声称等价于线上推荐提升。

## 推荐简历描述

**大规模社交图好友推荐与分布式性能优化｜Hadoop、Spark、Python**

- 基于Docker搭建4节点Hadoop/Spark集群，在包含4,039个用户、88,234条关系的SNAP Facebook社交图上实现分布式候选好友生成与Top-K排序，并将核心计算由MapReduce迁移至PySpark。
- 实现Common Neighbors、Jaccard、Adamic-Adar及Resource Allocation链接预测算法，设计保护生成树的隐藏边评估协议；5个随机种子下Resource Allocation取得Recall@10=0.562、NDCG@10=0.528，较共同好友基线分别提升3.75、3.56个百分点。
- 通过Combiner对二跳路径进行本地聚合，将四分区模拟的中间记录量由约774万降至227万，减少70.7%；完成困难负样本、Top-K排序及多随机种子稳定性分析。

## 数据来源

Stanford SNAP: https://snap.stanford.edu/data/ego-Facebook.html

McAuley, J. and Leskovec, J. *Learning to Discover Social Circles in Ego Networks*. NeurIPS 2012.
