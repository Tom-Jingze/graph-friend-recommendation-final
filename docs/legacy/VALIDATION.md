# Validation Report

## Overall Assessment: Ready to share with caveats

## Methodology Review

数据节点数和边数与Stanford SNAP官方统计一致。测试边在训练邻接表构建前移除；生成树边受到保护，因此每个训练图保持连通。负边从完整图非边中抽取，不会把真实边错误标成负例。

## Calculation Spot-Checks

- 数据规模：4,039个节点、88,234条去重无向边，已与官方页面核对。
- 划分互斥：测试边不出现在训练边中。
- 连通性：测试覆盖的每个训练图均由受保护生成树保证连通。
- AUC：使用平均秩处理并列分数，单元测试覆盖并列情况。
- 多随机种子：结果文件包含5个种子×4种方法，共20行。
- 结论一致性：Resource Allocation在困难AUC、AP、Precision@10、Recall@10、NDCG@10和HitRate@10上均排名第一。

## Required Caveats

- SNAP图没有时间戳，隐藏边恢复不等价于真实未来好友预测。
- Combiner结果是四分区中间记录量模拟，不是实际Spark/Hadoop墙钟时间。
- 原简历中的“73%+”不能继续使用；本次可复算结果为70.7%。
