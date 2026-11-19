# 大规模社交图好友推荐与链接预测

本项目使用 Stanford SNAP `ego-Facebook` 合并图（4,039 个节点、88,234 条无向边），将原有“共同好友 + MapReduce”课程项目升级为可离线评估的图链接预测实验。

## 方法

- Common Neighbors
- Jaccard Similarity
- Adamic-Adar
- Resource Allocation

## 无泄漏评估协议

每个随机种子先在完整图上构造一棵生成树，保护树边后从其余边中隐藏 10%。因此训练图始终连通，测试边不会出现在训练邻接表中。成对评价使用等量、且在完整图中不存在的负边；Top-K 指标按用户聚合隐藏好友。

指标包括 AUC、Average Precision、Precision@10、Recall@10、NDCG@10 和 HitRate@10，默认运行 5 个随机种子并报告均值与标准差。AUC/AP 同时使用随机负边和更严格的二跳困难负边；主结论采用困难负边，避免随机负采样使指标虚高。

## 已验证结果

Resource Allocation取得最佳结果：困难负样本AUC为`0.9772±0.0007`，Recall@10为`0.5617±0.0054`，NDCG@10为`0.5283±0.0019`。相较Common Neighbors，Recall@10和NDCG@10分别提高3.75和3.56个百分点。

四分区Combiner模拟将中间楔形记录由约774万条减少到227万条，平均减少70.7%。这是记录量模拟，不是实际集群运行时间。

## 复现

```powershell
python src/experiment.py --data data/facebook_combined.txt.gz --output results
python src/summarize.py --results results
```

## 数据来源

Stanford SNAP: https://snap.stanford.edu/data/ego-Facebook.html

McAuley, J. and Leskovec, J. *Learning to Discover Social Circles in Ego Networks*. NeurIPS 2012.
