# 历史文件说明

本目录保存升级前的 README.md、VALIDATION.md 和 FINAL_REPORT_zh.md 原文，仅供追溯。

它们描述旧版纯 Python 算法实验和四分区记录数模拟。原文中的 Hadoop/Spark 集群和迁移表述没有被本次原始 ZIP 中的实现佐证，不能当作 V2 已完成的实测结论。原文的「无泄漏」也仅指静态隐藏边协议中的训练边隔离，不代表真实时间外推。

仓库根目录 results/ 为历史实验产物。旧版 Average Precision 在并列分数内按原输入顺序排序，而输入先放正例，存在乐观偏差；V2 已改成按同分阈值分组的非插值 AP。旧 AP 不属于 V2 新结果。既有 AUC、Top-K 和模拟记录量也保留原实验身份，不能改称本次 Spark 实测。

当前能力、验证方法分别以根目录 README.md、VALIDATION.md 为准；本轮真实运行的环境、输入规模、成绩和限制以 IMPLEMENTATION_REPORT_zh.md 及其所指向的 manifest/eventlog 为准。
