# V2 验证范围与复现协议

本文件说明应验证什么，以及各类证据能支撑哪些表述。本轮实际运行的命令、环境、数据规模和结果统一记录在 IMPLEMENTATION_REPORT_zh.md；此处不预填集群成功、全图处理或加速成绩。

## 已执行的基础检查

指标与事件日志两个测试文件共 38 项通过。另对 486 组含大量并列分数的数据，将 AP、AUC 与 sklearn 独立比对，误差均小于 1e-12。该检查验证指标计算，不验证 Spark 集群能力。

AP 采用按同分阈值分组的非插值定义：各阈值的召回增量乘对应精确率再求和。常数预测的 AP 应等于正例比例，不随输入正负例先后顺序改变。ROC AUC 使用平均秩处理同分。空输入、长度不符、非二元标签、非有限分数及 AUC 单类别均明确拒绝。

负采样检查可用非边数量；稠密图在有界拒绝采样后使用回退方法，不会无限循环。连接保护划分必须确实有可隐藏边，不能悄悄少取测试集。静态隐藏边恢复仍不能视为未来边预测。

## Spark 正确性与发布协议

验收时保留 manifest、quality、environment、timings、执行计划和原始 eventlog，至少检查：

1. 正反边及重复输入最终只保留一条标准无向边；注释/空行与各类坏行分类总量等于输入行数。
2. 邻接行数等于清洗边数的两倍；节点度数与小图手算一致。
3. 候选不含自环或已有边；四种分数与独立 Python 参考计算一致；两种模式的候选集合一致、浮点分数在声明容差内一致。
4. 每用户每方法最多 K 条，使用已声明的 12 位 HALF_EVEN 排序分数及候选 ID 确定并列顺序。
5. 成功后 manifest 与 _SUCCESS 同时可见，成功 run-id 拒绝覆盖；失败后只有暂存结果可见，同 run-id 可重跑且不会发布失败半成品。
6. 超出楔形预算时在候选自连接前失败，并保留质量诊断。精确二跳算法的高阶节点开销仍然存在。
7. Iceberg 模式记录实际 snapshot_id，按 manifest 中表标识读取验证；不将单表快照承诺解释成跨表事务。

本地原子发布依赖暂存与最终目录位于同一本地文件系统；没有实现对象存储/HDFS 的同等发布协议。被操作系统强杀后可能留下锁，需先确认持锁进程已退出。

## 事件日志指标口径

日志测试覆盖成功、失败、被取消、stage 重试、task 重试、推测执行、相同 TaskEnd 重复、共享 stage、gzip、rolling 顺序和 CLI。

- all_attempts：同一 stage ID、stage attempt ID、task ID 只计一次；失败、取消和多余推测任务的资源消耗也计入。
- logical_successful_tasks：每个已成功完成的 stage ID、分区，取不晚于最后成功 stage attempt 的最新成功结果；同 stage attempt 内取最早成功完成的 task。它是逻辑结果的观测近似，不是实际节省字节数。
- job：按该 job 引用的 stage ID 归属；共享 stage 可出现在多个 job 下，因此不能求和当全局总量。
- shuffle read：Remote Bytes Read 加 Local Bytes Read，不重复加已包含在远端量中的 Read To Disk。写出量不等于读取网络流量。
- task 时长为 Finish Time 减 Launch Time；p50/p95 只对成功 task，用线性插值。CPU 和 shuffle-write time 为 ns，其余运行/GC/fetch wait 为 ms。
- 缺少 ApplicationEnd 或 Task Metrics 时报告警告，数字只代表观测到的下界，不能解释为无成本。

## 性能实验与历史边界

baseline/optimized 对照必须固定输入哈希、数据实际规模、版本、CPU/内存、分区、格式及运行方式。交替运行并保留多次观测；同时报告端到端时间、阶段时间、shuffle、spill 和成功 task p95。本轮 [Facebook 事件日志对照](validation/facebook-event-comparison.json)中，score 阶段 shuffle read/write 各从 baseline 的 26,924,883 B 增至实验模式的 56,147,761 B，增加 108.53%；score 含落表为 12.376 s 对 13.052 s，未体现收益。因此 CLI、函数、本地脚本和 Compose 默认统一采用 baseline，optimized 仅显式实验启用，并有默认选择回归测试。端到端差异受 clean 阶段与缓存条件影响，不据此宣称加速。该结论只覆盖已记录的这一次输入与环境。

Compose 文件是一个 master、两个 worker 和 submit 的单机容器配置；没有实测日志前不能称集群已经运行，更不能称四台物理机器或线性扩展。

原 results/ 和 docs/legacy/ 属于旧实验。旧 AP 在并列内先排正例，存在乐观偏差；没有被重新运行的旧结果不能作为 V2 新成绩。原四分区 70.7% 是记录量模拟，不能换写成真实 Spark shuffle 或耗时下降。
