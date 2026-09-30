# sched 开发范围与后续工作

本文只记录通用调度器工作。当前配置和 CLI 以 [reference.md](reference.md) 为准，
execution API 以 [execution-api.md](execution-api.md) 及其同仓验收为准。
尚未通过验收的计划不视为可用 API。

## 当前交付范围

执行隔离分支把客户专用协议移出调度器，为固定 executable/argv/env、输入 FD、
一次性启动与直接子进程 owner 提供自包含的通用执行层。
默认安装继续使用 subprocess，可选 Linux native 由本仓源码构建。
旧 strict/native 持久态保留兼容守卫，新任务使用通用公开接口。
责任与证据边界见 [execution-boundary.md](execution-boundary.md)。

## 本次新增交付

`sched execution` 已补充逐版本只读诊断和旧 session 摘要，区分原始 wait、
进程组清理与未知结果。查询不升级数据库，不重建 owner，不改变 replay 守卫。
字段和旧 schema 兼容限制见 [execution-api.md](execution-api.md)。

0.2.0 候选增加显式 `linux_fd_owner`：独立服务持有原始 child、wait 和清理，
daemon 通过认证绑定恢复查询；准备恢复不重放，取消与 duration 由服务独立升级。
设计与故障验收见 [persistent-execution-owner.md](persistent-execution-owner.md)。
该实现已合并，合并提交 CI 已通过；发布与生产切换另按
[execution-rollout.md](execution-rollout.md) 执行。

0.2.1 运维候选增加有限的冷配置保留期、只读的已记录连接/确认状态，以及
持久确认队列。历史确认每轮最多 8 条，失败退避，不扫描已确认历史或删除 binding；
覆盖一万条记录与确认提交故障。候选仍须通过自身 CI 和合并审查，不能据此切换生产。

## 后续候选

- 更多平台能力：需明确 capabilities 和不支持时的拒绝语义，不回退到权限或
  文件绑定较弱的执行方式。
- 配套客户端展示通用诊断：需单独增加 execution 查询与 UI，不改写 scheduler 语义。

这些候选没有自动授权部署，也不因某个研究项目需要而成为 sched 发布阻塞项。
MPC_OTSF 的科学加载、G1–G6 矩阵、冻结合同重审、正式实验和研究部署包，
全部属于该项目自己的 roadmap 与 goal。

## 已有调度能力

项目 GPU 开关、配额与亲和、主机资源准入、drain/resume、daemon 健康查询和
幂等维护请求由 sched 独立维护。配套插件只消费 CLI 契约，不重新实现调度。
隐私、文档引用、仓库边界及 Linux 回归在公共 CI 执行；它们不要求其他仓库存在。
