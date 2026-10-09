# 文档索引

当前行为以代码和下列契约文档为准。文档中的示例使用通用名称；真实账户、
节点、租约、会话、批次和部署目录保留在仓库外的私有运行记录中。

| 文档 | 用途 |
| --- | --- |
| [开发与提交检查](../CONTRIBUTING.md) | 公共 CI、隐私检查与本地提交钩子 |
| [reference.md](reference.md) | 配置、CLI、JSON、状态机与写操作契约 |
| [integration-contract.md](integration-contract.md) | 实例身份、结构化回执、幂等提交及候选等待/请求/产物诊断/批次失败策略接口 |
| [sqlite-lock-safety.md](sqlite-lock-safety.md) | SQLite 锁修复、事务诊断与旧 schema 回补边界 |
| [project-gpu-access.md](project-gpu-access.md) | 项目 GPU 开关的行为与验收依据 |
| [execution-boundary.md](execution-boundary.md) | 通用执行层、项目 adapter 和独立发布边界 |
| [recovery-policy.md](recovery-policy.md) | 候选恢复协议、smoke、FIFO、显存准入与守护 |
| [recovery-acceptance.md](recovery-acceptance.md) | 候选恢复完整故障/兼容矩阵与非生产 GPU 验收 |
| [execution-api.md](execution-api.md) | backend 冷注册、输入 FD 和自包含可选 native |
| [persistent-execution-owner.md](persistent-execution-owner.md) | 持久 owner、认证重连、运维确认与崩溃恢复证据 |
| [execution-rollout.md](execution-rollout.md) | 独立发布、安装与 schema 回退边界 |
| [release-preparation.md](release-preparation.md) | 完整 CI 原始产物校验、Release 草稿准备与中断续传 |
| [0.2.1 Release 说明](releases/0.2.1.md) | 功能、兼容性与安装/回退约束；发布事实以 GitHub Release 为准 |
| [0.2.2 Release 说明](releases/0.2.2.md) | 逐 backend 预检、Linux 矩阵与发布准备；正式来源和资产以 Release/tag 为准 |
| [0.3.0 Release 说明](releases/0.3.0.md) | 恢复协议、显存准入、前台守护与 schema 9；发布事实以 Release/tag 为准 |
| [0.4.0 Release 说明](releases/0.4.0.md) | 集成候选、schema 10 与独立消费端兼容边界 |
| [0.3.1 Release 说明](releases/0.3.1.md) | SQLite 锁修复、版本查询与旧 schema 升级边界；发布事实以 Release/tag 为准 |
| [native-integration.md](native-integration.md) | 旧实验接口的持久态保护与迁移限制 |
| [native-deployment.md](native-deployment.md) | 已外移的旧实验部署绑定记录 |
| [next-development.md](next-development.md) | 尚未完成的开发项 |
| [feedback-development.md](feedback-development.md) | 通用反馈完整阶段、交付纪律与验收进度 |
| [artifact-validation.md](artifact-validation.md) | 候选不可变首次产物验证、只读查询与原始 wait 边界 |
| [artifact-revalidation.md](artifact-revalidation.md) | 候选仅产物复验、CAS/幂等结算与独立验证计数 |
| [allocation-evidence.md](allocation-evidence.md) | 候选不可变分配、分层失败与 owner/worker 身份边界 |
| [storage-admission.md](storage-admission.md) | 候选 opt-in 磁盘/inode/可知用户 quota、控制面余量与只读解释 |
| [daemon-lease.md](daemon-lease.md) | 候选不可变启动来源、Slurm 持续验证与失效锁存；默认健康 JSON 不变 |
| [cpu-capacity.md](cpu-capacity.md) | 候选显式 CPU auto、保守容量/冻结预留、零值兼容与 opt-in 只读查询 |
| [cpu-isolation.md](cpu-isolation.md) | 候选 schema 18 显式 per-job 亲和、不可变 CPU claim、释放/重连与被动解释；不是 cgroup 硬隔离 |
| [cpu-scopes.md](cpu-scopes.md) | 候选委派 cpuset 原语、持久 intent/inode/CAS 和 schema 20 显式派发/清理；正向内核/设备验收未完成 |
| [device-policy.md](device-policy.md) | 候选原 scope 设备 BPF 原语；不代表 GPU 内核隔离验收 |
| [device-scopes.md](device-scopes.md) | 候选 schema 21 不可变设备意图/程序绑定/CAS、恢复与资源保留 |
| [device-inventory.md](device-inventory.md) | 候选 UUID/driver minor/节点映射、schema 22 原 allocation 冻结绑定与纯新鲜重验 |
| [device-controller.md](device-controller.md) | 候选 schema 23 显式设备安装/原 handle 启动/恢复观察；真实正向验收未完成 |
| [mig-capability.md](mig-capability.md) | 候选 schema 24/v2 原 UUID NVML MIG 能力区分、原证据冻结与旧 v1 保留；非隔离验收 |
| [scope-health.md](scope-health.md) | 候选 schema 25 已记录原委派根/BPF 前置健康、被动新鲜解释及非执行权限边界 |
| [execution-constraints.md](execution-constraints.md) | 候选三种 backend 的启动前 CPU/cgroup FD 原语；scheduler 硬隔离整体尚未完成 |
| [repository-hygiene.md](repository-hygiene.md) | 仓库中的隐私信息和运行记录边界 |

[2026-09-07 联合审查](code_review_sched_dsh_2026-09-07.md) 保留为修复记录，
其中测试计数、构建摘要和部署说明只对应当时版本，不代表当前验证结果。
更早的重复审查报告和一次性生产迁移记录已移出当前文档树；通用操作流程见
[项目操作指南](../AGENTS.md)，不按历史会话或租约执行。
