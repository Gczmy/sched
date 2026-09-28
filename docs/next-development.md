# sched 下一步开发内容

本文记录尚未实现的开发项，不是当前配置/API 参考。当前可用行为以
[`reference.md`](reference.md) 和实际 CLI 为准。

M2B 实验分支已整合，后续仍需接通 V2 的 retained-FD 启动后端与
正式 dispatcher 生命周期，并完成 MPC_OTSF 全链路验收。
这些工作尚未实现或排期；当前合并不授权正式研究任务执行。
范围与依赖见 [native-integration.md](native-integration.md)。

V2 冻结合同的内部持久化构造与启动前复核已补齐；正常 local submit 和 daemon
inbox 仍在落库前拒绝 V2，dispatcher 即使遇到内部 V2 task，也在 running claim
前拒绝。`strict` 批次现在不能通过 `clean` 重排。正式放行还需要研究仓库提供
经冻结附件约束的 V/P 执行接口、可信完成证据与取消／超时权威；调度器随后要
接入正式 native session 生命周期、崩溃接管及资源结算，并用真实 Linux C bridge
做跨仓端到端验收。当前内部 helper 仅在 `isolated_integration` 域完成 T1
active/latest/pending 同事务抢占与 owner-unbound session 预留，以及日志打开前独立提交
的 `log_attempted_at` CAS 和项目内唯一日志 FD 的 inode 持久绑定；隔离域 T2a 也可在
复核该 FD 后独立提交不可重置的 `monitor_launch_attempted_at` 一次性 M 启动前意图。
dispatcher 不调用这些 helper，T2a 不创建或启动 M owner；其提交后仍须重验新到的
cancel/timeout。真正的 T2 M 启动及原始 pidfd/wait 所有权、T3 在 V/P exec 前
重验耐久 cancel/timeout、T4 以实际 END/wait 和完整证据原子发布终态都未实现。
隔离域 native session 已在 legacy dispatcher 的恢复、取消和停止路径保守保留；
超时看门狗从 T1 的 `started_at` 起计时，为超过 `duration_min` 的隔离域
session 持久写入 `timed_out` 意图；尚未消费的日志与 M 意图 CAS 会拒绝后续尝试。
T2a 意图提交后、真正 M 启动前仍须再查取消／超时。看门狗不发进程组信号、
不结算任务；正式链还须确定 `duration_min` 是从预留还是实际 V/P 执行起算。
原始 owner/wait、启动后取消／超时执行与终态通路仍未接通，不能视为正式可运行。
`reserved` 在日志尝试失败后保留非空 `log_attempted_at`，同一 session 不可再次打开；
它与 `log_bound` 在崩溃/日志故障后均属已消费未决，不能自动回队或重放。T2a
提交结果不明或提交后失去 owner 时也不重试 M 启动。
现有 monitor close 不代表 formal phase 完成，不能用作 `done` 判据。

## 已完成的开发项

P2 看板维护操作已在调度器和配套插件分支实现：`request` 支持
`daemon drain`、`daemon drain --stop-when-idle` 和 `daemon resume`；插件在
实际 writer 上核验节点、完整状态与 CLI 能力，旧 CLI 禁用维护按钮。调度器
回归位于 `tests/test_daemon_maintenance_request.py` 和
`tests/run_host_resources_accept.py`；插件回归位于配套仓库。

两个仓库已加入统一的隐私／文档／差异检查入口、可选提交钩子和公共 Linux CI。
检查范围及本地命令见 [开发与提交检查](../CONTRIBUTING.md)；历史清理仍单独处理。

实验协议部署绑定已外置（2026-09-27）：Step5D／5E／5F 从显式传入的管理员
私有配置读取路径和身份，公共测试使用虚构向量；旧冻结报文保持兼容。调用方
迁移要求见 [native-deployment.md](native-deployment.md)。外部隔离启动器源码
清单已在研究仓库 `MPC_OTSF` 的 `d8164bf` 升级；本地 C bridge 的十项 S/M
生命周期验收已通过，正式 V/P 尚未接入。Git 历史尚未清理。

ND-01「项目级禁止使用 GPU」已实现（2026-09-07）：使用独立布尔字段
`projects.<name>.gpu_enabled`，默认 `true`，保留 `gpu_quota:0` 为无限制的语义。
支持热更新、提交与重试校验、排队暂停与恢复、CLI 查询和配套看板。
行为定案、兼容要求和验收记录已移至
[`project-gpu-access.md`](project-gpu-access.md)。
