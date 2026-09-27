# sched 下一步开发内容

本文记录尚未实现的开发项，不是当前配置/API 参考。当前可用行为以
[`reference.md`](reference.md) 和实际 CLI 为准。

M2B 实验分支已整合，后续仍需接通 V2 的 retained-FD 启动后端与
正式 dispatcher 生命周期，并完成外部 C bridge 和 MPC_OTSF 全链路验收。
这些工作尚未实现或排期；当前合并不授权正式研究任务执行。
范围与依赖见 [native-integration.md](native-integration.md)。

## 已完成的开发项

ND-01「项目级禁止使用 GPU」已实现（2026-09-07）：使用独立布尔字段
`projects.<name>.gpu_enabled`，默认 `true`，保留 `gpu_quota:0` 为无限制的语义。
支持热更新、提交与重试校验、排队暂停与恢复、CLI 查询和配套看板。
行为定案、兼容要求和验收记录已移至
[`project-gpu-access.md`](project-gpu-access.md)。
