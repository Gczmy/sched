# sched 下一步开发内容

本文记录尚未实现的开发项，不是当前配置/API 参考。当前可用行为以
[`reference.md`](reference.md) 和实际 CLI 为准。

M2B 实验分支已整合，后续仍需接通 V2 的 retained-FD 启动后端与
正式 dispatcher 生命周期，并完成外部 C bridge 和 MPC_OTSF 全链路验收。
这些工作尚未实现或排期；当前合并不授权正式研究任务执行。
范围与依赖见 [native-integration.md](native-integration.md)。

## 进行中的开发项

P2 看板维护操作的调度器接口已补齐：`request` 支持 `daemon drain`、
`daemon drain --stop-when-idle` 和 `daemon resume`；直接调用与请求包装均校验
计算节点，健康 JSON 提供查询 CLI 的 `request_actions`。请求重放不会重新执行
排空或恢复；中断后结果未知的请求仍返回 `75`。

对应回归位于 `tests/test_daemon_maintenance_request.py`，真实本地 daemon 验收
位于 `tests/run_host_resources_accept.py`。配套插件的 writer 能力校验、维护按钮、
旧 CLI 提示与跨仓联调仍待完成，不能把调度器接口完成当成看板功能已交付。

## 已完成的开发项

两个仓库已加入统一的隐私／文档／差异检查入口、可选提交钩子和公共 Linux CI。
检查范围及本地命令见 [开发与提交检查](../CONTRIBUTING.md)；历史清理仍单独处理。

实验协议部署绑定已外置（2026-09-27）：Step5D／5E／5F 从显式传入的管理员
私有配置读取路径和身份，公共测试使用虚构向量；旧冻结报文保持兼容。调用方
迁移要求见 [native-deployment.md](native-deployment.md)。外部隔离启动器源码
清单升级与 C bridge 验收仍属于后续工作，Git 历史尚未清理。

ND-01「项目级禁止使用 GPU」已实现（2026-09-07）：使用独立布尔字段
`projects.<name>.gpu_enabled`，默认 `true`，保留 `gpu_quota:0` 为无限制的语义。
支持热更新、提交与重试校验、排队暂停与恢复、CLI 查询和配套看板。
行为定案、兼容要求和验收记录已移至
[`project-gpu-access.md`](project-gpu-access.md)。
