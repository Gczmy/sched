# 文档索引

当前行为以代码和下列契约文档为准。文档中的示例使用通用名称；真实账户、
节点、租约、会话、批次和部署目录保留在仓库外的私有运行记录中。

| 文档 | 用途 |
| --- | --- |
| [开发与提交检查](../CONTRIBUTING.md) | 公共 CI、隐私检查与本地提交钩子 |
| [reference.md](reference.md) | 配置、CLI、JSON、状态机与写操作契约 |
| [project-gpu-access.md](project-gpu-access.md) | 项目 GPU 开关的行为与验收依据 |
| [execution-boundary.md](execution-boundary.md) | 通用执行层、项目 adapter 和独立发布边界 |
| [execution-api.md](execution-api.md) | backend 冷注册、输入 FD 和自包含可选 native |
| [persistent-execution-owner.md](persistent-execution-owner.md) | 持久 owner、认证重连和崩溃恢复证据 |
| [execution-rollout.md](execution-rollout.md) | 独立发布、安装与 schema 回退边界 |
| [native-integration.md](native-integration.md) | 旧实验接口的持久态保护与迁移限制 |
| [native-deployment.md](native-deployment.md) | 已外移的旧实验部署绑定记录 |
| [next-development.md](next-development.md) | 尚未完成的开发项 |
| [repository-hygiene.md](repository-hygiene.md) | 仓库中的隐私信息和运行记录边界 |

[2026-09-07 联合审查](code_review_sched_dsh_2026-09-07.md) 保留为修复记录，
其中测试计数、构建摘要和部署说明只对应当时版本，不代表当前验证结果。
更早的重复审查报告和一次性生产迁移记录已移出当前文档树；通用操作流程见
[项目操作指南](../AGENTS.md)，不按历史会话或租约执行。
