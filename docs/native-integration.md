# 旧 native 接口兼容记录

本文记录从历史 M2B 实验集成到通用 execution API 的边界变化，不能作为新任务提交
或研究执行授权。当前接口见 [execution-api.md](execution-api.md)，责任划分见
[execution-boundary.md](execution-boundary.md)。

历史 `main@ccbf600` 包含 MPC_OTSF 的 Step5D/E/F 协议、部署绑定和 native bridge
调用。专用源码、冻结向量、测试及原始快照已迁至研究仓库的独立命名空间；
研究仓库不能再提供或注入 `gsched._m2b_scheduler_native`。
原分支历史仍可由 Git 查看，历史测试计数和运行记录只对应当时版本。

## 旧持久态

旧 `native_exec_profiles`、`mode: "strict"` 和内部 `_native_exec_*` 字段仅供
迁移识别。新提交拒绝旧 strict/native 输入，不能借这些键启用旧研究执行路径。
`retry`、`resubmit` 和 `clean` 也识别旧内部字段，包括只有 V2 metadata 的部分记录，
拒绝将其重新入队；清理不能先删产物再发现旧执行无法安全重放。
已有 native session 表、尝试字段、名称消费记录和指纹绑定保留，避免把未知结果
解释为未启动或重新授予一次启动机会。旧 session 的恢复、取消和停止守卫继续
保留其未解决状态；没有原 owner/wait 证据时，不推断成功、不自动回队或重放。

数据库 schema 与公开 CLI JSON 的 `schema_version` 分别演进。
迁移代码不删除旧 session，也不直接清理运行目录。部署前需先读取本机持久态，
按 CLI 契约处理未解决尝试；此文档不授权操作正在使用的远程实例。

## 研究迁移

新的通用 `linux_fd` 接口只绑定 executable、argv、env 与输入 FD，
不解析旧 M2B wire、Step5 合同或科学阶段。项目 adapter 在独立子进程中运行，
因此旧合同的 direct-parent、peer identity、进程角色和 wait 权威需要新版审查。
普通 wrapper 的成功退出不能直接替代完整研究链证据。

旧冻结协议的兼容测试和研究 G1–G6 属于 MPC_OTSF 的独立工作目标。
`sched` 公共 CI 和发布不等待这些 gate，也不读取研究合同路径。
