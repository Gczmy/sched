# 执行层与项目边界

`sched` 是独立的通用调度器。它的安装、回归、发布和部署验收由本仓库负责，
不以某个研究项目的 gate、科学加载或数值结果作为发布前置条件。
`dsh-node-sched` 是 CLI 的配套查询与操作客户端；其他项目通过公开接口使用调度器。

## 责任分配

| 层 | 拥有的行为与证据 |
| --- | --- |
| `sched` 调度层 | 队列、依赖、项目策略、资源预留、job/version/revision、启动与取消意图、deadline、终态发布和资源结算 |
| `gsched.execution` | 通用可执行文件与输入 FD 绑定、直接子进程所有权、真实 wait、取消和清理事实；可选 Linux native 实现在本仓库构建 |
| 项目程序与 adapter | 业务报文、阶段、数据和运行时验证、科学成功判据、业务角色与证据正文 |
| 项目合同与验收 | 自身协议兼容性、科学闭包和端到端实验；独立记录通过或未通过 |

daemon 不导入项目代码、动态 Python backend 或项目提供的 C extension。
管理员用冷配置注册可执行文件与固定参数，任务仅选择已允许的 backend 并绑定输入。
调度器检查文件、摘要、FD 和生命周期，不解析输入文件里的业务协议。
项目 adapter 运行在受调度的子进程中；其进程身份和业务内部角色由项目合同定义。

通用接口见 [execution-api.md](execution-api.md)。输入字节不能成为绕过调度策略、
主机守卫、资源授予或启动一次性约束的授权来源。

## 旧实验代码的迁移

历史版本将 MPC_OTSF 的 M2B/Step5 协议放入 `gsched/native_step5*`，
并要求研究仓库提供 `gsched._m2b_scheduler_native`。这些协议、部署绑定、
冻结向量和专用验收归研究仓库维护，新的公共执行层不承诺旧 frozen wire 自动兼容。
项目迁移到外部 adapter 后，父子进程、peer identity 和 wait 权威会变化；
研究合同必须按新边界重新审查，普通 wrapper 的退出码不等于科学链完整证明。

`sched` 只保留读取旧持久态所需的兼容守卫。旧 `strict`、native metadata 和
session 记录不能作为新任务执行授权；已消费或结果不明的旧尝试不能重放。
旧数据库中的 session 表和字段也不因移出研究代码而直接删除。
兼容限制见 [native-integration.md](native-integration.md)。

## 生命周期的共同约束

启动前先持久化唯一执行意图，随后消耗一次启动机会。启动返回中断或结果不明时，
保留 execution id、绑定和资源，查询原 owner 的事实；不能换 id 重试。
取消、timeout 和 shutdown 先形成调度意图，再由执行 owner 消费。
接受取消请求不等于子进程退出，日志、业务 receipt 和 PID 消失也不等于真实 wait。

只有原始 owner 的真实 wait 与完整进程组清理，才能按退出结果发布成功或失败。
daemon 重启不能从磁盘 PID 恢复已失去的 wait 权威；进程组尚存或无法确认时，
保留未解决尝试与资源。确认原进程组消失后，仅按 `interrupted` 结算资源，
保留未知退出码和 rusage，不重放已消费的尝试，也不将 PID 消失解释为成功。
显式 `linux_fd_owner` 可凭不可变绑定重连仍存活的原 owner；它不重建 wait 权威，
也不重发 start。身份封装、认证与终局确认合同见
[persistent-execution-owner.md](persistent-execution-owner.md)。

## 发布与工作目标

本仓库 roadmap 只记录调度器及通用执行接口的工作。研究项目的 G1–G6、
科学加载、正式实验及研究部署包属于该项目自己的目标和记录。
跨仓验收可以证明某个客户兼容，但不能替代 sched 的公共验收，也不能把未完成的
客户研究目标写成 sched 的发布缺陷。开发边界由
`python scripts/check_execution_boundary.py` 和公共 CI 检查。
