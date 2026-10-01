# 持久 execution owner

本设计只属于 sched。`linux_fd_owner` 是显式选择的 Linux backend；现有
`linux_fd`、普通 subprocess 和历史记录不自动迁移。运行时仍无第三方依赖。

## 身份与启动

每个尝试由独立 owner 进程持有 retained executable/cwd/input FD 和原始 child。
owner 不随 daemon 退出，只有原始 owner 调用 native wait 和发送进程组信号。
daemon 先持久化 attempt，再准备 owner，随后在同一事务内持久化 owner binding
和一次性 launch intent，提交后才能发 start。owner 的 start 只能消费一次；
重复 start 返回原尝试，不产生第二个 child。恢复只查询或废弃尚未启动的准备，
不重新发送 start。

绑定包含 attempt、随机 owner ID、Linux abstract Unix endpoint、PID/start ticks、
boot ID 和私有随机密钥。连接同时验证 SO_PEERCRED 的 UID/PID、进程 start ticks
及 boot ID；请求与响应用密钥和独立 nonce 认证。密钥不出现在 argv、日志或公开 JSON。
owner 不写 scheduler DB，不加载客户模块，不从 payload 提取身份或授权。

新 backend 的 FD4 使用 `sched_execution_owner_identity/v1`：包含原始不可变
`sched_execution_identity/v1` attempt 和实际 owner PID/start ticks/boot ID/owner ID。
child 的直属父是 owner。旧 backend 的 FD4 v1 字节和含义保持不变；客户程序需
明确支持新封装后才选择新 backend。

## 恢复与终局

daemon 重启后，凭持久绑定重连仍存活的原 owner，继续取得它实际产生的
wait/rusage 和进程组清理事实。已提交意图但 owner 仍处于 prepared 时，将其
消费为 not_started；不能借恢复启动。prepared 有限期自动废弃，已运行 child
不因连接断开而停止。声明的 duration 和已接受的取消升级由 owner 自己驱动。
prepared 默认保留 30 秒，清理完成的终局默认保留 3600 秒等待确认；
运行中的 child 不因这些保留期停止，未声明 duration 的任务继续运行。

连接暂时不可用时保持 unknown，保留 running 和资源，并在后续 tick 重查。
owner 确实消失时进入既有 authority_lost 守卫；只有进程组确已消失才释放资源，
退出码和 rusage 保持未知。已经持久化的真实终局不被改写。owner 不自动重建，
应用结果、PID、日志或侧文件都不能替代原始 wait。

终局观察和任务状态提交后，daemon 才确认并关闭 owner。终局等待确认有有限
保留期；超过保留期而没有持久终局的记录仍属于未知结果，不补造成功。
0.2.0 使用写 schema 6，0.2.1使用 schema 7；只读查询兼容 schema 1–7
且不升级，旧尝试没有 owner binding 时仍按原守卫处理。

## 保留配置与运维观察（0.2.1）

`execution_backends[ID].owner` 仅适用于 `linux_fd_owner`，属于冷配置：

```json
{
  "owner": {
    "prepare_timeout_sec": 30,
    "terminal_retention_sec": 3600
  }
}
```

`prepare_timeout_sec` 允许 1–300 秒，`terminal_retention_sec` 允许 60–604800 秒；
均须为有限数字，布尔值不接受。字段省略时使用默认值，省略整个 `owner` 不改变
旧配置的绑定摘要。修改须排空 daemon 后重启；既有服务继续使用启动时的值。
保留期只约束未启动准备和已完成清理的终局，不终止 running child。

schema 7 增加独立的运维记录，保留原不可变 binding 和 wait。终局与任务状态提交后
进入确认队列；每轮从索引选择最多 8 条到期记录，2 秒后不再开始新的确认。
已开始的 RPC 与本地服务 wait 使用各自有限超时，因此 2 秒不是整轮硬上限。
失败记录持久退避 10 秒，重启后继续确认；成功记录不再进入扫描或内存历史集合。
迁移只回填一次旧 binding 的确认状态，不删除历史，不补发 start。

`sched execution ... --json` 的持久尝试增加 `owner_health`，所有字段来自已提交记录，
`source:"recorded"` 明确表示查询不连接 owner，也不能据此判断当前服务是否存活。
连接值为 `unknown/responsive/unreachable/lost`，同时返回 `last_observed_at`。
确认状态 `active/pending/acknowledged` 描述关闭 owner 的工作，不是 child 进程组清理状态；
`active` 也不证明服务存活。旧 schema 缺少记录时为 `unknown`，其他字段为 null。
确认成功分别记录 `closed`（取得关闭响应）或 `owner_lost`（原服务已消失）；后者不提供
新的退出事实。`cleanup_attempts`、`retry_after`、`last_cleanup_at`、`cleanup_error`、
`acknowledged_at` 和 `acknowledgement` 用于跟踪确认与重试，不能替代原始 observation。

## 验收

本地独立 Linux 夹具覆盖真实 child wait、daemon 重启重连、取消与超时独立升级、
重复 start、提交后丢失响应、连接中断、错误认证、owner 丢失、尚未启动的恢复、
终局提交前后崩溃、资源释放及独立 wheel 安装。运维验收覆盖确认后元数据提交失败、
保留期到期、冷配置边界、schema 6 只读与迁移，以及一万条已确认历史记录下的有界队列。
status/task/history 契约保持 schema 1；execution 仅追加公开 owner 元数据和已记录健康。
配套客户端只核对公开契约。
生产部署仍按 [execution-rollout.md](execution-rollout.md) 的维护与回退要求执行。
