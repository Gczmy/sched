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
当前 prepared 默认保留 30 秒，清理完成的终局默认保留 3600 秒等待确认；
运行中的 child 不因这些保留期停止，未声明 duration 的任务继续运行。

连接暂时不可用时保持 unknown，保留 running 和资源，并在后续 tick 重查。
owner 确实消失时进入既有 authority_lost 守卫；只有进程组确已消失才释放资源，
退出码和 rusage 保持未知。已经持久化的真实终局不被改写。owner 不自动重建，
应用结果、PID、日志或侧文件都不能替代原始 wait。

终局观察和任务状态提交后，daemon 才确认并关闭 owner。终局等待确认有有限
保留期；超过保留期而没有持久终局的记录仍属于未知结果，不补造成功。
数据库写 schema 为 6，新增不可变的 owner binding；只读查询兼容 schema 1–5
且不升级，旧尝试没有 owner binding 时仍按原守卫处理。

## 验收

本地独立 Linux 夹具覆盖真实 child wait、daemon 重启重连、取消与超时独立升级、
重复 start、提交后丢失响应、连接中断、错误认证、owner 丢失、尚未启动的恢复、
终局提交前后崩溃、资源释放及独立 wheel 安装。status/task/history 契约保持 schema 1；
execution 仅追加公开 owner 元数据和连接不确定原因。配套客户端只核对公开契约。
生产部署仍按 [execution-rollout.md](execution-rollout.md) 的维护与回退要求执行。
