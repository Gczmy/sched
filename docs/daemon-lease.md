# 候选 daemon 租约来源与持续校验

源码候选写 schema 17、完整只读 schema 1–17；包版本不代表部署能力，使用
`identity --json` 协商 `sched-daemon-lease-v1`。未部署到生产，不能把 CPU/fake-GPU
验收当作真实 Slurm 失效或 CUDA 验收。后续候选的 [CPU 自动容量](cpu-capacity.md)
复用此来源和校验；per-job 硬隔离仍另行开发。

## 配置与派发边界

```json
{"lease_validation":{"mode":"auto","unknown_policy":"pause","interval_sec":30}}
```

这是冷配置：`mode` 为 `auto|enforce|observe`，`unknown_policy` 为 `pause|allow`，
`interval_sec` 为 1..30 整数；未知字段拒绝。默认 auto 在启动环境出现
`SLURM_JOB_ID` 时执行校验，即使该值非法；普通无 Slurm 环境保留 standalone
派发兼容。enforce 总是执行；observe 仅记录，不限制派发，是显式放弃保护。
unknown 默认暂停；明确配置 allow 可以容忍未知，但不把 unknown 变为 valid，
也不能恢复已经确认 invalid 的执行型租约。observe 则仍只是观察，无阻断保证。
热更不能修改这些策略；须在目标租约内明确重启。

daemon 取得精确 PID/start-token/physical-host lease 后，在首次就绪 heartbeat
之前，以同一 SQLite 事务保存出生记录及首次校验。文件 owner 可在该事务之前
短暂出现，不表示已就绪。白名单只包含 job/step、CPU 声明、任务数和 cluster name；
不保存完整环境、启动命令、密码、令牌或客户数据。另保存 UID、启动时间、物理
节点、affinity、`Cpus_allowed_list` 与 cgroup；不能补造旧 daemon 的来源。

计算节点 helper 只调用 `scontrol --json show job <original-id>` 与必要的
`scontrol show hostnames`。原 ID、UID、start time、restart count、nodes 与 CPU
声明组成不可变 allocation 绑定；首次不可读时后续首次已知观察另行记录绑定，
不覆盖出生记录。helper 总截止 5 秒、单次命令 2 秒，超时/不支持/警告/错误为
unknown；挂起 helper 未结束时不再创建同类 helper。不用错误展示文本推断终态。

只有原 job RUNNING、UID/节点/不可变绑定一致、内核上下文未变，并且 job/step
cgroup 可核对时才报告 valid。通用 system cgroup、只有 Slurm 环境变量、hostname
别名不可核对、控制器不可读或采样过期，都不冒充 valid。这些记录仍不是独占
设备、执行 owner、真实 worker wait 或 per-job 硬隔离证明。

原 job 明确不再 RUNNING、成功的精确查询明确不存在、被复用/重排/resize、到达
租约结束时间、UID 或自身 kernel 身份/affinity/cgroup 改变时，当前 daemon
永久锁存 invalid。后来的 RUNNING 或同节点新租约不能重新绑定、自动恢复。
每 tick、派发入口及启动 CAS 前复核；Slurm 查询按 interval 缓存，内核上下文每次
复核，观测超过 45 秒不可用于有效性。存在采样与启动间的时间窗口，不能描述成
Slurm 原子授权或硬隔离。

暂停只影响新派发：running 保留真实 owner/wait、取消、超时、资源释放与终态
结算路径。pending 不改成失败，不发信号、不删除产物、不迁移租约。恢复需要
在新目标租约内显式重启；有运行任务时使用 drain，不能用 stop 模拟无损排空。

## 已记录事实查询

```sh
sched daemon-lease --json
sched daemon-lease --lease-id <32-hex-id> --limit 20 --after-seq 0 --json
sched daemon status --json --include-lease
```

默认 `daemon status --json` 与 `status.daemon_health` 原有字段不变、不打开 DB。
只有显式 include-lease 才通过私有 DB/WAL 快照增加 `lease` 对象：选择当前 owner，
无 owner 时展示最后记录的出生；旧 owner 无记录明确 origin_not_recorded。
配套插件的严格 scalar 健康合同不默认接收这项嵌套扩展。

daemon-lease 返回 `schema_version:1`、query、contract、instance_id、effect=none、
admission_granted=false、available、leases、truncated、next_cursor。旧完整库返回
available=false/migration_required，不迁移。summary 的 limit 为 1..100、默认 20，
lease ID/cursor 均为 32 hex，按 ID 实时 keyset 续页，不是跨页完整当前态；显式
lease-id 与 cursor 互斥。after-seq 必须绑定 exact lease-id，取 0..2^63-1；exact
事件按 seq 分页，events_truncated/next_seq 与 summary 分页独立。

每条包含不可变 origin、recorded_check、recorded_exit、current_owner_binding、
observation_age_s、allocation_state 与 admission_granted=false。recorded_check 是
当时观察，可保留原 valid/invalid；当前展示只有精确 owner 匹配、未记录退出、
最近检查 ≤45 秒才保留该值，否则 unknown。同物理主机沿既有只读健康协议核对
PID/start token，PID 复用/不可确认不显示 valid；网关不探测自己的同号 PID。
不调用 Slurm、不采样网关 affinity/cgroup、不连 execution owner、不授予派发权。
health 的 healthy 表示 tick 正常，并不等价于租约有效；查看租约必须用本合同。

出生/事件 append-only 并有 hash/身份/事件链校验。单记录最多 256 KiB；查询先
检查 evidence 总量 ≤3 MiB，再物化 payload；输出最多 4 MiB，超限报错，降低 limit。
精确任务 allocation 只链接 lease_id/instance_id/当时 allocation_state；不是 worker
身份。正常释放保留退出 context；可写的异常退出也记录语义。SIGKILL/断电/DB 不可写
可能没有 exit，只保留最后观察；绝不能由消失 PID 或日志补造 worker wait。

## 诊断与通知

每个 daemon 第一次确认 invalid 时记录一条 lease_invalid incident。append-only
校验事件永久保留；通用 incident 展示可能受既有保留策略影响。通知需显式加入
`notify.on:["lease_invalid"]`，沿现有 file/email/command 渠道最佳努力发送一次；
默认 batch done/blocked 不变。事件 ID 固定绑定 lease ID，file 已存在或已 ack 不
重复投递；用 notify-inbox/notify-ack 查询确认。通知结果仅保存渠道数量/成功数量，
不保存可能含 endpoint/凭据的错误文本；通知失败不放行、不推断执行退出。进程
被硬杀或通知线程失败不保证送达，审计以已记录校验为准。

## 验收与迁移

[纯回归](../tests/test_cluster_lease.py) 覆盖未知/失效/锁存、PID 复用、白名单、
严格策略、正常退出、不可变历史、事务回滚、事件分页/容量与旧库只读迁移边界。
[Linux CPU/CLI 验收](../tests/run_cluster_lease_accept.py) 先只读观察真实上下文，
真实来源 unknown 时验证默认 pause 不创建 allocation/worker，再使用 state 外的
scontrol 测试夹具：显式 allow 未知后启动有界 CPU child，模拟
CANCELLED 时保留 running、暂停 pending、绑定 allocation、确认通知；后来 RUNNING
不能恢复，真实子进程自然完成；只有显式私有重启后另一任务运行一次。该测试
不取消真实 allocation、不新建租约、不占真实 GPU、不改生产 daemon/config。
其他 CPU fixture 显式 observe，不宣称验证外层真实租约。

迁移仅新增空 daemon_leases/daemon_lease_events、索引与不可变/保留触发器，不
回填历史、不改 job 状态/wait、原执行身份、allocation、请求与 instance。
旧 0.4.0/schema 10–16 writer 不能回接 schema 17；回退只能用已核验升级前恢复点，
禁止删除审计或降低 user_version，生产切换需要单独授权。

Slurm JSON/hostnames 语义见 [scontrol 官方文档](https://slurm.schedmd.com/scontrol.html)，
cgroup 所有权不能由环境或 screen 推断，见
[Slurm cgroup v2 文档](https://slurm.schedmd.com/cgroup_v2.html)。
