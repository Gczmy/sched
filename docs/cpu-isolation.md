# per-job CPU 亲和

此功能在 schema 18 引入，已随 [v0.5.0](releases/0.5.0.md) 发布；writer 为 schema 25、完整只读 1–25。目标部署状态以 CLI 和私有交付记录为准。实际 CLI 需报告命名合同
`sched-cpu-isolation-v1`；包版本相同不表示旧安装已支持。只通过 sched CLI 操作 state。

## 显式启用与边界

```json
{"cpu_isolation": {"mode": "affinity"}}
```

省略为 `mode: "off"`，保留默认执行行为。候选还接受显式 cgroup + delegated_root，
其严格委派/原 lease 与验收边界见 [CPU scope](cpu-scopes.md)，不是任务字段。
它是冷配置：配置改变后现有 daemon 暂停新派发，必须在指定
有效租约内排空并显式重启；运行任务正常结束，不因配置切换迁移 mask。

启用时每个任务按原 allocation 的 CPU 预留取得互不重叠的逻辑 CPU 编号，普通
CPU/GPU 任务、linux_fd 和 linux_fd_owner 均在用户 exec 前应用同一约束。子进程
继承 mask；设置/读回不一致、缺少 memfd/sealing 或 native 约束接口则失败，不退回
无约束启动。`daemon check --json` 的 cpu_affinity_primitives 只做前置检查，
不证明原租约有效，也不代替最终启动验证。

亲和可被应用主动放宽，不是不可越界的 cpuset、cgroup 或设备限制；所有报告保留
`hard_isolation:false`。它只保证本 sched 实例的原始 claim 不重叠，不隔离外部进程、
其他实例或恶意同 UID 程序。不会创建 cgroup、修改 Slurm 父级、申请新租约或限制 GPU
设备。通用底层接口和未完成工作见 [启动约束](execution-constraints.md)。

## CPU 池与预留

CPU 池来自原 daemon 的精确 affinity，容量按可验证原 Slurm 声明/affinity 的保守
最小值解析；不会使用查询网关的 CPU 编号或后来创建的租约。kernel owner/cgroup/
affinity 漂移、未知原容量或已确认失效时暂停新派发。没有 job cgroup 归属证明时，
显式启动祖先兼容模式可验证冻结的原租约来源，未知仍暂停；旧 observe/unknown allow
则是有告警的降级策略。两者均不证明 cgroup 硬隔离，见 [租约策略](daemon-lease.md)。

0.6.0 候选修复了启动祖先模式下 claim 选择漏读原 anchor 的阻塞；每次选择 CPU
前对冻结的原 anchor 做新鲜观察，不能只依赖监测缓存中的 valid。
v0.5.0 原始来源未完成此组合的联合验收；已通过范围见[兼容路线](student-compatibility.md)。

`cpus_total=0` 仍是不启用总声明预算，不静默变成 auto。启用 affinity 后，独立的
CPU-ID 池仍是有限的；固定/auto 预算、CPU-only 并发和 GPU 等其他 gate 同时生效。
每任务最多 8192 个 CPU，池最多 65536 个编号，编号范围 0..1048575；未知或超界拒绝。
大任务等候不阻止可放入空位的小任务，不抢占。已有 running 缺少完整 CPU 绑定时
暂停新亲和派发，不能为旧 worker 补造或重新设置 mask。

最终 writer 事务再次核对 claim，在启动前原子持久化 allocation 的 cpu_binding、
CPU 唯一 claim 和资源事件。唯一索引拒绝同一 CPU 重复占用，失败回滚；allocation
是意图，不是已执行证明。CPU claim 绑定原 allocation/job/version，重试、取消或
新版本不会暗中搬移旧 claim。普通原 wait/group 清理或 configured 原 owner 的已确认
无 child/空 group 才释放；仍有进程、启动未决、连接或清理未知时保留，不补造 wait。

持久 owner 重连使用原 attempt/child/mask，不再次 start；后继 daemon 不重分配旧
CPU。直接 owner 丢失时仍沿用“不重建 wait，确认原 group 消失后才释放”的执行契约。
释放删除活动 claim，但 allocation 和 cpu_affinity_reserved/released 原始事件保留。

## 只读查询

```bash
sched cpu-isolation --json
sched cpu-isolation --limit 20 --cursor <allocation-id> --json
sched admission-explain <batch-id>:<task-id> --json
sched allocations <batch-id>:<task-id> --allocation-id <id> --json
```

cpu-isolation 返回已记录活动 claim，默认 50、最多 100 个 allocation；分页是实时
keyset，不是可合并的完整快照。输出包含 mode、allocation/job/version、CPU 集合与
原租约绑定，`runtime_probed:false`、`admission_granted:false`；不探测、连接 owner、
迁移、释放资源或更改 revision。schema 17 及更旧查询报告 migration_required。

启用 affinity 时 admission-explain 增加独立 cpu_isolation，复用真实池解析及 claim
选择函数。使用当前精确 owner 的已记录出生/检查，最多有效 45 秒；过期、缺失、
旧 owner、已退出或配置观察滞后明确 unknown，resource_fit 为 null。CPU 池已满时
即使 cpus_total=0 也报告 CPU 拒绝；候选 CPU 集合不授予启动权。默认严格
status/task/history JSON、wait_reason 的 cpu 和 FD4 身份格式保持不变。

## 迁移与验证

schema 18 原子新增空 cpu_assignments 表/索引/更新禁止触发器，不补造旧任务的 CPU
事实，不改 instance、原 wait、请求或历史 allocation。后续 schema 19 新增
[scope 生命周期记录](cpu-scopes.md)，默认 affinity 不创建 scope；有 scope 记录时
原 CPU claim 要等 recorded removed/未创建 abandoned，未知不能借终态释放，也不能
仅 affinity 启动降级。schema 20 接通独立委派 cpuset 与 cleanup_ready，不改变
默认 affinity。schema 21 引入 [设备记录与资源守卫](device-scopes.md)，该阶段仅记录；
后续 schema 23 接入 [显式设备 controller](device-controller.md)，24/25 增加 MIG 能力和
原委派根健康记录。当前完整 schema 1–25 只读查询不迁移；默认 off/affinity 不启用
cgroup 或设备隔离。旧 writer 不得回接更高写库，回退只能使用已验证的升级前恢复点，不能删
claim、降低 user_version 或手改 state。发布与生产切换仍按
[独立 rollout](execution-rollout.md) 另行授权。

[纯决策/事务回归](../tests/test_cpu_isolation.py) 覆盖重复 claim、回滚、未知保留、
原绑定释放、缺失 claim 不降级、网关被动解释、旧库查询和迁移。
[计算节点 CPU/CLI 验收](../tests/run_cpu_isolation_accept.py) 使用独立临时 state、
fake GPU 与 state 外的控制器回答：实际 mask/后代、池耗尽/补位、取消/释放、native
原 wait、持久 owner 故障重连和冷配置；原租约只读观察另行记录，不取消真实 Slurm job。
没有用户 cgroup 委派的环境不能用于证明 cgroup join/cpuset 或设备硬隔离。
